import io
import os
import json
import types
from copy import deepcopy
#from nibabel import nifti1
#from pydicom import dcmread
#from scipy.io import loadmat
import boto3
import botocore
import requests
from requests import Session
from requests.exceptions import HTTPError, ConnectionError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from joblib import Parallel, delayed
if 'BLACKFYNN_LOG_LEVEL' not in os.environ:
    # silence agent import warning
    os.environ['BLACKFYNN_LOG_LEVEL'] = 'CRITICAL'

from blackfynn import log as _bflog
# blackfynn.log sets logging.basicConfig which pollutes logs from
# other programs that are sane and do not use the root logger
# so we have to undo the damage done by basic config here
# we add the sparcur local handlers back in later
from sparcur.utils import log, silence_loggers
__bflog = _bflog.get_logger()
silence_loggers(__bflog)
__bflog.addHandler(log.handlers[0])

from blackfynn import Blackfynn, Collection, DataPackage, Organization, File
from blackfynn import Dataset, BaseNode
from blackfynn import base as bfb
from blackfynn.api import transfers
from blackfynn.api.data import PackagesAPI
from pyontutils.utils import Async, deferred
from pyontutils.iterio import IterIO
from sparcur import exceptions as exc
from sparcur.core import lj
from .config import auth


def upload_fileobj(
        file,  # aka Path
        s3_host,
        s3_port,
        s3_bucket,
        s3_keybase,
        region,
        access_key_id,
        secret_access_key,
        session_token,
        encryption_key_id,
        upload_session_id=None,
        ):
    """ streaming upload
        the object passed in as 'file'
        doesn't have to be Path at all
        it just needs to implement the following methods
        `name`, `size`, and `data`
    """
    local_path = file

    try:
        # account for dev connections
        resource_args = {}
        config_args = dict(signature_version='s3v4')
        if 'amazon' not in s3_host.lower() and len(s3_host)!=0:
            resource_args = dict(endpoint_url="http://{}:{}".format(s3_host, s3_port))
            config_args = dict(s3=dict(addressing_style='path'))

        # connect to s3
        session = boto3.session.Session()
        s3 = session.client('s3',
            region_name = region,
            aws_access_key_id = access_key_id,
            aws_secret_access_key = secret_access_key,
            aws_session_token = session_token,
            config = botocore.client.Config(**config_args),
            **resource_args
        )

        # s3 key
        s3_key = '{}/{}'.format(s3_keybase, local_path.name)

        # override seek to raise an IOError so
        # we don't get a TypeError
        # FIXME IterIO stores a buffer of the whole generator >_<
        f = IterIO(local_path.data, sentinel=b'')
        def _seek(self, *args):
            raise IOError('nope')
        f.seek = _seek

        # upload file to s3
        s3.upload_fileobj(
            Fileobj=f,  # FIXME checksumming wrapper probably ...
            Bucket=s3_bucket,
            Key=s3_key,
            #Callback=progress,
            ExtraArgs=dict(
                ServerSideEncryption="aws:kms",
                SSEKMSKeyId=encryption_key_id,
                #Metadata=checksums,  # hca does it this way
                # annoyingly this means that you have to read the file twice :/
            ))

        return s3_key

    except Exception as e:
        log.error(e)
        raise e


transfers.upload_file = upload_fileobj


def check_files(files):
    """ of course they don't exist """


transfers.check_files = check_files


#_orgid = auth.get('blackfynn-organization')  # HACK
# this works, but then we have to do the multipart upload dance
# which the bf client does not implement at the moment
def get_preview(self, files, append):
    paths = files

    params = dict(
        append = append,
        # dataset_id=?,
    )

    payload = { "files": [
        {
            "fileName": p.name,
            "size": p.size,
            "uploadId": i,
            "processing": True,
        } for i, p in enumerate(paths)
    ]}

    # current web dance
    # /upload/preview/organizations/{_orgid}?append= append ?dataset_id= integer???
    # /upload/fineuploaderchunk/organizations/{_orgid}/id/{importId}?multipartId= from-prev-resp
    # /upload/complete/organizations/{_orgid}/id/{importId}?datasetId=
    response = self._post(
        endpoint=self._uri('/files/upload/preview'),
        #endpoint=self._uri(f'/upload/preview/organizations/{_orgid}'),
        params=params,
        json=payload,
        )

    import_id_map = dict()
    for p in response.get("packages", list()):
        import_id = p.get("importId")
        warnings = p.get("warnings", list())
        for warning in warnings:
            logger.warn("API warning: {}".format(warning))
        for f in p.get("files", list()):
            index = f.get("uploadId")
            import_id_map[paths[index]] = import_id
    return import_id_map


transfers.IOAPI.get_preview = get_preview


def init_file(self, filename):
    """ not using any of this """


transfers.UploadManager.init_file = init_file


# monkey patches


@property
def patch_session(self):
    """
    Make requests-futures work within threaded/distributed environment.
    """
    if self._session is None:
        self._session = Session()
        self._set_auth(self._token)
        try:
            backoff_factor = auth.get('blackfynn-backoff-factor')
        except Exception as e:
            log.exception(e)
            backoff_factor = 1

        # Enable retries via urllib
        adapter = HTTPAdapter(
            pool_connections=1000,  # wheeee
            pool_maxsize=1000,  # wheeee
            max_retries=Retry(
                total=self.settings.max_request_timeout_retries,
                backoff_factor=backoff_factor,
                status_forcelist=[502, 503, 504] # Retriable errors (but not POSTs)
            )
        )
        self._session.mount('http://', adapter)
        self._session.mount('https://', adapter)

    return self._session

# monkey patch to avoid session overflow during async
bfb.ClientSession.session = patch_session


def get(self, id, update=True):
    return self._api.core.get(id, update=update)


# monkey patch Blackfynn so that it doesn't eat errors and hide their type
Blackfynn.get = get


def download(self, destination):
    """ remove prefix functionality since there are filenames without extensions ... """
    if self.type=="DirectoryViewerData":
        raise NotImplementedError("Downloading S3 directories is currently not supported")

    if os.path.isdir(destination):
        # destination dir
        f_local = os.path.join(destination, os.path.basename(self.s3_key))
    else:
        # exact location
        f_local = destination

    r = requests.get(self.url, stream=True)
    with io.open(f_local, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk: f.write(chunk)

    # set local path
    self.local_path = f_local

    return f_local

# monkey patch File.download to
File.download = download


# package meta
def id_to_type(id):
    if id.startswith('N:package:'):
        return DataPackage
    elif id.startswith('N:collection:'):
        return Collection
    elif id.startswith('N:dataset:'):
        return Dataset
    elif id.startswith('N:organization:'):
        return Organization


class FakeBFile(File):
    """ Fake file to simplify working with package metadata """

    id = None  # unforunately these don't seem to have made it through

    def __init__(self, package, **kwargs):
        self.package = package
        self._json = kwargs
        def move(*tuples):
            for f, t in tuples:
                kwargs[t] = kwargs.pop(f)

        move(('createdAt', 'created_at'),
             ('updatedAt', 'updated_at'),
             ('fileType', 'type'),
             ('packageId', 'pkg_id'),
        )

        if 'size' not in kwargs:
            kwargs['size'] = None  # if we have None on a package we know it is not zero

        if 'checksum' in kwargs:
            cs = kwargs['checksum']
            kwargs['chunksize'] = cs['chunkSize']
            kwargs['checksum'] = cs['checksum']  # overwrites but ok
        else:
            kwargs['checksum'] = None

        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def state(self):
        return self.package.state

    @property
    def owner_id(self):
        return self.package.owner_id

    @property
    def dataset(self):
        return self.package.dataset

    @property
    def parent(self):
        # cut out the middle man (hopefully)
        return self.package.parent

    def __repr__(self):
        return ('<' +
                self.__class__.__name__ +
                ' '.join(f'{k}={v}' for k, v in self._json.items()) +
                '>')


    
# monkey patch for PackagesAPI.get
from future.utils import string_types
def get(self, pkg, include='files,source'):
    """
    Get package object

    pkg:     can be DataPackage ID or DataPackage object.
    include: list of fields to force-include in response (if available)
    """
    pkg_id = self._get_id(pkg)

    params = {'includeAncestors': 'true'}
    if include is not None:
        if isinstance(include, string_types):
            params.update({'include': include})
        elif hasattr(include, '__iter__'):
            params.update({'include': ','.join(include)})

    resp = self._get(self._uri('/{id}', id=pkg_id), params=params)

    # TODO: cast to specific DataPackages based on `type`
    pkg = self._get_package_from_data(resp)
    pkg._resp = resp
    #log.debug(lj(resp.json()))
    return pkg

PackagesAPI.get = get


@property
def packages(self, pageSize=1000, includeSourceFiles=True):
    yield from self._packages(pageSize=pageSize, includeSourceFiles=includeSourceFiles)


def packagesByName(self, pageSize=1000, includeSourceFiles=True, filenames=tuple()):
    if filenames:
        for filename in filenames:
            yield from self._packages(pageSize=pageSize, includeSourceFiles=includeSourceFiles, filename=filename)
    else:
        yield from self._packages(pageSize=pageSize, includeSourceFiles=includeSourceFiles)


def _packages(self, pageSize=1000, includeSourceFiles=True, raw=False, latest_only=False, filename=None):
    """ python implementation to make use of /dataset/{id}/packages """
    remapids = {}
    def restructure(j):
        """ restructure package json to match what api needs? """
        # FIXME something's still wonky here
        c = j['content']
        c['int_id'] = c['id']
        c['id'] = c['nodeId']  # FIXME indeed packages do seem to be missing ids!?
        remapids[c['int_id']] = c['id']
        c['int_datasetId'] = c['datasetId']
        c['datasetId'] = c['datasetNodeId']
        if 'parentId' in c:
            pid = c['parentId']
            c['parent'] = remapids[pid]  # key error to signal out of order
            #if pid in remapids:
            #else:
                #c['parent'] = f'WTF ERROR: {pid}'
                #print('wtf', pid, c['id'], c['datasetId'])
            #else:
                #c['parent'] = remapids['latest']
        return j

    index = {self.id:self}  # make sure that dataset is in the index
    session = self._api.session
    #cursor
    #pageSize
    #includeSourceFiles
    #types
    #filename
    filename_args = f'&filename={filename}' if filename is not None else ''
    cursor_args = ''
    out_of_order = []
    while True:
        try:
            resp = session.get(f'https://api.blackfynn.io/datasets/{self.id}/packages?'
                            f'pageSize={pageSize}&'
                            f'includeSourceFiles={str(includeSourceFiles).lower()}'
                            f'{filename_args}'
                            f'{cursor_args}')
        except requests.exceptions.RetryError as e:
            log.exception(e)
            # sporadic 504 errors that we probably need to sleep on
            breakpoint()
            raise e

        #print(resp.url)
        if resp.ok:
            j = resp.json()
            packages = j['packages']
            if raw:
                yield from packages
                if latest_only:
                    break
                else:
                    continue

            if out_of_order:
                packages += out_of_order
                # if a parent is on the other side of a
                # pagination boundary put the children
                # at the end and move on
            out_of_order = [None]
            while out_of_order:
                #log.debug(f'{out_of_order}')
                if out_of_order[0] is None:
                    out_of_order.remove(None)
                elif packages == out_of_order:
                    if filename is not None:
                        out_of_order = None
                    elif 'cursor' not in j:
                        raise RuntimeError('We are going nowhere!')
                    else:
                        # the missing parent is in another castle!
                        break
                else:
                    packages = out_of_order
                    out_of_order = []
                for count, package in enumerate(packages):
                    if isinstance(package, dict):
                        id = package['content']['nodeId']
                        name = package['content']['name']
                        bftype = id_to_type(id)
                        dcp = deepcopy(package)
                        try:
                            #if id.startswith('N:package:'):
                                #log.debug(lj(package))
                            rdp = restructure(dcp)
                        except KeyError as e:
                            if out_of_order is None:  # filename case
                                # parents will simply not be listed
                                # if you are using filename then beware
                                if 'parentId' in dcp['content']:
                                    dcp['content'].pop('parentId')
                                rdp = restructure(dcp)
                            else:
                                out_of_order.append(package)
                                continue

                        bfobject = bftype.from_dict(rdp, api=self._api)
                        if name != bfobject.name:
                            log.critical(f'{name} != {bfobject.name}')
                        bfobject._json = package
                        bfobject.dataset = index[bfobject.dataset]
                    else:
                        bfobject = package

                    if isinstance(bfobject.parent, str) and bfobject.parent in index:
                        parent = index[bfobject.parent]
                        if parent._items is None:
                            parent._items = []
                        parent.items.append(bfobject)
                        bfobject.parent = parent
                        # only put objects in the index when they have a parent
                        # that is a bfobject, this ensures that you can always
                        # recurse to base once you get an object from this function
                        index[bfobject.id] = bfobject
                        if parent.state == 'DELETING':
                            if not bfobject.state == 'DELETING':
                                bfobject.state = 'PARENT-DELETING'
                        elif parent.state == 'PARENT-DELETING':
                            if not bfobject.state == 'DELETING':
                                bfobject.state = 'PARENT-DELETING'

                        yield bfobject  # only yield if we can get a parent
                    elif out_of_order is None:  # filename case
                        yield bfobject
                    elif bfobject.parent is None:
                        # both collections and packages can be at the top level
                        # dataset was set to its bfobject repr above so safe to yield
                        if bfobject.dataset is None:
                            log.debug('No parent no dataset\n'
                                      + json.dumps(bfobject._json, indent=2))
                        index[bfobject.id] = bfobject
                        yield bfobject
                    else:
                        out_of_order.append(bfobject)
                        continue

                    if isinstance(bfobject, DataPackage):
                        bfobject.fake_files = []
                        if 'objects' not in bfobject._json:
                            log.error(f'{bfobject} has no files!??!')
                        else:
                            for i, source in enumerate(bfobject._json['objects']['source']):
                                # TODO package id?
                                if len(source) > 1:
                                    log.info(f'more than one key in source {sorted(source)}')

                                ff = FakeBFile(bfobject, **source['content'])
                                bfobject.fake_files.append(ff)
                                yield ff

                                if i == 1:  # only log once
                                    log.critical(f'MORE THAN ONE FILE IN PACKAGE {bfobject.id}')

            if 'cursor' in j:
                cursor = j['cursor']
                cursor_args = f'&cursor={cursor}'
            else:
                break

        else:
            break


@property
def packageTypeCounts(self):
    session = self._api.session
    resp = session.get(f'https://api.blackfynn.io/datasets/{self.id}/packageTypeCounts')
    if resp.ok:
        j = resp.json()
        return j
    else:
        resp.raise_for_status()


# monkey patch Dataset to implement packages endpoint
Dataset._packages = _packages
Dataset.packages = packages
Dataset.packagesByName = packagesByName
Dataset.packageTypeCounts = packageTypeCounts


@property
def users(self):
    session = self._api.session
    resp = session.get(f'https://api.blackfynn.io/datasets/{self.id}/collaborators/users')
    return resp.json()


# monkey patch Dataset to implement teams endpoint
Dataset.users = users


@property
def teams(self):
    session = self._api.session
    resp = session.get(f'https://api.blackfynn.io/datasets/{self.id}/collaborators/teams')
    return resp.json()


# monkey patch Dataset to implement teams endpoint
Dataset.teams = teams


@property
def doi(self):
    session = self._api.session
    resp = session.get(f'https://api.blackfynn.io/datasets/{self.id}/doi')
    if resp.ok:
        return resp.json()
    else:
        if resp.status_code != 404:
            log.warning(f'{self} doi {resp.status_code}')


# monkey patch Dataset to implement doi endpoint
Dataset.doi = doi


def delete(self):
    """ actually delete """
    session = self._api.session
    resp = session.delete(f'https://api.blackfynn.io/datasets/{self.id}')
    if resp.ok:
        return resp.json()
    else:
        if resp.status_code != 404:
            log.warning(f'{self} issue deleting {resp.status_code}')


# monkey patch Dataset to implement delete
Dataset.delete = delete


@property
def contributors(self):
    session = self._api.session
    resp = session.get(f'https://api.blackfynn.io/datasets/{self.id}/contributors')
    return resp.json()


# monkey patch Dataset to implement contributors endpoint
Dataset.contributors = contributors


@property
def banner(self):
    session = self._api.session
    resp = session.get(f'https://api.blackfynn.io/datasets/{self.id}/banner')
    return resp.json()


# monkey patch Dataset to implement banner endpoint
Dataset.banner = banner


@property
def readme(self):
    session = self._api.session
    resp = session.get(f'https://api.blackfynn.io/datasets/{self.id}/readme')
    return resp.json()


# monkey patch Dataset to implement readme endpoint
Dataset.readme = readme


@property
def status_log(self):
    session = self._api.session
    resp = session.get(f'https://api.blackfynn.io/datasets/{self.id}/status-log')
    return resp.json()


# monkey patch Dataset to implement status_log endpoint
Dataset.status_log = status_log


@property
def meta(self):
    session = self._api.session
    resp = session.get(f'https://api.blackfynn.io/datasets/{self.id}')
    return resp.json()


# monkey patch Dataset to implement just the dataset metadata endpoint
Dataset.meta = meta


@property
def teams(self):
    session = self._api.session
    resp = session.get(f'https://api.blackfynn.io/organizations/{self.id}/teams')
    return resp.json()


# monkey patch Organization to implement teams endpoint
Organization.teams = teams


class PackageMeta:

    class PackageMetaError(Exception):
        """ Ya done goofed part 3 """

    class NotDataPackage(Exception):
        """ only packages have """

    class NoPackageMetaError(Exception):
        """ no package meta """

    def __init__(self, hbfo):
        bfobject = hbfo.bfobject
        if not type(bfobject) == DataPackage:
            msg = f'{bfobject} is not a DataPackage'
            raise self.NotDataPackage(msg)

        elif not hasattr(bfobject, '_json'):
            msg = f'{bfobject} was not spawned from /datasets/{{id}}/packages'
            raise self.NoPackageMetaError(msg)

        self.hbfo = hbfo
        self.bfobject = bfobject
        self.json = hbfo.bfobject._json

    @property
    def size(self):
        self.json['objects']['source']


class BFLocal:

    class NoBfMeta(Exception):
        """ There is no bf id for this file. """

    def __init__(self, project_id, anchor=None):
        # no changing local storage prefix in the middle of things
        # if you want to do that create a new class

        self._project_id = project_id
        self.bf = self._get_connection(self._project_id)

        self.organization = self.bf.context
        self.project_name = self.bf.context.name

    def _get_connection(self, project_id):
        try:
            return Blackfynn(api_token=auth.user_config.secrets('blackfynn', self._project_id, 'key'),
                             api_secret=auth.user_config.secrets('blackfynn', self._project_id, 'secret'))
        except KeyError as e:
            msg = f'need record in secrets for blackfynn organization {self._project_id}'
            raise exc.MissingSecretError(msg) from e

    def __getstate__(self):
        state = self.__dict__
        if 'bf' in state:
            state.pop('bf')  # does not picle well due to connection

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.bf = self._get_connection(self._project_id)

    @property
    def root(self):
        return self.organization.id

    def create_package(self, local_path):
        pkg = DataPackage(local_path.name, package_type='Generic')  # TODO mimetype -> package_type ...
        pcache = local_path.parent.cache
        pkg.dataset = pcache.dataset.id
        if pcache.id != pkg.dataset:
            pkg.parent = pcache.id

        # FIXME this seems to create an empty package with no files?
        # have to do aws upload first or something?
        return self.bf._api.packages.create(pkg)

    def get(self, id, attempt=1, retry_limit=3):
        log.debug('We have gone to the network!')
        if id.startswith('N:dataset:'):
            try:
                thing = self.bf.get_dataset(id)  # heterogenity is fun!
            except Exception as e:
                if 'No dataset matching name or ID' in str(e):
                    # sigh no error types
                    raise exc.NoRemoteFileWithThatIdError(id) from e
                else:
                    raise e

        elif id.startswith('N:organization:'):
            if id == self.organization.id:
                return self.organization  # FIXME staleness?
            else:
                # if we start form local storage prefix for everything then
                # this would work
                raise BaseException('TODO org does not match need other api keys.')
        else:
            try:
                thing = self.bf.get(id)
            except requests.exceptions.HTTPError as e:
                resp = e.response
                if resp.status_code == 404:
                    msg = f'{resp.status_code} {resp.reason!r} when fetching {resp.url}'
                    raise exc.NoRemoteFileWithThatIdError(msg) from e

                log.exception(e)
                thing = None
            except bfb.UnauthorizedException as e:
                log.error(f'Unauthorized to access {id}')
                thing = None

        if thing is None:
            if attempt > retry_limit:
                raise exc.NoMetadataRetrievedError(f'No blackfynn object retrieved for {id}')
            else:
                thing = self.get(id, attempt + 1)

        return thing

    def get_file_url(self, id, file_id):
        resp = self.bf._api.session.get(f'https://api.blackfynn.io/packages/{id}/files/{file_id}')
        if resp.ok:
            resp_json = resp.json()
        elif resp.status_code == 404:
            msg = f'{resp.status_code} {resp.reason!r} when fetching {resp.url}'
            raise exc.NoRemoteFileWithThatIdError(msg)
        else:
            resp.raise_for_status()

        try:
            return resp_json['url']
        except KeyError as e:
            log.debug(lj(resp_json))
            raise e


class FakeBFLocal(BFLocal):
    class bf:
        """ tricksy hobbitses """
        @classmethod
        def get_dataset(cls, id):
            #import inspect
            #stack = inspect.stack(0)
            #breakpoint()
            #return CacheAsDataset(id)
            class derp:
                """ yep, this is getting called way down inside added
                    in the extras pipeline :/ """
                doi = None
                status = 'FAKE STATUS ;_;'
            return derp

    def __init__(self, project_id, anchor):
        self.organization = CacheAsBFObject(anchor)  # heh
        self.project_name = anchor.name
        self.project_path = anchor.local


class CacheAsBFObject(BaseNode):
    def __init__(self, cache):
        self.cache = cache
        self.id = cache.id
        self.cache.meta

    @property
    def name(self):
        return self.cache.name

    @property
    def parent(self):
        parent_cache = self.cache.parent #.local.parent.cache
        if parent_cache is not None:
            return self.__class__(parent_cache)

    @property
    def parents(self):
        parent = self.parent
        while parent:
            yield parent
            parent = parent.parent

    def __iter__(self):
        for c in self.cache.local.children:
            cache = c.cache
            if cache.is_dataset():
                child = CacheAsDataset(cache)
            elif cache.is_organization():
                child = CacheAsOrganization(cache)
            elif cache.is_dir():
                child = CacheAsCollection(cache)
            elif cache.is_broken_symlink():
                child = CacheAsFile(cache)

            yield child

    @property
    def members(self):
        return []


class CacheAsFile(CacheAsBFObject, File): pass
class CacheAsCollection(CacheAsBFObject, Collection): pass
class CacheAsDataset(CacheAsBFObject, Dataset):
    """ yep, this is getting called way down inside added
        in the extras pipeline :/ """
    doi = None
    status = 'FAKE STATUS ;_;'
    @property
    def created_at(self): return self.cache.meta.created
    @property
    def updated_at(self): return self.cache.meta.updated
    @property
    def owner_id(self): return self.cache.meta.user_id
class CacheAsOrganization(CacheAsBFObject, Organization): pass
