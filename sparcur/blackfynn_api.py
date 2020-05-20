import io
import os
import json
import types
import asyncio
from copy import deepcopy
#from nibabel import nifti1
#from pydicom import dcmread
#from scipy.io import loadmat
import yaml
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
from blackfynn import Blackfynn, Collection, DataPackage, Organization, File
from blackfynn import Dataset, BaseNode
from blackfynn.models import BaseCollection
from blackfynn import base as bfb
from blackfynn.api import transfers
from blackfynn.api.data import PackagesAPI
from pyontutils.utils import Async, deferred, async_getter, chunk_list
from pyontutils.iterio import IterIO
from sparcur import exceptions as exc
from sparcur.core import log, lj
from sparcur.metastore import MetaStore
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

        # Enable retries via urllib
        adapter = HTTPAdapter(
            pool_connections=1000,  # wheeee
            pool_maxsize=1000,  # wheeee
            max_retries=Retry(
                total=self.settings.max_request_timeout_retries,
                backoff_factor=.5,
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
        resp = session.get(f'https://api.blackfynn.io/datasets/{self.id}/packages?'
                           f'pageSize={pageSize}&'
                           f'includeSourceFiles={str(includeSourceFiles).lower()}'
                           f'{filename_args}'
                           f'{cursor_args}')
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


# monkey patch Dataset to implement packages endpoint
Dataset._packages = _packages
Dataset.packages = packages
Dataset.packagesByName = packagesByName


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


prefix = 'https://app.blackfynn.io/'
lp = len(prefix)
def destructure_uri(uri):
    if not uri.startswith(prefix):
        return None  # TODO less cryptic return value or exception

    suffix = uri[lp:]
    for maybe_id in suffix.split('/'):
        if maybe_id.startswith('N:'):
            yield maybe_id

    'N:organization:89dfbdad-a451-4941-ad97-4b8479ed3de4'
    '/datasets/'
    'N:dataset:bedda0db-c275-4d79-87ce-fc7bf1e11600'
    '/files'


def make_filename(file):
    # we have to do this because the type on the package is unreliable
    # so we have to get it from s3 because that is what will roundtrip
    _, file_name_s3 = file.s3_key.rsplit('/', 1)
    return file_name_s3

def process_package(package, path, doasync=True):
    # packages are the souless shells of files
    # you can retrieve them quickly, but they have no substance

    if not path.exists():
        path.mkdir(parents=True)

    if isinstance(package, Collection):
        npath = path / package.name
        if doasync:  # FIXME recursive async doesn't quite work
            for lst in Async(debug=True)(deferred(pp)(npackage, npath)
                                        for npackage in package):
                yield from lst
        else:
            yield from (process_package(npackage, npath, False)
                        for npackage in package)

    else:
        for file in package.files:
            file_name = make_filename(file)
            log.debug(file_name)
            file_path = path / file_name
            yield file, file_path


def pp(package, path, doasync=True):
    return list(process_package(package, path, doasync))


def inner(thing):
    """ one level """
    if isinstance(thing, DataPackage):
        return thing,
    else:
        return list(thing)

def outer(dataset):
    return [e for t in Async()(deferred(inner)(thing) for thing in dataset) for e in t]

def heh(c):
    #print(c)
    return list(c)

def asynchelper(chunk):
    import asyncio
    asyncio.set_event_loop(asyncio.new_event_loop())
    wat = async_getter(heh, [(c,) for c in chunk]) 
    log.debug('chunkdone')
    return [e for t in wat for e in t]

def get_packages(package_or_collection, path):
    """ flatten collections into packages """
    if isinstance(package_or_collection, Collection):
        npath = path / NormFolder(package_or_collection.name)
        yield package_or_collection, path
        for npc in package_or_collection:
            yield from get_packages(npc, npath)
    else:
        log.debug(f'{path} {package_or_collection}')
        yield package_or_collection, path


def get_packages_(dataset):
    """ flatten collections into packages """

    hrm1 = outer(dataset)
    print(len(dataset))
    print(len(hrm1))
    hrm2 = outer(hrm1)
    print(len(hrm2))
    chunks = chunk_list([c for c in hrm2 if isinstance(c, Collection)], 1000)
    #wat = async_getter(heh, [(c,) for c in hrm2 if isinstance(c, Collection)][:1000]) 
    Parallel(n_jobs=8, backend="threading")(delayed(asynchelper)(chunk) for chunk in chunks)
    breakpoint()
    return
    if collector is None:
        collector = []

    files = []
    folders = []
    if isinstance(package_or_collection, Collection):
        list(package_or_collection)
    else:
        files.append(package_or_collection, path)

def blah():
    if isinstance(package_or_collection, Collection):
        [t for npc in package_or_collection
         for t in (list(npc) if isinstance(npc, Collection) else (npc,))]
    else:
        yield package_or_collection, path

def pkgs_breadth(package_or_collection, path):
    print(path)
    if isinstance(package_or_collection, Collection):
        npath = path / NormFolder(package_or_collection.name)
        to_iter = list(package_or_collection)
        #print(to_iter)
        coll = []
        for thing in Async()(deferred(get_packages)(npc, npath) for npc in to_iter):
            coll.append(thing)
            #yield from thing

        yield coll
        Async()(deferred(list)(t) for t in thing)
        #yield (list(c) for c in coll)
        #[v for h in hrm for v in h]

        #for npc in package_or_collection:
            #for poc_p in 
            #yield from get_packages(npc, npath)
    else:
        yield package_or_collection, path

def pkgs_depth(mess):
    hrm = Async()(deferred(list)(t) for t in mess)

class FakeFile:
    def __init__(self, package):
        self.s3_key = '/' + package.name
        self.size = -1
        self.id = -1
        self.pkg_id = package.id
        self.created_at = package.created_at
        self.updated_at = package.updated_at

def gfiles(package, path):
    # print(p.name)
    # the fanout is horrible ...
    for i in range(100):
        try:
            return path, package.files
        except HTTPError as e:
            status_code = e.response.status_code
            print(e)
            asyncio.sleep(2)
    else:
        return path, FakeFile(package)

def unlink_fakes(attrs, fake_paths, metastore):
    for fpath in fake_paths:
        fattrs = {k:v for k, v in norm_xattrs(fpath.xattrs()).items() if k != 'bf.error'}
        if fattrs == attrs:
            fpath.unlink()
            metastore.remove(fpath)
        else:
            print('WARNING: fake xattrs and real xattrs do not match!', attrs, fattrs)


def norm_xattrs(attrs):
    # FIXME update Path.xattrs to accept a normalization function
    out = {}
    ints = 'bf.file_id', 'bf.size'
    strings = 'bf.id', 'bf.created_at', 'bf.updated_at'
    for k, v in attrs.items():
        if isinstance(k, bytes):
            k = k.decode()  # still handled by Path

        if k in strings:
            v = v.decode('utf-8')
        elif k in ints:
            v = int(v)

        out[k] = v

    return out


def make_file_xattrs(file):
    return {
        'bf.id':file.pkg_id,
        'bf.file_id':file.id,
        'bf.size':file.size,
        'bf.created_at':file.updated_at,
        'bf.updated_at':file.created_at,
        'bf.checksum':'',  # TODO
        # 'bf.old_id': '',  # TODO does this work? also naming previous_id, old_version_id etc...
    }


def make_folder_xattrs(folder):
    """ I weep for the world where files and folders were the same thing.
        I just want this file though! NO YOU MUST TAKE IT ALL.
        Does have some usability issues, but would have made metadata
        SO much easier to deal with and explain to people. Of course
        there are some major antipatterns for a system that can do that. """
    out = {'bf.id':folder.id}
    if not folder.id.startswith('N:organization:'):
        out.update({'bf.created_at':folder.updated_at,
                    'bf.updated_at':folder.created_at,})

    return out


def fetch_file(file_path, file, metastore, limit=False, overwrite=False):
    if not file_path.parent.exists():
        file_path.parent.mkdir(parents=True, exist_ok=True)

    fake_paths = list(file_path.parent.glob(file_path.name + '.fake*'))

    if file_path.exists() and not overwrite:
        print('already have', file_path)
        attrs = norm_xattrs(file_path.xattrs())
        unlink_fakes(attrs, fake_paths, metastore)
        return

    limit_mb = 2
    file_mb = file.size / 1024 ** 2
    skip = 'jpeg', 'jpg', 'tif', 'png'
    file_xattrs = make_file_xattrs(file)

    if (not limit or
        (file_mb < limit_mb and
         (not file_path.suffixes or
          file_path.suffixes[0][1:].lower() not in skip))):
        print('fetching', file)
        for i in range(4):  # try 4 times
            try:
                # FIXME I think README_README is an s3_key related error
                file.download(file_path.as_posix())
                file_path.setxattrs(file_xattrs)
                #metastore.setxattrs(file_path, file_xattrs)  # FIXME concurrent access kills this
                attrs = norm_xattrs(file_path.xattrs())  # yes slow, but is a sanity check
                # TODO validate the checksum when we get it
                unlink_fakes(attrs, fake_paths, metastore)
                return
            except (HTTPError, ConnectionError) as e:
                error = str(e)
                status_code = e.response.status_code
                asyncio.sleep(3)
        else:
            print(error)
            error_path = file_path.with_suffix(file_path.suffix + '.fake.ERROR')
            error_path.touch()
            file_xattrs['bf.error'] = str(status_code)
            error_path.setxattrs(file_xattrs)
            #metastore.setxattrs(error_path, file_xattrs)
    else:
        fsize = str(int(file_mb)) + 'M' if file_mb >= 1 else str(file.size // 1024) + 'K'
        fakepath = file_path.with_suffix(file_path.suffix + '.fake.' + fsize)
        fakepath.touch()
        fakepath.setxattrs(file_xattrs)
        #metastore.setxattrs(fakepath, file_xattrs)


def make_files_meta(collection):
    # TODO file fetching status? file hash?
    return {make_filename(file):[package.id, file.id, file.size]
            for package in collection
            if isinstance(package, DataPackage)
            for file in package.files}


class NormFolder(str):
    def __new__(cls, value):
        return str.__new__(cls, cls.normalize(value))

    @classmethod
    def normalize(cls, value):
        # I CAN'T BELIEVE YOU'VE DONE THIS
        return value.replace('/', '_')


def make_folder_and_meta(parent_path, collection, metastore):
    """ maps to collection dataset or organization """
    folder_path = parent_path / NormFolder(collection.name)
    #meta_file = folder_path / collection.id
    #if isinstance(collection, Organization):
        #files_meta = {}
    #else:
        #files_meta = make_files_meta(collection)
    folder_path.mkdir(parents=True, exist_ok=True)
    attrs = make_folder_xattrs(collection)
    folder_path.setxattrs(attrs)
    # sadly xattrs are easy to accidentally zap :/
    #metastore.setxattr(folder_path, 'bf.id', collection.id)
    #with open(meta_file, 'wt') as f:
        #yaml.dump(files_meta, f, default_flow_style=False)


def get_file_by_id(get, file_path, pid, fid):
    package = get(pid)
    if package is None:
        print('WARNING package does not exist', file_path, pid, fid)
        return None, None

    for f in package.files:
        if f.id == fid:
            return file_path, f
    else:
        print('WARNING file does not exist', file_path, pid, fid)
        return None, None


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
        """ There is not bf id for this file. """

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

    def recover_meta(self, path):
        pattrs = norm_xattrs(path.parent.xattrs())
        codid = pattrs['bf.id']
        thing = self.get(codid)
        if thing is None:
            return {}
            raise BaseException('What are you doing!??!?!')

        test_path = path
        while '.fake' in test_path.suffixes:
            test_path = test_path.with_suffix('')

        for poc in thing:
            if NormFolder(poc.name) == test_path.stem:  # FIXME
                if isinstance(poc, Collection):
                    return {'bf.id':poc.id}

                for file in poc.files:  # FIXME files vs folders
                    filename = make_filename(file)
                    if filename == test_path.name:
                        return make_file_xattrs(file)

        else:
            raise BaseException(f'\'{path}\' could not recover meta!')

    def get_file_meta(self, path):
        attrs = norm_xattrs(path.xattrs())
        if 'bf.id' not in attrs:
            # TODO maintain a single backup mapping of xattrs to paths
            # and just use the xattrs for performance
            attrs = self.metastore.xattrs(path)
            if attrs:
                # TODO checksum ... (sigh git)
                path.setxattrs(attrx)
                attrs = norm_xattrs(path.xattrs())
            else:
                raise self.NoBfMeta

        pid = attrs['bf.id']
        if pid.startswith('N:package:'):
            fid = int(attrs['bf.file_id'])
            file_path = path
            while '.fake' in file_path.suffixes:
                file_path = file_path.with_suffix('')

            return file_path, pid, fid
        else:
            log.warning(f'what is going on with {path} {attrs}')

    def fetch_path(self, path, overwrite=False):
        """ Fetch individual big files.
            `path` argument must be to a fake file which has the meta stored in xattrs """
        # automatic async function application inside a list comp ... would be fun
        fetch_file(*get_file_by_id(self.bf.get, *self.get_file_meta(path)), self.metastore, overwrite=overwrite)

    def fetch_errors(self):
        bfiles = {fp:f for fp, f in
                  Async()(deferred(get_file_by_id)(self.bf.get, file_path, pid, fid)
                          for file_path, pid, fid in self.error_meta)}
        Async()(deferred(fetch_file)(filepath, file, self.metastore) for filepath, file in bfiles.items() if not filepath.exists())

    def file_fetch_dict(self, packages):
        return {folder_path / make_filename(file):file
                for folder_path, files in
                Async()(deferred(gfiles)(package, path) for package, path in packages)
                for file in files}


class OldStuff:
    def populate_metastore(self):
        """ This should be run after find_missing_meta. """
        # FIXME need a way to delete files
        all_attrs = {path:norm_xattrs(path.xattrs()) for path in self.project_path.rglob('*')
                     if '.git' not in path.as_posix()}
        bad = [path for path, attrs in all_attrs.items() if not attrs]
        if bad:
            log.warning(f'{bad} is missing meta, run find_missing_meta')
            all_attrs = {p:a for p, a in all_attrs.items() if a}

        self.metastore.bulk(all_attrs)

    def find_missing_meta(self):
        for path in self.project_path.rglob('*'):
            if '.git' in path.as_posix():
                continue
            attrs = norm_xattrs(path.xattrs())
            if not attrs:
                log.warning(f'Found path with missing metadata {path}')
                attrs = self.metastore.xattrs(path)
                if not attrs:
                    log.error('No local metadata was found for {path}')
                    attrs = self.recover_meta(path)

                path.setxattrs(attrs)
                # TODO checksum may no longer match since we changed it


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


def mvp():
    """ In order to be performant for large numbers of packages we have
        to get all the packages first and then async retrieve all the files
    """
    from sparcur import paths
    local_storage_prefix = paths.Path('~/files/blackfynn_local/').expanduser()
    bf = Blackfynn(api_token=auth.dynamic_config.secrets('blackfynn-mvp-key'),
                    api_secret=auth.dynamic_config.secrets('blackfynn-mvp-secret'))


    ds = bf.datasets()
    useful = {d.id:d for d in ds}  # don't worry, I've made this mistake too

    project_name = bf.context.name

    helm = useful['N:dataset:bedda0db-c275-4d79-87ce-fc7bf1e11600']
    helmr = useful['N:dataset:d412a972-870c-4b63-9865-8d790065bd43']
    datasets = helm, helmr  # TODO add more datasets here
    packages = []
    for dataset in datasets:
        dataset_name = dataset.name
        ds_path = local_storage_prefix / project_name / dataset_name
        for package_or_collection in dataset:
            packages.extend(get_packages(package_or_collection, ds_path))

    bfiles = {folder_path / make_filename(file):file
              for folder_path, files in
              Async()(deferred(gfiles)(package, path) for package, path in packages)
              if files is not None  # FIXME how to deal with this!?
              for file in files}

    # beware that this will send as many requests as it can as fast as it can
    # which is not the friendliest thing to do to an api
    Async()(deferred(fetch_file)(*fpf, self.metastore) for fpf in bfiles.items() if not fp.exists())
    self.populate_metastore()  # FIXME workaround for concurrent access issues, probably faster, but :/

    return bf, bfiles


def process_files(bf, files):
    from IPython import embed
    niftis = [nifti1.load(f.as_posix()) for f in files if '.nii' in f.suffixes]
    mats = [loadmat(f.as_posix()) for f in files if '.mat' in f.suffixes]
    dicoms = [dcmread(f.as_posix()) for f in files if '.dcm' in f.suffixes]  # loaded dicom files
    embed()  # XXX you will drop into an interactive terminal in this scope


def mvp_main():
    bf, files = mvp()
    process_files(bf, files)

