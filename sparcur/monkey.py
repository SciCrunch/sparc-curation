import os
import io
import sys
import json
from copy import deepcopy
import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from .config import auth
from sparcur.utils import log


def _get_json(self, url, *, retry_limit=3, retry_count=0):
    session = self._api.session
    resp = session.get(url)
    if resp.ok:
        try:
            return resp.json()
        except requests.exceptions.JSONDecodeError as e:
            log.critical(f'resp.ok but json malformed???\n{resp.text}')
            if retry_count >= retry_limit:
                raise e
            else:
                return _get_json(self, url, retry_limit=retry_limit, retry_count=retry_count + 1)
    else:
        resp.raise_for_status()


def bind_agent_command(agent_module, transfers_module, go=False):
    # FIXME this is a nearly perfect use case for orthauth wrapping
    default_agent_cmd = agent_module.agent_cmd
    def agent_cmd():
        rcp = auth.get_path('remote-cli-path')
        if rcp is None:
            return default_agent_cmd()
        else:
            return rcp.as_posix()

    agent_module.agent_cmd = agent_cmd

    if go:
        def agent_env(settings):
            env = {
                #"PENNSIEVE_API_ENVIRONMENT": "local",
                "PENNSIEVE_API_KEY": settings.api_token,
                "PENNSIEVE_API_SECRET": settings.api_secret,
                "PENNSIEVE_AGENT_PORT": '9191',
                "HOME": "/dev/null",
            }
            if sys.platform in ["win32", "cygwin"]:
                env["SYSTEMROOT"] = os.getenv("SYSTEMROOT")

            breakpoint()
            return env

        agent_module.agent_env = agent_env

        def validate_agent_installation(settings): pass

        agent_module.validate_agent_installation = validate_agent_installation
        transfers_module.validate_agent_installation = validate_agent_installation


@property
def patch_session(self):
    """
    Make requests-futures work within threaded/distributed environment.
    """
    if self._session is None:
        self._session = Session()
        self._set_auth(self._token)
        try:
            backoff_factor = auth.get('remote-backoff-factor')
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


def Blackfynn_get(self, id, update=True):
    return self._api.core.get(id, update=update)


def File_download(self, destination):  # FIXME not clear that we use this?
    """ remove prefix functionality since there are filenames without
        extensions ... """

    if self.type == "DirectoryViewerData":
        raise NotImplementedError(
            "Downloading S3 directories is currently not supported")

    if os.path.isdir(destination):
        # destination dir
        f_local = os.path.join(destination, os.path.basename(self.s3_key))
    else:
        # exact location
        f_local = destination

    r = requests.get(self.url, stream=True)
    with io.open(f_local, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

    # set local path
    self.local_path = f_local

    return f_local


def PackagesAPI_get(self, pkg, include='files,source'):
    """
    Get package object

    pkg:     can be DataPackage ID or DataPackage object.
    include: list of fields to force-include in response (if available)
    """
    pkg_id = self._get_id(pkg)

    params = {'includeAncestors': 'true'}
    if include is not None:
        if isinstance(include, str):
            params.update({'include': include})
        elif hasattr(include, '__iter__'):
            params.update({'include': ','.join(include)})

    resp = self._get(self._uri('/{id}', id=pkg_id), params=params)

    # TODO: cast to specific DataPackages based on `type`
    pkg = self._get_package_from_data(resp)
    pkg._resp = resp
    return pkg


# package meta


def bind_packages_File(File):
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


    def _packages(self,
                  pageSize=1000,  # XXX > 1000 -> silent error and no results
                  includeSourceFiles=True,
                  raw=False,
                  latest_only=False,
                  filename=None,
                  retry_limit=3,):
        """ python implementation to make use of /dataset/{id}/packages """
        remapids = {}

        def restructure(j):
            """ restructure package json to match what api needs? """
            # FIXME something's still wonky here
            c = j['content']
            c['int_id'] = c['id']
            c['id'] = c['nodeId']  # FIXME packages seem to be missing ids!?
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

        index = {self.id: self}  # make sure that dataset is in the index
        session = self._api.session
        # cursor
        # pageSize
        # includeSourceFiles
        # types
        # filename
        filename_args = f'&filename={filename}' if filename is not None else ''
        cursor_args = ''
        out_of_order = []
        retry_count = 0
        while True:
            try:
                _url = (
                    f'{self._api._host}/datasets/{self.id}/packages?'
                    f'pageSize={pageSize}&'
                    f'includeSourceFiles={str(includeSourceFiles).lower()}'
                    f'{filename_args}'
                    f'{cursor_args}')
                resp = session.get(_url)  # XXX FIXME infinite hang here due to SSL issue?
                log.log(9, f'wat:\n{resp.url}')
            except requests.exceptions.ChunkedEncodingError as e:
                # this fails stochastically during pypy3 -m sparcur.simple.retrieve
                # --sparse-limit -1 --no-index --parent-parent-path {} --dataset-id {}
                #log.exception(e)
                #breakpoint()
                #raise e
                log.critical('pennsieve connection broken causing ChunkedEncodingError')
                # loop around and try again something has gone wrong on the remote
                # at a point in the call where requests cannot retry
                continue
            except requests.exceptions.RetryError as e:
                log.exception(e)
                # sporadic 504 errors that we probably need to sleep on
                breakpoint()
                raise e

            #print(resp.url)
            if resp.ok:
                try:
                    j = resp.json()
                    retry_count = 0
                except requests.exceptions.JSONDecodeError as e:
                    log.critical(f'resp.ok but json malformed???\n{resp.text}')
                    if retry_count >= retry_limit:
                        raise e
                    else:
                        retry_count += 1
                        # try to fetch the exact same url again
                        continue

                packages = j['packages']
                #log.log(9, f'what is going on 0\n{packages!r}')
                if raw:
                    yield from packages
                    if latest_only:
                        break
                    else:
                        if 'cursor' in j:
                            cursor = j['cursor']
                            cursor_args = f'&cursor={cursor}'
                            continue
                        else:
                            break

                if out_of_order:
                    packages += out_of_order
                    # if a parent is on the other side of a pagination boundary put
                    # the children at the end and move on
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
                            bftype = self._id_to_type(id)
                            dcp = deepcopy(package)
                            try:
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

                        to_yield = None
                        # FIXME TODO tweak updated time of parents when a file
                        # is deleteing to match posix ctime/mtime behavior ???
                        # we may not need this for the most part though? no
                        # FIXME we are missing a test case where an older file
                        # gets deleted and updated cache transitive remains the
                        # same but the file list is different, that should error
                        # right now, the problem with this packages endpoint is
                        # that if you miss the delete window for some reason
                        # you wouldn't get the intervening dates
                        if (isinstance(bfobject.parent, str)
                            and bfobject.parent in index):

                            parent = index[bfobject.parent]
                            if parent._items is None:
                                parent._items = []
                            parent.items.append(bfobject)
                            bfobject.parent = parent
                            # only put objects in the index when they have a
                            # parent that is a bfobject, this ensures that you
                            # can always recurse to base once you get an object
                            # from this function
                            index[bfobject.id] = bfobject
                            if parent.state == 'DELETING':
                                if not bfobject.state == 'DELETING':
                                    bfobject.state = 'PARENT-DELETING'
                            elif parent.state == 'PARENT-DELETING':
                                if not bfobject.state == 'DELETING':
                                    bfobject.state = 'PARENT-DELETING'
                            # FIXME TODO see if we have to deal with mixed deleting/restoring

                            if bfobject.type != 'Collection':
                                log.log(9, f'what is going on 1 {bfobject}')

                            if bfobject.state == 'DELETED':
                                log.log(9, f'object was deleted {bfobject}')
                            elif bfobject.state == 'RESTORING':
                                log.log(9, f'object stuck restoring {bfobject}')
                            else:
                                to_yield = bfobject  # only yield if we can get a parent

                        elif out_of_order is None:  # filename case
                            log.log(9, f'what is going on 2 {bfobject}')
                            if bfobject.state == 'DELETED':
                                log.log(9, f'object was deleted {bfobject}')
                            elif bfobject.state == 'RESTORING':
                                log.log(9, f'object stuck restoring {bfobject}')
                            else:
                                to_yield = bfobject
                        elif bfobject.parent is None:
                            # both collections and packages can be at the top
                            # level dataset was set to its bfobject repr above
                            # so safe to yield
                            if bfobject.dataset is None:
                                log.debug('No parent no dataset\n' +
                                          json.dumps(bfobject._json, indent=2))
                            index[bfobject.id] = bfobject
                            if bfobject.type != 'Collection':
                                log.log(9, f'what is going on 3 {bfobject}')

                            if bfobject.state == 'DELETED':
                                log.log(9, f'object was deleted {bfobject}')
                            elif bfobject.state == 'RESTORING':
                                log.log(9, f'object stuck restoring {bfobject}')
                            else:
                                to_yield = bfobject
                        else:
                            out_of_order.append(bfobject)
                            continue

                        if isinstance(bfobject, self._dp_class):
                            bfobject.fake_files = []
                            bfobject._has_multiple_files = False
                            max_updated = _tsnorm(bfobject._json['content']['updatedAt'])
                            if 'objects' not in bfobject._json:
                                log.error(f'{bfobject} has no files!??!')
                            else:
                                for i, source in enumerate(
                                        bfobject._json['objects']['source']):
                                    # TODO package id?
                                    if len(source) > 1:
                                        # normally source has a single key "content"
                                        log.info('more than one key in source '
                                                 f'{sorted(source)}')

                                    updated = _tsnorm(source['content']['updatedAt'])
                                    if max_updated < updated:
                                        # see ../docs/file-time-metadata.org
                                        max_updated = updated

                                    ff = FakeBFile(
                                        bfobject, **source['content'])
                                    bfobject.fake_files.append(ff)
                                    #yield ff  # XXX don't yield here because we need to know if there are multiple files first

                                    if i == 1:  # only log once
                                        bfobject._has_multiple_files = True  # this needs to be detected BEFORE we yield the package
                                        msg = ('MORE THAN ONE FILE IN PACKAGE '
                                               f'{bfobject.id}')
                                        log.debug(msg)

                                # end for
                                if bfobject._has_multiple_files:
                                    bfobject._has_multiple_files = i + 1

                            # end else
                            bfobject._max_updated = max_updated
                            for ff in bfobject.fake_files:
                                ff._max_updated = max_updated
                                ff._has_multiple_files = bfobject._has_multiple_files

                        if to_yield is not None:
                            yield to_yield
                            if isinstance(to_yield, self._dp_class):
                                yield from to_yield.fake_files

                if 'cursor' in j:
                    cursor = j['cursor']
                    cursor_args = f'&cursor={cursor}'
                else:
                    break

            else:
                break

    return FakeBFile, _packages


def _tsnorm(timestring):
    if len(timestring) == 27:
        return timestring
    elif len(timestring) < 27:
        if len(timestring) == 20:
            # truncated down to dropping the micros so no decimal which
            # produces e.g. 530000000Z seconds instead of 53.000000Z
            # yet another example of why truncating iso8601 datetimes
            # when there are trailing zeros is a bad idea, i really hope
            # they don't truncate seconds too >_<
            return f'{timestring[:-1] + ".":0<26}Z'
        elif len(timestring) < 20:
            f'SIGH I hate being right: {timestring!r}'
            raise NotImplementedError(msg)

        return f'{timestring[:-1]:0<26}Z'
    else:
        msg = f'unhandled timestring length {len(timestring)} for {timestring!r}'
        raise ValueError(msg)


@property
def packages(self, pageSize=1000, includeSourceFiles=True):
    yield from self._packages(
        pageSize=pageSize,
        includeSourceFiles=includeSourceFiles)


def packagesByName(self,
                   pageSize=1000,
                   includeSourceFiles=True,
                   filenames=tuple()):
    if filenames:
        for filename in filenames:
            yield from self._packages(
                pageSize=pageSize,
                includeSourceFiles=includeSourceFiles,
                filename=filename)
    else:
        yield from self._packages(
            pageSize=pageSize,
            includeSourceFiles=includeSourceFiles)


@property
def packages_raw(self, pageSize=1000, includeSourceFiles=True):
    yield from self._packages(
        raw=True,
        pageSize=pageSize,
        includeSourceFiles=includeSourceFiles)


@property
def packageTypeCounts(self):
    return _get_json(self,
        f'{self._api._host}/datasets/{self.id}/packageTypeCounts')

@property
def publishedMetadata(self):
    session = self._api.session
    offset = 0
    limit = 200  # for low dataset int_id values many results can be returned
    for i in range(100):
        resp = session.get((
            f'{self._api._host}/discover/search/datasets?query={self.int_id}'
            f'&limit={limit}&offset={offset}'))
        if resp.ok:
            try:
                j = resp.json()
            except requests.exceptions.JSONDecodeError as e:
                log.critical(f'resp.ok but json malformed???\n{resp.text}')
                raise e  # FIXME retry

            if j['totalCount'] == 1:
                return j['datasets'][0]
            elif j['totalCount'] == 0:
                return
            else:
                # FIXME this will fail to find datasets not published from sparc I think ???
                org_int_id = self._api._context.int_id
                cands = [d for d in j['datasets'] if
                        d['sourceDatasetId'] == self.int_id and
                        d['organizationId'] == org_int_id]
                lc = len(cands)
                if lc == 1:
                    return cands[0]
                elif lc == 0:
                    if j['totalCount'] > (j['limit'] + offset):
                        # yes - 1 leads to overlap but that's ok
                        offset += (j['limit'] - 1)
                        continue

                    return
                else:
                    return sorted(cands, key=lambda d:d['version'], reverse=True)[0]

            return j
        else:
            resp.raise_for_status()

    else:
        msg = 'that is a lot of results {j["totalCount"]}'
        raise ValueError(msg)


def publishedVersionMetadata(self, id_published, published_version):
    # TODO cache this because technically it is static
    session = self._api.session
    return _get_json(self, (
        f'{self._api._host}/discover/datasets/'
        f'{id_published}/versions/{published_version}/metadata'))


@property
def Dataset_users(self):
    return _get_json(self,
        f'{self._api._host}/datasets/{self.id}/collaborators/users')


@property
def Dataset_teams(self):
    return _get_json(self,
        f'{self._api._host}/datasets/{self.id}/collaborators/teams')


@property
def Dataset_doi(self):
    session = self._api.session
    resp = session.get(
        f'{self._api._host}/datasets/{self.id}/doi')
    if resp.ok:
        try:
            return resp.json()
        except requests.exceptions.JSONDecodeError as e:
            log.critical(f'resp.ok but json malformed???\n{resp.text}')
            raise e  # FIXME retry
    else:
        if resp.status_code != 404:
            log.warning(f'{self} doi {resp.status_code}')


def Dataset_delete(self):
    """ actually delete """
    session = self._api.session
    resp = session.delete(f'{self._api._host}/datasets/{self.id}')
    if resp.ok:
        try:
            return resp.json()
        except requests.exceptions.JSONDecodeError as e:
            log.critical(f'resp.ok but json malformed???\n{resp.text}')
            raise e  # FIXME retry
    else:
        if resp.status_code != 404:
            log.warning(f'{self} issue deleting {resp.status_code}')


@property
def Dataset_contributors(self):
    return _get_json(self, f'{self._api._host}/datasets/{self.id}/contributors')


@property
def Dataset_banner(self):
    return _get_json(self, f'{self._api._host}/datasets/{self.id}/banner')


@property
def Dataset_readme(self):
    return _get_json(self, f'{self._api._host}/datasets/{self.id}/readme')


@property
def Dataset_status_log(self):
    return _get_json(self, f'{self._api._host}/datasets/{self.id}/status-log')


@property
def Dataset_meta(self):
    return _get_json(self, f'{self._api._host}/datasets/{self.id}')


@property
def Organization_teams(self):
    return _get_json(self, f'{self._api._host}/organizations/{self.id}/teams')

def bind_dga(Dataset):
    def insert_extra(ds, self):
        d = Dataset.from_dict(ds, api=self.session)
        d.publication_status = ds['publication']['status']
        d.canPublish = ds['canPublish']
        return d

    def DatasetAPI_get_all_old(self):
        resp = self._get(self._uri("/?includePublishedDataset=true"))
        return [insert_extra(ds, self) for ds in resp]

    def DatasetAPI_get_all(self):
        resp = self._get(self._uri("/paginated?limit=2147483647&includePublishedDataset=true"))
        return [insert_extra(ds, self) for ds in resp['datasets']]

    return DatasetAPI_get_all
