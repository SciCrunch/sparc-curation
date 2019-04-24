"""Accessing files via the blackfynn api

# Install these python packages.
``` bash
pip install blackfynn nibabel pydicom
git clone https://github.com/tgbugs/pyontutils.git
pushd pyontutils
python setup.py develop --user
ontutils devconfig --write
# you can edit ./pyontutils/devconfig.yaml to match your system if needs be
touch ${HOME}/pyontutils-secrets.yaml
chmod 0600 ${HOME}/pyontutils-secrets.yaml
```

# Get a blackfynn api key and api secret
navigate to https://app.blackfynn.io/${blackfynn_organization}/profile/
You have to find your way through the UI if you don't know your org id :/

SPARC MVP is at
https://app.blackfynn.io/N:organization:89dfbdad-a451-4941-ad97-4b8479ed3de4/profile/
SPARC Consortium is at
https://app.blackfynn.io/N:organization:618e8dd9-f8d2-4dc4-9abb-c6aaab2e78a0/profile/

add the following lines to your secrets.yaml file from the blackfynn site
you can (and should) save those keys elsewhere as well
```
blackfynn-mvp-key: ${apikey}
blackfynn-mvp-secret: ${apisecret}
```

"""

import io
import os
import json
import asyncio
from copy import deepcopy
from nibabel import nifti1
from pydicom import dcmread
import yaml
import requests
from requests import Session
from requests.exceptions import HTTPError, ConnectionError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from joblib import Parallel, delayed
from blackfynn import Blackfynn, Collection, DataPackage, Organization, File
from blackfynn import Dataset, BaseNode
from blackfynn.models import BaseCollection
from blackfynn import base as bfb
from pyontutils.utils import Async, deferred, async_getter, chunk_list
from pyontutils.config import devconfig
from sparcur import exceptions as exc
from sparcur.core import log
from sparcur.paths import Path
from sparcur.config import local_storage_prefix
from sparcur.metastore import MetaStore
from scipy.io import loadmat


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

    id = ''  # unforunately these don't seem to have made it through

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

        for k, v in kwargs.items():
            if k == 'size' and v is None:
                v = None  # FIXME hack

            setattr(self, k, v)

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


@property
def packages(self, pageSize=1000, includeSourceFiles=True):
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
    cursor_args = ''
    out_of_order = []
    while True:
        resp = session.get(f'https://api.blackfynn.io/datasets/{self.id}/packages?'
                           f'pageSize={pageSize}&'
                           f'includeSourceFiles={str(includeSourceFiles).lower()}'
                           f'{cursor_args}')
        #print(resp.url)
        if resp.ok:
            j = resp.json()
            packages = j['packages']
            if out_of_order:
                packages += out_of_order
                # if a parent is on the other side of a
                # pagination boundary put the children
                # at the end and move on
            out_of_order = [None]
            while out_of_order:
                log.debug(f'{out_of_order}')
                if out_of_order[0] is None:
                    out_of_order.remove(None)
                elif packages == out_of_order:
                    breakpoint()
                    raise RuntimeError('We are going nowhere!')
                else:
                    packages = out_of_order
                    out_of_order = []
                for count, package in enumerate(packages):
                    if isinstance(package, dict):
                        id = package['content']['nodeId']
                        bftype = id_to_type(id)
                        try:
                            rdp = restructure(deepcopy(package))
                        except KeyError as e:
                            out_of_order.append(package)
                            continue

                        bfobject = bftype.from_dict(rdp, api=self._api)
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
                        yield bfobject  # only yield if we can get a parent
                    elif bfobject.parent is None:
                        # both collections and packages can be at the top level
                        # dataset was set to its bfobject repr above so safe to yield
                        log.debug(json.dumps(bfobject._json, indent=2))
                        index[bfobject.id] = bfobject
                        yield bfobject
                    else:
                        out_of_order.append(bfobject)
                        continue

                    if isinstance(bfobject, DataPackage):
                        if 'objects' not in bfobject._json:
                            log.error(f'{bfobject} has no files!??!')
                        else:
                            for i, source in enumerate(bfobject._json['objects']['source']):
                                # TODO package id?
                                if len(source) > 1:
                                    log.info(f'more than one key in source {sorted(source)}')

                                yield FakeBFile(bfobject, **source['content'])
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
Dataset.packages = packages


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
        #if not isinstance(local_storage_prefix, Path):
            #local_storage_prefix = Path(local_storage_prefix)

        # no changing local storage prefix in the middle of things
        # if you want to do that create a new class

        try:
            self.bf = Blackfynn(api_token=devconfig.secrets('blackfynn', project_id, 'key'),
                                api_secret=devconfig.secrets('blackfynn', project_id, 'secret'))
        except KeyError as e:
            raise exc.MissingSecretError from e
        self.organization = self.bf.context
        self.project_name = self.bf.context.name

        if anchor is not None:  # FIXME decouple
            self.project_path = anchor.local
            self.metastore = MetaStore(self.project_path.parent / (self.project_name + ' xattrs.db'))

    @property
    def error_meta(self):
        for path in list(self.project_path.rglob('*ERROR')):
            yield self.get_file_meta(path)

    @property
    def fake_files(self):
        yield from self.project_path.rglob('*.fake.*')

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

    def get(self, id):
        if id.startswith('N:dataset:'):
            thing = self.bf.get_dataset(id)  # heterogenity is fun!
        elif id.startswith('N:organization:'):
            if id == self.organization.id:
                return self.organization  # FIXME staleness?
            else:
                # if we start form local storage prefix for everything then
                # this would work
                raise BaseException('TODO org does not match need other api keys.')
        else:
            thing = self.bf.get(id)

        return thing

    #def get_homogenous(self, id):
        #return HomogenousBF(self.get(id))

    #def datasets(self):
        #for d in self.bf.datasets():
            #yield HomogenousBF(d)

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


class FakeBFLocal(BFLocal):
    class bf:
        """ tricksy hobbitses """

    def __init__(self, project_id, anchor):
        self.organization = CacheAsBFObject(anchor)  # heh
        self.project_name = anchor.name
        self.project_path = anchor.local
        self.metastore = MetaStore(self.project_path.parent / (self.project_name + ' xattrs.db'))


class CacheAsBFObject(BaseNode):
    def __init__(self, cache):
        self.cache = cache
        self.id = cache.id
        self.cache.meta

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
            yield self.__class__(c.cache)


def mvp():
    """ In order to be performant for large numbers of packages we have
        to get all the packages first and then async retrieve all the files
    """
    bf = Blackfynn(api_token=devconfig.secrets('blackfynn-mvp-key'),
                    api_secret=devconfig.secrets('blackfynn-mvp-secret'))

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


def main():
    from IPython import embed
    bfl = BFLocal()
    #bfl.cons()
    #bfl.fetch_errors()
    ff = list(bfl.fake_files)
    embed()

if __name__ == '__main__':
    main()
