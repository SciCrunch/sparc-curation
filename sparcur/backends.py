import os
from pathlib import PurePosixPath, PurePath
from datetime import datetime
import idlib
import requests
from pyontutils.utils import Async, deferred
from sparcur import exceptions as exc
from sparcur.utils import log
from sparcur.core import BlackfynnId
import augpathlib as aug
from augpathlib import PathMeta
from sparcur.blackfynn_api import BFLocal, FakeBFLocal, id_to_type  # FIXME there should be a better way ...
from blackfynn import Collection, DataPackage, Organization, File
from blackfynn import Dataset
from blackfynn.models import BaseNode

from ast import literal_eval


class BlackfynnRemote(aug.RemotePath):

    _api_class = BFLocal
    _async_rate = None
    _local_dataset_name = object()

    @property
    def uri_human(self):
        # org /datasets/ N:dataset /files/ N:collection
        # org /datasets/ N:dataset /files/ wat? /N:package  # opaque but consistent id??
        # org /datasets/ N:dataset /viewer/ N:package

        id = self.id
        N, type, suffix = id.split(':')
        if id.startswith('N:package:'):
            prefix = '/viewer/'
        elif id.startswith('N:collection:'):
            prefix = '/files/'
        elif id.startswith('N:dataset:'):
            prefix = '/'  # apparently organization needs /datasets after it
            return self.parent.uri_human + prefix + id
        elif id.startswith('N:organization:'):
            return f'https://app.blackfynn.io/{id}/datasets'
        else:
            raise exc.UnhandledTypeError(type)

        if self.dataset_id is None:
            raise exc.NotInProjectError(f'{self}')

        return self.dataset.uri_human + prefix + id

    @property
    def uri_api(self):
        if self.is_dataset():  # functions being true by default is an antipattern for stuff like this >_<
            endpoint = 'datasets/' + self.id
        elif self.is_organization:
            endpoint = 'organizations/' + self.id
        elif self.file_id is not None:
            endpoint = f'packages/{self.id}/files/{self.file_id}'
        else:
            endpoint = 'packages/' + self.id

        return 'https://api.blackfynn.io/' + endpoint

    @property
    def errors(self):
        yield from self._errors
        if self.remote_in_error:
            yield 'remote-error'

    @property
    def remote_in_error(self):
        return self.state == 'ERROR'

    @property
    def state(self):
        if hasattr(self.bfobject, 'state'):
            return self.bfobject.state

    @staticmethod
    def get_file(package, file_id):
        files = package.files
        if len(files) > 1:
            log.critical(f'MORE THAN ONE FILE IN PACKAGE {package.id}')
        for file in files:
            if file.id == file_id:
                return file

        else:
            raise FileNotFoundError(f'{package} has no file with id {file_id} but has:\n{files}')

    @classmethod
    def get_file_url(cls, id, file_id):
        if file_id is not None:
            return cls._api.get_file_url(id, file_id)

    @classmethod
    def get_file_by_id(cls, id, file_id):
        url = cls.get_file_url(id, file_id)
        yield from cls.get_file_by_url(url)

    @classmethod
    def get_file_by_url(cls, url):
        """ NOTE THAT THE FIRST YIELD IS HEADERS """
        resp = requests.get(url, stream=True)
        headers = resp.headers
        yield headers
        log.debug(f'reading from {url}')
        for chunk in resp.iter_content(chunk_size=4096):  # FIXME align chunksizes between local and remote
            if chunk:
                yield chunk

    def __init__(self, id_bfo_or_bfr, *, file_id=None, cache=None, local_only=False):
        self._seed = id_bfo_or_bfr
        self._file_id = file_id
        if not [type_ for type_ in (self.__class__,
                                    BaseNode,
                                    str,
                                    PathMeta)
                if isinstance(self._seed, type_)]:
            raise TypeError(self._seed)

        if cache is not None:
            self._cache_setter(cache, update_meta=False)

        self._errors = []

        self._local_only = local_only

    def bfobject_retry(self):
        """ try to load a local only id again """
        if hasattr(self, '_bfobject') and self._bfobject.id == self._local_dataset_name:
            stored = self._bfobject
            delattr(self, '_bfobject')
            try:
                self.bfobject
            except BaseException as e:
                self._bfobject = stored
                raise e

        return self._bfobject

    @property
    def bfobject(self):
        if hasattr(self, '_bfobject'):
            return self._bfobject

        if isinstance(self._seed, self.__class__):
            bfobject = self._seed.bfobject

        elif isinstance(self._seed, BaseNode):
            bfobject = self._seed

        elif isinstance(self._seed, str):
            try:
                bfobject = self._api.get(self._seed)
            except Exception as e:  # sigh
                if self._local_only:
                    _class = id_to_type(self._seed)
                    if issubclass(_class, Dataset):
                        bfobject = _class(self._local_dataset_name)
                        bfobject.id = self._seed
                    else:
                        raise NotImplementedError(f'{_class}') from e
                else:
                    raise e

        elif isinstance(self._seed, PathMeta):
            bfobject = self._api.get(self._seed.id)

        else:
            raise TypeError(self._seed)

        if hasattr(bfobject, '_json'):
            # constructed from a packages query
            # which we need in order for things to be fastish
            self._bfobject = bfobject
            return self._bfobject

        if isinstance(bfobject, DataPackage):
            def transfer(file, bfobject):
                file.parent = bfobject.parent
                file.dataset = bfobject.dataset
                file.state = bfobject.state
                file.package = bfobject
                return file

            files = bfobject.files
            parent = bfobject.parent
            if files:
                if self._file_id is not None:
                    for file in files:
                        if file.id == self._file_id:
                            bfobject = transfer(file, bfobject)

                elif len(files) > 1:
                    log.critical(f'MORE THAN ONE FILE IN PACKAGE {bfobject.id}')
                    if (len(set(f.size for f in files)) == 1 and
                        len(set(f.name for f in files)) == 1):
                        log.critical('Why are there multiple files with the same name and size here?')
                        file = files[0]
                        bfobject = transfer(file, bfobject)
                    else:
                        log.critical(f'There are actually multiple files ...\n{files}')

                else:
                    file = files[0]
                    bfobject = transfer(file, bfobject)

                bfobject.parent = parent  # sometimes we will just reset a parent to itself
            else:
                log.warning(f'No files in package {bfobject.id}')

        self._bfobject = bfobject
        return self._bfobject

    def is_anchor(self):
        return self.anchor == self

    @property
    def anchor(self):
        """ NOTE: this is a slight depature from the semantics in pathlib
            because this returns the path representation NOT the string """
        return self.organization

    @property
    def organization(self):
        # organization is the root in this system so
        # we do not want to depend on parent to look this up
        # nesting organizations is file, but we need to know
        # what the 'expected' root is independent of the actual root
        # because if you have multiple virtual trees on top of the
        # same file system then you need to know the root for
        # your current tree assuming that the underlying ids
        # can be reused (as in something like dat)

        if not hasattr(self.__class__, '_organization'):
            self.__class__._organization = self.__class__(self._api.organization)

        return self._organization

    def is_organization(self):
        return isinstance(self.bfobject, Organization)

    def is_dataset(self):
        return isinstance(self.bfobject, Dataset)

    @property
    def dataset_id(self):
        """ save a network transit if we don't need it """
        dataset = self._bfobject.dataset
        if isinstance(dataset, str):
            return dataset
        else:
            return dataset.id

    @property
    def dataset(self):
        remote = self.__class__(self.dataset_id)
        remote.cache_init()
        return remote

        dataset = self._bfobject.dataset
        if isinstance(dataset, str):
            dataset = self.organization.get_child_by_id(dataset)
            self.bfobject.dataset = dataset.bfobject
        else:
            dataset = self.__class__(dataset)

        return dataset

    def get_child_by_id(self, id):
        for c in self.children:
            if c.id == id:
                return c

    @property
    def from_packages(self):
        return hasattr(self.bfobject, '_json')

    @property
    def stem(self):
        name = PurePosixPath(self._name)
        return name.stem
        #if isinstance(self.bfobject, File) and not self.from_packages:
            #return name.stem
        #else:
            #return name.stem

    @property
    def suffix(self):
        # fixme loads of shoddy logic in here
        name = PurePosixPath(self._name)
        if isinstance(self.bfobject, File) and not self.from_packages:
            return name.suffix
        elif isinstance(self.bfobject, Collection):
            return ''
        elif isinstance(self.bfobject, Dataset):
            return ''
        elif isinstance(self.bfobject, Organization):
            return ''
        else:
            raise NotImplementedError('should not be needed anymore when using packages')
            if hasattr(self.bfobject, 'type'):
                type = self.bfobject.type.lower()  # FIXME ... can we match s3key?
            else:
                type = None

            if type not in ('unknown', 'unsupported', 'generic', 'genericdata'):
                pass

            elif hasattr(self.bfobject, 'properties'):
                for p in self.bfobject.properties:
                    if p.key == 'subtype':
                        type = p.value.replace(' ', '').lower()
                        break

            return ('.' + type) if type is not None else ''

    @property
    def _name(self):
        name = self.bfobject.name
        if isinstance(self.bfobject, File) and not self.from_packages:
            realname = os.path.basename(self.bfobject.s3_key)
            if name != realname:  # mega weirdness
                if realname.startswith(name):
                    name = realname

                else:
                    realpath = PurePath(realname)
                    namepath = PurePath(name)
                    if namepath.suffixes:
                        log.critical('sigh {namepath!r} -?-> {realpath!r}')

                    else:
                        path = namepath
                        for suffix in realpath.suffixes:
                            path = path.with_suffix(suffix)

                        old_name = name
                        name = path.as_posix()
                        log.info(f'name {old_name} -> {name}')

        if '/' in name:
            bads = ','.join(f'{i}' for i, c in enumerate(name) if c == '/')
            self._errors.append(f'slashes {bads}')
            log.critical(f'GO AWAY {self}')
            name = name.replace('/', '_')
            self.bfobject.name = name  # AND DON'T BOTHER US AGAIN

        return name

    @property
    def name(self):
        if isinstance(self.bfobject, File) and self.from_packages:
            return self.bfobject.filename
        else:
            return self.stem + self.suffix

    @property
    def id(self):
        if isinstance(self._seed, self.__class__):
            id = self._seed.bfobject.id

        elif isinstance(self._seed, BaseNode):
            if isinstance(self._seed, File):
                id = self._seed.pkg_id
            else:
                id = self._seed.id

        elif isinstance(self._seed, str):
            id = self._seed

        elif isinstance(self._seed, PathMeta):
            id = self._seed.id

        else:
            raise TypeError(self._seed)

        return BlackfynnId(id)

    @property
    def doi(self):
        try:
            blob = self.bfobject.doi
            #print(blob)
            if blob:
                return idlib.Doi(blob['doi'])
        except exc.NoRemoteFileWithThatIdError as e:
            log.exception(e)
            if self.cache is not None and self.cache.exists():
                self.cache.crumple()

    @property
    def size(self):
        if isinstance(self.bfobject, File):
            return self.bfobject.size

    @property
    def created(self):
        if not isinstance(self.bfobject, Organization):
            return self.bfobject.created_at

    @property
    def updated(self):
        if not isinstance(self.bfobject, Organization):
            return self.bfobject.updated_at

    @property
    def file_id(self):
        if isinstance(self.bfobject, File):
            return self.bfobject.id

    @property
    def old_id(self):
        return None

    def exists(self):
        try:
            bfo = self.bfobject
            if not isinstance(bfo, BaseNode):
                _cache = bfo.refresh(force=True)
                bf = _cache.remote.bfo

            return bfo.exists
        except exc.NoRemoteFileWithThatIdError as e:
            return False

    def is_dir(self):
        try:
            bfo = self.bfobject
            return not isinstance(bfo, File) and not isinstance(bfo, DataPackage)
        except Exception as e:
            breakpoint()
            raise e

    def is_file(self):
        bfo = self.bfobject
        return (isinstance(bfo, File) or
                isinstance(bfo, DataPackage) and
                (hasattr(bfo, 'fake_files') and bfo.fake_files
                    or
                 not hasattr(bfo, '_json') and
                 not (not log.warning('going to network for files') and self._has_remote_files())
                 # massively inefficient but probably shouldn't get here?
                ))

    def _has_remote_files(self):
        """ this will fetch """
        bfobject = self.bfobject
        if not isinstance(bfobject, DataPackage):
            return False

        files = bfobject.files
        if not files:
            return False

        if len(files) > 1:
            log.critical(f'{self} has more than one file! Not switching bfobject!')
            return True

        file, = files
        file.parent = bfobject.parent
        file.dataset = bfobject.dataset
        file.package = bfobject
        self._bfobject = file
        return True

    @property
    def checksum(self):  # FIXME using a property is inconsistent with LocalPath
        if hasattr(self.bfobject, 'checksum'):
            checksum = self.bfobject.checksum
            if checksum and '-' not in checksum:
                return bytes.fromhex(checksum)

    @property
    def etag(self):
        """ NOTE returns checksum, count since it is an etag"""
        # FIXME rename to etag in the event that we get proper checksumming ??
        if hasattr(self.bfobject, 'checksum'):
            checksum = self.bfobject.checksum
            if checksum and '-' in checksum:
                log.debug(checksum)
                if isinstance(checksum, str):
                    checksum, strcount = checksum.rsplit('-', 1)
                    count = int(strcount)
                    #if checksum[-2] == '-':  # these are 34 long, i assume the -1 is a check byte?
                        #return bytes.fromhex(checksum[:-2])
                    return bytes.fromhex(checksum), count

    @property
    def chunksize(self):
        if hasattr(self.bfobject, 'chunksize'):
            return self.bfobject.chunksize

    @property
    def owner_id(self):
        if not isinstance(self.bfobject, Organization):
            # This seems like an oversight ...
            return self.bfobject.owner_id

    @property
    def parent(self):
        if hasattr(self, '_c_parent'):
            # WARNING staleness could occure because of this
            # wow does it save on the network roundtrips
            # for rchildren though
            return self._c_parent

        if isinstance(self.bfobject, Organization):
            return self  # match behavior of Path

        elif isinstance(self.bfobject, Dataset):
            return self.organization
            #parent = self.bfobject._api._context

        else:
            parent = self.bfobject.parent
            if parent is None:
                parent = self.bfobject.dataset

        if False and isinstance(parent, str):
            if parent in self.helper_index:
                return self.helper_index[parent]
            else:
                raise TypeError('grrrrrrrrrrrrrrr')

        if parent:
            parent_cache = self.cache.parent if self.cache is not None else None
            self._c_parent = self.__class__(parent, cache=parent_cache)
            return self._c_parent

    @property
    def children(self):
        yield from self._children()

    def _children(self, create_cache=True):
        if isinstance(self.bfobject, File):
            return
        elif isinstance(self.bfobject, DataPackage):
            return  # we conflate data packages and files
        elif isinstance(self.bfobject, Organization):
            for dataset in self.bfobject.datasets:
                child = self.__class__(dataset)
                if create_cache:
                    self.cache / child  # construction will cause registration without needing to assign
                    assert child.cache

                yield child

        else:
            for bfobject in self.bfobject:
                child = self.__class__(bfobject)
                if create_cache:
                    self.cache / child  # construction will cause registration without needing to assign
                    assert child.cache

                yield child

    @property
    def rchildren(self):
        yield from self._rchildren()

    def _dir_or_file(self, child, deleted, exclude_uploaded):
        state = child.bfobject.state
        if state != 'READY':
            log.debug (f'{state} {child.name} {child.id}')
            if state == 'DELETING' or state == 'PARENT-DELETING':
                deleted.append(child)
                return
            if exclude_uploaded and state == 'UPLOADED':
                return

        if child.is_file():
            cid = child.id
            existing = [c for c in self.cache.local.children
                        if (c.is_file() and c.cache or c.is_broken_symlink())
                        and c.cache.id == cid]
            if existing:
                unmatched = [e for e in existing if child.name != e.name]
                if unmatched:
                    log.debug(f'skipping {child.name} becuase a file with that '
                                f'id already exists {unmatched}')
                    return

        return True

    def _rchildren(self,
                   create_cache=True,
                   exclude_uploaded=True,
                   sparse=False,):
        if isinstance(self.bfobject, File):
            return
        elif isinstance(self.bfobject, DataPackage):
            return  # should we return files inside packages? are they 1:1?
        elif any(isinstance(self.bfobject, t) for t in (Organization, Collection)):
            for child in self._children(create_cache=create_cache):
                yield child
                yield from child._rchildren(create_cache=create_cache)
        elif isinstance(self.bfobject, Dataset):
            deleted = []
            if sparse:
                filenames = self._sparse_stems
                sbfo = self.bfobject
                _parents_yielded = set()
                _int_id_map = {}
                for bfobject in self.bfobject.packagesByName(filenames=filenames):
                    child = self.__class__(bfobject)
                    if child.is_dir() or child.is_file():
                        if not self._dir_or_file(child, deleted, exclude_uploaded):
                            continue
                    else:
                        # probably a package that has files
                        #log.debug(f'skipping {child} becuase it is neither a directory nor a file')
                        continue

                    parent = child
                    parents = []
                    while True:
                        log.debug(parent)
                        parent_int_id = None
                        if (parent.from_packages and
                            'parentId' in parent.bfobject.package._json['content']):
                            # FIXME HACK
                            # FIXME incredibly slow, but still faster than non-sparse
                            parent_int_id = parent.bfobject.package._json['content']['parentId']
                            if parent_int_id not in _int_id_map:
                                parent = self.__class__(parent.id)
                                parent.bfobject
                                _int_id_map[parent_int_id] = parent.parent

                        if not parents:  # add the child as the last parent
                            parents.append(parent)

                        if parent.parent_id in (sbfo, self.id):
                            break  # child yielded below
                        else:
                            if parent_int_id is not None:
                                parent = _int_id_map[parent_int_id]
                            else:
                                parent = parent.parent
                            # TODO create cache
                            if parent.id not in _parents_yielded:
                                _parents_yielded.add(parent.id)
                                # actually yield below in the reverse order
                                parents.append(parent)
                                #yield parent
                            else:
                                break

                    if create_cache:
                        pcache = self.cache
                        for parent in reversed(parents):
                            yield parent
                            pcache = pcache / parent
                    else:
                        yield from reversed(parents)

                return

            for bfobject in self.bfobject.packages:
                child = self.__class__(bfobject)
                if child.is_dir() or child.is_file():
                    if not self._dir_or_file(child, deleted, exclude_uploaded):
                        continue

                    if create_cache:
                        # FIXME I don't think existing detection is working
                        # correctly here so this get's triggered incorrectly?
                        self.cache / child  # construction will cause registration without needing to assign
                        assert child.cache is not None

                    yield child
                else:
                    # probably a package that has files
                    log.debug(f'skipping {child} becuase it is neither a directory nor a file')

            else:  # for loop else
                self._deleted = deleted

        else:
            raise exc.UnhandledTypeError  # TODO

    def children_pull(self, existing_caches=tuple(), only=tuple(), skip=tuple(), sparse=tuple()):
        """ ONLY USE FOR organization level """
        # FIXME this is really a recursive pull for organization level only ...
        sname = lambda gen: sorted(gen, key=lambda c: c.name)
        def refresh(c):
            updated = c.meta.updated
            newc = c.refresh()
            if newc is None:
                return

            nupdated = newc.meta.updated
            if nupdated != updated:
                return newc

        existing = sname(existing_caches)
        if not self._debug:
            skipexisting = {e.id:e for e in
                            Async(rate=self._async_rate)(deferred(refresh)(e) for e in existing)
                            if e is not None}
        else:  # debug ...
            skipexisting = {e.id:e for e in
                            (refresh(e) for e in existing)
                            if e is not None}

        # FIXME
        # in theory the remote could change betwee these two loops
        # since we currently cannot do a single atomic pull for
        # a set of remotes and have them refresh existing files
        # in one shot

        if not self._debug:
            yield from (rc for d in Async(rate=self._async_rate)(
                deferred(child.bootstrap)(recursive=True, only=only, skip=skip, sparse=sparse)
                for child in sname(self.children)
                #if child.id in skipexisting
                # TODO when dataset's have a 'anything in me updated'
                # field then we can use that to skip things that haven't
                # changed (hello git ...)
                ) for rc in d)
        else:  # debug
             yield from (rc for d in (
                child.bootstrap(recursive=True, only=only, skip=skip, sparse=sparse)
                for child in sname(self.children))
                #if child.id in skipexisting
                # TODO when dataset's have a 'anything in me updated'
                # field then we can use that to skip things that haven't
                # changed (hello git ...)
                for rc in d)

    def isinstance_bf(self, *types):
        return [t for t in types if isinstance(self.bfobject, t)]

    def refresh(self, update_cache=False, update_data=False,
                update_data_on_cache=False, size_limit_mb=2, force=False):
        """ use force if you have a file from packages """
        try:
            old_meta = self.meta
        except exc.NoMetadataRetrievedError as e:
            log.error(f'{e}\nYou will need to individually refresh {self.local}')
            return
        except exc.NoRemoteFileWithThatIdError as e:
            log.exception(e)
            return

        if self.is_file() and not force:  # this will tigger a fetch
            pass
        else:
            try:
                self._bfobject = self._api.get(self.id)
            except exc.NoRemoteFileWithThatIdError as e:
                log.exception(e)
                return

            self.is_file()  # trigger fetching file in the no file_id case

        if update_cache or update_data:
            file_is_different = self.update_cache()
            update_existing = file_is_different and self.cache.exists()
            udoc = update_data_on_cache and file_is_different
            if update_existing or udoc:
                size_limit_mb = None

            update_data = update_data or update_existing or udoc

        if update_data and self.is_file():
            self.cache.fetch(size_limit_mb=size_limit_mb)

        return self.cache  # when a cache calls refresh it needs to know if it no longer exists

    @property
    def parent_id(self):
        # work around inhomogenous
        if self == self.organization:
            return self.id  # this behavior is consistent with how Path.parent works
        elif not hasattr(self._bfobject, 'dataset'):
            return self.organization.id
        else:
            pid = getattr(self._bfobject, 'parent')
            if pid is None:
                return self._bfobject.dataset

    def _on_cache_move_error(self, error, cache):
        if self.bfobject.package.name != self.bfobject.name:
            argh = self.bfobject.name
            self.bfobject.name = self.bfobject.package.name
            try:
                log.critical(f'Non unique filename :( '
                                f'{cache.name} -> {argh} -> {self.bfobject.name}')
                cache.move(remote=self)
            finally:
                self.bfobject.name = argh
        else:
            raise error

    @property
    def _single_file(self):
        if isinstance(self.bfobject, DataPackage):
            files = list(self.bfobject.files)
            if len(files) > 1:
                raise BaseException('TODO too many files')

            file = files[0]
        elif isinstance(self.bfobject, File):
            file = self.bfobject
        else:
            file = None

        return file

    @property
    def _uri_file(self):
        file = self._single_file
        if file is not None:
            return file.url

    @property
    def data(self):
        uri_file = self._uri_file
        if uri_file is None:
            return

        gen = self.get_file_by_url(uri_file)
        try:
            self.data_headers = next(gen)
        except exc.NoRemoteFileWithThatIdError as e:
            raise FileNotFoundError(f'{self}') from e

        yield from gen

    @data.setter
    def data(self):
        if hasattr(self, '_seed'):
            # upload the new file
            # delete the old file
            # or move to .trash  self._api.bf.move(target, self.id)
            # where target is the bfobject for .trash
            raise NotImplementedError('TODO')
        else:  # doesn't exist yet
            # see https://github.com/HumanCellAtlas/dcp-cli/pull/252
            # for many useful references
            raise NotImplementedError('TODO')

    def _lchildmeta(self, child):
        # FIXME all of this should be accessible from local and/or cache directly ...
        lchild = self.cache.local / child.name  # TODO LocalPath.__truediv__ ?
        excache = None
        lchecksum = None
        echecksum = None
        modified = False
        if lchild.exists() and False:  # TODO XXX
            lmeta = lchild.meta
            lchecksum = lmeta.checksum
            excache = lchild.cache
            if excache is None:
                lmeta = lchild.meta
                lmeta.id = None
                echecksum = lmeta.checksum
            else:
                echecksum = excache.checksum

            if lchecksum != echecksum:
                log.debug(f'file has been modified {lchild}')
                modified = True
        return lchild, excache, lmeta, lchecksum, echecksum, modified

    def _lchild(self, child):
        l = self.cache.local
        if l is None:
            l = self.cache.parent.local / self.name
        
        return l / child.name

    def __truediv__(self, other):  # XXX
        """ this is probably the we want to use for this at all
            it is kept around as a reminder NOT to do this
            however might revisit this at some point if we want
            to explore translating remote semantics to file system
            on the RemotePath class ... """

        # probably better to work from the cache class
        # since it is the one that knows that the file doesn't
        # exist at the remote and can provide a way to move data
        # to the remote using copy_to or something like that
        children = list(self._children(create_cache=False))
        names = {}
        for c in children:
            if c.name not in names:
                names[c.name] = []

            names[c.name].append(c)

        opath = PurePath(other)
        if len(opath.parts) > 1:
            # FIXME ... handle/paths/like/this
            raise NotImplementedError('TODO')
        else:
            if other in names:
                childs = names[other]
                if len(childs) > 1:
                    op = PurePath(other)
                    # TODO I think it might make sense for this to come first?
                    # of course the remote checksums aren't actually implemented
                    # unless it is at the dataset package level (sigh)

                    #if lchecksum is not None and False:  # TODO XXX
                        #matches = [c for c in childs if c.checksum == lchecksum]

                    package_names = {c._bfobject.package.name
                                     if c.is_file() else
                                     c.name
                                     :c for c in childs}

                    if op.stem in package_names:
                        child = package_names[op.stem]
                    else:
                        # there are 3 possible values that we could return here
                        # 1. None
                        # 2. self._temp_child()
                        # 3. the oldest/newest/arbitrary child
                        # returning the newest child seems to be the right
                        # behavior because it is possible to check at a later
                        # time (e.g. in _stream_from_local) whether there is a
                        # package id conflict or not

                        # FIXME test vs updated or vs created?
                        oldest_first = sorted(childs, key=lambda c: c.updated)
                        child = oldest_first[-1]
                else:
                    child = childs[0]

                self.cache / child  # THIS SIDE EFFECTS TO UPDATE THE CHILD CACHE
                return child

            else:  # create an empty
                return self._temp_child(other)

    def _temp_child(self, child_name):
        """ construct child with a fake placeholder bfobject """
        # FIXME hack around instantiation-existence issue
        child = object.__new__(self.__class__)
        child._parent = self
        class TempBFObject(BaseNode):  # this will cause a type error if actually used
            name = child_name
            exists = False

        if self != self.organization:
            if self.id.startswith('N:dataset:'):  # FIXME sigh
                TempBFObject.dataset = self.id
            else:
                TempBFObject.dataset = self._bfobject.dataset
                TempBFObject.parent = self.id

        tbfo = TempBFObject()
        child._bfobject = tbfo
        child._seed = tbfo
        return child

    def _mkdir_child(self, child_name):
        """ direct children only for this, call in recursion for multi """
        if self.is_organization():
            bfobject = self._api.bf.create_dataset(child_name)
        elif self.is_dir():  # all other possible dirs are already handled
            # sigh it would be nice if creation would just fail
            # if a folder with that name already existed :/
            maybe_child = self / child_name
            if maybe_child.exists():
                log.warning(f'folder with name {child_name} already exists '
                            'one level of its children have been included')
                list(maybe_child.children)  # force a single level of instantiation
                return maybe_child

            bfobject = self.bfobject.create_collection(child_name)
        else:
            raise exc.NotADirectoryError(f'{self}')

        return self.__class__(bfobject)

    def mkdir(self, parents=False):  # XXX
        # note that under the current implementation it is impossible for self to exist
        # and not have a _seed
        # same issue as with __rtruediv__
        if hasattr(self, '_seed'):
            raise exc.PathExistsError(f'remote already exists {self}')

        bfobject = self._parent._mkdir_child(self.name)
        self._seed = bfobject
        self._bfobject = bfobject

    def rmdir(self):
        if self.is_organization():
            raise exc.SparCurError("can't remove organizations right now")

        elif self.is_dataset():
            if list(self.children):  # FIXME super inefficient ...
                raise exc.PathNotEmptyError(self)

            self.bfobject.delete()
        elif self.is_dir():
            if list(self.children):  # FIXME super inefficient ...
                raise exc.PathNotEmptyError(self)

            self.bfobject.delete()
        else:
            raise exc.NotADirectoryError(f'{self}')

    @classmethod
    def _prepare_package_for(cls, local_file, mkdir=False):
        # TODO maybe ???
        raise NotImplementedError('I think doing it this way requires uploading to s3 first ...')
        if not isinstance(local_file, cls._local_class):
            raise TypeError(f'{local_file} {type(local_file)} is not a {cls._local_class}')

        local_file.name
        local_file.parent.remote  # FIXME recursively mkdir ???
        bfobject = cls._api.create_package(local_file)
        remote = cls(bfobject)
        return remote.cache

    @classmethod
    def _stream_from_local(cls, local_path, replace=True, local_backup=False):
        # FIXME touch_child -> stream data ??? as a bridge to sanity?
        # /packages/ endpoint should allow us to create a new empty package
        self = local_path.parent.remote

        if type(self) != cls:
            raise TypeError(f'{type(self)} != {cls}')

        # FIXME BAD forces us to read file twice
        checksum = local_path.checksum()

        try:
            old_remote = self / local_path.name
        except NotImplementedError as e:
            # well now this isn't failing ...
            #breakpoint()
            raise e

        rchecksum = None
        if old_remote is not None:
            rchecksum = old_remote.checksum
            if rchecksum is None and old_remote.cache is not None:
                # has side effect of caching the object on the off chance that
                # a local copy has not already been stashed (e.g. if we didn't
                # upload it using this function)
                rchecksum = old_remote.cache.checksum()

        if checksum == rchecksum:
            # TODO check to make sure nothing else has changed ???
            # also how does this interact with replace = true?
            raise exc.FileHasNotChangedError('no changes have been made, not uploading')

        manifests = self.bfobject.upload(local_path, use_agent=False)
        blob = manifests[0][0]  # there is other stuff in here but ignore for now
        id = blob['package']['content']['nodeId']
        remote = cls(id)
        if False:  # the web api endpoints are old and busted and don't ensure checksum generation
            for i in range(100):
                # sometimes you just need a blocking call ...
                # FIXME looks like there is a websocket connection that
                # will notify when a package _actually_ finished uploading ...
                if remote.state == 'READY':
                    break
                elif remote.state == 'UPLOADED':
                    try:
                        remote.bfobject.package.process()  # processing required to get a checksum
                    except BaseException as e:
                        log.exception(e)
                        break
                else:
                    # even if the remote state says uploaded we get 400 error bad url ...
                    log.debug(remote.state)
                    remote = cls(id)
            else:
                raise BaseException('too many retires when uploading {remote}')

            for i in range(100):
                if remote.state == 'READY':
                    log.debug(f'finally ready after {i + 1}')
                    with_checksum_maybe = [  # FIXME sigh ...
                        p for p in remote.dataset.bfobject._packages(filename=remote.stem)
                        if p.id == remote.id or hasattr(p, 'pkg_id') and p.pkg_id == remote.id]

                    with_checksum_maybe = with_checksum_maybe[-1] if with_checksum_maybe else None
                    if with_checksum_maybe.checksum:
                        remote.bfobject.checksum = with_checksum_maybe.checksum
                        breakpoint()

                    break
                else:
                    log.debug(remote.state)
                    #sleep(1)
                    remote = cls(id)

        if remote.meta.checksum is None:
            if hasattr(remote.bfobject, 'checksum'):
                raise BaseException('what is going on here!?')

            # FIXME EVIL the local path checksum could have changed
            # because we do not lock
            # the real fix is to make sure we are telling the blackfynn remote
            # the right things when we upload so that the packages endpoint will
            # generate the hashes for us ... the fact that it is possible to somehow
            # not generate the hashes is not good ...

            log.warning('FIX THIS NONSENSE')
            remote.bfobject.checksum = checksum.hex()  # FIXME HACK HACK HACK
            # this hack avoids us going to retrieve the remote after it
            # has been deleted since the upload code is broken at the moment
            # it is COMPLETELY BROKEN there are ZERO gurantees about remote
            # data integrity

        try:
            # do a little dance to update the name back to what it is supposed to be
            stem_diff = local_path.stem != remote.bfobject.package.name
            if replace and stem_diff:
                if old_remote.exists():  # FIXME nasty performance cost here ...
                    # oh man the concurrency story for multiple people adding files with the same name
                    # wow ... in restrospect this comment was not nearly cynical enough

                    # FIXME if checksum does not exist compute it before delete
                    # this is an issue because you usually can only get the checksum
                    # from the packages endpoint not for an individual file (sigh)
                    old_remote.bfobject.package.delete()  # FIXME this is terrifying ...
                    # though not as terrifying as running it before the upload
                    # of course now there is the renaming issue I'm sure ...

                    # LOL OH NO its a post https://developer.blackfynn.io/api/#/Data/deleteItems
                    assert not old_remote.bfobject.package.exists, 'delete failed?'

                    # FIXME OH NO concurrent package.delete is non-blocking !

                    # the fact that this can fail tells me that there is no
                    # synchronization on the blackfynn backend AT ALL
                    # operations seem to be executed as they arrive without
                    # any notion of consistency or ordering WAT
                    remote.bfobject.package.name = remote.bfobject.name
                    for i in range(100):
                        # I love spinlocks for concurency don't you?
                        try:
                            remote.bfobject.package.update()
                            break
                        except requests.exceptions.HTTPError as e:
                            if e.response.text != 'package name is already taken':  # ugh
                                raise e
                            elif i == 99:
                                raise BaseException('LOL') from e

            elif not stem_diff and old_remote.exists():
                log.critical(f'YOU MAY HAVE FUNKY DATA IN {local_path.cache.parent.uri_human}')

        except BaseException as e:
            log.exception(e)

        return remote, old_remote

    @property
    def meta(self):
        return PathMeta(size=self.size,
                        created=self.created,
                        updated=self.updated,
                        checksum=self.checksum,
                        etag=self.etag,
                        chunksize=self.chunksize,
                        id=self.id,
                        file_id=self.file_id,
                        old_id=None,
                        gid=None,  # needed to determine local writability
                        user_id=self.owner_id,
                        mode=None,
                        errors=self.errors)

    def __eq__(self, other):
        return self.id == other.id and self.file_id == other.file_id
        #return self.bfobject == other.bfobject

    def __hash__(self):
        return hash((self.__class__, self.id))

    def __repr__(self):
        file_id = f', file_id={self.file_id}' if self.file_id else ''
        return f'{self.__class__.__name__}({self.id!r}{file_id})'
