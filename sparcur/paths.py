import hashlib
import pathlib
from functools import wraps
from itertools import chain
try:
    import magic  # from sys-apps/file consider python-magic ?
except ImportError:
    pass

import augpathlib as aug
from dateutil import parser
from augpathlib import PrimaryCache, EatCache, SqliteCache, SymlinkCache
from augpathlib import RepoPath, LocalPath
from augpathlib import RemotePath  # FIXME just for reimport
from sparcur import backends
from sparcur import exceptions as exc
from sparcur.utils import log
from augpathlib import PathMeta


def cleanup(func):
    @wraps(func)
    def inner(self):
        meta = func(self)
        if meta is not None:
            if meta.id.startswith('EUf5'):
                if self.is_broken_symlink():
                    self.unlink()
                elif self.exists() and not self.is_symlink():
                    self.unlink()
                    self.touch()
                else:
                    pass

            else:
                return meta
            
    return inner


class BlackfynnCache(PrimaryCache, EatCache):
    xattr_prefix = 'bf'
    _backup_cache = SqliteCache
    _not_exists_cache = SymlinkCache
    cypher = hashlib.sha256  # set the remote hash cypher on the cache class

    uri_human = backends.BlackfynnRemote.uri_human
    uri_api = backends.BlackfynnRemote.uri_api

    @classmethod
    def decode_value(cls, field, value):
        if field in ('created', 'updated'):
            # if you get unicode decode error here it is because
            # struct packing of timestamp vs isoformat are fighting
            # in xattrs pathmeta helper
            return value.decode()  # path meta handles decoding for us

    @property
    def anchor(self):
        return self.organization

    @property
    def trash(self):
        return self.local_data_dir / 'trash'

    @property
    def organization(self):
        """ organization represents a permissioning boundary
            for blackfynn, so above this we would have to know
            in advance the id and have api keys for it and the
            containing folder would have some non-blackfynn notes
            also it seems likely that the same data could appear in
            multiple different orgs, so that would mean linking locally
        
            NOTE: unlike RemotePath, CachePath should use the local file
            structure to search for the local anchor.
        """

        # FIXME in reality files can have more than one org ...
        if self.meta is not None:
            id = self.id
            if id is None:
                parent = self.parent
                if parent == self:  # we have hit a root
                    return None

                organization = parent.organization

                if organization is not None:
                    # TODO repair
                    pass
                else:
                    raise exc.NotInProjectError

            elif id.startswith('N:organization:'):
                return self

        if self.parent and self.parent != self:
            # we have a case of missing metadata here as well
            return self.parent.organization

    def is_organization(self):  # FIXME ?
        return self.id and self.id.startswith('N:organization:')

    def is_dataset(self):
        return self.id and self.id.startswith('N:dataset:')

    @property
    def dataset(self):
        if self.is_dataset():
            return self

        elif self.parent and self.parent != self:  # Path('.') issue
            log.warning(self.parent)
            return self.parent.dataset

    @property
    def dataset_id(self):
        dataset = self.dataset
        if dataset:
            return dataset.id

    @property
    def file_id(self):
        return self.meta.file_id

    @property
    def cache_key(self):
        return f'{self.id}-{self.file_id}'

    @property
    def data(self):
        """ get the 'cached' data which isn't really cached at the moment
            once we implement an index for local files then we can hit that
            first from here """
        # we don't keep two copies of the local data
        # unless we are doing a git-like thing
        if self.is_dir():
            raise TypeError('can\'t retrieve data for a directory')

        meta = self.meta
        if meta.file_id is None:
            raise NotImplementedError('can\'t fetch data without a file id')

        #cands = list(self.local_object_cache_dir.glob(self.cache_key))
        # FIXME this does not play well with old_id ...
        # can probably get away with just globing for the old_id in
        # most cases
        # TODO where to store the chain of prior versions? i.e. do
        # we just keep the xattrs in the object cache? how about file moves?
        # sigh git ...
        if self.local_object_cache_path.exists():
            gen = chain((f'from local cache {self.local_object_cache_path}',),
                        self.local_object_cache_path.data)
        else:
            gen = self._remote_class.get_file_by_id(meta.id, meta.file_id)

        try:
            self.data_headers = next(gen)
        except exc.NoRemoteFileWithThatIdError as e:
            log.error(f'{self} {e}')
            raise exc.CacheNotFoundError(f'{self}') from e  # have to raise so that we don't overwrite the file

        log.debug(self.data_headers)
        if self.local_object_cache_path.exists():
            yield from gen
        else:
            yield from self.local_object_cache_path._data_setter(gen)
            self.local_object_cache_path.cache_init(self.meta)  # FIXME self.meta be stale here?!

    def _bootstrap_recursive(self, only=tuple(), skip=tuple(), sparse=False):
        """ only on the first call to this function should sparse be a tuple """
        # bootstrap the rest if we told it to
        if self.id.startswith('N:organization:'):  # FIXME :/
            yield from self.remote.children_pull(self.children,
                                                 only=only,
                                                 skip=skip,
                                                 sparse=sparse,)

        else:
            if isinstance(sparse, list) and self.id.startswith('N:dataset:'):
                sparse = self.id in sparse

            yield from super()._bootstrap_recursive(sparse=sparse)

    _sparse_stems = (
        'manifest', 'dataset_description', 'submission', 'subjects', 'samples'
    )

    def _sparse_include(self):
        sl = self.stem.lower()
        return bool([an for an in self._sparse_stems if an in sl])


BlackfynnCache._bind_flavours()


class Path(aug.XopenPath, aug.RepoPath, aug.LocalPath):  # NOTE this is a hack to keep everything consistent
    """ An augmented path for all the various local
        needs of the curation process. """

    _cache_class = BlackfynnCache

    @property
    def cache_id(self):
        """ fast way to get cache.id terrifyingly this has roughly
        an order of magnitude less overhead than self.cache.id """

        try:
            return (self.getxattr('bf.id').decode()
                    if self.is_dir() or self.is_file() else
                    self.readlink().parts[1])
        except OSError as e:
            raise exc.NoCachedMetadataError(self) from e

    def upload(self, replace=True, local_backup=False):
        # FIXME we really need a staging area ...
        remote, old_remote = (
            self
            ._cache_class
            ._remote_class
            ._stream_from_local(self,
                                replace=True,
                                local_backup=False))

        if True:  # local_backup = True:  # force True until we can get remote checksums
            # FIXME there must be a better way to do this ...
            object_path = self._cache_class._anchor.local_objects_dir / remote.cache_key
            # TODO possibly use _data_setter to calculate the hash at the same time?
            object_path.copy_from(self)
            object_path.cache_init(remote.meta)

        if self.cache is None:
            # FIXME didn't we already figure out the right way to do this?
            cache = self.cache_init(remote.meta)
            self._cache = cache
            cache._remote = remote
            remote._cache = cache
        else:
            # remote has no cache and self already has a cache in this case
            # so we can't just assign directly so we have to say which
            # cache we want to update, and make sure that names match
            # the cache should already have been updated EXCEPT for the
            # fact that the remote fileid is not known, but we are inside
            # the code that actually makes that update ...
            # and the absense of a staging area / history makes the cache.meta
            # the only place we can do this besides the object store, especially
            # if we lack the ability to retrieve checksums for single files (sigh)

            # we already have the latest data (ignoring concurency)
            remote.update_cache(cache=self.cache, fetch=False) # FIXME fetch=False => different diff rule

        return remote


Path._bind_flavours()


class StashCache(BlackfynnCache):
    def remote(self):
        return None

    def _meta_setter(self, value):
        if not self.meta:
            super()._meta_setter(value)
        else:
            raise TypeError('you dont want to set a stashed cache')


class StashPath(Path):
    _cache_class = StashCache
    @property
    def remote(self):
        return None


# assign defaults

BlackfynnCache._local_class = Path
backends.BlackfynnRemote._new(Path, BlackfynnCache)
backends.BlackfynnRemote.cache_key = BlackfynnCache.cache_key
backends.BlackfynnRemote._sparse_stems = BlackfynnCache._sparse_stems
backends.BlackfynnRemote._sparse_include = BlackfynnCache._sparse_include
