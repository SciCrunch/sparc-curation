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
            return parser.parse(value.decode())  # FIXME with timezone vs without ...

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
            raise FileNotFoundError(f'{self}') from e  # have to raise so that we don't overwrite the file

        log.debug(self.data_headers)
        if self.local_object_cache_path.exists():
            yield from gen
        else:
            yield from self.local_object_cache_path._data_setter(gen)
            self.local_object_cache_path.cache_init(self.meta)  # FIXME self.meta be stale here?!

    def _bootstrap_recursive(self, only=tuple(), skip=tuple()):
        # bootstrap the rest if we told it to
        if self.id.startswith('N:organization:'):  # FIXME :/
            yield from self.remote.children_pull(self.children, only=only, skip=skip)

        else:
            yield from super()._bootstrap_recursive()


BlackfynnCache._bind_flavours()


class Path(aug.XopenPath, aug.RepoPath, aug.LocalPath):  # NOTE this is a hack to keep everything consistent
    """ An augmented path for all the various local
        needs of the curation process. """

    _cache_class = BlackfynnCache

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
