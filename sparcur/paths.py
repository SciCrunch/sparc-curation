import logging
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
from sparcur import backends
from sparcur import exceptions as exc
from sparcur.utils import log


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
    def _id_parts(self):
        ntid = self.id
        if ntid is None:
            raise exc.NoCachedMetadataError(self)

        N, thing, id = ntid.split(':')
        return N, thing, id

    @property
    def _id_uuid(self):
        N, thing, id = self._id_parts
        return id  # TODO inverse function

    @property
    def _fs_safe_id(self):
        N, thing, id = self._id_parts
        return thing[0] + '-' + id

    @property
    def _trashed_path(self):
        sid = self._fs_safe_id
        suuid = self._id_uuid
        try:
            pid = self.parent._fs_safe_id
        except exc.NoCachedMetadataError as e:
            msg = f'Projects cannnot be trashed when trying to trash {self}'
            raise TypeError(msg) from e

        fid = f'{self.file_id}-' if self.file_id else ''
        # we use sid as the sparsification folder here to ensure
        # a more uniform distribution of deletions across folders
        # therefore if you need to find files deleted from a specific folder
        # use find -name '*collection-id-part*'
        return self.trash  / suuid[:2] / f'{sid}-{pid}-{fid}{self.name}'

    @property
    def _trashed_path_short(self):
        return self.trash / 'short' / self.name

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
            log.debug(self.parent)
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

    def _dataset_metadata(self, force_cache=False):
        """ get metadata about a dataset from the remote metadata store """
        # TODO figure out the right default for force_cache
        dataset = self.dataset
        if dataset == self:
            if not hasattr(self, '_c__dataset_metadata'):
                bdd = BlackfynnDatasetData(self)
                try:
                    blob = bdd.fromCache()
                except FileNotFoundError as e:
                    # FIXME TODO also check cached rmeta dates during pull
                    if force_cache:
                        raise e
                    else:
                        log.warning(e)
                        blob = bdd()

                self._c__dataset_metadata = blob

            return self._c__dataset_metadata

        else:
            return dataset._dataset_metadata()

    def _package_count(self):
        if self.is_dataset():
            return sum(self._dataset_metadata()['package_counts'].values())
        else:
            raise NotImplementedError('unused at the moment')

    def _sparse_materialize(self, *args, sparse_limit=None):
        """ use data from the remote mark or clear datasets as sparse """
        if sparse_limit is None:
            sparse_limit = auth.get('sparse-limit')  # yay for yaml having int type
            #sparse_limit = _sl if _sl is None else int_(sl)

        if self.is_dataset():
            package_count = self._package_count()
            sparse_remote = (False
                             if sparse_limit is None else
                             package_count >= sparse_limit)
            sparse_cache = self.is_sparse()
            if sparse_remote:
                if not sparse_cache:
                    self._mark_sparse()
            elif sparse_cache:  # strange case where number of packages decreases
                self._clear_sparse()

        else:
            msg = 'at the moment only datasets can be marked as sparse'
            raise NotImplementedError(msg)

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

    def _meta_is_root(self, meta):
        return meta.id.startswith('N:organization:')

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

    def populateJsonMetadata(self, blob):
        """ populate a json blob with file system metadata"""
        # TODO schema for this
        if blob is None:
            blob = {}

        blob.update(self._jsonMetadata())

    def _jsonMetadata(self, do_expensive_operations=False):
        """ path level json metadata """
        # FIXME this is going to be very slow due to the local.cache call overhead
        # it will be much better implement this from Path directly using xattrs()
        drp = self.local.relative_to(self.dataset.local)
        meta = self.meta  # TODO see what we need from this
        N, package = self.id.split(':', 1)
        uri_api = self.uri_api  # FIXME vs self.uri_api_package ?
        uri_human = self.uri_human,
        blob = {
            'type': 'path',
            'uri_api': uri_api,  # -> @id in the @context
            'uri_human': uri_human,
            'package_id': package,  # FIXME N:package:asdf is nasty for jsonld but ...
            'dataset_relative_path': drp,
        }

        mimetype = self.mimetype
        if mimetype:
            blob['mimetype'] = mimetype

        if do_expensive_operations:
            blob['magic_mimetype'] = asdf

        return blob


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

    def populateJsonMetadata(self, blob):
        """ populate a json blob with file system metadata"""
        # FIXME TODO implement this without resolving .cache
        return self.cache.populateJsonMetadata(blob)

    def pull(self, *args,
             time_now=None,
             debug=False,
             n_jobs=12,
             cache_anchor=None,
             log_name=None,
             log_level='INFO',
             # pass the paralle in at run time if needed
             Parallel=None,
             delayed=None,
             _in_parallel=False,):
        # TODO usage errors

        if _in_parallel:
            _log = logging.getLogger(log_name)
            _log.setLevel(log_level)
            rc = self._remote_class
            if not hasattr(rc, '_cache_anchor'):
                rc.anchorTo(cache_anchor)

        else:
            _log = log

        cache = self.cache

        if cache.is_organization():
            if debug or Parallel is None:
                for child in self.children:
                    child.pull()
            else:
                Parallel(n_jobs=n_jobs)(
                    delayed(child.pull)(_in_parallel=True,
                                        time_now=time_now,
                                        cache_anchor=cache.anchor,
                                        log_name=_log.name,
                                        log_level=log_level,)
                    for child in self.children)

        elif cache.is_dataset():
            self._pull_dataset(time_now)  # XXX actual pull happens in here

        else:
            raise NotImplementedError(self)

    def _pull_dataset(self, time_now):
        cache = self.cache
        if cache.is_organization():
            raise TypeError('can\' use this method on organizations')
        elif not cache.is_dataset():
            return self.dataset._pull_dataset()

        children = list(self.children)
        if not children:
            return list(self.cache.rchildren)

        # instantiate a temporary staging area for pull
        ldd = cache.local_data_dir
        contain_upstream = ldd / 'temp-upstream'
        contain_upstream.mkdir(exist_ok=True)

        # create the new directory with the xattrs needed to pull
        working = self
        upstream = contain_upstream / working.name
        upstream.mkdir()
        upstream.setxattrs(working.xattrs())  # handy way to copy xattrs

        # FIXME TODO error handling

        # pull and prep for comparison of relative paths
        upstream_c_children = list(upstream.cache.rchildren)  # XXX actual pull happens here
        upstream_children = [c.local.relative_to(upstream)
                            for c in upstream_c_children]
        working_children = [c.relative_to(working)
                            for c in working.rchildren]

        # FIXME TODO comparison/diff and support for non-curation workflows

        # replace the working tree with the upstream ca
        upstream.swap_carefree(working)

        upstream_now_old = upstream

        # neither of these cleanup approaches is remotely desireable
        suf = f'-{time_now.START_TIMESTAMP_LOCAL_SAFE}'
        upstream_now_old.rename(upstream_now_old.parent /
                                (upstream_now_old.name + suf))  # FIXME

    def updated_cache_transitive(self):
        """ fast get the date for the most recently updated cached path """
        if self.cache.is_organization():
            gen = (rc for c in self.children for rc in c.rchildren)
        elif self.cache.is_dataset():
            gen = self.rchildren
        else:
            gen = chain((self,), self.rchildren)

        simple_meta = [aug.PathMeta(updated=c.getxattr('bf.updated').decode())
                       if not c.is_broken_symlink() else aug.PathMeta.from_symlink(c)
                       for c in gen]
        if simple_meta:
            return max(m.updated for m in simple_meta)

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

# end imports (woo circular deps)
from sparcur.datasources import BlackfynnDatasetData
