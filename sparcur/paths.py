import os
import ast
import uuid
import logging
import hashlib
import pathlib
from functools import wraps
from itertools import chain
import orthauth as oa
import augpathlib as aug
from sxpyr import sxpyr
from dateutil import parser as dateparser
from augpathlib import PrimaryCache, EatCache, SqliteCache, SymlinkCache
from augpathlib import RepoPath, LocalPath
from pyontutils.utils_fast import TZLOCAL, utcnowtz, timeformat_friendly, isoformat
from sparcur import backends
from sparcur import exceptions as exc
from sparcur.utils import log, logd, GetTimeNow, register_type, unicode_truncate
from sparcur.utils import transitive_dirs, transitive_files, transitive_paths, is_list_or_tuple
from sparcur.utils import levenshteinDistance, symlink_latest
from sparcur.utils import BlackfynnId, LocId, PennsieveId, PDId
from sparcur.config import auth


# http://fileformats.archiveteam.org/wiki/Main_Page
# https://www.nationalarchives.gov.uk/PRONOM/Format/proFormatSearch.aspx?status=new
# FIXME case senstivity
suffix_mimetypes = {
    ('.jpx',): 'image/jpx',
    ('.jp2',): 'image/jp2',

    # sequencing
    ('.fq',):           'application/fastq',  # wolfram lists chemical/seq-na-fastq which is overly semantic
    ('.fq', '.gz'):     'application/x-gz-compressed-fastq',
    ('.fastq',):        'application/fastq',  # XXX not official
    ('.fastq', '.gz'):  'application/x-gz-compressed-fastq',
    ('.fastq', '.bz2'): 'application/x-bzip-compressed-fastq',
    ('.bam',):          'application/x.vnd.hts.bam',

    ('.mat',):          'application/x-matlab-data',  # XXX ambiguous, depending on the matlab version
    ('.m',):            'application/x-matlab',

    ('.nii',):       'image/nii',  # XXX not official
    ('.nii', '.gz'): 'image/gznii',  # I'm not sure that I believe this # XXX not official

    # http://ced.co.uk/img/TrainDay.pdf page 7
    ('.smr',):       'application/x.vnd.cambridge-electronic-designced.spike2.32.data',
    ('.smrx',):      'application/x.vnd.cambridge-electronic-designced.spike2.64.data',

    ('.s2r',):       'application/x.vnd.cambridge-electronic-designced.spike2.resource',
    ('.s2rx',):      'application/x.vnd.cambridge-electronic-designced.spike2.resource+xml',

    ('.abf',):       'application/x.vnd.axon-instruments.abf',

    ('.nd2',):       'image/x.vnd.nikon.nd2',
    ('.czi',):       'image/x.vnd.zeiss.czi',
}

banned_basenames = (
    '.DS_Store',
    'Thumbs.db',
)


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


class BFPNCacheBase(PrimaryCache, EatCache):

    _backup_cache = SqliteCache
    _not_exists_cache = SymlinkCache
    _actually_crumple = False

    cypher = hashlib.sha256  # set the remote hash cypher on the cache class

    _suffix_mimetypes = suffix_mimetypes

    _xattr_fs_version = b'sparcur.fsversion'
    _local_xattr = b'sparcur.from_local'

    @classmethod
    def fromLocal(cls, path, update=False):
        existing = path.xattrs()
        if update and not existing:
            'cannot update an cache that does not exist'
            raise exc.CacheNotFoundError(msg)
        elif not update and existing:
            msg = f'Cache exists! Not seeting fromLocal for {path}'
            # calling cls(path, meta=meta) below in a case where
            # there is existing metadata is extremely dangerous and
            # can result in the file being irreversably deleted !!!
            # due to the aweful design of the default _meta_updater
            raise exc.CacheExistsError(msg)

        meta = path.meta
        if path.is_dir():
            if path.parent.cache.is_organization():
                type = 'dataset'
            else:
                type = 'collection'
        else:
            type = 'package'

        if update:
            meta.__dict__['id'] = existing[f'{cls.xattr_prefix}.id'.encode()].decode()
            meta.__dict__['parent_id'] = existing[f'{cls.xattr_prefix}.parent_id'.encode()].decode()
        else:
            meta.__dict__['id'] = f'N:{type}:' + uuid.uuid4().urn[9:]
            meta.__dict__['parent_id'] = path.parent.cache_id

        if update:
            # do this as close in time as possible
            # to minimize error risk
            [path.delxattr(k) for k in existing if k.startswith(f'{cls.xattr_prefix}.'.encode())]
            if [k for k in path.xattrs() if k.startswith(f'{cls.xattr_prefix}.'.encode())]:
                # something has gone very wrong
                breakpoint()

        self = cls(path, meta=meta)
        self.setxattr(cls._local_xattr, b'1')
        return self

    def meta_from_local(self):
        value = self.getxattr(self._local_xattr.decode())
        return bool(value)

    @classmethod
    def decode_value(cls, field, value):
        if field in ('created', 'updated'):
            # if you get unicode decode error here it is because
            # struct packing of timestamp vs isoformat are fighting
            # in xattrs pathmeta helper
            return value.decode()  # path meta handles decoding for us

    @property
    def local_index_dir(self):
        # FIXME TODO probably move this to augpathlib?
        return self.local_data_dir / 'index'

    def local_data_dir_init(self, *args, **kwargs):
        super().local_data_dir_init(*args, **kwargs)
        lid = self.local_index_dir
        lid.mkdir(exist_ok=True)
        for type in ('pull',):
            (self.local_index_dir / type).mkdir(exist_ok=True)

    def _set_fs_version(self, version=None):
        """ materialize the file system translation version to xattrs
        """
        # FIXME this WILL go stale if only called on the anchor
        # because the anchor xattrs are not updated when datasets
        # are pulled later
        if version is None:
            version = self._remote_class._translation_version

        self.setxattr(self._xattr_fs_version, str(version).encode())

    def _fs_version(self):
        if not hasattr(self, '_cache_fs_version'):
            try:
                _fs_version = self.getxattr(self._xattr_fs_version.decode())
                fs_version = int(_fs_version)
                return fs_version
            except exc.NoStreamError as e:
                if self == self.anchor:
                    self._cache_fs_version = 0
                else:
                    self._cache_fs_version = self.parent._fs_version()

        return self._cache_fs_version

    @property
    def anchor(self):
        return self.organization

    @property
    def identifier(self):
        ntid = self.id
        if ntid is None:
            raise exc.NoCachedMetadataError(self)

        return self._id_class(ntid, file_id=self.file_id)

    @property
    def _fs_safe_id(self):
        id = self.identifier
        return id.type[0] + '-' + id.uuid

    def crumple(self):
        """Avoid creating massive numbers of inodes by trashing paths during
           sync of old datasets."""
        # FIXME PrimaryCache._meta_updater needs a return value
        # and yes _meta_updater is a disaster zone
        if self.__class__._actually_crumple:
            super().crumple()
        else:
            if self.is_dir():
                self.rmdir()  # the dir has to be empty
            else:
                self.unlink()

    @property
    def _trashed_path(self):
        id = self.identifier
        suuid = id.uuid
        sid = id.type[0] + '-' + suuid  # must match _fs_safe_id
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

        # nearly every file system we are likely to work with has 255 BYTE
        # limit on filename length, that is NOT CHAR length, so we have to
        # encode and then decode and truncate
        name = unicode_truncate(f'{sid}-{pid}-{fid}{self.name}', 255)

        return self.trash  / suuid[:2] / name

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
            identifier = self.identifier
            if identifier is None:
                parent = self.parent
                if parent == self:  # we have hit a root
                    return None

                organization = parent.organization

                if organization is not None:
                    # TODO repair
                    pass
                else:
                    raise exc.NotInProjectError

            elif identifier.type == 'organization':
                return self

        if self.parent and self.parent != self:
            # we have a case of missing metadata here as well
            return self.parent.organization

    def is_organization(self):  # FIXME ?
        return self.id and self.identifier.type == 'organization'

    def is_dataset(self):
        return self.id and self.identifier.type == 'dataset'

    @property
    def dataset(self):
        if self.is_dataset():
            return self

        elif self.parent and self.parent != self:  # Path('.') issue
            log.log(8, self.parent)  # even level 9 is too high for this
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
        id = self.identifier
        uuid = id.uuid
        file_id = self.file_id
        return BFPNCacheBase._cache_key(uuid, file_id)

    @staticmethod
    def _cache_key(uuid, file_id):
        if file_id is None:
            # FIXME not sure if want?
            file_id = ''  # needed to simplify globing e.g. from indexes

        return f'{uuid[:2]}/{uuid}-{file_id}'

    def _dataset_metadata(self, force_cache=False):
        """ get metadata about a dataset from the remote metadata store """
        # TODO figure out the right default for force_cache
        dataset = self.dataset
        if dataset == self:
            if not hasattr(self, '_c__dataset_metadata'):
                pdd = backends.PennsieveDatasetData(self)
                try:
                    blob = pdd.fromCache()
                except FileNotFoundError as e:
                    # FIXME TODO also check cached rmeta dates during pull
                    if force_cache:
                        raise e
                    else:
                        log.warning(e)
                        blob = pdd()

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

        if self.is_dataset():
            package_count = self._package_count()
            sparse_remote = (
                False
                if sparse_limit is None or sparse_limit < 0 else
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
        rgen = None
        if self.local_object_cache_path.exists():
            locsize = self.local_object_cache_path.size
            if locsize != meta.size:
                msg = (f'Partial download detected {locsize} != {meta.size} at'
                       f'\n{self.local_object_cache_path}')
                log.info(msg)
                size = self.local_object_cache_path.size
                kwargs = {}
                if size > 0:
                    if (self.local == self.local_object_cache_path
                        and size > 4096):  # FIXME hardcoded chunksize
                        # XXX there is a fantastic edge case where if
                        # you try to read and write from the same file
                        # only the first chunk will be written and if
                        # you are retrieving from remote then the offset
                        # would be greater than the chunksize so there
                        # will be a gap, so we set chunksize here and
                        # issue a critical log
                        msg = ('You probably did not mean to do this. '
                               f'Refetching {size - 4096} bytes.')
                        log.critical(msg)
                        kwargs['ranges'] = ((4096,),)
                    else:
                        kwargs['ranges'] = ((size,),)

                if not hasattr(self._remote_class, '_api'):
                    # see note below
                    self._remote_class.anchorToCache(self.anchor)

                # make sure we can write to locp since cached objects
                # are set readonly on completion
                self.local_object_cache_path.chmod(0o0644)

                rgen = self._remote_class.get_file_by_id(
                    meta.id, meta.file_id, **kwargs)
                gen = chain(
                    (next(rgen),),
                    self.local_object_cache_path.data)
            else:
                gen = chain(
                    (f'from local cache {self.local_object_cache_path}',),
                    self.local_object_cache_path.data)
        else:
            if not hasattr(self._remote_class, '_api'):
                # NOTE we do not want to dereference self.remote
                # in this situation because we just want the file
                # not the FS metadata, so we have to ensure that _api
                # is bound
                self._remote_class.anchorToCache(self.anchor)

            gen = self._remote_class.get_file_by_id(meta.id, meta.file_id)

        try:
            self.data_headers = next(gen)
        except exc.NoRemoteFileWithThatIdError as e:
            log.error(f'{self} {e}')
            raise exc.CacheNotFoundError(f'{self}') from e  # have to raise so that we don't overwrite the file

        log.log(9, self.data_headers)
        if self.local_object_cache_path.exists():
            yield from gen
            if rgen is None:
                return

            yield from self.local_object_cache_path._data_setter(
                rgen, append=True)

        else:
            # FIXME we MUST write the metadata first so that we know the expected size
            # so that in the event that the generator is only partially run out we know
            # that we can pick up where we left off with the fetch, this also explains
            # why all the cases where the cached data size did not match were missing
            # xattrs entirely
            _lcp = self.local_object_cache_path.parent
            if not _lcp.exists():
                # FIXME sigh, no obvious way around having to check
                # every time other than creating all the cache
                # subfolders in advance
                # XXX SOMEHOW between the call in the if statement above
                # this call right here it is possible for something running
                # in async in pypy3 to manage to have another threadthing
                # create the directory ... this is beyond annoying
                _lcp.mkdir(exist_ok=True)

            self.local_object_cache_path.touch()
            self.local_object_cache_path.cache_init(meta)

            yield from self.local_object_cache_path._data_setter(gen)

        # ensure we don't accidentally modify cached files if they are symlinked
        self.local_object_cache_path.chmod(0o0444)  # unlink works on 444

        ls = self.local_object_cache_path.size
        if ls != meta.size:
            self.local_object_cache_path.unlink()
            msg = f'{ls} != {meta.size} for {self}'
            raise ValueError(msg)  # FIXME TODO

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
        'manifest', 'dataset_description', 'submission', 'performances', 'subjects', 'samples'
    )

    _sparse_exts = (
        'xlsx', 'csv', 'tsv', 'json'
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
        """ path (cache) level json metadata """
        # FIXME this is going to be very slow due to the local.cache call overhead
        # it will be much better implement this from Path directly using xattrs()
        d = self.dataset
        l = self.local
        drp = l.relative_to(d.local)
        meta = self.meta  # TODO see what we need from this
        blob = {
            'type': 'path',
            'dataset_id': d.identifier.curie,
            'dataset_relative_path': drp,
            'uri_api': self.uri_api,  # -> @id in the @context
            'uri_human': self.uri_human,
            }
        if hasattr(self, '_remote_class'):  # FIXME hack to detect CacheL cases
            blob['remote_id'] = (
                # XXX a horrible hack, but it kinda works
                f'{self.identifier.curie}/files/{meta.file_id}'
                if self.is_file() or self.is_broken_symlink() else
                self.identifier.curie)

        # FIXME N:package:asdf is nasty for jsonld but ...
        # yes the bf_id will have to be parsed to know what
        # endpoint to send it to ... yay for scala thinking amirite !? >_<
        # but at least for this particular structure it

        # NOTE packages are collections of size 1 > more
        # and and when they happend to have only a single
        # member they are conflated with the single file
        # they contain

        if meta.checksum is not None:
            blob['checksums'] = [{'type': 'checksum',
                                  # FIXME cypher should ALWAYS travel with checksum
                                  # not be implicit and based on the implementation
                                  'cypher': self.cypher.__name__.replace('openssl_', ''),
                                  'hex': meta.checksum.hex(),}]

        mimetype = l.mimetype
        if mimetype:
            blob['mimetype'] = mimetype

        if do_expensive_operations:
            blob['magic_mimetype'] = asdf  # FIXME NameError

        return blob


class BlackfynnCache(BFPNCacheBase, PrimaryCache, EatCache):

    xattr_prefix = 'bf'

    uri_human = backends.BlackfynnRemote.uri_human
    uri_api = backends.BlackfynnRemote.uri_api

    _id_class = BlackfynnId


BlackfynnCache._bind_flavours()


class PennsieveCache(BFPNCacheBase, PrimaryCache, EatCache):

    #xattr_prefix = 'pn'  # FIXME TODO we will likely need an xattr conversion fix
    xattr_prefix = 'bf'

    uri_human = backends.PennsieveRemote.uri_human
    uri_api = backends.PennsieveRemote.uri_api

    _id_class = PennsieveId


PennsieveCache._bind_flavours()


class PennsieveDiscoverCache(PrimaryCache, EatCache):

    xattr_prefix = 'pd'
    _xattr_fs_version = b'sparcur.fsversion'  # XXX not clear we actually need this but whatever

    uri_human = backends.PennsieveDiscoverRemote.uri_human
    uri_api = backends.PennsieveDiscoverRemote.uri_api

    _id_class = PDId

    _project_id = backends.PennsieveDiscoverRemote._project_id
    _uri_api_template = backends.PennsieveDiscoverRemote._uri_api_template

    _jsonMetadata = BFPNCacheBase._jsonMetadata
    populateJsonMetadata = BFPNCacheBase.populateJsonMetadata
    cypher = None

    @property
    def identifier(self):
        # FIXME TODO hack to keep _jsonMetadata happy
        return self._id_class(self.id)

    @property
    def remote(self):
        if self.is_file():
            # only dataset level folder has a remote
            return None

        return super().remote

    def local_data_dir_init(self):
        # local data dir was always something of a tenuous idea
        # let's try this iteration using {:user-cache-path} ?
        # TODO see if we want to do the check here
        if not self.local_objects_dir.exists():
            self.local_objects_dir.mkdir(parents=True)

    @property
    def version(self):
        return str(self.meta.file_id)

    @property
    def local_objects_dir(self):
        cache_path = self._local_class(auth.get_path('cache-path'))
        return cache_path / 'discover' / 'datasets'  # FIXME hardcoded

    @property
    def anchor(self):
        if hasattr(self, '_anchor'):
            return self._anchor
        elif self.id == self._remote_class._project_id:
            self.__class__._anchor = self
            return self
        elif self == self.parent:
            # we hit root and found nothing
            return
        else:
            return self.parent.anchor

    @property
    def cache_key(self):
        return pathlib.PurePath(self.id) / self.version / 'data.zip'

    def is_organization(self):
        # FIXME not meaningful on discover but needed by update_cache_transitive
        return self.id == self._remote_class._project_id

    def is_dataset(self):
        return self.parent == self.anchor

    @property
    def dataset(self):
        if self.anchor is None:
            # FIXME possible to have datasets without
            # an explicit anchor in the future?
            raise ValueError('no anchor so no dataset')
        if self.is_dataset():
            return self
        else:
            return self.parent.dataset

    def _ds_only_check(self):
        if not self.is_dir():
            # can't pull/fetch individual files for this backend
            msg = 'can only pull from top level of dataset'
            raise NotImplementedError(msg)

    def pull_fetch(self):
        self._ds_only_check()
        # there is only a single operation for the dataset level
        # which combines pull and fetch (for now) switching versions
        # etc. will be a bit more work
        zip_path = self.local_object_cache_path
        size_mismatch = False  # TODO
        if not zip_path.exists():
            zip_path.parent.mkdir(parents=True, exist_ok=True)
            zip_path.data = self.remote.data
        elif size_mismatch:
            # TODO fix partial fetchs
            msg = 'TODO'
            raise NotImplementedError(msg)
        else:
            pass

        self.unpack()

    def unpack(self):
        """ unpack cached zip to folder structure and add the path meta that we do have """
        self._ds_only_check()
        import json
        zip_path = self.local_object_cache_path
        zp = aug.ZipPath(zip_path)
        zi = zp.asInternal()
        discover_root = next(zi.children)
        # FIXME using discover_root / 'manifest.json' results in missing zip info
        discover_manifest = [c for c in discover_root.children if c.name == 'manifest.json'][0]
        try:
            with discover_manifest.open() as f:
                manifest = json.load(f)
        finally:
            discover_manifest.close()

        sds_root = discover_root / 'files'
        # iterate and make directories
        real_dirs = [self.local / c.relative_to(sds_root) for c in sds_root.rchildren if c.is_dir()]
        [d.mkdir(parents=True, exist_ok=True) for d in real_dirs]
        # XXX FIXME needed to avoid issues in export for time being
        dcaches = [r.cache_init(aug.PathMeta(id='discover-meta-has-no-id-sigh')) for r in real_dirs]

        # iterate and create files
        # TODO package ids probably
        def unzip(f):
            target = self.local / f.relative_to(sds_root)
            target.data = f.data
            return target

        targets = [unzip(c) for c in sds_root.rchildren if not c.is_dir()]
        # XXX we are kind of doing this out of order metadata should
        # usually be written first but not a big deal in this case
        path_metas = {d['path'].split('/', 1)[-1]:
         aug.PathMeta(
             id=d['sourcePackageId'],
             size=d['size'],
         )
         for d in manifest['files'] if 'files/' in d['path']}
        reals = set((t.relative_to(self)).as_posix() for t in targets)
        metas = set(path_metas)
        reme = reals - metas
        mere = metas - reals
        if reme:
            log.error(f'extra real paths {reme}')

        if mere:
            log.error(f'missing real paths {mere}')

        def add_path_meta(f):
            path_meta = path_metas[f.relative_to(self).as_posix()]
            cache = f.cache_init(path_meta)
            return cache

        caches = [add_path_meta(t) for t in targets]

    def fetch(self):
        msg = 'you probably want pull_fetch for discover'
        raise NotImplementedError(msg)

    _fs_version = BFPNCacheBase._fs_version
    _set_fs_version = BFPNCacheBase._set_fs_version


PennsieveDiscoverCache._bind_flavours()


class PathHelper:

    _xattr_meta = aug.EatCache.meta
    _symlink_meta = aug.SymlinkCache.meta

    @property
    def dataset_relative_path(self):
        # FIXME broken for local validation
        # update: I'm pretty sure this works for total local validation
        # because we use CacheL, so the fix here is all we need to cover
        # the file changed case
        try:
            if self.cache is None and self.parent.cache is not None:
                # file has been modified case
                return self.relative_path_from(self.parent.cache.dataset.local)
            else:
                return self.relative_path_from(self.cache.dataset.local)
        except exc.CircularSymlinkNameError as e:
            if self.parent.cache is not None:
                return self.relative_path_from(self.parent.cache.dataset.local)
            else:
                msg = 'symlink name error and parent also missing cache'
                log.error(msg)
                raise e
        except AttributeError as e:
            raise exc.NoCachedMetadataError(self) from e

    @property
    def rchildren_dirs(self):
        # FIXME windows support if find not found
        if self.is_dir():
            yield from transitive_dirs(self)

    def populateJsonMetadata(self, blob):
        """ populate a json blob with file system metadata"""
        # FIXME TODO implement this without resolving .cache
        if self.cache is not None:
            return self.cache.populateJsonMetadata(blob)
        else:
            return self._jsonMetadata()

    def _jm_common(self, do_expensive_operations=False):
        # FIXME WARNING resolution only works if we were relative to
        # the current working directory
        if self.is_broken_symlink():
            self = self.absolute()
        else:
            self = self.resolve()  # safety since we have to go hunting paths

        project_path = self.find_cache_root()

        if project_path is None:
            # FIXME TODO I think we used dataset_description as a hint?
            project_path = self.__class__('/')  # FIXME FIXME
            log.critical(f'No dataset path found for {self}!')
            #raise NotImplementedError('current implementation cant anchor with current info')

        dataset_path = [p for p in chain((self,), self.parents) if p.parent == project_path][0]
        drp = self.relative_path_from(dataset_path)  # FIXME ...

        dsid = dataset_path.cache_identifier

        blob = {
            'type': 'path',
            'dataset_id': dsid,
            'dataset_relative_path': drp,
            'basename': self.name,  # for sanity's sake
        }

        mimetype = self.mimetype
        if mimetype:
            blob['mimetype'] = mimetype

        if do_expensive_operations:
            blob['magic_mimetype'] = self._magic_mimetype

        if not (self.is_broken_symlink() or self.exists()):
            # TODO search for closest match
            cands = self._closest_existing_matches()
            msg = f'Path does not exist!\n{self}'
            # FIXME check manifest to see whether candidates are already in the
            if cands:
                _fcands = [(r, n) for r, n in cands if r < 10]
                fcands = _fcands if _fcands else cands
                msg += f'\n{0: <4} {self.name}\n'
                msg += '\n'.join([f'{r: <4} {n}' for r, n in fcands])
            # do not log the error here, we won't have
            # enough context to know where we got a bad
            # path, but the caller should, maybe a case for
            # inversion of control here
            blob['errors'] = [{'message': msg,
                               'blame': 'submissions',  # FIXME maybe should be pipeline?
                               'pipeline_stage': 'sparcur.paths.Path._jm_common',
                               'candidates': cands,}]

        return blob, project_path, dsid

    def _closest_existing_matches(self):
        if self.parent.exists():
            # we probably don't need this, the numbers
            # should usually be small enough
            #childs = [c for c in self.parent.children
                      #if c.is_file() or c.is_broken_symlink()]
            name = self.name
            cands = sorted(
                [(levenshteinDistance(c.name, name), c.name)
                 for c in self.parent.children])
            return cands

    def _jsonMetadata(self, do_expensive_operations=False):
        blob, project_path, dsid = self._jm_common(do_expensive_operations=do_expensive_operations)
        return blob

    def updated_cache_transitive(self):
        """ fast get the date for the most recently updated cached path """
        if self.cache.is_organization():
            gen = (rc for c in self.children for rc in c.rchildren)
        elif self.cache.is_dataset():
            gen = self.rchildren
        else:
            gen = chain((self,), self.rchildren)

        def updated_hierarchy(local):
            """ try to get the cache updated time, if the cache doesn't exist,
                e.g. because the file has been overwritten and there are no
                xattrs, then use the local updated time """
            try:
                updated = local.getxattr('bf.updated').decode()
                return aug.PathMeta(updated=updated)
            except exc.NoStreamError:
                return local.meta_no_checksum


        simple_meta = [updated_hierarchy(c)
                       if not c.is_broken_symlink() else
                       aug.PathMeta.from_symlink(c)
                       for c in gen]
        if simple_meta:
            return max(m.updated for m in simple_meta)
        else:
            # in the event that the current folder is empty
            updated = self.getxattr('bf.updated').decode()
            return aug.PathMeta(updated=updated).updated

    @property
    def cache_meta(self):
        """ Access cached metadata without constructing the cache instance. """
        if self.is_broken_symlink():
            # NOTE check ibs first otherwise symlink meta could
            # raise a NoCachedMetadataError, it may raise other
            # errors if something goes wrong when reading the
            # symlink, but not that particular error
            return self._symlink_meta
        else:
            return self._xattr_meta

    def _cache_jsonMetadata(self, do_expensive_operations=False, with_created=False):
        # XXX REMINDER changes here should bump __pathmeta_version__ in objects.py
        blob, project_path, dsid = self._jm_common(do_expensive_operations=do_expensive_operations)
        project_meta = project_path.cache_meta
        meta = self.cache_meta  # FIXME this is very risky
        # and is the reason why I made it impossible to contsturct
        # cache classes when no cache was present, namely that if
        # there is no cache then PathMeta will still return the
        # correct structure, but it will be empty, which is bad
        if meta is None:
            log.critical(f'something is very wrong with path: {self}')
            raise exc.NoCachedMetadataError(self)
        elif meta.id is None:
            raise exc.NoCachedMetadataError(self)

        if self.xattr_prefix in ('bf', 'pn'):
            _, bf_id = meta.id.split(':', 1)  # FIXME SODA use case
            idc = self._cache_class._id_class
            if meta.parent_id:
                _, bf_parent = meta.parent_id.split(':', 1)
                parent_id = idc(bf_parent)
            else:
                parent_id = None

            try:
                remote_id = (idc(bf_id, file_id=meta.file_id)
                             if self.is_file() or self.is_broken_symlink() else
                             idc(bf_id))
            except Exception as e:
                breakpoint()
                raise e
        elif self.xattr_prefix in ('pd',):
            parent_id = None  # missing from discover
            remote_id = self.cache_identifier
        else:
            msg = f'unknown xattr prefix {self.xattr_prefix!r}'
            raise NotImplementedError(msg)

        #identifier = self._cache_class._id_class('N:' + remote_id)
        uri_api = remote_id.uri_api
        uri_human = remote_id.uri_human(
            organization=project_path.cache_identifier,
            dataset=dsid)

        if parent_id:  # FIXME parent_id should probably be required
            # given that we try to keep transitive meta closed under
            # it, but in some cases (e.g. pd) we can't eforce it here?
            blob['parent_id'] = parent_id

        blob['remote_id'] = remote_id
        if with_created:
            blob['timestamp_created'] = meta.created  # leaving this out

        blob['timestamp_updated'] = meta.updated  # needed to simplify transitive update
        blob['uri_api'] = uri_api
        blob['uri_human'] = uri_human

        if meta.size is not None:
            blob['size_bytes'] = meta.size

        if (self.is_file() or self.is_broken_symlink()):
            #if hasattr(remote_id, 'file_id') and remote.file_id is not None:
                # handle discover case where there aren't file_ids
            if meta.multi is not None:
                # XXX NOTE because we leave multi out of xattrs if it is not multi
                # there are some cases where old forms can sneek through, but nearly
                # all of those cases will only happen in dev because in prod there will
                # be a bump in Remote._internal_version from 0 to 1 which will force
                # everything to refetch and the absense of multi will be accurate
                # the reason for this tradoff is because multi is something that simply
                # should not exist at all so if it isn't there we don't want to keep any
                # metadata about it at all
                blob['multi'] = meta.multi

            blob['remote_inode_id'] = remote_id.file_id
            if meta.checksum is not None:
                # FIXME known checksum failures !!!!!!!
                blob['checksums'] = [{'type': 'checksum',
                                    # FIXME cypher should ALWAYS travel with checksum
                                    # not be implicit and based on the implementation
                                    'cypher': self._cache_class.cypher.__name__.replace('openssl_', ''),
                                    'hex': meta.checksum.hex(),}]

        if meta.errors:
            # propagate errors caused by issues with remote
            errors = [
                {'message': error,
                 'blame': 'remote',
                 'pipeline_stage': 'sparcur.paths.Path._cache_jsonMetadata',}
                for error in meta.errors]
            if 'errors' not in blob:
                blob['errors'] = errors
            else:
                blob['errors'].extend(errors)

        return blob

    def _transitive_metadata(self):
        def hrm(p):
            try:
                cjm = p._cache_jsonMetadata()
            except exc.NoCachedMetadataError:
                # FIXME TODO figure out when to log this
                # probably only log when there is a dataset id
                return
            except Exception as e:
                breakpoint()
                pass
            # this is not always needed so it is not included by default
            # but we do need it here for when we generate the rdf export
            p = cjm['dataset_relative_path'].parent
            if p.name != '':  # we are at the root of the relative path aka '.'
                cjm['parent_drp'] = p
                # FIXME parent_id is embedded in the meta now right?
                # however the question is what we should do if something
                # has been reparented locally ...
                # XXX the fact that we don't use _transitive_metadata for diff and push
                # is probably why this has splipped through until now but might need to
                # need to differentiate current_parent_id from cache_parent_id !!!
            return cjm

        def sort_dirs_first(d):
            # NOTE THAT mimetype IS NOT A REQUIRED FIELD
            return ((not ('mimetype' in d and
                          d['mimetype'] == 'inode/directory')),
                    'mimetype' in d)

        exclude_patterns = '*.~lock*#', '.DS_Store'  # TODO make this configurable
        _tm = hrm(self)
        _tm['dataset_relative_path'] = ''  # don't want . since it is ambiguous?
        this_metadata = [_tm]  # close the transitive metadata under parent_id
        # FIXME perf boost but not portable
        rchildren = transitive_paths(self, exclude_patterns=exclude_patterns)
        _rcm = [_ for _ in [hrm(c) for c in rchildren] if _ is not None]
        rc_metadata = sorted(_rcm, key=sort_dirs_first)
        metadata = this_metadata + rc_metadata
        index = {b['dataset_relative_path']:b for b in metadata}
        pid = this_metadata[0]['remote_id']
        transitive_updated = 'FIXME-TODO'  # XXX
        for m in metadata:
            p = m.pop('parent_drp', None)
            if p in index:
                current_parent_id = index[p]['remote_id']
                if 'parent_id' in m and current_parent_id != m['parent_id']:
                    # not all remotes have parent_id
                    msg = f'SOMETHING MOVED! {current_parent_id} != {parent_id} for {self}'
                    log.critical(msg)
                    m['current_parent_id'] = current_parent_id
            else:
                # this branch correctly resets parent_id to pid
                # while it is technically wrong for subfolders,
                # and is even technically wrong for datasets,
                # it means that there will not be a null pointer
                # to something which is not in the full list
                if 'parent_id' in m:
                    # put the null pointer somewhere else
                    m['external_parent_id'] = m['parent_id']

                m['parent_id'] = pid

        return metadata, transitive_updated


class DiscoverPath(aug.XopenPath, aug.LocalPath, PathHelper):

    _cache_class = PennsieveDiscoverCache

    xattr_prefix = PennsieveDiscoverCache.xattr_prefix

    # TODO factor out the common functionality between discover and
    # internal into a shared parent class, as it stands discover does
    # not and should not use many of the things we need for curation

    @property
    def cache_identifier(self):
        return self.cache.identifier


DiscoverPath._bind_flavours()


class Path(aug.XopenPath, aug.RepoPath, aug.LocalPath, PathHelper):  # NOTE this is a hack to keep everything consistent
    """ An augmented path for all the various local
        needs of the curation process. """

    _cache_class = BlackfynnCache

    _dataset_cache = {}

    _suffix_mimetypes = suffix_mimetypes

    xattr_prefix = BlackfynnCache.xattr_prefix

    @classmethod
    def fromJson(cls, blob):
        # FIXME the problem here is that we can't distingish between cases where
        # the original path was absolute, vs cases where we are embedding this data
        # the the path was relative to start with
        return blob

        #return Path(blob['dataset_relative_path'])  # doesn't work, want full path
        if (hasattr(cls, '_cache_class') and
            hasattr(cls._cache_class, '_anchor') and
            cls._cache_class._anchor is not None):
            a = cls._cache_class._anchor
            #l = a.local
            did = 'N:' + blob['dataset_id']
            if did not in cls._dataset_cache:
                cls._dataset_cache.update({c.id:c.local for c in a.children})

            dataset_local = cls._dataset_cache[did]
            path_relative_string = blob['dataset_relative_path']
            return dataset_local / path_relative_string
        else:
            return blob  # FIXME TODO

    @property
    def project_relative_path(self):
        try:
            return self.relative_path_from(self._cache_class._anchor.local)
        except Exception as e:
            # _cache_class._anchor is an implementation detail which is not
            # used in all cases, so try to fail over to the visible api
            return self.relative_path_from(self.cache.anchor.local)

    @property
    def cache_id(self):
        """ fast way to get cache.id terrifyingly this has roughly
        an order of magnitude less overhead than self.cache.id """

        try:
            return (self.getxattr('bf.id').decode()
                    if self.is_dir() or self.is_file() else
                    self.readlink().parts[1])
        except FileNotFoundError as e:
            # in the event we try to blindly get cached metadata for a
            # path in a manifest that does not exist
            raise e
        except OSError as e:
            raise exc.NoCachedMetadataError(self) from e

    @property
    def cache_file_id(self):
        if self.is_dir():
            return

        try:
            return (int(self.getxattr('bf.file_id').decode())
                    if self.is_file() else
                    self.cache_meta.file_id)
        except FileNotFoundError as e:
            # in the event we try to blindly get cached metadata for a
            # path in a manifest that does not exist
            raise e
        except OSError as e:
            raise exc.NoCachedMetadataError(self) from e

    @property
    def cache_identifier(self):
        return self._cache_class._id_class(
            self.cache_id, file_id=self.cache_file_id)

    def manifest_record(self, manifest_parent_path):
        filename = self.relative_path_from(manifest_parent_path)
        description = None
        filetype = self.mimetype  # TODO failover
        additional_type = None  # TODO
        if self.is_file():
            meta = self.meta
            # use updated because most file systems don't have a
            # meaningful created time aka birthdate
            timestamp = meta.updated
            checksum = meta.checksum
        else:
            meta = self.cache.meta
            timestamp = meta.created  # XXX created on remote platform time
            checksum = None  # XXX avoid promulgating unchecked values

        return (
            filename,
            timestamp,
            description,
            filetype,
            additional_type,
            checksum,
        )

    def generate_manifest(self, include_directories=False):
        """ generate a tabular manifest of all contents of a directory
            serialization is handled by the caller if it is required """

        if not self.is_dir():
            log.error('Can only generate manifests for directories!')
            raise NotADirectoryError(self)

        if include_directories:
            return [c.manifest_record(self) for c in self.rchildren]
        else:
            return [c.manifest_record(self) for c in self.rchildren
                    if not c.is_dir()]

    @classmethod
    def _file_type_status_lookup(cls):
        import json  # FIXME
        if not hasattr(cls, '_sigh_ftslu'):
            resources = auth.get_path('resources')  # FIXME breaks outside git
            with open(resources / 'mimetypes.json', 'rt') as f:
                classification = json.load(f)

            mimetypes = {mimetype:status for status,objs in
                         classification.items() for obj in objs
                         for mimetype in (obj['mimetype']
                                          if is_list_or_tuple(obj['mimetype']) else
                                          (obj['mimetype'],))}
            suffixes = {obj['suffix']:status for status,objs in
                        classification.items() for obj in objs}
            cls._mimetypes_lu, cls._suffixes_lu = mimetypes, suffixes
            cls._sigh_ftslu = True


        return cls._mimetypes_lu, cls._suffixes_lu

    _banned_basenames = banned_basenames
    _unknown_suffixes = set()
    _unclassified_mimes = set()
    @classmethod
    def validate_path_json_metadata(cls, path_meta_blob):
        from sparcur.core import HasErrors  # FIXME
        he = HasErrors(pipeline_stage=cls.__name__ + '.validate_path_json_metadata')
        mimetypes, suffixes = cls._file_type_status_lookup()  # SIGH this overhead is 2 function calls and a branch
        for i, path_meta in enumerate(path_meta_blob['data']):
            if path_meta['basename'] in cls._banned_basenames:
                msg = f'illegal file detect {path_meta["basename"]}'
                dsrp = path_meta['dataset_relative_path']
                if he.addError(msg, path=dsrp, json_path=('data', i)):
                    logd.error(msg)
                status = 'banned'
                path_meta['status'] = status
                continue

            if 'magic_mimetype' in path_meta and 'mimetype' in path_meta:
                # FIXME NOT clear whether magic_mimetype should be used by itself
                # usually magic and file extension together work, magic by itself
                # can give some completely bonkers results
                source = 'magic_mimetype'
                mimetype = path_meta['magic_mimetype']
                muggle_mimetype = path_meta['mimetype']
                if mimetype != muggle_mimetype:
                    msg = f'mime types do not match {mimetype} != {muggle_mimetype}'
                    dsrp = path_meta['dataset_relative_path']
                    if he.addError(msg, path=dsrp, json_path=('data', i)):
                        log.error(msg)
            elif 'magic_mimetype' in path_meta:
                source = 'magic_mimetype'
                mimetype = path_meta['magic_mimetype']
            elif 'mimetype' in path_meta:
                source = 'mimetype'
                mimetype = path_meta['mimetype']
            else:
                mimetype = None

            if mimetype is not None:
                try:
                    status = mimetypes[mimetype]
                    if status == 'banned':
                        msg = f'banned mimetype detected {mimetype}'
                        dsrp = path_meta['dataset_relative_path']
                        if he.addError(msg, path=dsrp,
                                       json_path=('data', i, source)):
                            logd.error(msg)
                except KeyError as e:
                    status = 'known'
                    if mimetype not in cls._unclassified_mimes:
                        cls._unclassified_mimes.add(mimetype)
                        log.info(f'unclassified mimetype {mimetype}')
            else:
                status = 'unknown'
                dsrp = path_meta['dataset_relative_path']
                if isinstance(dsrp, str):
                    if not dsrp:
                        msg = f'FIXME top level folder needs a mimetype!'
                    else:
                        msg = f'unknown mimetype {path_meta["basename"]}'
                else:
                    msg = f'unknown mimetype {"".join(dsrp.suffixes)}'
                    cls._unknown_suffixes.add(tuple(dsrp.suffixes))
                if he.addError(msg, path=dsrp, json_path=('data', i)):
                    logd.warning(msg)

            path_meta['status'] = status

        if he._errors_set:
            he.embedErrors(path_meta_blob)

    def pull(self, *args,
             paths=None,
             time_now=None,
             debug=False,
             n_jobs=12,
             cache_anchor=None,
             log_name=None,
             log_level='INFO',
             # pass in Parallel in at run time if needed
             Parallel=None,
             delayed=None,
             _in_parallel=False,
             exclude_uploaded=False,):
        # TODO usage errors

        if time_now is None:
            time_now = GetTimeNow()
            log.debug('No time provided to pull so using '
                      f'{time_now.START_TIMESTAMP}')

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
            if debug or Parallel is None or n_jobs <= 1:
                for child in self.children:
                    if paths is None or child in paths:
                        child.pull()
            else:
                Parallel(n_jobs=n_jobs)(
                    delayed(child.pull)(_in_parallel=True,
                                        time_now=time_now,
                                        cache_anchor=cache.anchor,
                                        log_name=_log.name,
                                        log_level=log_level,
                                        exclude_uploaded=exclude_uploaded,)
                    for child in self.children
                    if paths is None or child in paths)

        elif cache.is_dataset():
            self._pull_dataset(time_now, exclude_uploaded)  # XXX actual pull happens in here

        else:
            raise NotImplementedError(self)

    def _pull_dataset(self, time_now, exclude_uploaded):
        cache = self.cache
        try:
            _old_eu = self._remote_class._exclude_uploaded
            cache._remote_class._exclude_uploaded = exclude_uploaded
            return self._pull_dataset_internal(time_now)
        finally:
            cache._remote_class._exclude_uploaded = _old_eu

    def _index_file_key_read(self):
        return 'LATEST'

    def _index_file_key_write(self, updated_cache_transitive):
        # we must use transitive updated for this NOT datasetup updated
        # because datasetup updated can change when no paths have changed
        # leading to aliasing in the index
        tu_zulu = updated_cache_transitive
        tu_local = tu_zulu.astimezone(TZLOCAL())
        friendly_tu_local = timeformat_friendly(tu_local)
        return friendly_tu_local

    _noindex = False
    def _generate_pull_index(self, dataset, caches):
        if self._noindex:
            # the index is only needed for human curation workflows
            # for the automated pipelines, that said they could be
            # useful for generating simple diffs
            return

        # all the remotes are already in memory
        duuid = dataset.cache_identifier.uuid
        id_name, parent_children = {duuid: dataset.name}, {}
        # TODO do we need file_id ?
        updated_cache_transitive = None
        if not caches:  # empty dataset case
            if hasattr(dataset, '_remote'):
                r = dataset._remote
                updated_cache_transitive = r.updated
            else:
                updated_cache_transitive = dataset.cache_meta.updated

        elif hasattr(caches[0], '_remote'):
            for c in caches:
                r = c._remote
                id = r.id
                if not c.is_dir():
                    id = id, r.file_id

                # needed for lost metadata case since we are putting name in xattrs
                id_name[id] = r._name
                pid = r.parent_id

                if pid not in parent_children:
                    parent_children[pid] = []

                parent_children[pid].append(id)
                if updated_cache_transitive is None or r.updated >= updated_cache_transitive:
                    updated_cache_transitive = r.updated

        else:  # in the event that we need to regenerate indexes for some reason
            for c in caches:
                # TODO avoid meta overhead
                cmeta = c.meta  # c.local.cache_meta ?
                id = c.id
                if not c.is_dir():
                    id = id, c.file_id

                id_name[id] = cmeta.name
                pid = cmeta.parent_id

                if pid not in parent_children:
                    parent_children[pid] = []

                parent_children[pid].append(id)

                if updated_cache_transitive is None or cmeta.updated >= updated_cache_transitive:
                    updated_cache_transitive = cmeta.updated

        if isinstance(updated_cache_transitive, str):
            # we don't always parse remote dates for performance reasons
            # so convert here if the date is still a string
            updated_cache_transitive = dateparser.parse(updated_cache_transitive)

        self._write_pull_index(
            duuid, updated_cache_transitive, id_name, parent_children)

    def _write_pull_index(self, duuid, updated_cache_transitive, id_name, parent_children):
        ifkey = self._index_file_key_write(updated_cache_transitive)
        _ucts = isoformat(updated_cache_transitive)
        to_write = ('v02', ifkey, _ucts, id_name, parent_children)
        base = self.cache.local_index_dir / 'pull' / duuid
        if not base.exists():
            base.mkdir()
        index = base / ifkey
        latest = base / self._index_file_key_read()
        with open(index, 'wt') as f:
            # read with ast.literal_eval
            # TODO compression probably
            f.write(repr(to_write).replace(': ', ':\n'))  # FIXME slow ?

        symlink_latest(index, latest)

    def _reindex_done_manifest(self, manifest, remotes):
        # read indexes
        # update cache metadata (I don't think we did this?) # XXX should be done in another function
        # find and modify entries affected by manifest
        # produce new id name and parent children mappings
        dataset_id, id_name, parent_children, name_id, updated_transitive = self._read_indexes()
        duuid = dataset_id.uuid
        # XXX building the inverted index can be much faster when the manifest includes the remote ids
        child_parent = {c:p for p, v in parent_children.items() for c in v}

        # I think the new remote info is already pulled after rename and reparent right now? yep!
        # XXX ok, first issue, how do renames and reparents interact with update time?
        # because a file that gets reparented does not have an updated event but the folder it was
        # previously parented to might?
        # FIXME hrm, a rename of a folder followed by a reparent that it is involved in could
        # result in a stale updated time

        # FIXME all the information to modify the index should already be in the manifest at this point
        # we we don't have to recompute it here
        new_id_name = {k:v for k, v in id_name.items()}
        new_parent_children = {k:list(v) for k, v in parent_children.items()}
        updated_cache_transitive = dateparser.parse(updated_transitive)
        for d in manifest:
            p = self / d['path']
            meta = p.cache_meta
            if meta.updated > updated_cache_transitive:
                updated_cache_transitive = meta.updated

            if meta.file_id is not None:
                id = meta.id, meta.file_id
            else:
                id = meta.id

            for o in d['ops']:
                # FIXME issue with the reparent rename case losing metadata it seems?
                # XXX looks like it actually just faild to reparent? and only renamed?
                if o == 'rename':
                    new_id_name[id] = meta.name  # should already have been updated at this point
                elif o == 'reparent':
                    old_parent_id = child_parent[id]
                    new_parent_id = meta.parent_id
                    new_parent_children[old_parent_id].remove(id)
                    if new_parent_id not in new_parent_children:  # previously empty folder case
                        new_parent_children[new_parent_id] = []
                    new_parent_children[new_parent_id].append(id)
                else:
                    msg = f'NOT IMPLEMENTED BUT NOT ERROR HOW DID WE GET HERE? {d}'
                    log.error(msg)

        self._write_pull_index(
            duuid, updated_cache_transitive, new_id_name, new_parent_children)

    def _pull_dataset_internal(self, time_now):
        cache = self.cache
        if cache.is_organization():
            raise TypeError('can\'t use this method on organizations')
        elif not cache.is_dataset():
            return self.dataset._pull_dataset_internal(time_now)

        children = list(self.children)
        if not children:
            # note that there is a second marker for actuall pull below
            working_c_children = list(self.cache.rchildren)  # XXX actual pull happens here
            #working_children = [c.local.relative_to(parent)
                                #for c in caches]
            self._generate_pull_index(self, working_c_children)
            self.cache._set_fs_version()
            # not sure why we return something here we don't return anything below
            return working_c_children  # XXX FIXME inconsistent return value

        # instantiate a temporary staging area for pull
        ldd = cache.local_data_dir
        contain_upstream = ldd / 'temp-upstream'
        contain_upstream.mkdir(exist_ok=True)

        # create the new directory with the xattrs needed to pull
        working = self
        upstream = contain_upstream / working.name
        try:
            upstream.mkdir()
            # if upstream fails that means that somehow a previous
            # cleanup did not succeed (for some reason), this time it
            # should succeed and next time everything should work
        except FileExistsError as e:
            log.exception(e)
            error_suf = f'{time_now.START_TIMESTAMP_LOCAL_SAFE}-ERROR'
            upstream.rename(upstream.parent / (upstream.name + error_suf))
            upstream.mkdir()
        except BaseException as e:
            error_suf = f'{time_now.START_TIMESTAMP_LOCAL_SAFE}-ERROR'
            upstream.rename(upstream.parent / (upstream.name + error_suf))
            # TODO do we need to log here or can we log up the stack?
            raise e

        # if mkdir succeeds then any failure that happens between
        # then and the point where we swap needs to move the folder
        # to a unique path marked as an error so that future calls
        # to this function are not stopped by an existing directory
        # that failed to be cleaned up
        upstream.setxattrs(working.xattrs())  # handy way to copy xattrs
        upstream.cache._set_fs_version()  # fs version can change
        # pull and prep for comparison of relative paths
        upstream_c_children = list(upstream.cache.rchildren)  # XXX actual pull happens here
        self._generate_pull_index(upstream, upstream_c_children)
        #upstream_children = [c.local.relative_to(upstream)
                             #for c in upstream_c_children]
        #working_children = [c.relative_to(working)
                            #for c in working.rchildren]

        # FIXME TODO comparison/diff and support for non-curation workflows
        # materialize existing files that we have donwloaded but not changed

        # replace the working tree with the upstream
        # FIXME this has nasty and confusing consequences if
        # working == aug.AugmentedPath.cwd()
        upstream.swap_carefree(working)

        upstream_now_old = upstream

        # neither of these cleanup approaches is remotely desireable
        suf = f'-{time_now.START_TIMESTAMP_LOCAL_SAFE}'
        old = upstream_now_old.parent / (upstream_now_old.name + suf)
        upstream_now_old.rename(old)  # FIXME
        # TODO cleanup old either by removing things older than X
        # or by symlinking unmodified files to their package cache
        old._symlink_packages_transitive()

        if not hasattr(old, 'rmdirtree'):
            # remove this branch once new version of augpathlib is published
            def rmdirtree(path):
                """ like rmtree but only for empty folders
                find path -type d -empty -delete
                """
                if path.is_symlink():
                    # match behavior of find ${symlink}
                    msg = f'{self} is a symlink'
                    raise NotADirectoryError(msg)

                deleted = set()
                for path_string, subs, files in os.walk(path, topdown=False):
                    has_subs = False
                    sps = set()
                    for sub in subs:
                        sub_path_string = os.path.join(path_string, sub)
                        if sub_path_string not in deleted:
                            has_subs = True
                            break
                        else:
                            sps.add(sub_path_string)

                    if not files and not has_subs:
                        path.__class__(path_string).rmdir()
                        deleted.add(path_string)
                        # remove any deleted subs because the next
                        # level up only cares about this level
                        # keeps potential memory usage down
                        deleted.difference_update(sps)

            rmdirtree(old)

        else:
            old.rmdirtree()

    def _eq_data(self, other):
        for schunk, ochunk in zip(self.data, other.data):
            if schunk != ochunk:
                return False

        return True

    def _symlink_packages_transitive(self, remove_circular=True):
        # the ext4 defaults on are at least an order of magnitude too low to
        # retain all the broken symlinks for thousands of snapshots, we also
        # don't particularly need to keep them around

        # find all files
        # if real file
        # determine if they differ from the cache XXX this is going to be slow right now
        # if not changed remove the file in the tree and symlink it to the package
        # if changed leave the modified file
        # if broken symlink unlink to keep inodes under control
        for c in self.rchildren:
            if c.cache is not None:  # FIXME need a way to recover the original package?
                locp = c.cache.local_object_cache_path
                if (c.is_file() and locp.exists() and
                    c.size == locp.size and
                    c._eq_data(locp)):
                    c.unlink()
                    relsym = c.relative_path_to(locp)
                    c.symlink_to(relsym)
                if remove_circular and c.is_broken_symlink():
                    c.unlink()

    def transitive_fix_missing_metadata(self):
        # beware this will compute checksums for any files missing metadata
        nometa = [f for f in transitive_files(self) if not f.xattrs()]
        [f._cache_class.fromLocal(f) for f in nometa]

    def _read_indexes(self):
        # TODO this should be straight forward for DiscoverPath since they never change?
        dataset = self.cache.dataset.local
        dataset_id = dataset.cache_identifier
        ifkey = self._index_file_key_read()
        base = self.cache.local_index_dir / 'pull' / dataset_id.uuid
        index = (base / ifkey).resolve()
        if not base.exists():
            msg = ('index does not exist! you should probably pull '
                   'this dataset again using the newest version of sparcur: '
                   f'{base}')
            raise FileNotFoundError(msg)

        with open(index, 'rt') as f:
            s = f.read()

        version, *rest = ast.literal_eval(s)
        if version == 'v01':
            (friendly_dsu_local, id_name, parent_children) = rest
            # FIXME TODO maybe parse back from friendly_dsu_local? XXX it more or less works, except
            # that we don't actually need this because anything with the old format almost certainly
            # needs to be pulled again, also side thought ... we probably want to check that the
            # remote hasn't changed? oh dear, yeah, that isn't going to work out very well
            #hrm = dateparser.parse(friendly_dsu_local)
            msg = f'index version {version} too old, pull the dataset'
            log.warning(msg)
            updated_transitive = None
        elif version == 'v02':
            (friendly_dsu_local, updated_transitive, id_name, parent_children,
             ) = rest

        name_id = {}
        for id, name in id_name.items():
            if name not in name_id:
                name_id[name] = []

            name_id[name].append(id)

        return dataset_id, id_name, parent_children, name_id, updated_transitive

    def _debug_diff(self, update_cache_transitive, updated_cache):
        rcs = list(self.rchildren)
        sigh = [(r, r._cache_class._not_exists_cache(r)._meta_impl(r, match_name=False)
                    if r.is_symlink()
                    else (r.cache.meta
                        if r.cache is not None
                        else r.meta))
                for r in rcs]
        [(s, m.updated) for s, m in sigh if m.updated >= updated_cache_transitive]
        [(s, m.updated) for s, m in sigh if m.updated >= updated_transitive]
        hrm = [(s, m.updated) for s, m in sigh if m.updated in (updated_cache_transitive, updated_transitive)]
        breakpoint()
        # ok so it is clear that the way the test is working does not generate the index in the way we want
        # ah, rather, this is because the command to add files generates fake cache data which is ... unrealistic
        [print(r._cache_class._not_exists_cache(r)._meta_impl(r, match_name=False).as_pretty())
            if r.is_symlink()
            else (print(r.cache.meta.as_pretty())
                if r.cache is not None
                else print('likely new locally', r))
            for r in rcs]
        [c.cache_meta.updated for c in rcs]

    def diff(self):
        (rows, _others, updated_cache_transitive, updated_transitive, dataset_id
         ) = self._transitive_changes()

        if updated_cache_transitive > updated_transitive:
            msg = ('index out of sync with files! '
                   f'{updated_cache_transitive} > {updated_transitive}')
            #self._debug_diff(update_cache_transitive, updated_cache)
            raise ValueError(msg)  # FIXME error type

        blobs = [dict(path=p.dataset_relative_path.as_posix(),
                      type=path_type,
                      updated=updated,
                      ops=ops) for p, path_type, updated, ops in
                 # sort so we can quickly get transitive changes
                 sorted(rows, key=lambda r: r[2])
                 ]
        # XXX we cannot blindly sort blobs to find the real updated time
        # without discarding no-metadata and similar cases where the updated
        # time comes from the local system instead of the remote
        # XXX further, we have to use updated_transitive which comes from the
        # because there are routine cases where the most recently updated file
        # will itself be overwritten so updated_cache_transitive will wind up
        # being less than updated_transitive from the index, and we use the
        # value from the index in the identifier for the push folder

        #latest = blobs[-1]
        #updated = latest['updated']
        #_iuct = isoformat(updated_cache_transitive)
        #assert updated == _iuct, (breakpoint(), f'wat {updated!r} != {_iuct!r}')
        fblobs = [b for b in blobs if 'no-change' not in b['ops']]

        # XXX we must use updated_transitive here NOT dataset updated
        # the use case is separate, if dataset metadata changes but no
        # internal paths changed we don't want to bump the index
        return dataset_id, updated_transitive, fblobs

    def _push_cache_dir(self, updated_transitive, push_id):
        if not self.cache.is_dataset():
            raise TypeError('can only run this on datasets')

        cache_path = self.__class__(auth.get_path('cache-path'))
        uuid = self.cache_identifier.uuid
        updated_friendly = timeformat_friendly(updated_transitive)
        return cache_path / 'push' / uuid / updated_friendly / push_id

    def _ensure_push_match(self, dataset_id, updated_transitive, updated_cache_transitive):
        cuuid = self.cache_identifier.uuid
        if cuuid != dataset_id.uuid:
            msg = f'{cuuid} != {dataset_id.uuid}'
            raise ValueError(msg)  # FIXME error type?

        if updated_transitive != updated_cache_transitive:
            f'{updated} != {updated_cache_transitive} for: {cuuid}'
            raise ValueError(msg)  # FIXME error type?

    def _push_list_and_diff_to_manifest(self, diff, push_list):
        return manifest

    def _push_manifest(self, diff, push_list):
        drp = self.dataset_relative_path
        index = {d['path']:d for d in diff}
        bads = []
        manifest = []
        for p in push_list:
            d = index[p['path']]
            manifest.append(p)
            if d['ops'] != p['ops']:
                bads.append((p, d))

        if bads:
            msg = f'ops mismatch! {bads!r}'
            raise ValueError(msg)  # FIXME error type
        # TODO more features than just copying ... checksums etc.
        return manifest

    def _push_feature_check(self, manifest, allowed_ops):
        # make sure that the changes to the dataset include only those
        # that are supported at the moment e.g. only moves in first version
        desired_ops = set(o for d in manifest for o in d['ops'])
        bads = []
        for d in manifest:
            for o in d['ops']:
                if o not in allowed_ops:
                    bads.append((d['path'], o))

        if bads:
            msg = f'Forbidden ops among selected files!\n{bads}'
            raise ValueError(msg)

    def _write_push_list(self, dataset_id, updated_transitive, diff, paths_to_add):
        # TODO ensure dataset_id matches probably
        push_id = timeformat_friendly(utcnowtz())
        pcd = self._push_cache_dir(updated_transitive, push_id)
        paths = pcd / 'paths.sxpr'

        index = {d['path']:d for d in diff}
        push_list = [{
            'path': p,
            'type': index[p]['type'],
            'ops': index[p]['ops'],  # apparently ops format is wrong according to note in viewer.rkt
        } for p in paths_to_add]
        pl = sxpyr.python_to_sxpr(push_list, str_as_string=True)
        sxpr = pl._print(sxpyr.configure_print_plist(newline_keyword=False))
        pcd.mkdir(parents=True)
        with open(paths, 'wt') as f:
            f.write(sxpr)

        return push_id

    def make_push_manifest(self, dataset_id, updated_transitive, push_id):
        pcd = self._push_cache_dir(updated_transitive, push_id)
        paths = pcd / 'paths.sxpr'
        path_manifest = pcd / 'manifest.sxpr'
        if not paths.exists():
            raise FileNotFoundError(paths)

        if path_manifest.exists():
            # if you encounter this error it usually means that you have a duplicate
            # push-id and should check how they are being generated
            msg = f'Manifest already exists: {manifest}'
            raise exc.PathExistsError(msg)

        id, updated_cache_transitive, diff = self.diff()

        self._ensure_push_match(dataset_id, updated_transitive, updated_cache_transitive)

        # XXX yes we call diff every time and yes it is slow but we
        # have to make sure nothing changed and even then if someone
        # is mucking about we might get in trouble
        with open(paths, 'rt') as f:
            push_list = oa.utils.sxpr_to_python(f.read())

        manifest = self._push_manifest(diff, push_list)
        self._push_feature_check(manifest, ('rename', 'reparent'))

        def serialize_manifest(manifest):
            pl = sxpyr.python_to_sxpr(manifest, str_as_string=True)
            serialized_manifest = pl._print(sxpyr.configure_print_plist(newline_keyword=False))
            return serialized_manifest

        serialized_manifest = serialize_manifest(manifest)
        with open(path_manifest, 'wt') as f:
            f.write(serialized_manifest)

    def push_from_manifest(self, dataset_id, updated_transitive, push_id):
        pcd = self._push_cache_dir(updated_transitive, push_id)
        paths = pcd / 'paths.sxpr'
        path_manifest = pcd / 'manifest.sxpr'
        complete = pcd / 'complete.sxpr'
        done = pcd / 'done.sxpr'
        # pcdz = pcd.with_suffix('.xz')  # TODO ? XXX no, we can do cleanup and compression for these folders on pull when updated changes
        if done.exists():
            # FIXME TODO compress when done? XXX no, because we might want to make it possible to run multiple sync operations ? or no?
            # that is tricky because we download to confirm as well
            msg = f'Push already complete: {done}'
            raise ValueError(msg)  # FIXME error type

        if complete.exists():
            # check the match between complete and manifest to see if we are done
            msg = 'TODO push from manifest and partial complete'
            raise NotImplementedError(msg)

        id, updated_cache_transitive, diff = self.diff()  # FIXME may be able to skip this (see checking > updated below)

        self._ensure_push_match(dataset_id, updated_transitive, updated_cache_transitive)
        with open(path_manifest, 'rt') as f:
            manifest = oa.utils.sxpr_to_python(f.read())

        # XXX additional features to be implemented in the future

        # ensure manifest matches diff subset
        # make a copy of all new files to a safe directory ???? maybe ??? XXX NO

        # TODO as we iterate through the files if we ever detect a file with
        # modified time greater than updated_cache_transtivie

        # XXX FOR NOW ONLY rename and reparent (more strict checks come last)
        self._push_feature_check(manifest, ('rename', 'reparent'))
        #d = manifest[0]
        #breakpoint()
        remotes = []
        # reparent first to avoid mismatched names on symlinks until  XXX not clear this actually matters at all ...
        #operation_order = 'reparent', 'rename'
        for d in manifest:
            local = self / d['path']
            remote = local.remote  # needs augpathlib 28

            remotes.append(remote)
            for op in d['ops']:
                # FIXME this is bad, this is the kind of stuff that should
                # probably go in the manifest directly so that the full record
                # is there or something ???
                if op == 'rename':
                    remote.rename(local.name)
                elif op == 'reparent':
                    new_parent_id = local.parent.cache_id
                    remote.reparent(new_parent_id)
                else:
                    msg = f'Most Impressive. {op}'
                    raise ValueError(msg)

        self._reindex_done_manifest(manifest, remotes)

    def _transitive_changes(self, do_expensive_operations=False):
        _warned = False
        def changedp(c, cache_size, cache_checksum, cache_checksum_cypher=None):
            size = aug.FileSize(c.size)
            if cache_checksum_cypher is None:
                if cache_checksum:
                    nonlocal _warned
                    if not _warned:
                        msg = f'please refetch: file meta missing checksum_cypher for {self}'
                        log.warning(msg)
                        _warned = True

                cypher = self._cache_class.cypher
            else:
                cypher = aug.utils.cypher_lookup[cache_checksum_cypher]
            return (size != cache_size or
                    ((do_expensive_operations or size.mb < 2) and
                     c.checksum(cypher=cypher) != cache_checksum))

        _lattr = self._cache_class._local_xattr
        #tm = self._transitive_metadata(do_expensive_operations=do_expensive_operations)
        #dataset = self.cache.dataset.local
        #updated_cache_transitive = dataset.updated_cache_transitive  # FIXME horribly inefficient
        # FIXME if someone happens to overwrite the most recently changed file (which could easily happen when working on metadata files)
        # then we are hosed and will not be able to find updated_cache_transitive at all ... should probably come from the index itself
        # and we should probably symlink to latest and then if we hit an updated date greater than that we bail out with a warning that
        # the index and the cache are out of sync
        dataset_id, id_name, parent_children, name_id, updated_transitive = self._read_indexes()
        if updated_transitive is None:
            msg = 'index version too old, please pull dataset again'
            raise ValueError(msg)  # FIXME error type

        rchildren = transitive_paths(self)
        nochanges = []
        # move is ambiguous, it could be reparent or rename
        reparents = []  # parent id different
        renames = []  # parent id same but name is different
        changes = []  # usually missing metadata but name exists or size mismatch or checksum mismatch (expensive)
        adds = []  # missing metadata and name does not exist, might be a rename and a change rolled into one
        new_dirs = []  # cases where a new folder is not in the index (will probably error elsewhere first)
        removes = []  # name that was in index for latest pull is now missing
        missing_metadata = []
        ambig = []
        noindexes = []
        _others = reparents, renames, changes, adds, new_dirs, removes, missing_metadata, ambig, noindexes
        rows = []
        # rewrite = missing metadata probably?
        add, reparent, rename, remove, change, nometa, noindex, nochange = (
            'add', 'reparent', 'rename', 'remove', 'change', 'no-metadata', 'no-index', 'no-change')
        #d = self.cache.dataset.local
        updated_cache_transitive = ''
        for c in rchildren:
            # TODO make this its own function probably
            ops = []  # this is probably dumb
            toend = False
            meta_from_local = False
            local_parent_id = c.parent.cache_id
            local_name = c.name

            if c.is_broken_symlink():
                raw_symlink = c.readlink(raw=True)
                symlink = pathlib.PurePosixPath(raw_symlink)

                # XXX hard coded to symlink cache mdv3
                expected_local_name, id, rest = symlink.parts
                #breakpoint()
                fields = rest.split('.')
                md_version = fields[1]
                if md_version >= 'mdv5':
                    fieldsep_esc = aug.meta._PathMetaAsSymlink.fieldsep_esc
                else:
                    fieldsep_esc = '|'

                if md_version == 'mdv3':
                    _i_parent_id, _i_updated, _i_file_id, _i_size, _i_checksum, _i_checksum_cypher, _i_name = (
                        9, 5, 2, 3, 6, None, 14,)
                elif md_version in ('mdv4', 'mdv5'):
                    _i_parent_id, _i_updated, _i_file_id, _i_size, _i_checksum, _i_checksum_cypher, _i_name = (
                        10, 5, 2, 3, 6, 7, 15,)
                elif md_version in ('mdv6',):
                    _i_parent_id, _i_updated, _i_file_id, _i_size, _i_checksum, _i_checksum_cypher, _i_name = (
                        11, 5, 2, 3, 6, 7, 16,)
                else:
                    msg = f'unknown metadata version {md_version}'
                    raise NotImplementedError(msg)

                cache_parent_id = fields[_i_parent_id]
                cache_updated = fields[_i_updated]
                # don't need file_id for symlinks since they can't loose xattrs
                # but DO need it to detect if a new symlink appeared which would
                # be weird, but can happen if the index is not regenerated
                cache_file_id = (
                    int(fields[_i_file_id])
                    if fields[_i_file_id] != '#'
                    else None)  # '#' remote-unavailable error case
                id = id, cache_file_id
                cache_size = fields[_i_size]
                cache_checksum = fields[_i_checksum]
                cache_checksum_cypher = (
                    None if _i_checksum_cypher is None else fields[_i_checksum_cypher])
                cache_name = fields[_i_name].replace(fieldsep_esc, aug.meta._PathMetaAsSymlink.fieldsep)
                if local_name != cache_name:
                    ops.append(rename)
                    renames.append(c)
                if id not in id_name:
                    # we cannot to assume meta from local in this case because no sane person
                    # should be creating symlinks like the ones we use to store metadata
                    ops.append(noindex)
                    noindexes.append(c)
            else:
                xattrs = c.xattrs()
                if xattrs:
                    try:
                        cache_name = xattrs[b'bf.name'].decode()
                    except KeyError as e:
                        msg = (
                            'transitive changes requires bf.name field '
                            'you need to pull the dataset using a newer '
                            'version of sparcur that includes name in meta')
                        log.error(msg)
                        raise e

                    id = xattrs[b'bf.id'].decode()
                    cache_parent_id = xattrs[b'bf.parent_id'].decode()
                    cache_updated = xattrs[b'bf.updated'].decode()
                    meta_from_local = bool(xattrs[_lattr]) if _lattr in xattrs else False
                    if c.is_file():
                        cache_size = int(xattrs[b'bf.size'].decode())
                        cache_checksum = (
                            xattrs[b'bf.checksum']
                            if b'bf.checksum' in xattrs
                            # FIXME if this is missing very bad news all change
                            # the issue of course is that there are a whole bunch of empty files
                            # due to the as yet to be resolved issue with bulk downloads
                            else None)
                        cache_checksum_cypher = (
                            xattrs[b'bf.checksum_cypher'].decode()
                            if b'bf.checksum_cypher' in xattrs
                            else None)
                        cache_file_id = int(xattrs[b'bf.file_id'].decode())
                        id = id, cache_file_id

                else:
                    cache_updated = ''

                if not xattrs or id not in id_name:
                    # id not in id_name is the
                    # XXX case where we might have materialized local metadata prior to running this
                    # or if we had a failure before writing to disk ... which we can detect by
                    # having our index use the dataset modified time or transitive modified time
                    # for the file name
                    ops.append(nometa)
                    missing_metadata.append(c)
                    if local_parent_id not in parent_children:
                        # the parent is a new folder, so cross reference this
                        new_dirs.append(c.parent)
                        if c.parent.name in name_id:
                            pcands = name_id[c.parent.name]

                        if local_name in name_id:
                            cands = name_id[local_name]

                        ops.append('add-or-rename')
                        ambig.append(c)
                        #adds.append(c)
                    elif old_id := [id for id in parent_children[local_parent_id] if id_name[id] == local_name]:
                        if len(old_id) > 1:
                            msg = f'multiple files with the same name and parent? {old_id}'
                            log.warning(msg)

                        id, file_id = old_id[0]
                        uuid = self._cache_class._id_class(id).uuid
                        ck = self._cache_class._cache_key(uuid, file_id)
                        ocp = self.cache.local_objects_dir / ck
                        if ((noe := not ocp.exists()) or
                            changedp(c, cache_size, cache_checksum, cache_checksum_cypher)):
                            if noe:
                                log.warning(f'old_id found but no object cache? {c} {old_id}')
                            ops.extend((change, {'old-id': old_id[0]}))
                            changes.append(c)
                        else:
                            ops.append(nochange)
                            nochanges.append(c)

                        toend = True
                        #if ocp.exists():  # XXX note that this does not exist in our current testing setup
                        #    # might have lost metadata by accident but without real changes
                        #    # TODO checksum
                        #    if changedp(c, cache_size, cache_checksum):
                        #    else:
                        #        ops.append('missing_metadata')
                        #else:
                        #    log.warning(f'old_id found but no object cache? {c} {old_id}')
                        #    ops.extend(('change', ':old-id', old_id[0]))
                        #    changes.append(c)

                    else:  # local pid is known but no old id with a matching name
                        # note: missing-metadata + add could mean missing-metadata + rename
                        # so we do still have to check at the end to see if there are files
                        # in the index that are unaccounted for
                        ops.append('add-or-rename')
                        ambig.append(c)
                        #adds.append(c)  # can't say this for sure yet
                        pass
                        # TODO at the end we have to check to see if we have anything missing
                        # from the index and then we can resolve whether this is a true add or
                        # a rename move based on user input (probably)
                    if not xattrs:
                        toend = True
                elif c.name != id_name[id]:
                    ops.append(rename)
                    renames.append(c)
                else:
                    # name matches name in index so do nothing
                    pass

            # common

            if not toend:
                if not meta_from_local:
                    if cache_updated >= updated_cache_transitive:
                        updated_cache_transitive = cache_updated

                    if cache_updated > updated_transitive:
                        # we have to double test here because there will surely be cases where
                        # paths are traversed not in updated order, in which case any older paths
                        # will not produce a warning if we were to run this additional test only
                        # in cases where cache_updated > updated_cache_transitive
                        # FIXME must also check to see if this is local meta
                        msg = f'Newer than index latest! {c}'
                        # the error will be raised at the end, need this for debug
                        log.warning(msg)

                if local_parent_id != cache_parent_id:
                    ops.append(reparent)
                    reparents.append(c)

                if c.is_file():
                    # FIXME abstract determining cypher ...
                    if cache_checksum_cypher is None:
                        if cache_checksum:
                            if not _warned:
                                msg = f'please refetch: file meta missing checksum_cypher for {self}'
                                log.warning(msg)
                                _warned = True

                        cypher = self._cache_class.cypher  # XXX can't use None here
                    else:
                        cypher = aug.utils.cypher_lookup[cache_checksum_cypher]

                    size = aug.FileSize(c.size)
                    if (size != cache_size or
                        ((do_expensive_operations or size.mb < 2) and
                        c.checksum(cypher=cypher) != cache_checksum)):
                        ops.append(change)  # racket side must handle this
                        changes.append(c)

            if not ops:
                ops.append(nochange)  # lol
                nochanges.append(c)

            path_type = (
                'dir' if c.is_dir() else ('link' if c.is_symlink() else 'file'))
            rows.append((c, path_type, cache_updated, ops))

        return (
            rows, _others,
            dateparser.parse(updated_cache_transitive),
            dateparser.parse(updated_transitive),
            dataset_id,
        )

    def _upload_raw(self):
        remote, old_remote = (
            self
            ._cache_class
            ._remote_class
            ._stream_from_local_raw(self))

    def upload(self, replace=True, local_backup=False):
        # FIXME we really need a staging area ...
        remote, old_remote = (
            self
            ._cache_class
            ._remote_class
            ._stream_from_local(self,
                                replace=replace,
                                local_backup=local_backup))

        if True:  # local_backup = True:  # force True until we can get remote checksums
            # FIXME there must be a better way to do this ...
            object_path = self._cache_class._anchor.local_objects_dir / remote.cache_key
            # TODO possibly use _data_setter to calculate the hash at the same time?
            if not object_path.parent.exists():
                # XXX FIXME multi-level cache keys are a ticking timebomb
                object_path.parent.mkdir(parents=True)

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

        # FIXME hitting remote-unavailable errors :/
        return remote

    def create_remote(self):
        if self.is_file():
            return self.upload()
        elif self.is_dir():
            return self.mkdir_remote()
        else:
            msg = f"don't know how to create remote equivalent of {self}"
            raise NotImplementedError(msg)


Path._bind_flavours()
register_type(Path, 'path')


class RemoteL:

    _remote_type = 'local'
    _cache_anchor = None

    def __new__(cls, id, *args, **kwargs):
        return cls._local_class(id.split(':', 1)[1])

    @classmethod
    def anchorToCache(cls, cache, *args, **kwargs):
        cls._cache_anchor = cache


class CacheL(aug.caches.ReflectiveCache, EatCache):

    _id_class = LocId

    _remote_class = RemoteL
    _asserted_anchor = None
    _dataset_dirs = []

    _xattr_fs_version = BFPNCacheBase._xattr_fs_version
    _jsonMetadata = BFPNCacheBase._jsonMetadata
    populateJsonMetadata = BFPNCacheBase.populateJsonMetadata
    cypher = BFPNCacheBase.cypher

    def _type(self):
        if self.is_file():
            return 'package'
        elif self.is_dir():
            if 'organization' in self.id:
                # have to do this to prevent recursion
                return 'organization'
            elif self == self._asserted_anchor:
                return 'organization'
            elif self in self._dataset_dirs or self.parent.is_organization():
                return 'dataset'
            else:
                return 'collection'
        elif not self.exists():
            log.warning(f'File should exist? {self!r}!')
            raise FileNotFoundError(self)
        else:
            msg = f"unsupported inode type at path\n'{self.as_posix()}'"
            raise TypeError(msg)

    @property
    def identifier(self):
        return self._id_class(self.id, self._type())

    @property
    def uri_api(self):
        return self.identifier.uri_api

    @property
    def uri_human(self):
        return self.identifier.uri_human(
            dataset=self.dataset,
            organization=self.anchor)

    @property
    def anchor(self):
        if self._asserted_anchor is not None:
            return self._asserted_anchor
        elif self.identifier.type == 'organization':
            return self
        elif self.parent != self:
            return self.parent.anchor
        else:
            raise exc.NotInProjectError(f'{self} is not in a project!')

    dataset = BlackfynnCache.dataset

    dataset_id = BlackfynnCache.dataset_id

    def is_organization(self):
        return self._type() == 'organization'

    def is_dataset(self):
        return self._type() == 'dataset'

    _fs_version = BFPNCacheBase._fs_version
    _set_fs_version = BFPNCacheBase._set_fs_version


CacheL._bind_flavours()


class PathL(Path):

    # FIXME this seems like a good idea until you realize some of the
    # other classes that we built inherit from Path, because it
    # simplifies the implementation, but we actually want the type
    # specialization to depend on the type that we passed to it, which
    # requires modifying new to match the constructed class, sigh XXX
    # the solution is to overwrite the cache class on instances that
    # are subclasses of Path during construction, I think that is the
    # only way to be sure, clunky and aweful, but unavoidable

    _cache_class = CacheL
    # setting remote class is required to avoid issue with
    # PennsieveRemote -> Cache/Path instead of to CacheL/PathL

    def find_cache_root(self):
        # straight forward when the anchor is asserted
        return self.cache.anchor

    @property
    def cache_identifier(self):
        return self.cache.identifier

    @property
    def project_relative_path(self):
        return self.relative_path_from(self.cache.anchor.local)


PathL._bind_flavours()
CacheL._local_class = PathL
RemoteL._local_class = PathL


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

def bind_defaults(Remote, Cache, Path=Path):
    Cache._local_class = Path
    Remote._new(Path, Cache)
    if hasattr(Cache, 'cache_key'):
        Remote.cache_key = Cache.cache_key
    if hasattr(Cache, '_sparse_stems'):
        Remote._sparse_stems = Cache._sparse_stems
    if hasattr(Cache, '_sparse_exts'):
        Remote._sparse_exts = Cache._sparse_exts
    if hasattr(Cache, '_sparse_exts'):
        Remote._sparse_include = Cache._sparse_include


bind_defaults(backends.BlackfynnRemote, BlackfynnCache)
# XXX supersedes the binding on BlackfynnRemote
bind_defaults(backends.PennsieveRemote, PennsieveCache)
bind_defaults(backends.PennsieveDiscoverRemote, PennsieveDiscoverCache, Path=DiscoverPath)
