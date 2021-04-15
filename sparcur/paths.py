import logging
import hashlib
import pathlib
from functools import wraps
from itertools import chain
import augpathlib as aug
from dateutil import parser
from augpathlib import PrimaryCache, EatCache, SqliteCache, SymlinkCache
from augpathlib import RepoPath, LocalPath
from sparcur import backends
from sparcur import exceptions as exc
from sparcur.utils import log, logd, GetTimeNow, register_type, unicode_truncate
from sparcur.utils import transitive_dirs, transitive_paths, is_list_or_tuple
from sparcur.utils import BlackfynnId, LocId, PennsieveId
from sparcur.config import auth


# http://fileformats.archiveteam.org/wiki/Main_Page
# https://www.nationalarchives.gov.uk/PRONOM/Format/proFormatSearch.aspx?status=new
# FIXME case senstivity
suffix_mimetypes = {
    ('.jpx',): 'image/jpx',
    ('.jp2',): 'image/jp2',

    # sequencing
    ('.fq',):          'application/fastq',  # wolfram lists chemical/seq-na-fastq which is overly semantic
    ('.fq', '.gz'):    'application/x-gz-compressed-fastq',
    ('.fastq',):       'application/fastq',
    ('.fastq', '.gz'): 'application/x-gz-compressed-fastq',
    ('.bam',):         'application/vnd.hts.bam',

    ('.mat',):         'application/x-matlab-data',  # XXX ambiguous, depending on the matlab version
    ('.m',):           'application/x-matlab',

    ('.nii',):       'image/nii',
    ('.nii', '.gz'): 'image/gznii',  # I'm not sure that I believe this

    # http://ced.co.uk/img/TrainDay.pdf page 7
    ('.smr',):       'application/vnd.cambridge-electronic-designced.spike2.32.data',
    ('.smrx',):      'application/vnd.cambridge-electronic-designced.spike2.64.data',

    ('.s2r',):       'application/vnd.cambridge-electronic-designced.spike2.resource',
    ('.s2rx',):      'application/vnd.cambridge-electronic-designced.spike2.resource+xml',

    ('.abf',):       'application/vnd.axon-instruments.abf',

    ('.nd2',):       'image/vnd.nikon.nd2',
    ('.czi',):       'image/vnd.zeiss.czi',
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


class BlackfynnCache(PrimaryCache, EatCache):

    xattr_prefix = 'bf'
    _backup_cache = SqliteCache
    _not_exists_cache = SymlinkCache

    cypher = hashlib.sha256  # set the remote hash cypher on the cache class

    _suffix_mimetypes = suffix_mimetypes

    uri_human = backends.BlackfynnRemote.uri_human
    uri_api = backends.BlackfynnRemote.uri_api

    _id_class = BlackfynnId

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
    def identifier(self):
        ntid = self.id
        if ntid is None:
            raise exc.NoCachedMetadataError(self)

        return self._id_class(ntid, file_id=self.file_id)

    @property
    def _fs_safe_id(self):
        id = self.identifier
        return id.type[0] + '-' + id.uuid

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
        # FIXME file system safe
        id = self.identifier
        uuid = id.uuid
        return f'{uuid[:2]}/{uuid}-{self.file_id}'

    def _dataset_metadata(self, force_cache=False):
        """ get metadata about a dataset from the remote metadata store """
        # TODO figure out the right default for force_cache
        dataset = self.dataset
        if dataset == self:
            if not hasattr(self, '_c__dataset_metadata'):
                bdd = backends.BlackfynnDatasetData(self)
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
            locsize = self.local_object_cache_path.size
            if locsize != meta.size:
                raise NotImplementedError('TODO yield from local then fetch the rest starting at offset')

            gen = chain((f'from local cache {self.local_object_cache_path}',),
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

        log.debug(self.data_headers)
        if self.local_object_cache_path.exists():
            yield from gen
        else:
            # FIXME we MUST write the metadata first so that we know the expected size
            # so that in the event that the generator is only partially run out we know
            # that we can pick up where we left off with the fetch, this also explains
            # why all the cases where the cached data size did not match were missing
            # xattrs entirely
            if not self.local_object_cache_path.parent.exists():
                # FIXME sigh, no obvious way around having to check
                # every time other than creating all the cache
                # subfolders in advance
                self.local_object_cache_path.parent.mkdir()

            self.local_object_cache_path.touch()
            self.local_object_cache_path.cache_init(meta)

            yield from self.local_object_cache_path._data_setter(gen)

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
        """ path (cache) level json metadata """
        # FIXME this is going to be very slow due to the local.cache call overhead
        # it will be much better implement this from Path directly using xattrs()
        d = self.dataset
        _, bf_ds_id = d.id.split(':', 1)  # split off the N:
        l = self.local
        drp = l.relative_to(d.local)
        meta = self.meta  # TODO see what we need from this
        _, bf_id = self.id.split(':', 1)  # FIXME SODA use case
        uri_api = self.uri_api  # FIXME vs self.uri_api_package ?
        uri_human = self.uri_human
        blob = {
            'type': 'path',
            'dataset_id': bf_ds_id,
            'dataset_relative_path': drp,
            'uri_api': uri_api,  # -> @id in the @context
            'uri_human': uri_human,
            'remote_id': (f'{bf_id}/files/{meta.file_id}'  # XXX a horrible hack, but it kinda works
                          if self.is_file() or self.is_broken_symlink() else
                          bf_id),
            }
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
            blob['magic_mimetype'] = asdf

        return blob


BlackfynnCache._bind_flavours()


class PennsieveCache(BlackfynnCache):

    xattr_prefix = 'pn'  # FIXME TODO we will likely need an xattr conversion fix

    uri_human = backends.PennsieveRemote.uri_human
    uri_api = backends.PennsieveRemote.uri_api

    _id_class = PennsieveId


PennsieveCache._bind_flavours()


class Path(aug.XopenPath, aug.RepoPath, aug.LocalPath):  # NOTE this is a hack to keep everything consistent
    """ An augmented path for all the various local
        needs of the curation process. """

    _cache_class = BlackfynnCache

    _dataset_cache = {}

    _suffix_mimetypes = suffix_mimetypes

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
        return self.relative_path_from(self._cache_class._anchor.local)

    @property
    def dataset_relative_path(self):
        # FIXME broken for local validation
        return self.relative_path_from(self.cache.dataset.local)

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


    xattr_prefix = BlackfynnCache.xattr_prefix
    _xattr_meta = aug.EatCache.meta
    _symlink_meta = aug.SymlinkCache.meta

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

    @staticmethod
    def _uri_api_bf(id):  # further SIGH
        if 'N:dataset:' in id:
            endpoint = 'datasets/' + id
        elif 'N:organization:' in id:
            endpoint = 'organizations/' + id
        elif 'N:collection:' in id:
            endpoint = 'collections/' + id
        else:
            endpoint = 'packages/' + id  # XXX if you use N:package:asdf/files/file-id this works

        return backends.BlackfynnRemote._base_uri_api + '/' + endpoint  # XXX FIXME SIGH

    @staticmethod
    def _uri_human_bf(oid, did, id):
        N, type, suffix = id.split(':')
        if id.startswith('N:package:'):
            #prefix = '/viewer/'
            prefix = '/files/00000000-0000-0000-0000-000000000000/'  # unprocessed cases
        elif id.startswith('N:collection:'):
            prefix = '/files/'
        elif id.startswith('N:dataset:'):
            prefix = '/'  # apparently organization needs /datasets after it
            return Path._uri_human_bf(None, None, oid) + prefix + id
        elif id.startswith('N:organization:'):
            return f'{backends.BlackfynnRemote._base_uri_human}/{id}/datasets'  # XXX FIXME SIGH
        else:
            raise exc.UnhandledTypeError(type)

        return Path._uri_human_bf(oid, None, did) + prefix + id

    def _cache_jsonMetadata(self, do_expensive_operations=False):
        blob, project_path = self._jm_common(do_expensive_operations=do_expensive_operations)
        project_meta = project_path.cache_meta
        meta = self.cache_meta  # FIXME this is very risky
        # and is the reason why I made it impossible to contsturct
        # cache classes when no cache was present, namely that if
        # there is no cache then PathMeta will still return the
        # correct structure, but it will be empty, which is bad
        if meta.id is None:
            raise exc.NoCachedMetadataError(self)

        _, bf_id = meta.id.split(':', 1)  # FIXME SODA use case
        remote_id = (f'{bf_id}/files/{meta.file_id}'  # XXX a horrible hack, but it kinda works
                     if self.is_file() or self.is_broken_symlink() else
                     bf_id)
        uri_api = self._uri_api_bf('N:' + remote_id)
        uri_human = self._uri_human_bf(project_meta.id,
                                       'N:' + blob['dataset_id'],
                                       # XXX NOTE that we can't use remote_id because
                                       # /viewer/ etc. doesn't work for the human uri
                                       meta.id)
        blob['remote_id'] = remote_id
        #blob['timestamp_created'] = meta.created  # leaving this out
        blob['timestamp_updated'] = meta.updated  # needed to simplify transitive update
        blob['uri_api'] = uri_api
        blob['uri_human'] = uri_human

        if meta.size is not None:
            blob['size_bytes'] = meta.size

        if (self.is_file() or self.is_broken_symlink()) and meta.checksum is not None:
            # FIXME known checksum failures !!!!!!!
            blob['checksums'] = [{'type': 'checksum',
                                  # FIXME cypher should ALWAYS travel with checksum
                                  # not be implicit and based on the implementation
                                  'cypher': self._cache_class.cypher.__name__.replace('openssl_', ''),
                                  'hex': meta.checksum.hex(),}]

        return blob

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
            project_path = Path('/')  # FIXME FIXME
            log.critical(f'No dataset path found for {self}!')
            #raise NotImplementedError('current implementation cant anchor with current info')

        dataset_path = [p for p in chain((self,), self.parents) if p.parent == project_path][0]
        drp = self.relative_path_from(dataset_path)  # FIXME ...


        # XXX FIXME handle case where there is no cache
        _, bf_ds_id = dataset_path.cache_id.split(':', 1)  # split off the N:

        blob = {
            'type': 'path',
            'dataset_id': bf_ds_id,
            'dataset_relative_path': drp,
            'basename': self.name,  # for sanity's sake
        }

        mimetype = self.mimetype
        if mimetype:
            blob['mimetype'] = mimetype

        if do_expensive_operations:
            blob['magic_mimetype'] = asdf

        if not (self.is_broken_symlink() or self.exists()):
            msg = f'Path does not exist! {self}'
            # do not log the error here, we won't have
            # enough context to know where we got a bad
            # path, but the caller should, maybe a case for
            # inversion of control here
            blob['errors'] = [{'message': msg}]

        return blob, project_path

    def _jsonMetadata(self, do_expensive_operations=False):
        blob, project_path = self._jm_common(do_expensive_operations=do_expensive_operations)
        return blob

    def _transitive_metadata(self):
        def hrm(p):
            try:
                cjm = p._cache_jsonMetadata()
            except exc.NoCachedMetadataError:
                # FIXME TODO figure out when to log this
                # probably only log when there is a dataset id
                return
            # this is not always needed so it is not included by default
            # but we do need it here for when we generate the rdf export
            p = cjm['dataset_relative_path'].parent
            if p.name != '':  # we are at the root of the relative path aka '.'
                cjm['parent_drp'] = p
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
        #pid = self._uri_api_bf(self.cache_id)
        pid = this_metadata[0]['remote_id']
        for m in metadata:
            p = m.pop('parent_drp', None)
            if p in index:
                m['parent_id'] = index[p]['remote_id']
            else:
                m['parent_id'] = pid

        return metadata

    @classmethod
    def _file_type_status_lookup(cls):
        import json  # FIXME
        if not hasattr(cls, '_sigh_ftslu'):
            resources = auth.get_path('resources')
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
                msg = f'unknown mimetype {path_meta["basename"]}'
                dsrp = path_meta['dataset_relative_path']
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

    def _pull_dataset_internal(self, time_now):
        cache = self.cache
        if cache.is_organization():
            raise TypeError('can\' use this method on organizations')
        elif not cache.is_dataset():
            return self.dataset._pull_dataset_internal(time_now)

        children = list(self.children)
        if not children:
            return list(self.cache.rchildren)  # XXX actual pull happens here

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

            # if mkdir succeeds then any failure that happens between
            # then and the point where we swap needs to move the folder
            # to a unique path marked as an error so that future calls
            # to this function are not stopped by an existing directory
            # that failed to be cleaned up
            upstream.setxattrs(working.xattrs())  # handy way to copy xattrs
            # pull and prep for comparison of relative paths
            upstream_c_children = list(upstream.cache.rchildren)  # XXX actual pull happens here
            upstream_children = [c.local.relative_to(upstream)
                                 for c in upstream_c_children]
            working_children = [c.relative_to(working)
                                for c in working.rchildren]
        except BaseException as e:
            error_suf = f'{time_now.START_TIMESTAMP_LOCAL_SAFE}-ERROR'
            upstream.rename(upstream.parent / (upstream.name + error_suf))
            # TODO do we need to log here or can we log up the stack?
            raise e

        # FIXME TODO comparison/diff and support for non-curation workflows

        # replace the working tree with the upstream
        # FIXME this has nasty and confusing consequences if
        # working == aug.AugmentedPath.cwd()
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
register_type(Path, 'path')


class CacheL(aug.caches.ReflectiveCache):

    _id_class = LocId

    _asserted_anchor = None
    _dataset_dirs = []

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
        else:
            raise TypeError('unsupported inode type')

    @property
    def identifier(self):
        return self._id_class(self.id, self._type())

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


PathL._bind_flavours()
CacheL._local_class = PathL


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
