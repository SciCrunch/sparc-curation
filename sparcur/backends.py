import os
import json
from pathlib import PurePosixPath, PurePath
from dateutil import parser as dateparser
import idlib
import augpathlib as aug
from augpathlib import PathMeta
from pyontutils.utils import Async, deferred
from sparcur import exceptions as exc
from sparcur.utils import log, logd, BlackfynnId, PennsieveId
from sparcur.config import auth
from urllib.parse import urlparse


def base_get_file_by_url(cls, url, ranges=tuple(), yield_headers=True):
    """ NOTE THAT THE FIRST YIELD IS HEADERS
    valid ranges are (start,) (-start,) (start, end)
    """

    def make_range_spec(ranges):
        return ', '.join(
            (str(r[0]) if r[0] < 0 else f'{r[0]}-')
            if len(r) == 1 else '-'.join(str(se) for se in r)
            for r in ranges)

    kwargs = {}
    if ranges:
        # TODO validate probably
        range_spec = make_range_spec(ranges)
        kwargs['headers'] = {'Range': f'bytes={range_spec}'}

    resp = cls._requests.get(url, stream=True, **kwargs)
    headers = resp.headers
    if yield_headers:
        yield headers

    log.log(9, f'reading from {url}')  # too much for debug
    # FIXME the failure case for this is really bad because we cannot
    # resume mid way and have to restart from scratch unless we count
    # chunks and do math here
    chunk_size = 4096  # FIXME align chunksizes between local and remote
    try:
        i = None  # if you fail on the first chunk and don't have i = 0, very bad time
        for i, chunk in enumerate(resp.iter_content(chunk_size=chunk_size)):
            if chunk:
                yield chunk
    except cls._requests.exceptions.ChunkedEncodingError as e:
        log.critical('pennsieve connection broken causing ChunkedEncodingError')
        bytes_read = 0 if i is None else chunk_size * (i + 1)
        # XXX NOTE we DO NOT ADD 1 to the bytes_read when calculating
        # the range because bytes read is alread 1 + the index of the
        # last byte read, so using it directly starts at the right place
        if ranges:
            if len(ranges) == 1 and len(ranges[0]) == 1:
                # a start only case
                ((current_start,),) = ranges
                ranges = ((current_start + bytes_read,),)
            else:
                # the full complexity is not straight forward to implement
                # and requires requires couting the number of bytes in each
                # range, making sure that the range starts are sequential
                # which is not possible in the general case ... etc.
                msg = 'TODO'
                raise NotImplementedError(msg)
        else:
            ranges = ((bytes_read,),)

        yield from cls.get_file_by_url(url, ranges, yield_headers=False)


def base_data(self):
    uri_file = self._uri_file()
    if uri_file is None:
        return

    gen = self.get_file_by_url(uri_file)
    try:
        self.data_headers = next(gen)
    except exc.NoRemoteFileWithThatIdError as e:
        raise FileNotFoundError(f'{self}') from e

    yield from gen


class BlackfynnRemote(aug.RemotePath):

    _remote_type = 'blackfynn'
    _translation_version = 1  # XXX increment if O,D,C,P,F -> D,F translation changes
    _id_class = BlackfynnId
    _api_class = None  # set in _setup
    _async_rate = None
    _local_dataset_name = object()

    _exclude_uploaded = False

    _base_uri_human = 'https://app.blackfynn.io'  # FIXME hardcoded
    _base_uri_api = 'https://api.blackfynn.io'  # FIXME hardcoded
    #_base_uri  # FIXME TODO

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    _renew = __new__

    def __new__(cls, *args, **kwargs):
        cls._setup(*args, **kwargs)
        return super().__new__(cls)

    @classmethod
    def init(cls, *args, **kwargs):
        cls._setup(*args, **kwargs)
        super().init(*args, **kwargs)

# XXX it is not actually meaningful to set fs version at project level since
# it is only meaningful to set it when we call pull at the dataset level
#    @classmethod
#    def dropAnchor(cls, parent_path=None):
#        cache_anchor = super().dropAnchor(parent_path=parent_path)
#        cache_anchor._set_fs_version(cls._translation_version)
#        return cache_anchor

    @staticmethod
    def _setup(*args, **kwargs):
        log.warning('blackfynn is long gone')

    @property
    def uri_human(self):
        # org /datasets/ N:dataset /files/ N:collection
        # org /datasets/ N:dataset /files/ wat? /N:package  # opaque but consistent id??
        # org /datasets/ N:dataset /viewer/ N:package

        if self.is_organization():
            return self.identifier.uri_human()
        else:
            oid = self.organization.identifier
            if self.is_dataset():
                return self.identifier.uri_human(organization=oid)
            else:
                did = self._id_class(self.dataset_id)
                return self.identifier.uri_human(organization=oid,
                                                 dataset=did)

    @property
    def uri_api(self):
        return self.identifier.uri_api

    @property
    def errors(self):
        yield from self._errors
        if self.remote_in_error:
            yield 'remote-error'
        if self.state == 'UNAVAILABLE':
            yield 'remote-unavailable'

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
            msg = f'{package} has no file with id {file_id} but has:\n{files}'
            raise FileNotFoundError(msg)

    @classmethod
    def get_file_url(cls, id, file_id):
        if file_id is not None:
            return cls._api.get_file_url(id, file_id)

    @classmethod
    def get_file_by_id(cls, id, file_id, ranges=tuple(), expect=None):
        url = cls.get_file_url(id, file_id)

        if expect is not None:
            up = urlparse(url)
            pp = PurePath(up.path)
            if pp.name != expect.name:
                # probably gonna hit a file size mismatch error because
                # we are about to download the wrong file due to a race
                msg = f'url.path.name != expect.name: {pp.name} != {expect.name}'
                log.critical(msg)

        log.log(9, f'file-api-mapping: {id} {file_id} {url}')
        yield from cls.get_file_by_url(url, ranges=ranges)

    get_file_by_url = classmethod(base_get_file_by_url)

    def __init__(self, id_bfo_or_bfr, *, file_id=None, cache=None, local_only=False):
        self._seed = id_bfo_or_bfr
        if isinstance(self._seed, self._id_class):
            if self._seed.file_id is not None:
                if file_id is not None:
                    if self._seed.file_id != file_id:
                        msg = f'file_id mismatch! {self._seed.file_id} != {file_id}'
                        raise ValueError(msg)

                file_id = self._seed.file_id

        self._file_id = file_id
        if not [type_ for type_ in (self.__class__,
                                    self._BaseNode,
                                    str,
                                    PathMeta,
                                    self._id_class,)
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

        elif isinstance(self._seed, self._BaseNode):
            bfobject = self._seed

        elif (isinstance(self._seed, str) or
              isinstance(self._seed, self._id_class)):
            if isinstance(self._seed, self._id_class):
                _seed = self._seed.id  # FIXME sadly this is the least bad place to do this :/
            else:
                _seed = self._seed

            try:
                bfobject = self._api.get(_seed)
            except Exception as e:  # sigh
                if self._local_only:
                    _class = self._id_to_type(_seed)
                    if issubclass(_class, self._Dataset):
                        bfobject = _class(self._local_dataset_name)
                        bfobject.id = _seed
                    else:
                        raise NotImplementedError(f'{_class}') from e
                elif (not hasattr(self, '_api') and
                      _seed.startswith('N:organization:')):
                    self.__class__.init(_seed)
                    return self.bfobject
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

        if isinstance(bfobject, self._DataPackage):
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
                        msg = f'There are actually multiple files ...\n{files}'
                        log.critical(msg)

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
            self.__class__._organization = self.__class__(
                self._api.organization)

        return self._organization

    def is_organization(self):
        return isinstance(self.bfobject, self._Organization)

    def is_dataset(self):
        return isinstance(self.bfobject, self._Dataset)

    @property
    def dataset_id(self):
        """ save a network transit if we don't need it """
        if self.is_dataset():
            dataset = self
        else:
            dataset = self._bfobject.dataset

        if isinstance(dataset, str):
            return dataset
        else:
            return dataset.id

    @property
    def dataset(self):
        if self.is_dataset():
            return self

        remote = self.__class__(self.dataset_id)
        remote.cache_init()
        return remote

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

    @property
    def suffix(self):
        # FIXME loads of shoddy logic in here
        name = PurePosixPath(self._name)
        if isinstance(self.bfobject, self._File) and not self.from_packages:
            return name.suffix
        elif isinstance(self.bfobject, self._Collection):
            return name.suffix
        elif isinstance(self.bfobject, self._Dataset):
            return name.suffix
        elif isinstance(self.bfobject, self._Organization):
            return name.suffix
        else:
            if self.from_packages or not list(self.errors):
                # still needed for cache.children when remote data is in error
                msg = 'should not be needed anymore when using packages'
                raise NotImplementedError(msg)
            else:
                log.warning(f'suffix needed for some reason on {self.uri_api}')

            if hasattr(self.bfobject, 'type'):
                type = self.bfobject.type.lower()  # FIXME ... can we match s3key?
            else:
                type = None

            not_ok_types = ('unknown', 'unsupported', 'generic', 'genericdata')
            if type not in not_ok_types:
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
        if isinstance(self.bfobject, self._File) and not self.from_packages:
            realname = os.path.basename(self.bfobject.s3_key)
            if name != realname:  # mega weirdness
                if realname.startswith(name):
                    # FIXME also check for other things that match?
                    name = realname
                else:
                    realpath = PurePath(realname)
                    namepath = PurePath(name)
                    if realname.count('_') > name.count('_'):
                        if namepath.suffixes == realpath.suffixes:
                            # something was normalized in the s3 key but
                            # we still have the suffix
                            pass
                        else:
                            # normalized and suffixes do not match
                            log.critical(f'norm {namepath!r} -?-> {realpath!r}')
                    elif namepath.suffixes:
                        log.critical(f'sigh {namepath!r} -?-> {realpath!r}')

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
            logd.critical(f'Forward slash in name for {self}')
            name = name.replace('/', '_')
            self.bfobject.name = name  # AND DON'T BOTHER US AGAIN

        if name.endswith(' '):  # breaks paths on windows
            self._errors.append(f'trailing whitespace in {name!r}')
            log.critical(f'Trailing whitespace in name for {self}')
            name = name.rstrip()
            self.bfobject.name = name  # AND DON'T BOTHER US AGAIN

        return name

    @property
    def name(self):
        bfo = self.bfobject
        filename = None
        if isinstance(bfo, self._File) and self.from_packages:
            #breakpoint()
            # at some point in time the blackfynn process that dumps
            # data into whatever is returned by the /packages endpoint
            # changed without warning, so there are two different
            # schemas implicit in the data returned by the endpoint
            # the way we work around the issue is to compare filename
            # and name to see if name has a suffix and whether the
            # suffixes match, there is a performance cost due to
            # having to repeatedly check the same files over and over
            # again, but at least this way we can determine whether
            # there is a fixed date that we could use as a proxy
            f = bfo.filename
            n = bfo.name
            pf = PurePath(f)
            pn = PurePath(n)
            sf = pf.suffix
            sn = pn.suffix
            filename = f
            real_out_name = n
            if (sf and not sn or
                sn and sf != sn or
                not sf and not sn):
                #name.suffix
                #name.suffix.suffix

                #name.other          <<<   this case is annoying
                #name.other.suffix
                out = f #return f
            #elif sf == sn and len(pf.suffixes) > 1:
                #return n
            else:
                if not hasattr(self, '_nps_log'):
                    msg = f'New /packages schema detected {self!r}'
                    log.log(9, msg)  # too verbose for debug
                    self._nps_log = True

                out = n #return n

        elif isinstance(bfo, self._File) and hasattr(bfo.package, '_json'):
            # this is the case where we had to get files for a package
            # directly because the packages endpoint did not include any files
            f = bfo.name  # XXX this is inverted bfo.name when bfo is a package >_<
            n = bfo.package.name
            out = f  # trigger the log critical, for real we want the package name
            filename = f
            real_out_name = n
            if filename != real_out_name and not hasattr(self, '_ni_logged_1'):
                msg = f'package-missing-files-name-issue {real_out_name} != {filename}'
                log.log(9, msg)
                self._ni_logged_1 = True
        else:
            out = self.stem + self.suffix #return self.stem + self.suffix
            real_out_name = bfo.name

        if out != real_out_name and not hasattr(self, '_log_name_mismatch'):
            # sed 's/\(bfo.name\|out\|filename\|uri_human\): /\n\1: /g'  # or grep -A 4 ?
            # have to use real_out_name because
            # File.name -> Package.filename and not File.package.name
            # see old discussion of the inhomogneous nature of the design
            msg = (f'name-check '
                   f'real_out_name: {real_out_name!r} '
                   f'out:      {out!r} '
                   f'filename: {filename!r} '
                   f'uri_human: {self.uri_human}')
            logd.critical(msg)
            self._log_name_mismatch = True

        return real_out_name

    @property
    def id(self):
        if isinstance(self._seed, self.__class__):
            id = self._seed.bfobject.id

        elif isinstance(self._seed, self._BaseNode):
            if isinstance(self._seed, self._File):
                id = self._seed.pkg_id
            else:
                id = self._seed.id

        elif isinstance(self._seed, str):
            id = self._seed

        elif isinstance(self._seed, PathMeta):
            id = self._seed.id

        elif isinstance(self._seed, self._id_class):
            id = self._seed.id

        else:
            raise TypeError(self._seed)

        # FIXME we can't return BlackfynnId here due to the fact that
        # augpathlib assumes that RemotePath.id is a string
        return str(id)

    @property
    def identifier(self):
        return self._id_class(self.id)

    @property
    def doi(self):
        try:
            blob = self.bfobject.doi
            #print(blob)
            if blob:
                return idlib.Doi(blob['doi'])
        except exc.NoRemoteFileWithThatIdError as e:
            # FIXME crumping datasets here is bad, but so is going to network for this :/
            log.exception(e)
            if self.cache is not None and self.cache.exists():
                self.cache.crumple()

    @property
    def size(self):
        if isinstance(self.bfobject, self._File):
            return self.bfobject.size

    @property
    def created(self):
        if not isinstance(self.bfobject, self._Organization):
            return self.bfobject.created_at

    @property
    def updated(self):
        if not isinstance(self.bfobject, self._Organization):
            if not hasattr(self, '_cache_updated'):
                if hasattr(self.bfobject, '_max_updated'):
                    # sane increasing modified dates for packages
                    updated_at = self.bfobject._max_updated
                else:
                    updated_at = self.bfobject.updated_at

                # string comparison of dates fails if the padding
                # is different, how extremely unfortunate :/
                if '.' in updated_at:
                    dt, sZ = updated_at.rsplit('.', 1)
                else:
                    # when someone hits the jackpot aka why you should
                    # never truncate your iso8601 timestamps :/
                    dt = updated_at[:-1]
                    sZ = 'Z'

                if len(sZ) < 7:
                    self._cache_updated = f'{dt},{sZ[:-1]:0<6}Z'
                else:
                    # and don't forget that even if it is correctly
                    # padded we have to switch to use , instead of .
                    self._cache_updated = f'{dt},{sZ}'

            return self._cache_updated

    @property
    def file_id(self):
        if isinstance(self.bfobject, self._File):
            return self.bfobject.id

    @property
    def old_id(self):
        return None

    def exists(self):
        try:
            bfo = self.bfobject
            if not isinstance(bfo, self._BaseNode):
                _cache = bfo.refresh(force=True)
                bf = _cache.remote.bfo

            return bfo.exists
        except exc.NoRemoteFileWithThatIdError as e:
            return False

    def is_dir(self):
        try:
            bfo = self.bfobject
            return (not isinstance(bfo, self._File) and
                    not isinstance(bfo, self._DataPackage))
        except exc.NoRemoteFileWithThatIdError as e:
            raise e
        except Exception as e:
            breakpoint()
            raise e

    def is_file(self):
        bfo = self.bfobject
        # given how self.bfobject works the only time we see
        # a data package is if something is in error in which
        # case it it definitely not a file
        return isinstance(bfo, self._File)

    @property
    def checksum(self):  # FIXME using a property is inconsistent with LocalPath
        if hasattr(self.bfobject, 'checksum'):
            checksum = self.bfobject.checksum
            if checksum and '-' not in checksum:
                try:
                    return bytes.fromhex(checksum)
                except ValueError as e:
                    msg = f'nonsensical checksum for {self}: {checksum!r}'
                    if msg not in self._errors:
                        logd.critical(msg)
                        self._errors.append(msg)

    @property
    def checksum_cypher(self):
        def detect_pennsieve_cypher(checksum):  # FIXME may also need/want size in here
            """ this is a bad hueristic specifically for dealing with the
                dataset packages endpoint """
            if type(checksum) != bytes:
                # len(bytes) and len(str) for hex str vs bytes causes mass confusion
                raise TypeError(f'{type(checksum)} is not bytes!')

            if len(checksum) == 16:
                return 'md5'
            elif len(checksum) == 32:
                return 'sha256'
            else:
                raise NotImplementedError(f'unknown cypher for {checksum!r}')

        checksum = self.checksum
        if checksum is None:
            return
        else:
            # technically returns cypher_name or cypher_algo or something ...
            return detect_pennsieve_cypher(checksum)

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
        if not isinstance(self.bfobject, self._Organization):
            # This seems like an oversight ...
            return self.bfobject.owner_id

    _member_cache = {}
    _known_orgs = set()
    @property
    def owner(self):
        class User:
            def __init__(self, u):
                self.__dict__.update(u.__dict__)

            @property
            def name_or_email(self):
                if len(self.last_name) <= 1 and len(self.first_name) <= 1:
                    return self.email.strip()
                else:
                    return (
                        self.last_name.strip()
                        + ', '
                        + self.first_name.strip())

        if not self._member_cache or self.organization.id not in self._known_orgs:
            self._known_orgs.add(self.organization.id)
            self._member_cache.update(
                {m.id:User(m) for m in self.organization.bfobject.members})
        oid = self.owner_id
        if oid in self._member_cache:
            return self._member_cache[oid]
        else:
            msg = f'owner no longer a member of organization? {self.owner_id}'
            log.warning(msg)
            return User(self.bfobject.owner())

    @property
    def parent(self):
        if hasattr(self, '_c_parent'):
            # WARNING staleness could occure because of this
            # wow does it save on the network roundtrips
            # for rchildren though
            return self._c_parent

        if isinstance(self.bfobject, self._Organization):
            return self  # match behavior of Path

        elif isinstance(self.bfobject, self._Dataset):
            return self.organization
            #parent = self.bfobject._api._context

        else:
            parent = self.bfobject.parent
            if parent is None:
                parent = self.bfobject.dataset

        if parent:
            parent_cache = (
                self.cache.parent if self.cache is not None else None)
            self._c_parent = self.__class__(parent, cache=parent_cache)
            return self._c_parent

    @property
    def children(self):
        yield from self._children()

    def _children(self, create_cache=True, retry_limit=3, retry_count=0):
        if isinstance(self.bfobject, self._File):
            return
        elif isinstance(self.bfobject, self._DataPackage):
            return  # we conflate data packages and files
        elif isinstance(self.bfobject, self._Organization):
            try:
                datasets = self.bfobject.datasets
            except self._requests.exceptions.ChunkedEncodingError as e:
                log.critical('pennsieve connection broken causing ChunkedEncodingError')
                if retry_count >= retry_limit:
                    raise e

                yield from self._children(
                    create_cache=create_cache,
                    retry_limit=retry_limit,
                    retry_count=retry_count + 1)
                return
            except TypeError as e:
                log.error(e)
                msg = ('likely internal error inside pennsieve '
                       'client related to network issues')
                log.critical(msg)
                if retry_count >= retry_limit:
                    raise e

                yield from self._children(
                    create_cache=create_cache,
                    retry_limit=retry_limit,
                    retry_count=retry_count + 1)
                return

            for dataset in datasets:
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
        # we control exclude uploaded via a class level variable so that
        # so that the cache implementation doesn't have to know about it
        # since cache shouldn't know anything about why the remote gives
        # it certain files, just that it does
        yield from self._rchildren(exclude_uploaded=self._exclude_uploaded)

    def _dir_or_file(self, child, deleted, exclude_uploaded, skip_existing):
        """ skip_existing sould be set if create_cache is set to true """
        # FIXME
        state = child.bfobject.state
        if state != 'READY':
            #log.debug (f'{state} {child.name} {child.id}')
            if state != 'UPLOADED':
                self._errors.append(f'State not READY and not UPLOADED is {state}')

            if state == 'DELETING' or state == 'PARENT-DELETING' or state == 'DELETED':
                deleted.append(child)
                return
            if exclude_uploaded and state == 'UPLOADED':
                logd.warning(f'File in {self.id} is UPLOADED not READY! {child!r}')  # TODO
                return

        if child.is_file():
            cid = child.id
            existing = [c for c in self.cache.local.children
                        if (c.is_file() and c.cache or c.is_broken_symlink())
                        and c.cache.id == cid]
            if existing:
                unmatched = [e for e in existing if child.name != e.name]
                if unmatched and skip_existing:
                    # NOTE this should run when create_cache is True to avoid
                    # creating files with duplicate names in the event that
                    # somehow there was a duplicate name, this is an internal
                    # implementation detail related to old decisions about how
                    # to populate the local cache
                    log.debug(f'skipping {child.name} becuase a file with that '
                              f'id already exists {unmatched}')
                    return

        return True

    def _rchildren(self,
                   create_cache=True,
                   # FIXME I am 99% sure that create_cache should default to False
                   # and I was also 99% sure that I HAD set it to be False by
                   # default so that remote.rchildren would not populate cache
                   # but that only cache.rchildren would, however neither of those
                   # is really correct in order to fix this need to review who is
                   # calling this for the create cache side effects and fix them
                   exclude_uploaded=False,
                   sparse=False,):
        if isinstance(self.bfobject, self._File):
            return
        elif isinstance(self.bfobject, self._DataPackage):
            return  # should we return files inside packages? are they 1:1?
        elif any(isinstance(self.bfobject, t)
                 for t in (self._Organization, self._Collection)):
            for child in self._children(create_cache=create_cache):
                yield child
                yield from child._rchildren(create_cache=create_cache)
        elif isinstance(self.bfobject, self._Dataset):
            sparse = sparse or self.cache.is_sparse()
            deleted = []
            if sparse:
                filenames = [s + '.' + ext
                             for s in self._sparse_stems
                             for ext in self._sparse_exts]
                sbfo = self.bfobject
                _parents_yielded = set()
                _int_id_map = {}
                for bfobject in self.bfobject.packagesByName(filenames=filenames):
                    child = self.__class__(bfobject)
                    if child.is_dir() or child.is_file():
                        if not self._dir_or_file(child, deleted, exclude_uploaded, create_cache):
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
            # end if sparse

            for bfobject in self.bfobject.packages:
                child = self.__class__(bfobject)
                if child.is_dir() or child.is_file():
                    if not self._dir_or_file(child, deleted, exclude_uploaded, create_cache):
                        continue

                    if create_cache:
                        # FIXME I don't think existing detection is working
                        # correctly here so this get's triggered incorrectly?
                        self.cache / child  # construction will cause registration without needing to assign
                        assert child.cache is not None

                    yield child
                else:
                    # probably a package that has files
                    # this log message is too noisey for regular use, even at the debug level 10
                    log.log(9, f'skipping {child} becuase it is neither a directory nor a file')

            else:  # for loop else
                self._deleted = deleted

        else:
            raise exc.UnhandledTypeError  # TODO

    def children_pull(self,
                      existing_caches=tuple(),
                      only=tuple(),
                      skip=tuple(),
                      sparse=tuple()):
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
                deferred(child.bootstrap)(recursive=True,
                                          only=only,
                                          skip=skip,
                                          sparse=sparse)
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
            # TODO figure out how to squash this logging when we
            # expect the file to be gone, this is hard because we have
            # to construct a new remote from the cache every time so
            # the fact that we have another instance that represents
            # this remote where rmdir was actually called
            log.error(e)
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
                ds = self._bfobject.dataset
                if isinstance(ds, str):
                    return ds
                else:
                    # we embed the object instead of just the id now
                    # so we have to handle that case
                    return ds.id
            else:
                if isinstance(pid, str):
                    return pid
                else:
                    return pid.id

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

    def _has_a_single_remote_file(self):
        """ this will fetch """
        bfobject = self.bfobject
        if not isinstance(bfobject, self._DataPackage):
            return False

        log.log(9, 'going to network for files')
        files = bfobject.files  # FIXME this is NOT the right place to do this because packages with multiple files become a massive problem especailly for determining the actual name we give the file, I have cases where there seem to be two identical files added to the same package, they have the same name ...
        if not files:
            return False

        if len(files) > 1:
            log.critical(f'{self} has more than one file! Not switching bfobject!')
            return False  # would need to be a folder or unpack individual files

        file, = files
        file.parent = bfobject.parent
        file.dataset = bfobject.dataset
        file.package = bfobject
        if bfobject.name != file.name and not hasattr(self, '_ni_logged_0'):
            msg = f'package-missing-files-name-issue {bfobject.name} != {file.name}'
            log.log(9, msg)
            self._ni_logged_0 = True

        self._bfobject = file
        return True

    def _single_file(self):
        if isinstance(self.bfobject, self._DataPackage):
            files = list(self.bfobject.files)
            if len(files) > 1:
                raise BaseException('TODO too many files')

            file = files[0]
        elif isinstance(self.bfobject, self._File):
            file = self.bfobject
        else:
            file = None

        return file

    def _multi(self):
        if (multi_file_number := (hasattr(self.bfobject, '_has_multiple_files') and self.bfobject._has_multiple_files)):
            return multi_file_number
        elif not self.from_packages and self.is_file():
            # if for some reason we hit this codepath instead of doing
            # a full resync then we will implement this until then ...
            msg = ('If you hit this during curation upload make sure '
                   'there are not old copies of metadata files that '
                   'have the same package id inside the dataset folder. '
                   'e.g a backup folder like SPARC/{dataset}/backup')
            log.error(msg)
            raise NotImplementedError('TODO')

    def _uri_file(self):
        file = self._single_file()
        if file is not None:
            return file.url

    data = property(base_data)

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
        raise NotImplementedError('pretty sure unused')
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

    def __truediv__(self, other, docache=True):  # XXX
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

                if docache:
                    self.cache / child  # THIS SIDE EFFECTS TO UPDATE THE CHILD CACHE

                return child

            else:  # create an empty
                return self._temp_child(other)

    def _temp_child(self, child_name):
        """ construct child with a fake placeholder bfobject """
        # FIXME hack around instantiation-existence issue
        child = object.__new__(self.__class__)
        child._parent = self
        class TempBFObject(self._BaseNode):  # this will cause a type error if actually used
            name = child_name
            exists = False

        if self != self.organization:
            if self.identifier.type == 'dataset':  # XXX FIXME enum these ?
                TempBFObject.dataset = self.id
            else:
                TempBFObject.dataset = self._bfobject.dataset
                TempBFObject.parent = self.id

        tbfo = TempBFObject()
        child._bfobject = tbfo
        child._seed = tbfo
        return child

    def rename(self, new_name, update_cache=True):  # FIXME TODO defer cache update to reduce roundtrips?
        # TODO consider a fused rename_reparent that might be able to reduce network roundtrips?
        # FIXME handle files vs folders vs datasets
        # bfobject instead of _bfobject so that we populate in case bfobject is a stub
        if self.is_file():
            bfobject = self.bfobject.package
        elif self.is_dir():
            bfobject = self.bfobject

        if bfobject.name != new_name:  # FIXME error here when trying to get/rename a package?
            bfobject.name = new_name
            bfobject.update()  # FIXME will surely fail on bfobjects from /files endpoint?
            _obfo = self._bfobject
            self._bfobject = self.__class__(bfobject).bfobject  # filter package to file, for collections this shouldn't do anything, but it might?

            if update_cache:
                # in the case where a thing is renamed and its cache value is not changed
                # when we then run rename one the remote, the cache will be stale, but the
                # local and the remote will be synchronized, thus when we call update_cache
                # the internal check that name and parent are consistent should pass without issue
                # essentially change local then local -> remote and then remote -> cache ensures
                # that everything will remain consistent at every step along the way
                # and the push manifest is there for use cases that need a record of the changes
                # TODO FIXME make sure that update_cache also updates the blob cache metadata probably?
                # because that can definitely go stale? yes but only relevant in cases where there
                # are files that have been pulled, not just symlinks

                # XXX nope, when we have a rename and a reparent at the same time then
                # using Remote.update_cache will never work because for the first op
                # either the parent doesn't match or the name doesn't match and there is
                # no way around that, so we have to use the internal method
                self.cache._meta_updater(self.meta, fetch=False)
        else:
            # FIXME likely going to need more info if we do actually hit this
            # but ok for now probably
            msg = f'tried to rename a file to its current name? {self}'
            log.debug(msg)

    def reparent(self, new_parent_id, update_cache=True):  # FIXME TODO defer cache update to reduce roundtrips?
        # XXX if update_cache == True then this assumes that self.cache is not None

        # TODO handle all the possible crazy possible values that new_parent could take on
        # FIXME given the use case, we need to expect new_parent_id by default to avoid
        # exploding the number of network calls, but the N:thing: checks are still annoying

        # TODO opportunity here to detect when a manifest has been moved and automatically
        # update the relative path references
        if new_parent_id.startswith('N:dataset:'):
            # pennsieve treats the root of a dataset as null with respect to parents
            new_parent_id = None
        elif new_parent_id.startswith('N:collection:'):
            pass
        else:
            msg = f"don't know how to move to a {new_parent_id}"
            raise NotImplementedError(msg)

        # bfobject instead of _bfobject so that we populate in case bfobject is a stub
        if self.is_file():
            package = self.bfobject.package
            package._api.data.move(new_parent_id, package.id)
            # need to fetch so take this opportunity to get a real
            # bfobject in the event the we are using a fake one from the files endpoint
            _reup = self.__class__(self.id)
            self._bfobject = _reup.bfobject
            new_package = self._bfobject.package
        else:
            self.bfobject._api.data.move(new_parent_id, self.id)
            self._bfobject.update()  # fetch the changes
            if hasattr(self, '_c_parent'):
                _op = self._c_parent
                del self._c_parent
                # ensure that the new parent actually propagates
                # XXX yes this makes these objects mutable, but deal with it

            # FIXME TODO check on updated time for old and new parent in case we need to
            # add them to the manifest as being modified by side-effect and thus need to be fetched and checked
            self.parent
            # see if this is what was causing the "missing folder"
            # I'm betting that the call to update_cache below results in the file
            # being moved back to its old parent because of the stale metadata
            # and indeed,
            # [2023-07-13 18:23:16,893] -  WARNING -     augpathlib -        caches.py:926  - A parent of current file has changed location!
            # /home/tom/git/sparc-curation/test/test-operation-25463/UCSD/test-dataset-2023-07-13T182111,820526-0700
            # dire-2/dire-3-3-np-r
            # dire-1/dire-3-3-np-r
            # N:collection:cd41747d-749e-4c23-a2dc-bec61c07ff7e
            # there it is in the logs, so this should be resolved now

        if update_cache:
            # FIXME we almost certainly need to update the old parent cache
            # and the new parent cache because they should both have modified
            # times (hopefully ...) that we want to record because the thing
            # itself will appear to be unmodified
            self.cache._meta_updater(self.meta, fetch=False)  # can't use remote.update_cache

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

    def rmdir(self, force=False):
        # XXX NOTE force= is bfpn specific and does not match normal pathlib behavior
        # but it is here because there are extra protections on certain dir types
        if self.is_organization():
            raise exc.SparCurError("can't remove organizations right now")

        elif self.is_dataset():
            if not force and list(self.children):  # FIXME super inefficient ...
                raise exc.PathNotEmptyError(self)

            self.bfobject.delete()
        elif self.is_dir():
            if list(self.children):  # FIXME super inefficient ...
                raise exc.PathNotEmptyError(self)

            self.bfobject.delete()
        else:
            raise exc.NotADirectoryError(f'{self}')

    @staticmethod
    def _upload(local, remote):
        return remote.bfobject.upload(local, use_agent=False)

    @classmethod
    def _stream_from_local_raw(cls, local_path):
        parent_remote = local_path.parent.remote
        _ = cls._upload(local_path.as_posix(), parent_remote)

    @classmethod
    def _stream_from_local(cls, local_path, replace=True, local_backup=False):
        # FIXME touch_child -> stream data ??? as a bridge to sanity?
        # /packages/ endpoint should allow us to create a new empty package
        self = local_path.parent.remote

        if type(self) != cls:
            raise TypeError(f'{type(self)} != {cls}')

        # FIXME BAD forces us to read file twice
        checksum = local_path.checksum()

        # must set docache False here to avoid automatically updating the local
        # local when we know that 99% of the time the file has been modified
        # and thus trying to retrieve the remote will fail with a LocalChangesError
        # old_remote = self.__truediv__(local_path.name, docache=False)
        # FIXME old_remote doesn't carry the checksum, have to get it from the dataset packages endpoint
        matches = [self.__class__(b) for b in self.dataset.bfobject._packages(filename=local_path.name)
                   if hasattr(b, 'package') and
                   b.state not in ('DELETING', 'DELETED')]
        sids = self.is_dataset()
        sid = self.id
        cands = [m for m in matches if m.name == local_path.name and
                 m.checksum and m.parent == self]
        scands = sorted(cands, key=lambda c:c.updated)
        # TODO maybe deal with multiple package names ... ?? such is the life of no replace?

        checksum_matches =[c for c in cands if c.checksum == checksum]
        if checksum_matches:
            # TODO check to make sure nothing else has changed ???
            # also how does this interact with replace = true?
            raise exc.FileHasNotChangedError(
                ('a file with the same name and checksum already exists, not uploading: '
                f'{[c.meta for c in checksum_matches]}'))
        elif scands:
            # take the latest in the event that there are multiple
            # e.g. because tests have been failing for ... years ... heh
            old_remote = scands[-1]
        else:
            old_remote = None

        _ = cls._upload(local_path.as_posix(), self)
        # the agent now returns nothing, we use display progres in
        # order to force the program to block until the upload has
        # completed, this means we should be able to get the checksum
        # from the packages endpoint in fairly short order
        if self.is_dataset():
            d = self
        else:
            d = local_path.parent.cache.dataset.remote

        # can't filter by filename because it misses on silent rename
        cands = [f for f in d.bfobject._packages(latest_only=True)
                 if hasattr(f, 'filename') and
                 #f.filename == local_path.name and  # renaming is hard
                 f.checksum == checksum.hex()]

        # and f.filename == local_path.name and f.checksum == checksum
        remotes = [cls(c) for c in cands]
        matches = [r for r in remotes if r.parent and r.parent == self]

        if not matches:
            raise Exception(
                f'No matching remote found for {local_path}!')
        elif len(matches) > 1:
            raise Exception(
                f'Too many remotes found for {local_path}!')
        else:
            remote = matches[0]

        try:
            # do a little dance to update the name back to what it is supposed to be
            stem_diff = local_path.stem != remote.bfobject.package.name
            if replace and stem_diff:
                if old_remote and old_remote.exists():  # FIXME nasty performance cost here ...
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
                    # FIXME issue with multiple packages with the same name

                    # FIXME OH NO concurrent package.delete is non-blocking !

                    # the fact that this can fail tells me that there is no
                    # synchronization on the blackfynn backend AT ALL
                    # operations seem to be executed as they arrive without
                    # any notion of consistency or ordering WAT
                    rbp = remote.bfobject.package
                    rbp.name = remote.bfobject.name
                    _odict = rbp.__dict__
                    rbp.__dict__ = {
                        k: (v.id if not isinstance(v, str) and
                            # HACK to do object -> id
                            hasattr(v, 'id') else v)
                        for k, v in rbp.__dict__.items()}
                    try:
                        for i in range(100):
                            # I love spinlocks for concurency don't you?
                            try:
                                rbp.update()
                                break
                            except self._requests.exceptions.HTTPError as e:
                                if e.response.text != 'package name is already taken':  # ugh
                                    raise e
                                elif i == 99:
                                    raise Exception('LOL') from e

                    finally:
                        rbp.__dict__ = _odict

            elif not stem_diff and old_remote and old_remote.exists():
                log.critical(f'YOU MAY HAVE FUNKY DATA IN {local_path.cache.parent.uri_human}')

        except Exception as e:
            log.exception(e)

        return remote, old_remote

    def ___old():
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
                        except self._requests.exceptions.HTTPError as e:
                            if e.response.text != 'package name is already taken':  # ugh
                                raise e
                            elif i == 99:
                                raise Exception('LOL') from e

            elif not stem_diff and old_remote.exists():
                log.critical(f'YOU MAY HAVE FUNKY DATA IN {local_path.cache.parent.uri_human}')

        except Exception as e:
            log.exception(e)

        return remote, old_remote

    @property
    def meta(self):
        return PathMeta(name=self.name,
                        size=self.size,
                        created=self.created,
                        updated=self.updated,
                        checksum=self.checksum,
                        checksum_cypher=self.checksum_cypher,
                        etag=self.etag,
                        chunksize=self.chunksize,
                        parent_id=self.parent_id,
                        id=self.id,
                        file_id=self.file_id,
                        old_id=None,
                        gid=None,  # needed to determine local writability
                        user_id=self.owner_id,
                        mode=None,
                        multi=self._multi(),
                        # XXX WARNING these errors can leak out and can break
                        # get_errors via extract_errors, see DatasetStructure.data_dir_structure
                        # for the fix
                        errors=self.errors)

    def __eq__(self, other):
        return self.id == other.id and self.file_id == other.file_id
        #return self.bfobject == other.bfobject

    def __hash__(self):
        return hash((self.__class__, self.id))

    def __repr__(self):
        file_id = f', file_id={self.file_id}' if self.file_id else ''
        return f'{self.__class__.__name__}({self.id!r}{file_id})'


class RemoteDatasetData:

    cache_path = auth.get_path('cache-path')
    cache_base = cache_path / 'remote-meta'

    _translation_version = 1  # XXX increment if contents of rmeta change

    @classmethod
    def _setup(cls, *args, **kwargs):
        from sparcur.core import JEncode
        cls._JEncode = JEncode

    def __init__(self, remote_cache_or_id):
        if isinstance(remote_cache_or_id, aug.RemotePath):
            self._remote = remote_cache_or_id
            self._bfobject = self._remote.bfobject
            self.id = self._remote.id
        elif isinstance(remote_cache_or_id, aug.CachePath):
            self._c_cache_path = remote_cache_or_id
            self.id = self._c_cache_path.id
        elif isinstance(remote_cache_or_id, aug.AugmentedPath):
            # on the off chance that we are passed a local path
            self._c_cache_path = remote_cache_or_id.cache
            self.id = self._c_cache_path.id
        else:
            self.id = remote_cache_or_id
            if '/' in self.id:
                # ntfs_safe_id broken for LocId asdf:/home/user/etc
                msg = f'likely trying to pass a local id as a remote id {self.id}'
                raise ValueError(msg)

        self.ntfs_safe_id = self.id.split(':')[-1]  # sigh
        self.cache = self.cache_base / self.ntfs_safe_id
        if not self.cache_base.exists():
            self.cache_base.mkdir(parents=True)

    @property
    def _cache_path(self):
        if not hasattr(self, '_c_cache_path'):
            raise NotImplementedError('if you need access to the cache '
                                      'pass one in at construction time')

        return self._c_cache_path

    @property
    def remote(self):
        if not hasattr(self, '_remote'):
            self._remote = self._cache_path.remote

        return self._remote

    @property
    def bfobject(self):
        if not hasattr(self, '_bfobject'):
            # NOTE if self.id is a string then this will likely
            # result in the NotImplementedError in _cache_path
            self._bfobject = self.remote.bfobject

        return self._bfobject

    def fromCache(self):
        """ retrieve cached results without hitting the network """
        # TODO FIXME error on no cache?
        if not self.cache.exists():
            # REMINDER: self.cache is NOT a cache_path in this context
            # super confusing I know ...
            msg = f'No cached metadata for {self.id}. Run `spc rmeta` to populate.'
            raise FileNotFoundError(msg)

        with open(self.cache, 'rt') as f:
            return json.load(f)

    @property
    def data(self):
        """ for uniformity MetadataFile type """
        return self.fromCache()

    def _no_return(self):
        self()

    def __call__(self):
        # XXX to debug run python -m sparcur.simple.fetch_remote_metadata
        # in a dataset folder

        # FIXME TODO switch to use dict transformers
        # self.bfobject.relationships()

        # if this fails with 503 errors, check the
        # blackfynn-backoff-factor config variable
        meta = self.bfobject.meta  # why this is not default in the python api the world may never know
        package_counts = self.bfobject.packageTypeCounts
        cont = meta['content']
        blob = {'id': self.remote.id,
                'id_int': cont['intId'],
                'type': self.__class__.__name__,  # registered as IdentityJsonType in core
                '_meta_version': self._translation_version,
                'name': cont['name'],  # title
                'readme': self.bfobject.readme,
                # 'banner': self.bfobject.banner,  # FIXME not persistent ...
                'status-log': self.bfobject.status_log,  # FIXME most recent only?
                'tags': cont['tags'],
                #'updated_at': cont['updatedAt'],  # redundant with xattr metadata
                #'created_at': cont['createdAt'],  # redundant with xattr metadata

                # FIXME strip emails before export
                # leave teams and users out of this for now, too much noise
                #'teams': self.bfobject.teams,
                #'users': self.bfobject.users,
                'contributors': self.bfobject.contributors,
                'package_counts': package_counts,
                'publication': meta['publication'],
                'canPublish': meta['canPublish'],
                'locked': meta['locked'],
                # there are a number of fields that we do not currently pull in
                # TODO consider whether to pull all the metadata so we don't have
                # to update this here
        }

        if 'description' in cont:
            blob['description'] = cont['description']  # subtitle

        if 'license' in cont:
            blob['license'] = cont['license']

        try:
            doi = self.remote.doi
        except Exception as e: # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX FIXME
            log.exception(e)  # TODO HasErrors
            doi = None

        if doi is not None:
            # FIXME somehow this managed to not be None and then self.remote.doi
            # was able to be None later !??!?!?!?!
            # I think this could happen if between timepoints something was published
            # or became resolvable or there was a network failure between the first
            # call and the second call
            blob['doi'] = doi

        pm = self.bfobject.publishedMetadata
        if pm is not None:
            # stash this in the cached metadata, we will likely need
            # it to resolve the stale doi issue via sparcron
            blob['remote_published_metadata'] = pm

            # note that there are discover records for unpublished
            # datasets but there is no mapping to the internal
            # accession id so we would never be able to find them
            # given the way bfobject.publishedMetadata works

            # FIXME one of these 204 not 32 :/ looks like there is an issue with the parts
            # 9dd7a07b-2037-4ce7-829d-395cc4382518/curation-export.json:      "id_published": 32,
            # b478b5d6-dfd6-4d6a-bd99-b1933987892f/curation-export.json:      "id_published": 32,
            blob['id_published'] = pm['id']
            # TODO identify the additional information that we want to embed
            blob['published_version'] = pm['version']
            blob['published_revision'] = pm['revision']
            if 'versionPublishedAt' in pm:
                # parse the date to avoid known pennsieve truncation issues
                vpa = dateparser.parse(pm['versionPublishedAt'])
                blob['timestamp_published_version'] = vpa
            # TODO do we want to save the published file manifest in the cache?
            # some of them can be ... quite large

        with open(self.cache, 'wt') as f:
            json.dump(blob, f, indent=2, sort_keys=True, cls=self._JEncode)

        return blob

    def _published_metadata(self):
        pm = self.bfobject.publishedMetadata
        pvm = self.bfobject.publishedVersionMetadata(pm['id'], pm['version'])
        return pvm

    def _published_files(self):
        pvm = self._published_metadata()
        def fixf(f):
            path = PurePosixPath(f['path'])
            if 'name' not in f:
                # the files schema may or may not have a name key
                # depending on when the dataset was published SIGH
                f['name'] = path.name
            if path.name != f['name']:
                msg = f'path name != name:\n{path.name}\n{f["name"]}'
                log.critical(msg)

            return f

        files = [fixf(f) for f in pvm['files']]
        return files

    def _published_package_name_index(self):
        return {f['sourcePackageId']:f for f in self._published_files()
                if 'sourcePackageId' in f}


class LocalDatasetData(RemoteDatasetData):
    """ confusingly ... """

    data = None

    def __init__(self, *args, **kwargs):
        # TODO source self.data from the metadata sheets and then all
        # we have to do is invert the function to post the data back
        # to the api endpoints
        self.data = {}

    def fromCache(self):
        return self.data

    def __call__(self):
        return self.data


class BlackfynnDatasetData(RemoteDatasetData):

    cache_base = RemoteDatasetData.cache_path / 'blackfynn-meta'

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    _renew = __new__

    def __new__(cls, *args, **kwargs):
        BlackfynnDatasetData._setup(*args, **kwargs)
        BlackfynnDatasetData.__new__ = BlackfynnDatasetData._renew
        return super().__new__(cls)


class PennsieveRemote(BlackfynnRemote):

    _remote_type = 'pennsieve'
    _id_class = PennsieveId
    _base_uri_human = 'https://app.pennsieve.io'  # FIXME hardcoded
    _base_uri_api = 'https://api.pennsieve.io'  # FIXME hardcoded

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    _renew = __new__

    def __new__(cls, *args, **kwargs):
        cls._setup(*args, **kwargs)
        return super().__new__(cls)

    @staticmethod
    def _setup(*args, **kwargs):
        if PennsieveRemote.__new__ == PennsieveRemote._renew:
            return  # we already ran the imports here

        import requests
        PennsieveRemote._requests = requests

        # FIXME there should be a better way ...
        from sparcur.pennsieve_api import PNLocal, id_to_type
        PennsieveRemote._api_class = PNLocal
        PennsieveRemote._id_to_type = staticmethod(id_to_type)
        PennsieveRemote.__new__ = PennsieveRemote._renew

        from pennsieve import Collection, DataPackage, Organization, File
        from pennsieve import Dataset
        from pennsieve.models import BaseNode
        Dataset._id_to_type = staticmethod(id_to_type)
        PennsieveRemote._Collection = Collection
        PennsieveRemote._DataPackage = DataPackage
        PennsieveRemote._Organization = Organization
        PennsieveRemote._File = File
        PennsieveRemote._Dataset = Dataset
        PennsieveRemote._BaseNode = BaseNode

    @staticmethod
    def _upload(local, remote):
        # XXX the agent upload is entirely asynchronous and there
        # doesn't seem to be an easy way, without monkey patching
        # to get a synchronous version that would idk, return the
        # new package id of the uploaded file ...

        # display progress at least blocks, even if it doesn't
        # return the package id
        return remote.bfobject.upload(local, display_progress=True)


class PennsieveDatasetData(RemoteDatasetData):

    cache_base = RemoteDatasetData.cache_path / 'pennsieve-meta'

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    _renew = __new__

    def __new__(cls, *args, **kwargs):
        PennsieveDatasetData._setup(*args, **kwargs)
        PennsieveDatasetData.__new__ = PennsieveDatasetData._renew
        return super().__new__(cls)


class PennsieveDiscoverRemote(aug.RemotePath):
    # only for datasets less than 5 gigs, come down as a zip
    # does not handle children and parents directly because that can
    # only be answered from the cache, the remote itself is dataset
    # level only, the s3 version probably can
    _remote_type = 'pennsieve-discover'
    'https://api.pennsieve.io/discover/datasets/292/versions/1/download?downloadOrigin=SPARC'
    's3://prd-sparc-discover-use1/292/1/'

    _translation_version = 1

    _uri_api_template = 'https://api.pennsieve.io/discover/datasets/{identifier}/versions/{version}/metadata'
    _uri_api_template = (
        'https://api.pennsieve.io/discover/datasets/'
        '{id}/versions/{version}')

    #_project_id = f'pennsieve-discover-root-{id(object())}'  # not parent so make one
    _project_id = 'https://api.pennsieve.io/discover/datasets'
    _project_name = 'Pennsieve-Discover'

    class _api_class:  # fake out super()._init behavior
        class organization:  # fake out lifters
            members = []
        def __init__(self, *args, **kwargs):
            self.root = self._project_id

    _api_class._project_id = _project_id

    @staticmethod
    def _setup(*args, **kwargs):
        import requests  # XXX SIGH
        PennsieveDiscoverRemote._requests = requests

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    _renew = __new__

    def __new__(cls, *args, **kwargs):
        cls._setup(*args, **kwargs)
        return super().__new__(cls)

    def __init__(self, id_published, version=None, cache=None):
        if cache is not None:
            #breakpoint()
            pass

        if id_published == self._project_id:
            self._name = self._project_name

        self._id = str(id_published)
        if self.id == self._project_id:
            self.version = None
        else:
            v = self._latest_version() if version is None else version
            self.version = str(v)

    @property
    def meta(self):
        return aug.PathMeta(
            id=self.id,
            # XXX note, we don't have a filed called version in the current
            # path meta spec, but we don't use it for folders so we reuse it
            # here to hold the version of the dataset, since the cache has
            # a different xattr_prefix the semantics are clear
            file_id=self.version,
        )

    def _meta_d(self):
        if not hasattr(self, '_cache_meta_d'):
            uri = f'https://api.pennsieve.io/discover/datasets/{self.id}'
            resp = self._requests.get(uri)
            self._cache_meta_d = resp.json()

        return self._cache_meta_d

    def is_file(self): return False  # top level directory only no remote listings available
    def is_dir(self): return True  # to is always a dir, but technically a zip

    def _impl__cache(self):  # have I mentioned by undying hatred of properties lately?
        if hasattr(self, '_c_cache'):
            return self._c_cache

        if self == self.parent:
            return self._cache_anchor

        parent_cache = self.parent.cache
        lp = parent_cache.local / self.name
        self._c_cache = self._cache_class(lp, remote=self)
        return self.cache

    @property
    def parent(self):
        if not hasattr(self.__class__, '_ds_parent'):
        # FIXME if we implement semi remote hierarchy
        # then the will need to handle other levels
            self._ds_parent = self.__class__(self._project_id)
            self._ds_parent._cache = self._cache_anchor

        return self._ds_parent

    @property
    def children(self):
        if self.id == self._project_id:
            j = self._meta()
            for d in j['datasets']:
                yield self.__class__(d['id'], d['version'])
        else:
            # TODO /metadata will list the files but we have to
            # reconstruct the directories if we want to list from
            # manifest without pulling the zip
            breakpoint()

    @property
    def name(self):
        if not hasattr(self, '_name'):
            j = self._meta_d()
            self._name = j['name']

        return self._name

    def _latest_version(self):
        j = self._meta_d()
        return j['version']

    @property
    def uri_api(self):
        if self.id == self._project_id:
            return self.id
        else:
            return self._uri_api_template.format(
                id=self.id,
                version=self.version)

    @property
    def uri_human(self):
        return f'https://discover.pennsieve.io/datasets/{self.id}/version/{self.version}'

    def _meta_project(self):
        block = 100
        offset = 0
        datasets = []
        while True:
            uri = self.uri_api + f'?offset={offset}&limit={block}&orderBy=date'
            resp = self._requests.get(uri)
            if resp.ok:
                try:
                    blob = resp.json()
                    total_count = blob['totalCount']
                    datasets.extend(blob['datasets'])
                    if len(blob['datasets']) < block:
                        assert total_count == len(datasets)
                        break  # we're at the end
                except self._requests.exceptions.JSONDecodeError as e:
                    # FIXME this needs to be wrapped at a global level
                    # because everything is horribly broken
                    msg = f'resp ok but json decode error?!\n{resp.text}'
                    log.critical(msg)
                    raise e
            else:
                resp.raise_for_status()

            offset += block

        return {'datasets': datasets,}

    def _meta(self):
        if not hasattr(self, '_cache_meta'):
            if self.id == self._project_id:
                uri = self.uri_api + '?limit=9999'  # FIXME will eventually need to paginate
                self._cache_meta = self._meta_project()
                return self._cache_meta
            else:
                uri = self.uri_api + '/metadata'

            resp = self._requests.get(uri)
            if resp.ok:
                try:
                    self._cache_meta = resp.json()
                except self._requests.exceptions.JSONDecodeError as e:
                    # FIXME this needs to be wrapped at a global level
                    # because everything is horribly broken
                    msg = f'resp ok but json decode error?!\n{resp.text}'
                    log.critical(msg)
                    raise e
            else:
                resp.raise_for_status()

        return self._cache_meta

    def _uri_file(self):
        #uri_file = self.uri_api + '/download?downloadOrigin=sparcur-backend'
        # XXX anything other than SPARC causes an internal server error
        # https://github.com/Pennsieve/discover-service/issues/46
        uri_file = self.uri_api + '/download?downloadOrigin=SPARC'
        return uri_file

    get_file_by_url = classmethod(base_get_file_by_url)

    data = property(base_data)


class S3Remote(aug.RemotePath):
    _remote_type = 's3'
    sigh = 's3://prd-sparc-discover-use1/301/1/'


