import os
import re
import sys
import logging
import warnings
import idlib
from idlib.utils import log as _ilog
from augpathlib.utils import log as _alog
from pyontutils.utils import (makeSimpleLogger,
                              python_identifier,  # FIXME update imports
                              TZLOCAL,
                              utcnowtz,
                              isoformat,
                              isoformat_safe,
                              timeformat_friendly)
from . import exceptions as exc
from .config import auth

_find_command = 'gfind' if sys.platform == 'darwin' else 'find'

log = makeSimpleLogger('sparcur')
logd = log.getChild('data')
loge = log.getChild('export')

# set augpathlib log format to pyontutils (also sets all child logs)
_alog.removeHandler(_alog.handlers[0])
_alog.addHandler(log.handlers[0])
# idlib logs TODO move to pyontutils probably?
_ilog.removeHandler(_alog.handlers[0])
_ilog.addHandler(log.handlers[0])

# silence known warnings on pypy3
if hasattr(sys, 'pypy_version_info'):
    warnings.filterwarnings('ignore', module='.+protobuf.+')

__type_registry = {None: None}
def register_type(cls, type_name):
    if type_name in __type_registry:
        if __type_registry[type_name] is cls:
            # better to do this check here than to force
            # all callers to check for themselves which
            # can fail if two separate systems try to
            # register the same type
            return

        raise ValueError(f'Cannot map {cls} to {type_name}. '
                         'Type already present! '
                         f'{type_name} -> {__type_registry[type_name]}')

    __type_registry[type_name] = cls


def register_all_types():
    # as a side effect this registers idlib streams and OntTerm
    # sigh doing anything in the top level of python :/
    import sparcur.core
    import sparcur.paths  # also a top level registration

    # this is not done at top level because it is quite slow
    from pysercomb.pyr import units as pyru
    [register_type(c, c.tag)
     for c in (pyru._Quant, pyru.Range, pyru.Approximately)]


class IdentityJsonType:
    """ use to register types that should not be recursed upon e.g.
        because they contain external use of type that does not align
        with our usage """

    @classmethod
    def fromJson(cls, blob):
        return blob


def fromJson(blob, *,
             # nct is a hack around the fact that external
             # data can provide a column called type and we
             # do not currently have a way to indicate that
             # fromJson should treat those subtrees specially
             _no_convert_type=False,
             _no_convert_type_keys=('subjects', 'samples')):
    def nitr(value):
        try:
            return value not in __type_registry
        except TypeError as e:
            log.critical(e)
            return False

    if isinstance(blob, dict):
        if 'type' in blob:
            t = blob['type']

            if t == 'identifier':
                type_name = blob['system']
            elif t in ('quantity', 'range'):
                type_name = t
            elif _no_convert_type:
                type_name = None
            elif nitr(t):
                breakpoint()
                # XXX FIXME should this be fatal ??? we have cases where
                # it is impossible to avoid because a user will use type
                # as a column header and then we are toast if we also want
                # to set the type of that object to convert back ... what
                # happens if we want to have subject metadata as a class?
                # do we just ... hope for the best that there are no collisions
                # and make collisions non-fatal ???
                msg = (f'TODO fromJson for type {t} '
                       f'currently not implemented\n{blob}')
                log.critical(msg)
                if False:  # set true if debugging when implementing new types
                    raise NotImplementedError(msg)
                type_name = None
            else:
                type_name = t

            cls = __type_registry[type_name]
            if cls is not None:
                return cls.fromJson(blob)

        return {k: v
                if k == 'errors' or k.endswith('_errors') else
                fromJson(v, _no_convert_type=(_no_convert_type or
                                              type(v) == list and
                                              k in _no_convert_type_keys))
                for k, v in blob.items()}

    elif isinstance(blob, list):
        return [fromJson(_, _no_convert_type=_no_convert_type) for _ in blob]
    else:
        return blob


def path_irs(*paths_or_strings):
    """Given one or more paths pointing to sparcur export
    json yield the python internal representation."""
    # TODO support for urls
    import json
    register_all_types()

    for path_or_string in paths_or_strings:
        with open(path_or_string) as f:
            blob = json.load(f)

        yield fromJson(blob)


def path_ir(path_or_string):
    """Given a path or string return the sparcur python ir."""
    return next(path_irs(path_or_string))


def render_manifest(rows):
    # FIXME checksum cypher
    return [[filename.as_posix(),
             isoformat(timestamp),
             desc,
             ft,
             at,
             checksum.hex() if checksum else checksum,]
            for filename, timestamp, desc, ft, at, checksum in rows]


def write_manifests(*args, parents=None, parents_rows=None, suffix='.csv',
                    include_directories=False,):
    header = ('filename', 'timestamp', 'description',  # FIXME no hardcode
              'file type', 'additional types', 'checksum')
    if parents is None and parents_rows is None:
        raise TypeError('one of parents or parents_rows is required')
    elif parents and parents_rows:
        raise TypeError('at most one of parents or parents_rows is allowed')

    if parents_rows:
        parents = [p for p, r in parents_rows]

    existing = []
    for parent in parents:
        manifest = parent / f'manifest{suffix}'
        if manifest.exists():
            existing.append(manifest)

    if existing:  # TODO overwrite etc.
        # FIXME TODO relative to some reference point
        msg = f'Existing manifest files detected not writing!\n{existing}'
        raise ValueError(msg)

    if parents_rows is None:
        parents_rows = [(path, path.generate_manifest())
                        for path in parents]

    manifests_rendered = []
    if suffix == '.csv':  # FIXME deal with different suffixes
        import csv
        paths_rendered = [(path, render_manifest(manifest))
                          for path, manifest in parents_rows]
        for path, rendered in paths_rendered:
            manifest = parent / f'manifest{suffix}'
            manifests_rendered.append((manifest, rendered))
            with open(manifest, 'wt') as f:
                csv.writer(f).writerows([header] + rendered)
    else:
        raise NotImplementedError(f"Don't know how to export {suffix}")

    return manifests_rendered


def expand_label_curie(rows_of_terms):
    return [[value for term in rot for value in
             (term.label if term is not None else '',
              term.curie if term is not None else '')]
            for rot in rows_of_terms]


def levenshteinDistance(s1, s2):
    if len(s1) > len(s2):
        s1, s2 = s2, s1

    distances = range(len(s1) + 1)
    for i2, c2 in enumerate(s2):
        distances_ = [i2+1]
        for i1, c1 in enumerate(s1):
            if c1 == c2:
                distances_.append(distances[i1])
            else:
                distances_.append(1 + min((distances[i1],
                                           distances[i1 + 1],
                                           distances_[-1])))
        distances = distances_
    return distances[-1]


class GetTimeNow:
    def __init__(self):
        self._start_time = utcnowtz()
        self._start_local_tz = TZLOCAL()  # usually PST PDT

    @property
    def _start_time_local(self):
        return self._start_time.astimezone(self._start_local_tz)

    @property
    def START_TIMESTAMP(self):
        return isoformat(self._start_time)

    @property
    def START_TIMESTAMP_SAFE(self):
        return isoformat_safe(self._start_time)

    @property
    def START_TIMESTAMP_FRIENDLY(self):
        return timeformat_friendly(self._start_time)

    @property
    def START_TIMESTAMP_LOCAL(self):
        return isoformat(self._start_time_local)

    @property
    def START_TIMESTAMP_LOCAL_SAFE(self):
        return isoformat_safe(self._start_time_local)

    @property
    def START_TIMESTAMP_LOCAL_FRIENDLY(self):
        return timeformat_friendly(self._start_time_local)


class SimpleFileHandler:

    _FIRST = object()

    def __init__(self, log_file_path, *logs, mimic=_FIRST):
        self.log_file_handler = logging.FileHandler(log_file_path.as_posix())
        if mimic is self._FIRST and logs:
            self.mimic(logs[0])
        elif mimic:
            self.mimic(mimic)

        for log in logs:
            self(log)

    def __call__(self, *logs_to_handle):
        for log in logs_to_handle:
            log.addHandler(self.log_file_handler)

    def mimic(self, log):
        self.log_file_handler.setFormatter(log.handlers[0].formatter)


def silence_loggers(*logs):
    for log in logs:
        parent = log
        while parent:
            [parent.removeHandler(h) for h in parent.handlers]
            parent = parent.parent


def bind_file_handler(log_file):
    # FIXME the this does not work with joblib at the moment
    from idlib.utils import log as idlog
    from protcur.core import log as prlog
    from orthauth.utils import log as oalog
    from ontquery.utils import log as oqlog
    from augpathlib.utils import log as alog
    from pyontutils.utils import log as pylog

    sfh = SimpleFileHandler(log_file, log)
    sfh(alog, idlog, oalog, oqlog, prlog, pylog)


class _log:
    """ logging prevents nice ipython recurions error printing
        so rename this class to log when you need fake logging """
    @staticmethod
    def debug(nothing): pass
    @staticmethod
    def info(nothing): pass
    @staticmethod
    def warning(nothing): print(nothing)
    @staticmethod
    def error(nothing): pass
    @staticmethod
    def critical(nothing): pass


want_prefixes = ('TEMP', 'FMA', 'UBERON', 'PATO', 'NCBITaxon', 'ilxtr',
                 'sparc', 'BIRNLEX', 'tech', 'unit', 'ILX', 'lex',)


def is_list_or_tuple(obj):
    return isinstance(obj, list) or isinstance(obj, tuple)


def symlink_latest(dump_path, path, relative=True):
    """ relative to allow moves of the containing folder
        without breaking links """

    if relative:
        dump_path = dump_path.relative_path_from(path)

    if path.exists():
        if not path.is_symlink():
            raise TypeError(f'Why is {path.name} not a symlink? '
                            f'{path!r}')

        path.unlink()

    path.symlink_to(dump_path)


def _transitive_(path, command):
    safe_command = command + ' -print0'
    with path:
        with os.popen(safe_command) as p:
            string = p.read()

    path_strings = string.split('\x00')
    # XXXXXXXXXXXXXXXXXXX REMINDER THAT THIS IS NOT SORTED
    # https://doi.org/10.1021/acs.orglett.9b03216
    paths = [path / s for s in path_strings if s][1:]  # leave out the parent folder itself
    return paths


def transitive_paths(path, exclude_patterns=tuple()):
    """Fast list of all child directories using unix find."""
    if sys.platform == 'win32':
        # XXX assumes that rchildren already implements exclude patterns
        return list(path.rchildren)

    hrm = ' '.join(['-not -path ' + repr(pat) for pat in exclude_patterns])
    if hrm:
        hrm = ' ' + hrm
    command = f"""{_find_command} -not -path '.operations*'{hrm}"""
    # TODO failover to builtin rglob
    return _transitive_(path, command)


def transitive_dirs(path):
    """Fast list of all child directories using unix find."""
    if sys.platform == 'win32':  # no findutils
        gen = os.walk(path)
        next(gen)  # drop path itself to avoid drp == Path('.')
        return [path / t[0] for t in gen]

    command = f"""{_find_command} -type d"""
    # TODO failover to builtin rglob + filter
    return _transitive_(path, command)


def unicode_truncate(string, length):
    """ Truncate unicode string to at most maximum length.
        If truncation splits a multibyte charachter then ignore
        the final truncated bytes. """
    return string.encode()[:length].decode(errors='ignore')


class ApiWrapper:
    """ Sometimes you just need one more level of indirection!
    Abstract base class to wrap Blackfynn and Pennsieve apis.
    """

    _id_class = None
    _api_class = None
    _sec_remote = None
    _dp_class = None

    @classmethod
    def _get_connection(cls, project_id):#, retry=10):
        try:
            return cls._api_class(
                api_token=auth.user_config.secrets(
                    cls._sec_remote, project_id, 'key'),
                api_secret=auth.user_config.secrets(
                    cls._sec_remote, project_id, 'secret'))
        except KeyError as e:
            msg = (f'need record in secrets for {cls._sec_remote} '
                   f'organization {project_id}')
            raise exc.MissingSecretError(msg) from e

        #except Exception as e:  # was absent dsn caching + rate limits
            #from time import sleep
            #if 0 < retry:
                # exponential falloff with the final wait being 10 seconds
                #sleep(10 ** (1 / retry))
                #cls._get_connection(project_id, retry=retry - 1)
            #else:
                #raise e

    def __init__(self, project_id, anchor=None):
        # no changing local storage prefix in the middle of things
        # if you want to do that create a new class

        if isinstance(project_id, self._id_class):
            # FIXME make the _id_class version the internal default
            project_id = project_id.id

        import requests  # SIGH
        self._requests = requests
        self.bf = self._get_connection(project_id)
        self.organization = self.bf.context
        self.project_name = self.bf.context.name
        self.root = self.organization.id
        self._project_id = project_id  # keep it around for set_state

    def __getstate__(self):
        state = self.__dict__
        if 'bf' in state:
            state.pop('bf')  # does not pickle well due to connection

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.bf = self._get_connection(self._project_id)

    def create_package(self, local_path):
        pkg = self._dp_class(local_path.name, package_type='Generic')  # TODO mimetype -> package_type ...
        pcache = local_path.parent.cache
        pkg.dataset = pcache.dataset.id
        if pcache.id != pkg.dataset:
            pkg.parent = pcache.id

        # FIXME this seems to create an empty package with no files?
        # have to do aws upload first or something?
        return self.bf._api.packages.create(pkg)

    def get(self, id, attempt=1, retry_limit=3):
        log.log(9, 'We have gone to the network!')  # too verbose for debug
        if isinstance(id, self._id_class):
            # FIXME inver this so the id form is internal or implement
            # __str__ differently from __repr__
            id = id.id

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
            if id == self.root:
                return self.organization  # FIXME staleness?
            else:
                # if we start form local storage prefix for everything then
                # this would work
                msg = 'TODO org does not match need other api keys.'
                raise Exception(msg)
        else:
            try:
                thing = self.bf.get(id)
            except self._requests.exceptions.HTTPError as e:
                resp = e.response
                if resp.status_code == 404:
                    msg = (f'{resp.status_code} {resp.reason!r} '
                           f'when fetching {resp.url}')
                    raise exc.NoRemoteFileWithThatIdError(msg) from e

                log.exception(e)
                thing = None
            except self._remotebase.UnauthorizedException as e:
                log.error(f'Unauthorized to access {id}')
                thing = None

        if thing is None:
            if attempt > retry_limit:
                msg = f'No remote object retrieved for {id}'
                raise exc.NoMetadataRetrievedError(msg)
            else:
                thing = self.get(id, attempt + 1)

        return thing

    def get_file_url(self, id, file_id):
        resp = self.bf._api.session.get(
            f'{self.bf._api._host}/packages/{id}/files/{file_id}')
        if resp.ok:
            resp_json = resp.json()
        elif resp.status_code == 404:
            msg = (f'{resp.status_code} {resp.reason!r} '
                   f'when fetching {resp.url}')
            raise exc.NoRemoteFileWithThatIdError(msg)
        else:
            resp.raise_for_status()

        try:
            return resp_json['url']
        except KeyError as e:
            raise e


def make_bf_cache_as_classes(BaseNode, File, Collection, Dataset, Organization):

    class FakeBFLocal(ApiWrapper):
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
    return (FakeBFLocal, CacheAsBFObject, CacheAsFile,
            CacheAsCollection, CacheAsDataset, CacheAsOrganization)


def make_bf_id_regex(top_level_domain):
    """ Generate the abstracted regex for a given
    blackfynn/pennsieve identifier scheme """

    uuid4_regex = ('([0-9a-f]{8}-'
                   '[0-9a-f]{4}-'
                   '4[0-9a-f]{3}-'
                   '[89ab][0-9a-f]{3}-'
                   '[0-9a-f]{12})')
    canonical_regex = (
        # XXX this is not implemented right now but could be
        f'^https://api.{top_level_domain}/id/N:([a-z]):' + uuid4_regex + '$'
    )
    curie_regex = '([a-z]+):' + uuid4_regex
    id_regex = 'N:' + curie_regex
    types = ('package', 'collection', 'dataset', 'organization', 'user', 'team')
    paths = 'viewer', 'files', *[t + 's' for t in types]  # WHO LOVES CLASS SCOPE WHEEEE LOL PYTHON
    path_elem_regex = '([a-z]+)'

    uri_api_regex = (f'https://api.{top_level_domain}/' + path_elem_regex + '/' + id_regex + '(?:/' + path_elem_regex + '/([^/]+))?/?')
    org_pref = f'https://app.{top_level_domain}/N:([a-z]+):'
    uri_human_regex = (org_pref + uuid4_regex + '/' + path_elem_regex +
                       '(?:/' + id_regex +
                       '(?:/' + path_elem_regex +
                       '(?:/(?:' + id_regex + '|' +
                       '([^/]+)' +
                       '(?:/' + id_regex + ')?))?' +
                       ')?)?/?'
                       )
    compiled = [
        (re.compile('^' + rx + '$'), rx_type) for rx, rx_type in
        # FIXME curie_regex likely needs to support /files/123 suffix as well?
        zip((curie_regex, id_regex, uuid4_regex, uri_api_regex, uri_human_regex),
            ('curie', 'id', 'uuid', 'uri_api', 'uri_human'),)]

    return (uuid4_regex, canonical_regex, curie_regex, id_regex,
            types, paths, path_elem_regex, uri_api_regex, org_pref,
            uri_human_regex, compiled)


class BlackfynnId(idlib.Identifier):
    """ put all static information derivable from a blackfynn id here """

    top_level_domain = 'blackfynn.io'
    (uuid4_regex, canonical_regex, curie_regex, id_regex,
     types, paths, path_elem_regex, uri_api_regex, org_pref,
     uri_human_regex, compiled) = make_bf_id_regex(top_level_domain)

    def __reduce__(self):
        return self.__class__, (self.id, self.type, self.file_id)

    def __new__(cls, id_uri_curie_uuid, type=None, file_id=None):
        # TODO validate structure
        # TODO binary uuid
        if isinstance(id_uri_curie_uuid, cls):
            return id_uri_curie_uuid

        for rx, match_type in cls.compiled:
            match = rx.match(id_uri_curie_uuid)
            if match is not None:
                break
        else:
            msg = f'{id_uri_curie_uuid.encode()!r} matched no known pattern'
            raise idlib.exc.MalformedIdentifierError(msg)

        groups = [g for g in match.groups() if g is not None]

        if match_type == 'uuid':
            if type is None:
                raise TypeError('A raw uuid must be acompanied by a type.')

            id_type = type
            uuid, = groups
            # FIXME this one probably should be resolved since it is composed

        elif match_type == 'uri_api' and 'files' in groups:
            *_, id_type, uuid, _, file_id_string = groups
            file_id = int(file_id_string)
        elif match_type == 'uri_human':
            # strip trailing garbage that is not a uuid
            _mgs = groups
            while _mgs and not cls.compiled[2][0].match(_mgs[-1]):
                _mgs = _mgs[:-1]

            *_, id_type, uuid = _mgs
        else:
            # there is a bunch of garbage up front
            *_, id_type, uuid = groups

        if type is not None and type != id_type:
            raise TypeError(f'type mismatch! {type} != {id_type}')

        id = f'N:{id_type}:{uuid}'

        self = super().__new__(cls, id)
        self.id = id
        self.type = id_type
        self.uuid = uuid
        self.file_id = file_id  # XXX NOTE yes, we are putting a
        # file_id on everything because it simplifies all the checking
        # FIXME this conflicts with aug.PathMeta api where we will need
        # to unpack file_id because id and file_id are dissociated
        return self

    def __init__(self, *args, **kwargs):
        self._identifier = self.curie # XXXXXXXXXXXX FIXME this is probably going to cause issues

    def __str__(self):
        # uri_api would be a more consistent approach but there is a balance
        return self.curie # XXXXXXXXXXXX FIXME this is probably going to cause issues

    def __repr__(self):
        file_id = '' if self.file_id is None else f', file_id={self.file_id}'
        return f'{self.__class__.__name__}({self.id}{file_id})'

    def __hash__(self):
        return hash((self.__class__, self.id, self.file_id))

    def __eq__(self, other):
        return (type(self) == type(other) and
                self.id == other.id and
                self.file_id == other.file_id)  # works because None == None

    def __lt__(self, other):
        return (type(self) == type(other) and
                self.id < other.id)

    @property
    def label(self):
        return self.curie

    @property
    def curie(self):
        return self.type + ':' + self.uuid

    @property
    def uri_api(self):
        # NOTE: this cannot handle file ids
        if self.type == 'package':
            if self.file_id is None:
                endpoint = 'packages/' + self.id
            else:
                endpoint = f'packages/{self.id}/files/{self.file_id}'
        elif self.type == 'collection':
            endpoint = 'collections/' + self.id
        elif self.type == 'dataset':
            endpoint = 'datasets/' + self.id
        elif self.type == 'organization':
            endpoint = 'organizations/' + self.id
        elif self.type == 'user':
            endpoint = 'users/' + self.id
        else:
            raise NotImplementedError(f'TODO type {self.type} needs api endpoint')

        return f'https://api.{self.top_level_domain}/' + endpoint

    def uri_human(self, organization=None, dataset=None):
        if self.type == 'package':
            # FIXME file_id?
            endpoint = f'/{organization.id}/datasets/{dataset.id}/files/v/{self.id}'
        elif self.type == 'collection':
            endpoint = f'/{organization.id}/datasets/{dataset.id}/files/{self.id}'
        elif self.type == 'dataset':
            endpoint = f'/{organization.id}/datasets/{self.id}'
        elif self.type == 'organization':
            endpoint =         f'/{self.id}/datasets'
        else:
            raise NotImplementedError(f'{self.type} TODO need app endpoint')

        return f'https://app.{self.top_level_domain}' + endpoint

    def asStr(self):
        # FIXME conflict with the fact that these ids are default
        # local and unqualified, but use uuids which are unique
        #return self.uri_api
        return self.id


class PennsieveId(BlackfynnId):
    # FIXME this isn't really a subclass but you know how bad oo
    # implementations tend to conflate type with functionality ...

    top_level_domain = 'pennsieve.io'
    (uuid4_regex, canonical_regex, curie_regex, id_regex,
     types, paths, path_elem_regex, uri_api_regex, org_pref,
     uri_human_regex, compiled) = make_bf_id_regex(top_level_domain)


class LocId(idlib.Identifier):

    def __init__(self, id, type):
        self.id = id
        self.type = type
        self.curie = id  # FIXME depends on how id was generated
        sysid, path = id.split(':', 1)
        if '/' not in path:
            raise TypeError(f'WHAT HAVE YOU DONE {path}')
        self.uri_api = 'file://' + path
        self._identifier = self.uri_api

    def uri_human(self, *args, **kwargs):
        return self.uri_api
