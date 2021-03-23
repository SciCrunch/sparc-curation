import io
import os
import re
import logging
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
from sparcur.config import config

log = makeSimpleLogger('sparcur')
logd = log.getChild('data')
loge = log.getChild('export')

# set augpathlib log format to pyontutils (also sets all child logs)
_alog.removeHandler(_alog.handlers[0])
_alog.addHandler(log.handlers[0])
# idlib logs TODO move to pyontutils probably?
_ilog.removeHandler(_alog.handlers[0])
_ilog.addHandler(log.handlers[0])


__type_registry = {None: None}
def register_type(cls, type_name):
    if type_name in __type_registry:
        if __type_registry[type_name] is cls:
            # better to do this check here than to force
            # all callers to check for themselves which
            # can fail if two separate systems try to
            # register the same type
            return

        raise ValueError(f'Cannot map {cls} to {type_name}. Type already present! '
                         f'{type_name} -> {__type_registry[type_name]}')

    __type_registry[type_name] = cls


def register_all_types():
    # as a side effect this registers idlib streams and OntTerm
    # sigh doing anything in the top level of python :/
    import sparcur.core
    import sparcur.paths  # also a top level registration

    # this is not done at top level because it is quite slow
    from pysercomb.pyr import units as pyru
    [register_type(c, c.tag) for c in (pyru._Quant, pyru.Range, pyru.Approximately)]


class IdentityJsonType:
    """ use to register types that should not be recursed upon e.g.
        because they contain external use of type that does not align
        with our usage """

    @classmethod
    def fromJson(cls, blob):
        return blob


def fromJson(blob):
    if isinstance(blob, dict):
        if 'type' in blob:
            t = blob['type']

            if t == 'identifier':
                type_name = blob['system']
            elif t in ('quantity', 'range'):
                type_name = t
            elif t not in __type_registry:
                breakpoint()
                raise NotImplementedError(f'TODO fromJson for type {t} '
                                          f'currently not implemented\n{blob}')
            else:
                type_name = t

            cls = __type_registry[type_name]
            if cls is not None:
                return cls.fromJson(blob)

        return {k:v
                if k == 'errors' or k.endswith('_errors') else
                fromJson(v)
                for k, v in blob.items()}

    elif isinstance(blob, list):
        return [fromJson(_) for _ in blob]
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


def expand_label_curie(rows_of_terms):
    return [[value for term in rot for value in
             (term.label if term is not None else '',
              term.curie if term is not None else '')]
            for rot in rows_of_terms]


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
    #from blackfynn.log import get_logger; bflog = get_logger()
    #silence_loggers(bflog.parent)  # let's not

    sfh = SimpleFileHandler(log_file, log)
    sfh(alog, idlog, oalog, oqlog, prlog, pylog)#, bflog)


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


want_prefixes = ('TEMP', 'FMA', 'UBERON', 'PATO', 'NCBITaxon', 'ilxtr', 'sparc',
                 'BIRNLEX', 'tech', 'unit', 'ILX', 'lex',)


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
    with path:
        with os.popen(command) as p:
            string = p.read()

    path_strings = string.split('\n')  # XXX posix path names can contain newlines
    # XXXXXXXXXXXXXXXXXXX REMINDER THAT THIS IS NOT SORTED
    # https://doi.org/10.1021/acs.orglett.9b03216
    paths = [path / s for s in path_strings if s][1:]  # leave out the parent folder itself
    return paths


def transitive_paths(path, exclude_patterns=tuple()):
    """Fast list of all child directories using unix find."""
    hrm = ' '.join(['-not -path ' + repr(pat) for pat in exclude_patterns])
    if hrm:
        hrm = ' ' + hrm
    command = f"""find -not -path '.operations*'{hrm}"""
    # TODO failover to builtin rglob
    return _transitive_(path, command)


def transitive_dirs(path):
    """Fast list of all child directories using unix find."""
    command = """find -type d"""
    # TODO failover to builtin rglob + filter
    return _transitive_(path, command)


def unicode_truncate(string, length):
    """ Truncate unicode string to at most maximum length.
        If truncation splits a multibyte charachter then ignore
        the final truncated bytes. """
    return string.encode()[:length].decode(errors='ignore')


class BlackfynnId(idlib.Identifier):
    """ put all static information derivable from a blackfynn id here """
    uuid4_regex = ('([0-9a-f]{8}-'
                   '[0-9a-f]{4}-'
                   '4[0-9a-f]{3}-'
                   '[89ab][0-9a-f]{3}-'
                   '[0-9a-f]{12})')
    canonical_regex = (
        # XXX this is not implemented right now but could be
        '^https://api.blackfynn.io/id/N:([a-z]):' + uuid4_regex + '$'
    )
    curie_regex = '([a-z]+):' + uuid4_regex
    id_regex = 'N:' + curie_regex
    types = ('package', 'collection', 'dataset', 'organization', 'user', 'team')
    paths = 'viewer', 'files', *[t + 's' for t in types]  # WHO LOVES CLASS SCOPE WHEEEE LOL PYTHON
    path_elem_regex = '([a-z]+)'

    uri_api_regex = ('https://api.blackfynn.io/' + path_elem_regex + '/' + id_regex + '(?:/' + path_elem_regex + '/([^/]+))?/?')
    org_pref = 'https://app.blackfynn.io/N:([a-z]+):'
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

    def __repr__(self):
        file_id = '' if self.file_id is None else f', file_id={self.file_id}'
        return f'{self.__class__.__name__}({self.id}{file_id})'

    def __hash__(self):
        return hash((self.__class__, self.id, self.file_id))

    def __eq__(self, other):
        return (type(self) == type(other) and
                self.id == other.id and
                self.file_id == other.file_id)  # works because None == None

    @property
    def curie(self):
        return self.type + ':' + self.uuid

    @property
    def uri_api(self):
        # NOTE: this cannot handle file ids
        if self.type == 'dataset':
            endpoint = 'datasets/' + self.id
        elif self.type == 'organization':
            endpoint = 'organizations/' + self.id
        elif self.type == 'collection':
            endpoint = 'collections/' + self.id
        elif self.type == 'package':
            if self.file_id is None:
                endpoint = 'packages/' + self.id
            else:
                endpoint = f'packages/{self.id}/files/{self.file_id}'
        else:
            raise NotImplementedError(f'{self.type} TODO need api endpoint')

        return 'https://api.blackfynn.io/' + endpoint

    def uri_human(self, organization=None, dataset=None):
        # a prefix is required to construct these
        return self  # TODO

    def asStr(self):
        # FIXME conflict with the fact that these ids are default
        # local and unqualified, but use uuids which are unique
        #return self.uri_api
        return self.id


class BlackfynnInst(BlackfynnId):
    # This isn't equivalent to BlackfynnRemote
    # because it needs to be able to obtain the
    # post pipeline data about that identifier
    @property
    def uri_human(self):
        pass
