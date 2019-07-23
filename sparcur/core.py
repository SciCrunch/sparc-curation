import copy
import json
import shutil
import itertools
from pathlib import Path
from datetime import datetime
from functools import wraps
from collections import deque
import rdflib
import htmlfn as hfn
import ontquery as oq
import requests
#from joblib import Memory
from ttlser import CustomTurtleSerializer
from xlsx2csv import Xlsx2csv, SheetNotFoundException
from scibot.utils import resolution_chain
from scibot.extract import normalizeDoi
from pyontutils.core import OntTerm as OTB, OntId as OIDB, cull_prefixes, makeGraph
from pyontutils.utils import isoformat, TZLOCAL
from pyontutils.namespaces import OntCuries, TEMP, sparc
from pyontutils.namespaces import prot, proc, tech, asp, dim, unit, rdf, owl, rdfs
from pysercomb.pyr.units import Expr as ProtcurExpression, _Quant as Quantity  # FIXME import slowdown
from sparcur import exceptions as exc
from sparcur.utils import log, logd, cache, sparc, FileSize, python_identifier  # FIXME fix other imports
from sparcur.config import config


# disk cache decorator
#memory = Memory(config.cache_dir, verbose=0)


xsd = rdflib.XSD
po = CustomTurtleSerializer.predicateOrder
po.extend((sparc.firstName,
           sparc.middleName,
           sparc.lastName,
           xsd.minInclusive,
           xsd.maxInclusive,
           TEMP.hasValue,
           TEMP.hasUnit,))

OntCuries({'orcid':'https://orcid.org/',
           'ORCID':'https://orcid.org/',
           'DOI':'https://doi.org/',
           'ror':'https://ror.org/',
           'dataset':'https://api.blackfynn.io/datasets/N:dataset:',
           'package':'https://api.blackfynn.io/packages/N:package:',
           'user':'https://api.blackfynn.io/users/N:user:',
           'unit': str(unit),
           'dim': str(dim),
           'asp': str(asp),
           'tech': str(tech),
           'awards':str(TEMP['awards/']),
           'sparc':str(sparc),})


class OntId(OIDB):
    pass
    #def atag(self, **kwargs):
        #if 'curie' in kwargs:
            #kwargs.pop('curie')
        #return hfn.atag(self.iri, self.curie, **kwargs)


class OntTerm(OTB, OntId):
    _known_no_label = 'dataset',
    pass
    #def atag(self, curie=False, **kwargs):
        #return hfn.atag(self.iri, self.curie if curie else self.label, **kwargs)  # TODO schema.org ...

    def tabular(self, sep='|'):
        if self.label is None:
            if self.prefix not in self._known_no_label:
                log.error(f'No label {self.curie if self.curie else self.iri}')

            return self.curie if self.curie else self.iri

        return self.label + sep + self.curie


def lj(j):
    """ use with log to format json """
    return '\n' + json.dumps(j, indent=2, cls=JEncode)


class JEncode(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, deque):
            return list(obj)
        elif isinstance(obj, ProtcurExpression):
            return obj.json()
        elif isinstance(obj, Path):
            return obj.as_posix()
        elif isinstance(obj, Quantity):
            return obj.json()

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def zipeq(*iterables):
    """ zip or fail if lengths do not match """

    sentinel = object()
    try:
        gen = itertools.zip_longest(*iterables, fillvalue=sentinel)
    except TypeError as e:
        raise TypeError(f'One of these is not iterable {iterables}') from e

    for zipped in gen:
        if sentinel in zipped:
            raise exc.LengthMismatchError('Lengths do not match! '
                                          'Did you remember to box your function?\n'
                                          f'{iterables}')

        yield zipped


class DoiPrefixes(oq.OntCuries):
    # set these manually since, sigh, factory patterns
    _dict = {}
    _n_to_p = {}
    _strie = {}
    _trie = {}


DoiPrefixes({'DOI':'https://doi.org/',
             'doi':'https://doi.org/',})


class DoiId(OntId):
    _namespaces = DoiPrefixes

    class DoiMalformedError(Exception):
        """ WHAT HAVE YOU DONE!? """

    def __new__(cls, doi_in_various_states_of_mangling=None, iri=None):
        if doi_in_various_states_of_mangling is None and iri is not None:
            doi_in_various_states_of_mangling = iri

        self = super().__new__(cls, prefix='doi', suffix=normalizeDoi(doi_in_various_states_of_mangling))
        self._unnormalized = doi_in_various_states_of_mangling
        return self

    @property
    def valid(self):
        return self.suffix is not None and self.suffix.startswith('10.')

    def asInstrumented(self):
        return DoiInst(self)


class URIInstrumentation(oq.terms.InstrumentedIdentifier):

    @property
    def headers(self):
        """ request headers """
        if not hasattr(self, '_headers'):
            resp = requests.head(self.iri)  # TODO status handling for all these
            self._headers = resp.headers

        return self._headers

    @property
    def resolution_chain(self):
        # FIXME what should an identifier object represent?
        # the eternal now of the identifier? or the state
        # that it was in when this particular representation
        # was created? This means that really each one of these
        # objects should be timestamped and that equiality of
        # instrumented objects should return false, which seems
        # very bad for usability ...
        if not hasattr(self, '_resolution_chain'):
            # FIXME the chain should at least be formed out of
            # IriHeader objects ...
            self._resolution_chain = [uri for uri in resolution_chain(self)]

        yield from self._resolution_chain

    def resolve(self, target_class=None):
        """ match the terminology used by pathlib """
        # TODO generic probing instrumented identifier matcher
        # by protocol, domain name, headers, etc.
        for uri in self.resolution_chain:
            pass

        if target_class is not None:
            return target_class(uri)

        else:
            return uri  # FIXME TODO identifier it


class DoiInst(URIInstrumentation, DoiId):
    # TODO FIXME pull this into idlib or whatever we are calling it
    #def __new__(self, doi_thing):
        # FIXME autofetch ... hrm ... data vs metadata ...
        #return super().__new__(doi_thing)

    @property
    def metadata(self):
        # e.g. crossref, datacite, etc.
        # so this stuff isnt quite to the spec that is doccumented here
        # https://crosscite.org/docs.html
        # nor here
        # https://support.datacite.org/docs/datacite-content-resolver
        accept = (
            'application/vnd.datacite.datacite+json, '  # first so it can fail
            'application/json, '  # undocumented fallthrough for crossref ?
        )
        resp = requests.get(self, headers={'Accept': accept})
        if resp.ok:
            return resp.json()
        else:
            resp.raise_for_status()  # TODO see if this is really a good idea or not

    @property
    def ttl(self):
        # both datacite and crossref produce in turtle
        resp = requests.get(self, headers={'Accept': 'text/turtle'})
        return resp.text

    @property
    def data(self):
        # FIXME TODO should the data associated with the doi
        # be the metadata about the object or the object itself?
        # from a practical standpoint derefercing the doi is
        # required before you can content negotiate on the
        # actual document itself, which is a practical necessity
        # if somewhat confusing
        return self.metadata


class OrcidPrefixes(oq.OntCuries):
    # set these manually since, sigh, factory patterns
    _dict = {}
    _n_to_p = {}
    _strie = {}
    _trie = {}


OrcidPrefixes({'orcid':'https://orcid.org/',
               'ORCID':'https://orcid.org/',})


class OrcidId(OntId):
    _namespaces = OrcidPrefixes

    class OrcidMalformedError(Exception):
        """ WHAT HAVE YOU DONE!? """

    class OrcidLengthError(OrcidMalformedError):
        """ wrong length """

    class OrcidChecksumError(OrcidMalformedError):
        """ failed checksum """

    @property
    def checksumValid(self):
        """ see
        https://support.orcid.org/hc/en-us/articles/360006897674-Structure-of-the-ORCID-Identifier
        """

        try:
            *digits, check_string = self.suffix.replace('-', '')
            check = 10 if check_string == 'X' else int(check_string)
            total = 0
            for digit_string in digits:
                total = (total + int(digit_string)) * 2

            remainder = total % 11
            result = (12 - remainder) % 11
            return result == check
        except ValueError as e:
            raise self.OrcidChecksumError(self) from e


class _PioPrefixes(oq.OntCuries): pass
PioPrefixes = _PioPrefixes.new()
PioPrefixes({'pio.view': 'https://www.protocols.io/view/',
             'pio.edit': 'https://www.protocols.io/edit/',  # sigh
             'pio.private': 'https://www.protocols.io/private/',
             'pio.fileman': 'https://www.protocols.io/file-manager/',
             'pio.api': 'https://www.protocols.io/api/v3/protocols/',
})


class PioId(OntId):
    _namespaces = PioPrefixes

    def __new__(cls, curie_or_iri=None, iri=None, prefix=None, suffix=None):
        if curie_or_iri is None and iri:
            curie_or_iri = iri

        if curie_or_iri is not None:
            # FIXME trailing nonsense for abstract etc
            normalized = curie_or_iri.replace('://protocols.io', '://www.protocols.io')
        else:
            normalized = None

        self = super().__new__(cls, curie_or_iri=normalized, iri=iri,
                               prefix=prefix, suffix=suffix)

        self._unnormalized = curie_or_iri if curie_or_iri else self.iri
        return self
        
    @property
    def uri_api(self):
        return self.__class__(prefix='pio.api', suffix=self.slug)

    def normalize(self):
        return self

    @property
    def slug(self):
        if self.suffix is None:
            breakpoint()
        return self.suffix.rsplit('/', 1)[0] if '/' in self.suffix else self.suffix


class PioInst(URIInstrumentation, PioId):
    """ instrumented protocols """
    # FIXME defining this here breaks the import chain
    # since protocols.py imports from core.py (sigh)
    _wants_instance = '.protocols.ProtocolData'  # this is an awful pattern
    # but what do you want :/

    @property
    def doi(self):
        data = self.data
        if data:
            doi = data['doi']
            if doi:
                return DoiId(doi)

    @property
    def uri_human(self):  # FIXME HRM ... confusion with pio.private iris
        """ the not-private uri """
        data = self.data
        if data:
            uri = data['uri']
            if uri:
                return self.__class__(prefix='pio.view', suffix=uri)

    @property
    def data(self):
        if not hasattr(self, '_data'):
            blob = self._protocol_data.protocol(self.iri)
            self._status_code = blob['status_code']
            self._data = blob['protocol']

        return self._data

    @property
    def hasVersions(self):
        return bool(self.data['has_versions'])

    @property
    def versions(self):
        yield from self.data['versions']  # TODO ...

    @property
    def created(self):
        # FIXME I don't think this is TZLOCAL for any reason beyond accident of circumstances
        # I think this is PDT i.e. the location of the protocols.io servers
        tzl = TZLOCAL()
        return datetime.fromtimestamp(self.data['created_on'], tz=tzl)

    @property
    def updated(self):
        tzl = TZLOCAL()
        return datetime.fromtimestamp(self.data['changed_on'], tz=tzl)

    @property
    def creator(self):
        return PioUserInst('pio.user:' + self.data['creator']['username'])

    @property
    def authors(self):
        for u in self.data['authors']:
            yield PioUserInst(prefix='pio.user', suffix=u['username'])


class _PioUserPrefixes(oq.OntCuries): pass
PioUserPrefixes = _PioUserPrefixes.new()
PioUserPrefixes({'pio.user': 'https://www.protocols.io/researchers/',
                 'pio.api.user': 'https://www.protocols.io/api/v3/researchers/',
})


class PioUserId(OntId):
    _namespaces = PioUserPrefixes


class PioUserInst(URIInstrumentation, PioUserId):
    _wants_instance = '.protocols.ProtocolData'  # this is an awful pattern

    @property
    def uri_human(self):
        return self.__class__(prefix='pio.user', suffix=self.suffix)

    @property
    def uri_api(self):
        return self.__class__(prefix='pio.api.user', suffix=self.suffix)

    @property
    def data(self):
        if not hasattr(self, '_data'):
            uri = self.uri_api + '?with_extras=1'
            # FIXME this is a dumb an convoluted way to get authed api access
            blob = self._protocol_data.get(uri)
            self._status_code = blob['status_code']
            self._data = blob['researcher']

        return self._data

    @property
    def orcid(self):
        orcid = self.data['orcid']
        if orcid is not None:
            return OrcidId(prefix='orcid', suffix=orcid)


class _RorPrefixes(oq.OntCuries): pass
RorPrefixes = _RorPrefixes.new()
RorPrefixes({'ror': 'https://ror.org/',
             'ror.api': 'https://api.ror.org/organizations/',
})


class RorId(OntId):
    _namespaces = RorPrefixes
    # TODO checksumming
    # TODO FIXME for ids like this should we render only the suffix
    # since the prefix is redundant with the identifier type?
    # initial answer: yes


class RorInst(URIInstrumentation, RorId):

    @property
    def data(self):
        return self._data(self.suffix)

    @cache(Path(config.cache_dir, 'ror_json'))
    def _data(self, suffix):
        # TODO data endpoint prefix ?? vs data endpoint pattern ...
        resp = requests.get(RorId(prefix='ror.api', suffix=suffix))
        if resp.ok:
            return resp.json()

    @property
    def name(self):
        return self.data['name']

    label = name  # map their schema to ours

    def asExternalId(self, id_class):
        eids = self.data['external_ids']
        if id_class._ror_key in eids:
            eid_record = eids[id_class._ror_key]
            if eid_record['preferred']:
                eid = eid_record['preferred']
            else:
                eid_all = eid_record['all']
                if isinstance(eid_all, str):  # https://github.com/ror-community/ror-api/issues/53
                    eid = eid_all
                else:
                    eid = eid_all[0]

            return id_class(eid)

    _type_map = {
        'Education': TEMP.Institution,
    }
    @property
    def institutionTypes(self):
        for t in self.data['types']:
            yield self._type_map[t]

    @property
    def synonyms(self):
        d = self.data
        # FIXME how to deal with type conversion an a saner way ...
        yield from [rdflib.Literal(a) for a in d['aliases']]
        yield from [rdflib.Literal(a) for a in d['acronyms']]
        yield from [rdflib.Literal(l['label'], lang=l['iso639']) for l in d['labels']]

    @property
    def triples_gen(self):
        """ produce a triplified version of the record """
        s = self.u
        a = rdf.type
        yield s, a, owl.NamedIndividual
        for o in self.institutionTypes:
            yield s, a, o

        yield s, rdfs.label, rdflib.Literal(self.label)
        for o in self.synonyms:
            yield s, a, rdflib.Literal(o)

        # TODO also yeild all the associated grid identifiers


class IsniId(OntId):  # TODO
    prefix = 'http://isni.org/isni/'
    _ror_key = 'ISNI'


class AutoId:
    """ dispatch to type on identifier structure """
    def __new__(cls, something):
        if '10.' in something:
            if 'http' in something and 'doi.org' not in something:
                pass  # probably a publisher uri that uses the handle
            else:
                return DoiId(something)

        if 'orcid' in something:
            return OrcidId(something)

        if '/ror.org/' in something or something.startswith('ror:'):
            return RorId(something)

        if 'protocols.io' in something:
            return PioId(something)

        return OntId(something)


class AutoInst:
    def __new__(cls, something):
        return AutoId(something).asInstrumented()


def get_right_id(uri):
    # FIXME this is a bad way to do this ...
    if isinstance(uri, DoiId) or 'doi' in uri:
        if isinstance(uri, DoiId):
            di = uri.asInstrumented()
        elif 'doi' in uri:
            di = DoiInst(uri)

        pi = di.resolve(PioId)

    else:
        if not isinstance(uri, PioId):
            pi = PioId(uri)  #.normalize()
        else:
            pi = uri

    return pi


class BlackfynnId(str):
    """ put all static information derivable from a blackfynn id here """
    def __new__(cls, id):
        # TODO validate structure
        self = super().__new__(cls, id)
        gotem = False
        for type_ in ('package', 'collection', 'dataset', 'organization'):
            name = 'is_' + type_
            if not gotem:
                gotem = self.startswith(f'N:{type_}:')
                setattr(self, name, gotem)
            else:
                setattr(self, name, False)

        return self

    @property
    def uri_api(self):
        # NOTE: this cannot handle file ids
        if self.is_dataset:
            endpoint = 'datasets/' + self.id
        elif self.is_organization:
            endpoint = 'organizations/' + self.id
        else:
            endpoint = 'packages/' + self.id

        return 'https://api.blackfynn.io/' + endpoint

    def uri_human(self, prefix):
        # a prefix is required to construct these
        return self  # TODO


class JTList:
    pass


class JTDict:
    pass


def JT(blob):
    """ this is not a class but is a function hacked to work like one """
    def _populate(blob, top=False):
        if isinstance(blob, list) or isinstance(blob, tuple):
            # TODO alternatively if the schema is uniform, could use bc here ...
            def _all(self, l=blob):  # FIXME don't autocomplete?
                keys = set(k for b in l
                           if isinstance(b, dict)
                           for k in b)
                obj = {k:[] for k in keys}
                _list = []
                _other = []
                for b in l:
                    if isinstance(b, dict):
                        for k in keys:
                            if k in b:
                                obj[k].append(b[k])
                            else:
                                obj[k].append(None)

                    elif any(isinstance(b, t) for t in (list, tuple)):
                        _list.append(JT(b))

                    else:
                        _other.append(b)
                        for k in keys:
                            obj[k].append(None)  # super inefficient

                if _list:
                    obj['_list'] = JT(_list)

                if obj:
                    j = JT(obj)
                else:
                    j = JT(blob)

                if _other:
                    #obj['_'] = _other  # infinite, though lazy
                    setattr(j, '_', _other)

                setattr(j, '_b', blob)
                #lb = len(blob)
                #setattr(j, '__len__', lambda: lb)  # FIXME len()
                return j

            def it(self, l=blob):
                for b in l:
                    if any(isinstance(b, t) for t in (dict, list, tuple)):
                        yield JT(b)
                    else:
                        yield b

            if top:
                # FIXME iter is non homogenous
                return [('__iter__', it), ('_all', property(_all))]
            #elif not [e for e in b if isinstance(self, dict)]:
                #return property(id)
            else:
                # FIXME this can render as {} if there are no keys
                return property(_all)
                #obj = {'_all': property(_all),
                       #'_l': property(it),}

                #j = JT(obj)
                #return j

                #nl = JT(obj)
                #nl._list = blob
                #return property(it)

        elif isinstance(blob, dict):
            if top:
                out = [('_keys', tuple(blob))]
                for k, v in blob.items():  # FIXME normalize keys ...
                    nv = _populate(v)
                    out.append((k, nv))
                    #setattr(cls, k, nv)
                return out
            else:
                return JT(blob)

        else:
            if top:
                raise exc.UnhandledTypeError('asdf')
            else:
                @property
                def prop(self, v=blob):
                    return v

                return prop

    def _repr(self, b=blob):  # because why not
        return 'JT(\n' + lj(b) + '\n)'

    def query(self, *path):
        """ returns None at first failure """
        j = self
        for key in path:
            j = getattr(j, key, None)
            if j is None:
                return

        return j

    # additional thought required for how to integrate these into this
    # shameling abomination
    #adopts
    #dt = DictTransformer

    #cd = {k:v for k, v in _populate(blob, True)}

    # populate the top level
    cd = {k:v for k, v in ((a, b) for t in _populate(blob, True)
                           for a, b in (t if isinstance(t, list) else (t,)))}
    cd['__repr__'] = _repr
    cd['query'] = query

    if isinstance(blob, dict):
        type_ = JTDict
    elif isinstance(blob, list):
        type_ = JTList
    else:
        type_ = object

    nc = type('JT' + str(type(blob)), (type_,), cd)  # use object to prevent polution of ns
    #nc = type('JT' + str(type(blob)), (type(blob),), cd)
    return nc()


class AtomicDictOperations:
    """ functions that modify dicts in place """

    # note: no delete is implemented at the moment ...
    # in place modifications means that delete can loose data ...

    __empty_node_key = object()

    @staticmethod
    def apply(function, *args,
              source_key_optional=False,
              extra_error_types=tuple(),
              failure_value=None):
        error_types = (exc.NoSourcePathError,) + extra_error_types
        try:
            return function(*args)
        except error_types as e:
            if not source_key_optional:
                raise e
            else:
                logd.debug(e)
                return failure_value
        except exc.LengthMismatchError as e:
            raise e

    @staticmethod
    def add(data, target_path, value, fail_on_exists=True, update=False):
        # type errors can occur here ...
        # e.g. you try to go to a string
        if not [_ for _ in (list, tuple) if isinstance(target_path, _)]:
            raise TypeError(f'target_path is not a list or tuple! {type(target_path)}')
        target_prefixes = target_path[:-1]
        target_key = target_path[-1]
        target = data
        for target_name in target_prefixes:
            if target_name not in target:  # TODO list indicies
                target[target_name] = {}

            target = target[target_name]

        if update:
            pass
        elif fail_on_exists and target_key in target:
            raise exc.TargetPathExistsError(f'A value already exists at path {target_path}\n'
                                            f'{lj(data)}')

        target[target_key] = value

    @classmethod
    def update(cls, data, target_path, value):
        cls.add(data, target_path, value, update=True)

    @classmethod
    def get(cls, data, source_path):
        """ get stops at lists because the number of possible issues explodes
            and we don't hand those here, if you encounter that, use this
            primitive to get the list, then use it again on the members in
            the function making the call where you have the information needed
            to figure out how to handle the error """

        source_key, node_key, source = cls._get_source(data, source_path)
        return source[source_key]

    @classmethod
    def pop(cls, data, source_path):
        """ allows us to document removals """
        source_key, node_key, source = cls._get_source(data, source_path)
        return source.pop(source_key)

    @classmethod
    def copy(cls, data, source_path, target_path):
        cls._copy_or_move(data, source_path, target_path)

    @classmethod
    def move(cls, data, source_path, target_path):
        cls._copy_or_move(data, source_path, target_path, move=True)

    @staticmethod
    def _get_source(data, source_path):
        #print(source_path, target_path)
        source_prefixes = source_path[:-1]
        source_key = source_path[-1]
        yield source_key  # yield this because we don't know if move or copy
        source = data
        for node_key in source_prefixes:
            if node_key in source:
                source = source[node_key]
            else:
                # don't move if no source
                msg = f'did not find {node_key!r} in {tuple(source.keys())}'
                raise exc.NoSourcePathError(msg)

        # for move
        yield (node_key if source_prefixes else
               AtomicDictOperations.__empty_node_key)

        if source_key not in source:
            try:
                msg = f'did not find {source_key!r} in {tuple(source.keys())}'
                raise exc.NoSourcePathError(msg)
            except AttributeError as e:
                raise TypeError(f'value at {source_path} has wrong type!{lj(source)}') from e
                #log.debug(f'{source_path}')

        yield source

    @classmethod
    def _copy_or_move(cls, data, source_path, target_path, move=False):
        """ if exists ... """
        source_key, node_key, source = cls._get_source(data, source_path)
        # do not catch errors here, deal with that in the callers that people use directly
        if move:
            _parent = source  # incase something goes wrong
            source = source.pop(source_key)
        else:
            source = source[source_key]

            if source != data:  # this should .. always happen ???
                source = copy.deepcopy(source)  # FIXME this will mangle types e.g. OntId -> URIRef
                # copy first then modify means we need to deepcopy these
                # otherwise we would delete original forms that were being
                # saved elsewhere in the schema for later
            else:
                raise BaseException('should not happen?')

        try:
            cls.add(data, target_path, source)
        finally:
            # this will change key ordering but
            # that is expected, and if you were relying
            # on dict key ordering HAH
            if move and  node_key is not AtomicDictOperations.__empty_node_key:
                _parent[node_key] = source


adops = AtomicDictOperations()


class _DictTransformer:
    """ transformations from rules """

    @staticmethod
    def BOX(function):
        """ Combinator that takes a function and returns a version of
            that function whose return value is boxed as a tuple.
            This makes it _much_ easier to understand what is going on
            rather than trying to count commas when trying to count
            how many return values are needed for a derive function """

        @wraps(function)
        def boxed(*args, **kwargs):
            return function(*args, **kwargs),

        return boxed

    @staticmethod
    def add(data, adds):
        """ adds is a list (or tuples) with the following structure
            [[target-path, value], ...]
        """
        for target_path, value in adds:
            adops.add(data, target_path, value)

    @staticmethod
    def update(data, updates, source_key_optional=False):
        """ updates is a list (or tuples) with the following structure
            [[path, function], ...]
        """

        for path, function in updates:
            value = adops.get(data, path)
            new = function(value)
            adopts.update(data, path, new)

    @staticmethod
    def get(data, gets, source_key_optional=False):
        """ gets is a list with the following structure
            [source-path ...] """

        for source_path in gets:
            yield adops.apply(adops.get, data, source_path,
                                              source_key_optional=source_key_optional)

    @staticmethod
    def pop(data, pops, source_key_optional=False):
        """ pops is a list with the following structure
            [source-path ...] """

        for source_path in pops:
            yield adops.apply(adops.pop, data, source_path,
                              source_key_optional=source_key_optional)

    @staticmethod
    def delete(data, deletes, source_key_optional=False):
        """ delets is a list with the following structure
            [source-path ...]
            THIS IS SILENT YOU SHOULD USE pop instead!
            The tradeoff is that if you forget to express
            the pop generator it will also be silent until
            until schema catches it """

        for source_path in deletes:
            adops.pop(data, source_path)

    @staticmethod
    def copy(data, copies, source_key_optional=False):  # put this first to clarify functionality
        """ copies is a list wth the following structure
            [[source-path, target-path] ...]
        """
        for source_path, target_path in copies:
            # don't need a base case for thing?
            # can't lift a dict outside of itself
            # in this context
            adops.apply(adops.copy, data, source_path, target_path,
                                        source_key_optional=source_key_optional)

    @staticmethod
    def move(data, moves, source_key_optional=False):
        """ moves is a list with the following structure
            [[source-path, target-path] ...]
        """
        for source_path, target_path in moves:
            adops.apply(adops.move, data, source_path, target_path,
                                        source_key_optional=source_key_optional)

    @classmethod
    def derive(cls, data, derives, source_key_optional=True, empty='CULL', cheaty_face=None):
        """ [[[source-path, ...], function, [target-path, ...]], ...] """
        # if you have source key option True and empty='OK' you will get loads of junk
        allow_empty = empty == 'OK' and not empty == 'CULL'
        error_empty = empty == 'ERROR'
        def empty(value):
            empty = (value is None or
                     hasattr(value, '__iter__')
                     and not len(value))
            if empty and error_empty:
                raise ValueError(f'value to add may not be empty!')
            return empty or allow_empty and not empty

        failure_value = tuple()
        for source_paths, derive_function, target_paths in derives:
            # FIXME zipeq may cause adds to modify in place in error?
            # except that this is really a type checking thing on the function
            def defer_get(*get_args):
                """ if we fail to get args then we can't gurantee that
                    derive_function will work at all so we wrap the lot """
                args = cls.get(*get_args)
                return derive_function(*args)

            def express_zip(*zip_args):
                return tuple(zipeq(*zip_args))

            try:
                if not target_paths:
                    # allows nesting
                    adops.apply(defer_get, data, source_paths,
                                source_key_optional=source_key_optional)
                    continue

                cls.add(data,
                        ((tp, v) for tp, v in
                         adops.apply(express_zip, target_paths,
                                     adops.apply(defer_get, data, source_paths,
                                                 source_key_optional=source_key_optional),
                                     source_key_optional=source_key_optional,
                                     extra_error_types=(TypeError,),
                                     failure_value=tuple())
                        if not empty(v)))
            except TypeError as e:
                log.error('wat')
                raise TypeError(f'derive failed\n{source_paths}\n'
                                f'{derive_function}\n{target_paths}\n') from e

    @staticmethod
    def _derive(data, derives, source_key_optional=True, allow_empty=False):
        # OLD
        """ derives is a list with the following structure
            [[[source-path, ...], derive-function, [target-path, ...]], ...]

        """
        # TODO this is an implementaiton of copy that has semantics for handling lists
        for source_path, function, target_paths in derives:
            source_prefixes = source_path[:-1]
            source_key = source_path[-1]
            source = data
            failed = False
            for i, node_key in enumerate(source_prefixes):
                log.debug(lj(source))
                if node_key in source:
                    source = source[node_key]
                else:
                    msg = f'did not find {node_key} in {source.keys()}'
                    if not i:
                        log.error(msg)
                        failed = True
                        break
                    raise exc.NoSourcePathError(msg)
                if isinstance(source, list) or isinstance(source, tuple):
                    new_source_path = source_prefixes[i + 1:] + [source_key]
                    new_target_paths = [tp[i + 1:] for tp in target_paths]
                    new_derives = [(new_source_path, function, new_target_paths)]
                    for sub_source in source:
                        _DictTransformer.derive(sub_source, new_derives,
                                                source_key_optional=source_key_optional)

                    return  # no more to do here

            if failed:
                continue  # sometimes things are missing we continue to others

            if source_key not in source:
                msg = f'did not find {source_key} in {source.keys()}'
                if source_key_optional:
                    return logd.info(msg)
                else:
                    raise exc.NoSourcePathError(msg)

            source_value = source[source_key]

            new_values = function(source_value)
            if len(new_values) != len(target_paths):
                log.debug(f'{source_paths} {target_paths}')
                raise TypeError(f'wrong number of values returned for {function}\n'
                                f'was {len(new_values)} expect {len(target_paths)}')
            #temp = b'__temporary'
            #data[temp] = {}  # bytes ensure no collisions
            for target_path, value in zip(target_paths, new_values):
                if (not allow_empty and
                    (value is None or
                     hasattr(value, '__iter__') and not len(value))):
                    raise ValueError(f'value to add to {target_path} may not be empty!')
                adops.add(data, target_path, value, fail_on_exists=True)
                #heh = str(target_path)
                #data[temp][heh] = value
                #source_path = temp, heh  # hah
                #self.move(data, source_path, target_path)

                #data.pop(temp)


    @staticmethod
    def subpipeline(data, runtime_context, subpipelines, update=True, source_key_optional=True, lifters=None):
        """
            [[[[get-path, add-path], ...], pipeline-class, target-path], ...]

            NOTE: this function is a generator, you have to express it!
        """

        class DataWrapper:
            def __init__(self, data):
                self.data = data

        prepared = []
        for get_adds, pipeline_class, target_path in subpipelines:
            selected_data = {}
            ok = True
            for get_path, add_path in get_adds:
                try:
                    value = adops.get(data, get_path)
                    if add_path is not None:
                        adops.add(selected_data, add_path, value)
                    else:
                        selected_data = value
                except exc.NoSourcePathError as e:
                    if source_key_optional:
                        yield get_path, e, pipeline_class
                        ok = False
                        break  # breaks the inner loop
                    else:
                        raise e

            if not ok:
                continue

            log.debug(lj(selected_data))
            prepared.append((target_path, pipeline_class, DataWrapper(selected_data),
                             lifters, runtime_context))

        function = adops.update if update else adops.add
        for target_path, pc, *args in prepared:
            p = pc(*args)
            if target_path is not None:
                function(data, target_path, p.data)
            else:
                p.data  # trigger the pipeline since it is stateful

            yield p

    @staticmethod
    def lift(data, lifts, source_key_optional=True):
        """ 
        lifts are lists with the following structure
        [[path, function], ...]

        the only difference from derives is that lift
        overwrites the underlying data (e.g. a filepath
        would be replaced by the contents of the file)

        """

        for path, function in lifts:
            try:
                old_value = adops.get(data, path)
            except exc.NoSourcePathError as e:
                if source_key_optional:
                    logd.exception(str(type(e)))
                    continue
                else:
                    raise e

            new_value = function(old_value)
            adops.add(data, path, new_value, fail_on_exists=False)

DictTransformer = _DictTransformer()


def copy_all(source_parent, target_parent, *fields):
    return [[source_parent + [field], target_parent + [field]]
            for field in fields]

def normalize_tabular_format(project_path):
    kwargs = {
        'delimiter' : '\t',
        'skip_empty_lines' : True,
        'outputencoding': 'utf-8',
    }
    sheetid = 0
    for xf in project_path.rglob('*.xlsx'):
        xlsx2csv = Xlsx2csv(xf, **kwargs)
        with open(xf.with_suffix('.tsv'), 'wt') as f:
            try:
                xlsx2csv.convert(f, sheetid)
            except SheetNotFoundException as e:
                log.warning(f'Sheet weirdness in {xf}\n{e}')


def extract_errors(dict_):
    for k, v in dict_.items():
        if k == 'errors':
            yield from v
        elif isinstance(v, dict):
            yield from extract_errors(v)


def get_all_errors(_with_errors):
    """ A better and easier to interpret measure of completeness. """
    # TODO deduplicate by tracing causes
    # TODO if due to a missing required report expected value of missing steps
    return list(extract_errors(_with_errors))


class JPointer(str):
    """ a class to mark json pointers for resolution """
