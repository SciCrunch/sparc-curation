import copy
import json
import shutil
import itertools
from pathlib import PurePath
from datetime import datetime
from functools import wraps
from collections import deque
import idlib
import rdflib
import htmlfn as hfn
import requests
import ontquery as oq
import augpathlib as aug
from idlib.utils import resolution_chain
from idlib.formats import rdf as _bind_rdf
from ttlser import CustomTurtleSerializer
from xlsx2csv import Xlsx2csv, SheetNotFoundException
from pysercomb.pyr.types import ProtcurExpression, Quantity  # FIXME import slowdown
from pyontutils.core import OntTerm as OTB, OntId as OIDB, cull_prefixes, makeGraph
from pyontutils.utils import isoformat, TZLOCAL
from pyontutils.namespaces import OntCuries, TEMP, sparc, NIFRID, definition
from pyontutils.namespaces import prot, proc, tech, asp, dim, unit, rdf, owl, rdfs
from sparcur import exceptions as exc
from sparcur.utils import log, logd, python_identifier  # FIXME fix other imports
from sparcur.utils import is_list_or_tuple
from sparcur.config import config, auth


__type_registry = {}
def register_type(cls, type_name):
    if type_name in __type_registry:
        raise ValueError(f'Cannot map {cls} to {type_name}. Type already present! '
                         f'{type_name} -> {__type_registry["type_name"]}')

    __type_registry[type_name] = cls


def fromJson(blob):
    if isinstance(blob, dict):
        if 'type' in blob:
            t = blob['type']

            if t == 'identifier':
                type_name = blob['system']
            elif t in ('quantity', 'range'):
                type_name = t
            else:
                breakpoint()
                raise NotImplementedError(f'TODO fromJson for type {t} '
                                          f'currently not implemented\n{blob}')

            return __type_registry[type_name].fromJson(blob)
        else:
            return {k:v
                    if k == 'errors' or k.endswith('_errors') else
                    fromJson(v)
                    for k, v in blob.items()}

    elif isinstance(blob, list):
        return [fromJson(_) for _ in blob]
    else:
        return blob


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
           'bibo': 'http://purl.org/ontology/bibo/',  # crossref
           'prism.basic': 'http://prismstandard.org/namespaces/basic/2.1/',  # crossref
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
    _known_no_label = 'dataset', 'pio.private'

    #def atag(self, curie=False, **kwargs):
        #return hfn.atag(self.iri, self.curie if curie else self.label, **kwargs)  # TODO schema.org ...

    _logged = set()

    @classmethod
    def fromJson(cls, blob):
        assert blob['identifier_type'] == cls.__name__
        identifier = blob['id']
        if isinstance(identifier, cls):
            return identifier
        else:
            return cls(identifier, label=blob['label'])

    @classmethod
    def _already_logged(cls, thing):
        case = thing in cls._logged
        if not case:
            cls._logged.add(thing)

        return case

    def asType(self, _class):
        return _class(self.iri)

    def asUri(self, asType=None):
        return (self.iri
                if asType is None else
                asType(self.iri))

    def asDict(self):
        out = {
            'type': 'identifier',
            'system': self.__class__.__name__,
            #'id': self.iri,
            'id': self,  # XXX
            'label': self.label,
        }
        if hasattr(self, 'synonyms') and self.synonyms:
            out['synonyms'] = self.synonyms

        return out

    def asCell(self, sep='|'):
        if self.label is None:
            _id = self.curie if self.curie else self.iri
            if self.prefix not in self._known_no_label:
                if not self._already_logged(_id):
                    log.error(f'No label {_id}')

            return _id

        return self.label + sep + self.curie

    @property
    def triples_simple(self):
        # method name matches convention from neurondm
        # but I still don't really like this pattern
        # especially since this wouldn't be derived directly
        # from the json from but would/could hit the network again
        s = self.asUri(rdflib.URIRef)
        yield s, rdf.type, owl.Class
        if self.label:
            yield s, rdfs.label, rdflib.Literal(self.label)
        if self.definition:
            yield s, definition, rdflib.Literal(self.definition)
        if self.deprecated:
            yield s, owl.deprecated, rdflib.Literal(True)
        for syn in self.synonyms:
            s, NIFRID.synonyms, rdflib.Literal(syn)


class HasErrors:
    message_passing_key = 'keys_with_errors'
    _already_logged = set()

    def __init__(self, *args, pipeline_stage=None, **kwargs):
        try:
            super().__init__(*args, **kwargs)
        except TypeError as e:  # this is so dumb
            super().__init__()

        self._pipeline_stage = pipeline_stage
        self._errors_set = set()

    @staticmethod
    def errorInKey(data, key):
        mpk = HasErrors.message_passing_key
        if mpk in data:
            data[mpk].append(key)
        else:
            data[mpk] = [key]

    def addError(self, error, pipeline_stage=None, logfunc=None, blame=None, path=None):
        do_log = error not in self._already_logged
        if do_log:
            self._already_logged.add(error)

        stage = (pipeline_stage if pipeline_stage is not None
                 else (self._pipeline_stage if self._pipeline_stage
                       else self.__class__.__name__))
        b = len(self._errors_set)
        et = (error, stage, blame, path)
        self._last_error = et
        self._errors_set.add(et)
        a = len(self._errors_set)
        return a != b and do_log

    def _render(self, e, stage, blame, path):
        o = {'pipeline_stage': stage,
                'blame': blame,}  # FIXME

        if path is not None:
            o['file_path'] = path

        if isinstance(e, str):
            o['message'] = e
            o['type'] = None  # FIXME probably wan our own?

        elif isinstance(e, BaseException):
            o['message'] = str(e)
            o['type'] = str(type(e))

        else:
            raise TypeError(repr(e))

        log.debug(o)
        return o

    @property
    def _errors(self):
        for et in self._errors_set:
            yield self._render(*et)

    def embedLastError(self, data):
        self.embedErrors(data, el=[self._render(*self._last_error)])

    def embedErrors(self, data, el=tuple()):
        if not el:
            el = list(self._errors)

        if el:
            if 'errors' in data:
                data['errors'].extend(el)
            elif el:
                data['errors'] = el


def lj(j):
    """ use with log to format json """
    return '\n' + json.dumps(j, indent=2, cls=JEncode)


def dereference_all_identifiers(obj, stage, *args, path=None, addError=None, **kwargs):
    try:
        dict_literal = _json_identifier_expansion(obj)
    except idlib.exceptions.ResolutionError as e:
        if hasattr(obj, '_cooldown'):
            return obj._cooldown()  # trigger cooldown to simplify issues down the line

        oops = json_export_type_converter(obj)
        msg = (f'{stage.lifters.id} could not resolve '  # FIXME lifters sigh
               f'{type(obj)}: {oops} {obj.asUri()}')
        error = dict(error=msg,
                     pipeline_stage=stage.__class__.__name__,
                     blame='submission',
                     path=tuple(path))

        if addError:
            if addError(**error):
                logd.error(msg)
        else:
            return {'errors': [error]}


def _json_identifier_expansion(obj, *args, **kwargs):
    if isinstance(obj, oq.OntTerm):
        obj.__class__ = OntTerm  # that this works is amazing/terrifying
        return obj.asDict()

    elif isinstance(obj, idlib.Stream):
        if obj._id_class is str:
            return obj.identifier
        else:
            return obj.asDict()
    else:
        return obj


def json_identifier_expansion(obj, *args, path=None, **kwargs):
    """ expand identifiers to json literal form """
    try:
        return _json_identifier_expansion(obj, *args, **kwargs)
    except idlib.exceptions.ResolutionError as e:
        oops = json_export_type_converter(obj)
        msg = f'could not resolve {type(obj)}: {oops}'
        out = {'id': obj,
               'type': 'identifier',
               'system': obj.__class__.__name__,
               'errors': [{'message': msg, 'path': path}]}
        return out


def json_export_type_converter(obj):
    if isinstance(obj, deque):
        return list(obj)
    elif isinstance(obj, ProtcurExpression):
        return obj.json()
    elif isinstance(obj, PurePath):
        return obj.as_posix()
    elif isinstance(obj, Quantity):
        return obj.json()
    elif isinstance(obj, oq.OntTerm):
        return obj.iri
        #return obj.asDict()  # FIXME need a no network/scigraph version
    elif isinstance(obj, idlib.Stream) and hasattr(obj, '_id_class'):
        if obj._id_class is str:
            return obj.identifier
        else:
            return json_export_type_converter(obj.identifier)
            #return obj.asDict()  # FIXME need a no network/scigraph version
    elif isinstance(obj, datetime):
        return isoformat(obj)


class JEncode(json.JSONEncoder):

    def default(self, obj):
        new_obj = json_export_type_converter(obj)
        if new_obj is not None:
            return new_obj
        #else:
            #log.critical(f'{type(obj)} -> {obj}')

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def JFixKeys(obj):
    def jetc(o):
        out = json_export_type_converter(o)
        return out if out else o

    if isinstance(obj, dict):
        return {jetc(k): JFixKeys(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [JFixKeys(v) for v in obj]
    else:
        return obj


def JApplyRecursive(function, obj, *args, condense=False, skip_keys=('errors',), path=None, **kwargs):
    """ *args, **kwargs, and path= are passed to the function """
    def testx(v):
        return v is not None and not (not v and (is_list_or_tuple(v) or isinstance(v, dict)))

    if path is None:
        path = []

    if isinstance(obj, dict):
        out = {k: JApplyRecursive(function, v, *args,
                                  condense=condense,
                                  skip_keys=skip_keys,
                                  path=path + [k],
                                  **kwargs)
               for k, v in obj.items() if k not in skip_keys}

        if condense:
            out = {k:v for k, v in out.items() if testx(v)}

        return function(out, *args, path=path, **kwargs)

    elif is_list_or_tuple(obj):
        out = [JApplyRecursive(function, v, *args,
                               condense=condense,
                               skip_keys=skip_keys,
                               path=path + [i],
                               **kwargs)
               for i, v in enumerate(obj)]
        if condense:
            out = [v for v in out if testx(v)]

        return function(out, *args, path=path, **kwargs)

    else:
        return function(obj, *args, path=path, **kwargs)


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


class BlackfynnInst(BlackfynnId):
    # This isn't equivalent to BlackfynnRemote
    # because it needs to be able to obtain the
    # post pipeline data about that identifier
    @property
    def uri_human(self):
        pass


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
    def get(cls, data, source_path, on_failure=None):
        """ get stops at lists because the number of possible issues explodes
            and we don't hand those here, if you encounter that, use this
            primitive to get the list, then use it again on the members in
            the function making the call where you have the information needed
            to figure out how to handle the error """

        try:
            source_key, node_key, source = cls._get_source(data, source_path)
            return source[source_key]
        except BaseException as e:
            if on_failure is not None:
                return on_failure
            else:
                raise e

    @classmethod
    def pop(cls, data, source_path):
        """ allows us to document removals """
        source_key, node_key, source = cls._get_source(data, source_path)
        if isinstance(source, tuple):
            value = source[source_key]
            new_node = tuple(v for i, v in enumerate(source) if i != source_key)
            parent_source_key, parent_node_key, parent_source = cls._get_source(data, source_path[:-1])
            assert node_key == parent_source_key
            # FIXME will fail if parent_source is a tuple
            parent_source[node_key] = new_node
            return value
        else:
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
            if isinstance(source, dict):
                if node_key in source:
                    source = source[node_key]
                else:
                    # don't move if no source
                    msg = f'did not find {node_key!r} in {tuple(source.keys())}'
                    raise exc.NoSourcePathError(msg)
            elif is_list_or_tuple(source):
                if not isinstance(node_key, int):
                    raise TypeError(f'Wrong type for node_key {type(node_key)} {node_key}. '
                                    'Expected int.')
                source = source[node_key]  # good good let the index errors flow through you
            else:
                raise TypeError(f'Unsupported type {type(source)} for {lj(source)}')

        # for move
        yield (node_key if source_prefixes else
               AtomicDictOperations.__empty_node_key)

        if isinstance(source, dict):
            if source_key not in source:
                try:
                    msg = f'did not find {source_key!r} in {tuple(source.keys())}'
                    raise exc.NoSourcePathError(msg)
                except AttributeError as e:
                    raise TypeError(f'value at {source_path} has wrong type!{lj(source)}') from e
                    #log.debug(f'{source_path}')

        elif is_list_or_tuple(source):
            try:
                source[source_key]
            except IndexError as e:
                msg = f'There is not a {source_key}th value in {lj(source)}'
                raise exc.NoSourcePathError(msg) from e

        else:
            raise TypeError(f'Unsupported type {type(source)} for {lj(source)}')

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
                idmsg = data['id'] if 'id' in data else ''
                raise TypeError(f'derive failed\n{source_paths}\n'
                                f'{derive_function}\n{target_paths}\n'
                                f'{idmsg}\n') from e

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


    @classmethod
    def subpipeline(cls, data, runtime_context, subpipelines, update=True,
                    source_key_optional=True, lifters=None):
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
                try:
                    function(data, target_path, p.data)
                except BaseException as e:
                    import inspect
                    __file = inspect.getsourcefile(pc)
                    __line = inspect.getsourcelines(pc)[-1]
                    if hasattr(p, 'path'):
                        __path = f'"{p.path}"'
                    else:
                        __path = 'unknown input'

                    raise exc.SubPipelineError(
                        f'Error while processing {p}.data for\n{__path}\n'
                        f'{__file} line {__line}') from e

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


# register idlib classes for fromJson
[register_type(i, i.__name__) for i in
 (idlib.Ror, idlib.Doi, idlib.Orcid, idlib.Pio, idlib.Rrid, OntTerm)]
