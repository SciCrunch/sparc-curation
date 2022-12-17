import copy
import json
import itertools
from types import GeneratorType
from pathlib import PurePath
from datetime import datetime, date, time
from functools import wraps
from collections import deque, defaultdict
import idlib
import rdflib
import ontquery as oq
from idlib.formats import rdf as _bind_rdf  # imported for side effect
from ttlser import CustomTurtleSerializer
from xlsx2csv import Xlsx2csv, SheetNotFoundException
from pysercomb.pyr.types import ProtcurExpression, Quantity, AJ, Measurement
from pyontutils.core import OntTerm as OTB, OntId as OIDB
from pyontutils.utils import isoformat
from pyontutils.namespaces import OntCuries, TEMP, sparc, NIFRID, definition
from pyontutils.namespaces import tech, asp, dim, unit, rdf, owl, rdfs
from sparcur import exceptions as exc
from sparcur.utils import log, logd  # FIXME fix other imports
from sparcur.utils import is_list_or_tuple, register_type, IdentityJsonType

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
           'pio.api': 'https://www.protocols.io/api/v3/protocols/',

           # FIXME -> pennsieve.io XXX and that is why you always embed the expansion rule
           'dataset':'https://api.pennsieve.io/datasets/N:dataset:',
           'collection':'https://api.pennsieve.io/collections/N:collection:',
           'package':'https://api.pennsieve.io/packages/N:package:',
           'user':'https://api.pennsieve.io/users/N:user:',

           'bibo': 'http://purl.org/ontology/bibo/',  # crossref
           'prism.basic': 'http://prismstandard.org/namespaces/basic/2.1/',  # crossref

           'unit': str(unit),
           'dim': str(dim),
           'asp': str(asp),
           'protcur': 'https://uilx.org/tgbugs/u/protcur/',
           'hyp-protcur': 'https://uilx.org/tgbugs/u/hypothesis/protcur/',
           'aspect-raw': 'https://uilx.org/tgbugs/u/aspect-raw/',
           'verb': 'https://uilx.org/tgbugs/u/executor-verb/',
           'fuzzy': 'https://uilx.org/tgbugs/u/fuzzy-quantity/',
           'tech': str(tech),
           'awards':str(TEMP['awards/']),
           'sparc':str(sparc),})


def curies_runtime(base):
    """ base is .e.g https://api.blackfynn.io/datasets/{dataset_id}/ """

    return {
        'local': base,
        'contributor': base + 'contributors/',
        'subject': base + 'subjects/',
        'sample': base + 'samples/',
    }


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
        assert blob['system'] == cls.__name__
        identifier = blob['id']
        if isinstance(identifier, cls):
            return identifier
        else:
            return cls(identifier, label=blob['label'])  # FIXME need the .fetch() impl

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
            if is_list_or_tuple(self.synonyms):
                out['synonyms'] = self.synonyms
            else:
                out['synonyms'] = self.synonyms,

        return out

    def asCell(self, sep='|'):
        if self.label is None:
            _id = self.curie if self.curie else self.iri
            if self.prefix not in self._known_no_label:
                if not self._already_logged(_id):
                    log.error(f'No label {_id}')

            return _id

        return self.label + sep + self.curie

    def asCellHyperlink(self):
        return f'=HYPERLINK("{self.iri}", "{self.label}")'

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

        if is_list_or_tuple(self.synonyms):  # workaround for ontquery list vs string issue
            for syn in self.synonyms:
                yield s, NIFRID.synonym, rdflib.Literal(syn)

        elif self.synonyms:
            yield s, NIFRID.synonym, rdflib.Literal(self.synonyms)


OntTerm._sinit()
OntTerm.set_repr_args('curie', 'label')


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

    def addError(self, error, pipeline_stage=None, blame=None, path=None, json_path=None):
        do_log = error not in self._already_logged
        if do_log:
            self._already_logged.add(error)

        stage = (pipeline_stage if pipeline_stage is not None
                 else (self._pipeline_stage if self._pipeline_stage
                       else self.__class__.__name__))
        b = len(self._errors_set)
        et = (error, stage, blame, path, json_path)
        self._last_error = et
        self._errors_set.add(et)
        a = len(self._errors_set)
        return a != b and do_log

    def _render(self, e, stage, blame, path, json_path):
        o = {'pipeline_stage': stage,
             'blame': blame,}  # FIXME

        if path is not None:
            o['file_path'] = path

        if json_path is not None:
            o['path'] = json_path

        if isinstance(e, str):
            o['message'] = e
            #o['error_type'] = None  # FIXME probably want our own? XXX nearly all error objects do not have type as a key

        elif isinstance(e, BaseException):
            o['message'] = str(e)
            o['error_type'] = str(type(e))

        else:
            raise TypeError(repr(e))

        log.log(9, o)  # too verbose for normal debug
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


def lj(j, limit=100):
    """ use with log to format json """
    out = '\n' + json.dumps(j, indent=2, cls=JEncode)
    if out.count('\n') > limit + 3:
        asdf = out.split('\n')
        ninclude = limit // 2
        head = asdf[:ninclude]
        tail = asdf[-ninclude:]
        # better to calculate explicitly given potential
        # weirdness with // 2
        nexcluded = len(asdf) - (len(head) + len(tail))
        lines = head + [f'\n ... Skipping {nexcluded} lines ... \n'] + tail
        return '\n'.join(lines)
    else:
        return out


def dereference_all_identifiers(obj, stage, *args, path=None, addError=None, **kwargs):
    try:
        dict_literal = _json_identifier_expansion(obj)
    except idlib.exc.RemoteError as e:
        if hasattr(obj, '_cooldown'):
            return obj._cooldown()  # trigger cooldown to simplify issues down the line

        error = dict(error=e,
                     pipeline_stage=stage.__class__.__name__,
                     blame='submission',
                     path=tuple(path))

        if addError:
            if addError(**error):
                log.exception(e)
                #logd.error(msg)
        else:
            return {'errors': [error]}

    except idlib.exc.ResolutionError as e:
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
    except Exception as e:
        log.critical(f'Unhandled exception {e} in {path}')
        error = dict(error=e,
                     pipeline_stage=stage.__class__.__name__,
                     blame='stage',
                     path=tuple(path))

        if addError:
            if addError(**error):
                log.exception(e)
                #logd.error(msg)
        else:
            return {'errors': [error]}


def _json_identifier_expansion(obj, *args, **kwargs):
    if not isinstance(obj, oq.OntTerm):
        if isinstance(obj, rdflib.URIRef):
            obj = OntId(obj)

        if isinstance(obj, oq.OntId):
            obj = obj.asInstrumented()

    if isinstance(obj, oq.OntTerm):
        oc = obj.__class__
        obj.__class__ = OntTerm  # that this works is amazing/terrifying
        try:
            return obj.asDict()
        finally:
            obj.__class__ = oc

    elif isinstance(obj, idlib.Stream):
        if obj._id_class is str:
            return obj.identifier
        else:
            try:
                return obj.asDict()
            except idlib.exc.RemoteError as e:
                logd.error(e)
                # we must return object here otherwise sanity destorying
                # None might creep in during expansion here, this really
                # shouldn't be able to happen i.e. it should be impossible
                # for asDict to raise a remote error at all, but we need
                # to catch it here as well
                return obj
    else:
        return obj


def json_identifier_expansion(obj, *args, path=None, **kwargs):
    """ expand identifiers to json literal form """
    try:
        return _json_identifier_expansion(obj, *args, **kwargs)
    except idlib.exceptions.RemoteError as e:
        oops = json_export_type_converter(obj)
        msg = f'remote error {e} for {type(obj)}: {oops}'
        out = {'id': obj,
               'type': 'identifier',
               'system': obj.__class__.__name__,
               'errors': [{'message': msg, 'path': path}]}
        return out
    except idlib.exceptions.ResolutionError as e:
        oops = json_export_type_converter(obj)
        msg = f'could not resolve {type(obj)}: {oops}'
        out = {'id': obj,
               'type': 'identifier',
               'system': obj.__class__.__name__,
               'errors': [{'message': msg, 'path': path}]}
        return out
    except Exception as e:
        oops = json_export_type_converter(obj)
        msg = f'Unhandled exception {e} in {path}'
        out = {'id': obj,
               'type': 'identifier',
               'system': obj.__class__.__name__,
               'errors': [{'message': msg, 'path': path}]}
        log.critical(msg)
        return out


def json_export_type_converter(obj):
    if isinstance(obj, deque):
        return list(obj)
    elif isinstance(obj, AJ):
        return obj.asJson()
    elif isinstance(obj, ProtcurExpression):
        return obj.json()
    elif isinstance(obj, PurePath):
        return obj.as_posix()
    elif isinstance(obj, Quantity):
        return obj.json()
    elif isinstance(obj, Measurement):
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
    elif isinstance(obj, date):
        return isoformat(obj)
    elif isinstance(obj, time):
        return isoformat(obj)
    elif isinstance(obj, BaseException):
        # FIXME hunt down where these are sneeking in from
        return repr(obj)


class JEncode(json.JSONEncoder):

    def default(self, obj):
        new_obj = json_export_type_converter(obj)
        if new_obj is not None:
            return new_obj
        #else:
            #log.critical(f'{type(obj)} -> {obj}')
        #if isinstance(obj, ValueError):
            #breakpoint()

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


def get_nested_by_key(obj, key, *args, path=None, asExport=True,
                      join_lists=True, collect=tuple()):
    if isinstance(obj, dict) and key in obj:
        value = obj[key]
        if is_list_or_tuple(value) and join_lists:
            for v in value:
                n = json_export_type_converter(v)
                collect.append(n if n is not None and asExport else v)
        else:
            n = json_export_type_converter(value)
            collect.append(n if n is not None and asExport else value)

    return obj  # have to return this otherwise somehow everything is turned to None?


def get_nested_by_type(obj, type, *args, path=None, collect=tuple()):
    if isinstance(obj, type):
        collect.append(obj)

    return obj


def JApplyRecursive(function, obj, *args,
                    condense=False,
                    skip_keys=tuple(),
                    preserve_keys=tuple(),
                    path=None,
                    **kwargs):
    """ *args, **kwargs, and path= are passed to the function """
    def testx(v):
        return (v is not None and
                not (not v and
                     (is_list_or_tuple(v) or isinstance(v, dict))))

    if path is None:
        path = []

    if isinstance(obj, dict):
        out = {k: JApplyRecursive(function, v, *args,
                                  condense=condense,
                                  skip_keys=skip_keys,
                                  preserve_keys=preserve_keys,
                                  path=path + [k],
                                  **kwargs)
               if k not in preserve_keys else v
               for k, v in obj.items() if k not in skip_keys}

        if condense:
            out = {k:v for k, v in out.items() if testx(v)}

        return function(out, *args, path=path, **kwargs)

    elif is_list_or_tuple(obj):
        out = [JApplyRecursive(function, v, *args,
                               condense=condense,
                               skip_keys=skip_keys,
                               preserve_keys=preserve_keys,
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
        msg = str(iterables)[:2000]
        raise TypeError(f'One of these is not iterable {msg}') from e

    for zipped in gen:
        # sadly we can't use len % 2 because
        # iterables have indefinite length
        if sentinel in zipped:
            msg = str(iterables)[:2000]
            raise exc.LengthMismatchError('Lengths do not match! '
                                          'Did you remember to box your function?\n'
                                          f'{msg}')

        yield zipped


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
    # shambling abomination
    #adops
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
        """ Note on semantics when target_path contains the type int.
            Normally when adding a path all the parents are added because
            we are expecting a direct path down. However, if the path
            contains int then it implicitly expects the list to alread
            exist. Therefore any failure on the way TO a list will
            immediately abort and not add the keys to the non-existent list.
            This is consistent with the approach where keys are not required
            but if their value is a list it must not be empty. Thus we abort
            so that we don't go around creating a bunch of empty lists that
            will show up later as errors when validating the schema. """
        # type errors can occur here ...
        # e.g. you try to go to a string
        if not [_ for _ in (list, tuple) if isinstance(target_path, _)]:
            msg = f'target_path is not a list or tuple! {type(target_path)}'
            raise TypeError(msg)

        if False and target_path == ['@context', '@base']:
            # use to debug TargetPathExistsError issues
            if '@tracker' not in data:
                data['@tracker'] = []
            try:
                raise BaseException('tracker')
            except BaseException as e:
                data['@tracker'].append(e)

            if '@context' in data and '@base' in data['@context']:
                log.critical(f'target present {data["id"]}')
            else:
                log.critical(f'target not present {data["id"]}')

        target_prefixes = target_path[:-1]
        target_key = target_path[-1]
        target = data
        is_subpath_add = int in target_path
        for i, target_name in enumerate(target_prefixes):
            if target_name is int:  # add same value to all objects in list
                if not is_list_or_tuple(target):
                    msg = (f'attempt to add to all elements of not a list '
                           f'{type(target)} target_path was {target_path} '
                           f'target_name was {target_name}')
                    raise TypeError(msg)
                # LOL PYTHON namespaces
                [AtomicDictOperations.add(subtarget, target_path[i + 1:], value)
                 for subtarget in target]
                return  # int terminates this level of an add

            if target_name not in target:  # TODO list indicies XXX that is really append though ...
                if is_subpath_add:
                    # if we are targeting objects in a list for addition
                    # abort the first time we would have to create a key
                    # because we will eventually create an empty list
                    # which we won't be able to add anything to and will
                    # likely cause schema validation errors
                    return

                target[target_name] = {}

            target = target[target_name]

        if update:
            pass
        elif fail_on_exists and target_key in target:
            msg = f'A value already exists at path {target_path} in\n{lj(data)}'
            raise exc.TargetPathExistsError(msg)

        target[target_key] = value

    @classmethod
    def update(cls, data, target_path, value):
        cls.add(data, target_path, value, update=True)

    @classmethod
    def get(cls, data, source_path, on_failure=None, on_failure_func=None):
        """ get stops at lists because the number of possible issues explodes
            and we don't hand those here, if you encounter that, use this
            primitive to get the list, then use it again on the members in
            the function making the call where you have the information needed
            to figure out how to handle the error """

        try:
            source_key, node_key, source = cls._get_source(data, source_path)
            return source[source_key]
        except Exception as e:
            if on_failure_func is not None:
                return on_failure_func(source_path)
            elif on_failure is not None:
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
        yield (node_key if source_prefixes else  # FIXME {'type': {'type': {'type': sigh}}}
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
            if not isinstance(source_key, int):
                msg = (f'path {source_path} indicates that schema does not '
                       'expect an array here')
                raise exc.BadDataError(msg)

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
            # FIXME I have no idea why this was running here but it causes some
            # weird bugs

            # this will change key ordering but
            # that is expected, and if you were relying
            # on dict key ordering HAH
            #if move:
                #breakpoint()
            #if move and  node_key is not AtomicDictOperations.__empty_node_key:
                #_parent[node_key] = source
            pass


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
            if path == ['metadata_file', int, 'contents', 'manifest_records', int, 'software_rrid']:
                breakpoint()
            if int in path:  # unlike adops.add, this has to be implemented in DT
                pivot = path.index(int)
                before = path[:pivot]
                after = path[pivot + 1:]
                try:
                    collection = adops.get(data, before)
                except exc.NoSourcePathError as e:
                    if source_key_optional:
                        continue
                    else:
                        raise e

                assert is_list_or_tuple(collection)

                if pivot + 1 == len(path):
                    # have to update list in place not just its elements
                    # because int is the terminal element in the path
                    # NOTE this is NOT functional, this modifies in place
                    _DictTransformer.update(
                        collection,
                        # fun rewite here
                        [[[i], function] for i in range(len(collection))],
                        source_key_optional=source_key_optional)
                else:
                    # nominally safe to run in parallel here
                    # if python could actually do it
                    for obj in collection:
                        _DictTransformer.update(
                            obj,
                            [[after, function]],  # construct mini updates spec
                            source_key_optional=source_key_optional)

                continue

            try:
                value = adops.get(data, path)
            except exc.NoSourcePathError as e:
                if source_key_optional:
                    continue
                else:
                    raise e

            new = function(value)
            if isinstance(new, GeneratorType):
                new = tuple(new)

            adops.update(data, path, new)  # this will fail if data is immutable

    @staticmethod
    def get(data, gets, source_key_optional=False, on_failure_func=None):
        """ gets is a list with the following structure
            [source-path ...] """
        # FIXME we need a way to pass on_failure

        for source_path in gets:
            yield adops.apply(adops.get, data, source_path, None, on_failure_func,
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
            if int in source_path and int in target_path:
                if source_path[-1] is int:
                    msg = ('It is not meaningful to move each element of a '
                           'source path individually.')
                    # FIXME we may not need to error here we may just
                    # be able to drop the int at the end and ignore it
                    raise TypeError(msg)

                # one way to implement this is to walk to the first
                # pivot in the source and get that list, then to walk
                # to the first pivot in the target and get that list
                # and then do pairwise moves between the lists, noting
                # that the lengths must match, which we will check
                # beforehand because we know we aren't dealing with an
                # arbitrary length stream
                pbac = []
                for i, path in enumerate((source_path, target_path)):
                    pivot = path.index(int)
                    before = path[:pivot]
                    after = path[pivot + 1:]
                    try:
                        collection = adops.get(data, before)
                        # FIXME behavior on missing keys in the target
                        # there isn't an obviously correct solution
                        # here but partial mutation is pretty much the
                        # only thing we can do and have to deal with
                        # fact that sometimes an error will leave the
                        # transformation in a partially transformed
                        # state
                    except exc.NoSourcePathError as e:
                        if i:  # target_path case
                            msg = ('TODO right now we only vary 1 element at '
                                   'a time but can eventually vary more')
                            raise NotImplementedError(msg)
                            # a this point we know we have to add all
                            # the target structure
                            if not after:
                                msg = ('How to move into a non-existent list? '
                                       'Have you considered using derive?')
                                # closer to derive I would think
                                raise NotImplementedError(msg)
                                #s_piv, s_bef, s_aft, source = pbac[0]
                                #collection = [
                                    #_DictTransformer.pop(d, s_aft)
                                #]

                            # construct empty types matching the
                            # source collection types FIXME this is
                            # wrong, it should be the type of the
                            # first element of after
                            # FIXME pretty sure this is incorrect
                            _len_s = len(pbac[0][-1])
                            _nt = after[0]
                            if _nt is int:  # list in list case
                                raise NotImplementedError('please no')
                            elif isinstance(_nt, str):
                                collection = [{} for _ in range(_len_s)]
                            elif isinstance(_nt, int):
                                # I'm pretty sure indexed target lists
                                # are just bad all around in this case
                                # because it would overwrite a value
                                msg = 'indexed target lists must already exist!'
                                raise TypeError(msg)

                            adops.add(data, before, collection)
                        elif source_key_optional:
                            break  # break out of inner loop we are done here
                        else:
                            raise e

                    assert is_list_or_tuple(collection)
                    pbac.append((pivot, before, after, collection))
                else:
                    # only run if we don't break
                    ((s_piv, s_bef, s_aft, source),
                     (t_piv, t_bef, t_aft, target)) = pbac
                    assert len(source) == len(target)
                    s_key, t_key = object(), object()
                    # transform two lists from paired holes into a single
                    # list of dicts with a source and target key and
                    # rewrite the move rule to move the after from source
                    # key to target key, this of course mutates in place
                    log.debug(pbac)
                    _sigh = [_DictTransformer.move(
                        {s_key: s,
                        t_key: t},
                        [[[s_key, *s_aft],
                          [t_key, *t_aft]]],
                        source_key_optional=source_key_optional)
                    for s, t in zip(source, target)]

                # NOTE we can't continue here, because there may be a
                # move at this point of the path as well XXX TODO
                # determing the semantics for multi-level moves in the
                # presences of a pivot is more than I'm up for right
                # now, so just move keys inside the same object and
                # let's not teleport them between lists of objects for
                continue
            elif int in source_path or int in target_path:
                msg = ('source_path and target_path must have the same holes')
                raise TypeError(msg)

            adops.apply(adops.move, data, source_path, target_path,
                        source_key_optional=source_key_optional)

    @classmethod
    def derive(cls, data, derives, source_key_optional=True, empty='CULL'):
        """ [[[source-path, ...], function, [target-path, ...], on_failure_func], ...] """
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
        class Sigh(Exception): """ SIGH """
        for source_paths, derive_function, target_paths, *on_failure_func in derives:
            # FIXME zipeq may cause adds to modify in place in error?
            # except that this is really a type checking thing on the function
            if on_failure_func:
                off, = on_failure_func
            else:
                def off(sp): raise Sigh(sp)

            def defer_get(*get_args):
                """ if we fail to get args then we can't gurantee that
                    derive_function will work at all so we wrap the lot """
                # we have to pass source_key_optional explicitly here because apply
                # used below captures source_key_optional and can't pass it along
                # because there is no way to tell that the callee function also wants it
                # however we know that we do want it here, on_failure_func allows us to
                # control cases where we want multi-arity functions to not fail immeidately
                try:
                    args = tuple(cls.get(
                        *get_args,
                        source_key_optional=source_key_optional,
                        on_failure_func=off))
                except Sigh as e:
                    # we log here because the default is to fail silently
                    # and continue of a source path is missing which we
                    # may not want for nairy function
                    if len(get_args[1]) > 1:
                        msg = ('nairy function failed to get source path! '
                               f'{e} {derive_function}')
                        logd.critical(msg)
                    raise exc.NoSourcePathError(source_paths) from e

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
                         adops.apply(express_zip,

                                     target_paths,
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
                except Exception as e:
                    import inspect
                    if isinstance(pc, object):
                        pi, pc = pc, pc.__class__

                    try:
                        __file = inspect.getsourcefile(pc)
                        __line = ' line ' + inspect.getsourcelines(pc)[-1]
                    except TypeError as e2:
                        __file = f'<Thing that is not defined in a file: {pc}>'
                        __line = ''

                    if hasattr(p, 'path'):
                        __path = f'"{p.path}"'
                    else:
                        __path = 'unknown input'

                    raise exc.SubPipelineError(
                        f'Error while processing {p}.data for\n{__path}\n'
                        f'{__file}{__line}') from e

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


def extract_errors(thing, path=None):
    """ recursively extract errors """
    if path is None:
        path = []

    if isinstance(thing, dict):
        for k, v in thing.items():
            if k == 'errors':
                for error in v:
                    yield tuple(path), error
            else:
                yield from extract_errors(v, path + [k])

    elif isinstance(thing, list):
        for i, v in enumerate(thing):
            yield from extract_errors(v, path + [i])


def get_all_errors(_with_errors):
    """ A better and easier to interpret measure of completeness. """
    # TODO deduplicate by tracing causes
    # TODO if due to a missing required report expected value of missing steps
    return list(extract_errors(_with_errors))


def get_by_invariant_path(errors):
    dd = defaultdict(list)
    for e in errors:
        k = tuple(int if isinstance(element, int)
                  else element for element in e['path'])
        dd[k].append(e)

    by_invariant_path = dict(dd)
    return by_invariant_path


def make_path_error_report(by_invariant_path):
    # FIXME to obtain the full final path you have to know where
    # if anywhere the schema being validated is located in the output
    path_error_report = {JPointer.fromList(['-1' if e is int else str(e)
                                            # -1 to indicate all instead of *
                                            for e in k]):
                         {'error_count': len(v),
                          'messages': sorted(set(e['message'] for e in v))}
                         for k, v in by_invariant_path.items()}
    return path_error_report


def hashable_converter(v):
    try:
        hash(v)
        return v
    except TypeError as e:
        if isinstance(v, dict):
            return frozenset((k, hashable_converter(v)) for k, v in v.items())
        else:
            # HACK will fail on non iterables
            return tuple(hashable_converter(v) for v in v)


def condense_over_stage(errors):
    """ condense shadoweded errors into their caster """
    dd = defaultdict(list)
    for error in errors:
        #log.critical(error)
        new_error = {k:hashable_converter(v) for k, v in error.items()}
        #copy.deepcopy(error)  # FIXME why the heck was this here !?
        new_error.pop('pipeline_stage', None)
        if 'schema_path' in new_error:  # XXX non json schema errors
            sp = new_error['schema_path']
            if 'inputs' in sp:
                # account for the fact that inputs are checked twice right now
                index = sp.index('inputs')
                new_error['schema_path'] = sp[index + 3:]
                # FIXME somehow the path and schema_path get misaligned

        dd[hashable_converter(new_error.items())].append(error)

    merged = dict(dd)
    compacted = []
    for frozen_new_error, these_errors in merged.items():
        if len(these_errors) > 1:
            stages = [e['pipeline_stage'] for e in these_errors]
            error = copy.deepcopy(these_errors[0])
            error['pipeline_stage'] = stages
            error['total_stage_errors'] = len(these_errors)
            compacted.append(error)
        else:
            compacted.extend(these_errors)

    return compacted, merged  # for maximum confusion


def merge_error_paths(path_errors):
    for path, error in path_errors:
        #log.critical((path, error))
        error = copy.deepcopy(error)
        oldpath = list(error['path']) if 'path' in error else []  # XXX non json schema errors
        error['path'] = list(path) + oldpath
        yield error


def compact_errors(path_errors):
    """ compact repeated errors in lists by lifting the list index
        to the int type, making it constaint over all indexes and
        making it possible to identify paths with the same structure """

    errors = list(merge_error_paths(path_errors))
    condensed, merged = condense_over_stage(errors)
    by_invariant_path = get_by_invariant_path(condensed)
    path_error_report = make_path_error_report(by_invariant_path)

    compacted = []
    for ipath, these_errors in by_invariant_path.items():
        if int in ipath:
            index = ipath.index(int)
            ints = sorted(set(e['path'][index] for e in these_errors))
            error = copy.deepcopy(these_errors[0])
            error['path'][index] = ints
            error['total_errors'] = len(these_errors)
            compacted.append(error)
        else:
            compacted.extend(these_errors)

    return compacted, path_error_report, by_invariant_path, errors


class JPointer(str):
    """ a class to mark json pointers for resolution """

    @staticmethod
    def pathToSchema(path):
        # search all possible valid paths for information about the
        # current path in the schema ... useful to check just a single
        # path in an object against a schema ... there is surely a better way
        raise NotImplementedError("This shouldn't be implemented here.")

    @staticmethod
    def pathFromSchema(schema_path):
        gen = (e for e in schema_path)
        while True:
            try:
                element = next(gen)
                if element in ('oneOf', 'anyOf', 'allOf'):
                    next(gen)
                    continue

                if element == 'properties':
                    yield next(gen)
                elif element == 'items':
                    # NOTE it is ok to use int in cases like this because even
                    # though it is in principle ambiguous with a case where
                    # someone happens to be using the python function int
                    # as a key in a dict they would never be able to serialize
                    # that to json without some major conversions, so I am
                    # ruling that it is safe to lift to type here since this
                    # is JPointer not rando dict pointer
                    yield int

            except StopIteration:
                break

    @classmethod
    def fromList(cls, iterable):
        return cls('#/' + '/'.join(iterable))
        
    def asList(self):
        return self.split('/')

    def dereference(self, blob):
        sharp, *path = self.asList()
        assert sharp == '#'
        return adops.get(blob, path)

    def update(self, blob, value):
        sharp, *path = self.asList()
        assert sharp == '#'
        adops.update(blob, path, value)


def resolve_context_runtime(specs, *blobs):
    """ [source-path lambda-function target-path] """
    # TODO see if we need multi source multi target
    for source, function, target in specs:
        s = JPointer(source)
        t = JPointer(target)
        for blob in blobs:
            value = function(s.dereference(blob))
            t.update(blob, value)


# register idlib classes for fromJson
[register_type(i, i.__name__) for i in
 (idlib.Ror, idlib.Doi, idlib.Orcid, idlib.Pio, idlib.Rrid, OntTerm)]

# register other types for fromJson
register_type(IdentityJsonType, 'BlackfynnRemoteMetadata')  # legacy type string
register_type(IdentityJsonType, 'BlackfynnDatasetData')
register_type(IdentityJsonType, 'PennsieveDatasetData')  # FIXME TODO fully abstract this

# handle old json export that won't stop due to missing BRM
# XXX let this be a reminder to never allow foreign json in unless it
# comes wrapped in an outer blob that can properly type it or where we
# can reasonably inject a type so that we can stop further recursion
# because it is (for now) json raw json
register_type(None, 'publication')
register_type(None, 'revision')
register_type(None, 'embargo')
register_type(None, 'removal')

# FIXME bad that we need to call these here
# needed in derives and needed when loading from ir
register_type(None, 'SampleDirs')
register_type(None, 'SubjectDirs')
