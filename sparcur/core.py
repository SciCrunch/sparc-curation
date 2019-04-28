import copy
import json
import shutil
import hashlib
import inspect
from collections import deque
import rdflib
import ontquery as oq
from joblib import Memory
from pathlib import Path
from pysercomb.pyr.units import ProtcParameter
from pyontutils.core import OntId
from pyontutils.utils import makeSimpleLogger
from sparcur import exceptions as exc
from sparcur.config import config
from functools import wraps

log = makeSimpleLogger('sparcur')
logd = makeSimpleLogger('sparcur-data')
sparc = rdflib.Namespace('http://uri.interlex.org/tgbugs/uris/readable/sparc/')

# disk cache decorator
memory = Memory(config.cache_dir, verbose=0)


def lj(j):
    """ use with log to format json """
    return '\n' + json.dumps(j, indent=2, cls=JEncode)


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


class JEncode(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, tuple):
            return list(obj)
        elif isinstance(obj, deque):
            return list(obj)
        elif isinstance(obj, ProtcParameter):
            return str(obj)
        elif isinstance(obj, OntId):
            return obj.curie + ',' + obj.label

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def python_identifier(string):
    """ pythonify a string for use as an identifier """
    ident = (string.strip()
             .replace('<', '')
             .replace('>', '')
             .replace('(', '')
             .replace(')', '')
             .replace(' ', '_')
             .replace('+', '')
             .replace('…','')
             .replace('.','_')
             .replace(',','_')
             .replace('/', '_')
             .replace('?', '_')
             .replace('#', 'number')
             .replace('-', '_')
             .replace(':', '_')
             .replace('\x83', '')  # FIXME should be stripped beforehand during format norm?
             .lower()  # sigh
                )
    if ident[0].isdigit():
        ident = 'n_' + ident

    return ident


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
    __firsts = 'iri',

    class MalformedOrcidError(Exception):
        """ WHAT HAVE YOU DONE!? """

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
            raise self.MalformedOrcidError(self) from e


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
    nc = type('JT' + str(type(blob)), (object,), cd)
    return nc()


class FileSize(int):
    @property
    def mb(self):
        return self / 1024 ** 2

    @property
    def hr(self):
        """ human readable file size """

        def sizeof_fmt(num, suffix=''):
            for unit in ['','K','M','G','T','P','E','Z']:
                if abs(num) < 1024.0:
                    return "%0.0f%s%s" % (num, unit, suffix)
                num /= 1024.0
            return "%.1f%s%s" % (num, 'Yi', suffix)

        if self is not None and self >= 0:
            return sizeof_fmt(self)
        else:
            return '??'  # sigh


_type_order = (
    bool, int, float, bytes, str, tuple, list, set, dict, object, type, None
)


def type_index(v):
    for i, _type in enumerate(_type_order):
        if isinstance(v, _type):
            return i

    return -1


def args_sort_key(kv):
    """ type ordering """
    k, v = kv
    return k, type_index(v), v


def cache_hash(pairs, cypher=hashlib.blake2s):
    pairs = sorted(pairs, key=args_sort_key)
    converted = []
    for k, v in pairs:
        if k == 'self':  # FIXME convention only ...
            v, _v = v.__class__, v
        converted.append(k.encode() + b'\x01' + str(v).encode())

    message = b'\x02'.join(converted)
    m = cypher()
    m.update(message)
    return m.hexdigest()


def argspector(function):
    argspec = inspect.getfullargspec(function)
    def spector(*args, **kwargs):
        for i, (k, v) in enumerate(zip(argspec.args, args)):
            yield k, v

        if argspec.varargs is not None:
            for v in args[i:]:
                yield argspec.varargs, v

        for k, v in kwargs.items():
            yield k, v

    return spector


def cache(folder, ser='json', clear_cache=False, create=False):
    """ outer decorator to cache output of a function to a folder """
        

    if ser == 'json':
        serialize = json.dump
        deserialize = json.load
        mode = 't'
    else:
        raise TypeError('Bad serialization format.')

    write_mode = 'w' + mode
    read_mode = 'r' + mode

    folder = Path(folder)
    if not folder.exists():
        if not create:
            raise FileNotFoundError(f'Cache base folder does not exist! {folder}')

        folder.mkdir(parents=True)

    if clear_cache:
        log.debug(f'clearing cache for {folder}')
        shutil.rmtree(folder)
        folder.mkdir()

    def inner(function):
        spector = argspector(function)
        fn = function.__name__
        @wraps(function)
        def superinner(*args, **kwargs):
            filename = cache_hash(spector(*args, ____fn=fn, **kwargs))
            filepath = folder / filename
            if filepath.exists():
                log.debug(f'deserializing from {filepath}')
                with open(filepath, read_mode) as f:
                    return deserialize(f)
            else:
                output = function(*args, **kwargs)
                if output is not None:
                    with open(filepath, write_mode) as f:
                        serialize(output, f)

                return output

        return superinner

    return inner


class AtomicDictOperations:
    """ functions that modify dicts in place """

    # note: no delete is implemented at the moment ...
    # in place modifications means that delete can loose data ...

    class __empty_node_key: pass

    @staticmethod
    def add(data, target_path, value, fail_on_exists=True):
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

        if fail_on_exists and target_key in target:
            raise exc.TargetPathExistsError(f'A value already exists at path {target_path}\n'
                                            f'{lj(data)}')

        target[target_key] = value

    @classmethod
    def get(cls, data, source_path):
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
        cls._copy_or_move(data, source_path, target_path, True)

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
            #log.debug(f'{source_path}')
            msg = f'did not find {source_key!r} in {tuple(source.keys())}'
            raise exc.NoSourcePathError(msg)

        yield source

    @classmethod
    def _copy_or_move(cls, data, source_path, target_path, move=False):
        """ if exists ... """
        # FIXME there is a more elegant way to do this
        try:
            source_key, node_key, source = cls._get_source(data, source_path)
        except exc.NoSourcePathError as e:
            log.warning(e)
            return

        if move:
            _parent = source  # incase something goes wrong
            source = source.pop(source_key)
        else:
            source = source[source_key]

            if source != data:  # this should .. always happen ???
                source = copy.deepcopy(source)
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
    def add(data, adds):
        """ adds is a list (or tuples) with the following structure
            [[target-path, value], ...]
        """
        for target_path, value in adds:
            adops.add(data, target_path, value)

    @staticmethod
    def get(data, gets):
        """ gets is a list with the following structure
            [source-path ...] """

        for source_path in adds:
            yield adops.get(data, source_path)

    @staticmethod
    def pop(data, pops):
        """ pops is a list with the following structure
            [source-path ...] """

        for source_path in adds:
            yield adops.pop(data, source_path)

    @staticmethod
    def delete(data, deletes):
        """ delets is a list with the following structure
            [source-path ...]
            THIS IS SILENT YOU SHOULD USE pop instead!
            The tradeoff is that if you forget to express
            the pop generator it will also be silent until
            until schema catches it """

        for source_path in adds:
            adops.pop(data, source_path)

    @staticmethod
    def copy(data, copies):  # put this first to clarify functionality
        """ copies is a list wth the following structure
            [[source-path, target-path] ...]
        """
        for source_path, target_path in copies:
            # don't need a base case for thing?
            # can't lift a dict outside of itself
            # in this context
            adops.copy(data, source_path, target_path)

    @staticmethod
    def move(data, moves):
        """ moves is a list with the following structure
            [[source-path, target-path] ...]
        """
        for source_path, target_path in moves:
            adops.move(data, source_path, target_path)

    @staticmethod
    def derive(data, derives, source_key_optional=True, allow_empty=False):
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
    def lift(data, lifts, source_key_optional=True):
        """ 
        lifts are lists with the following structure
        [[path, function], ...]

        the only difference from derives is that lift
        overwrites the underlying data (e.g. a filepath
        would be replaced by the contents of the file)

        old version:
        given a source object and a list of property names
            get the values at those property names and add them
            to the dict at those same names, this is essentially
            a deferred add ... really it should be a function
            that takes the value at the path and uses that information
            to transform that value into its replacement which is
            sort of what many of these classes actually do ...
            just in a rather indirect way
        this is half way between an add an a derive, it is a replace
        with derived data, so ... yes, lift is a reasonable word
        """

        for path, function in lifts:
            try:
                old_value = adops.get(data, path)
            except exc.NoSourcePathError as e:
                if source_key_optional:
                    msg = str(e)
                    logd.error(msg)
                    continue
                else:
                    raise e
            new_value = function(old_value)
            adops.add(data, path, new_value, fail_on_exists=False)

        return
        # TODO lifts, copies, etc can all go in a single structure
        # TODO proper mapping
        for section_name in lifts:
            try:
                section = next(getattr(self, section_name))  # FIXME multiple when require 1
                if section_name in data:
                    # just skip stuff that doesn't exist
                    data[section_name] = section.data_with_errors
            except StopIteration:
                #data[section_name] = {'errors':[{'message':'Nothing to see here?'}]}
                # these errors were redundant with the missing key that
                # the higher level schema will detect for us
                continue

        return data


DictTransformer = _DictTransformer()
