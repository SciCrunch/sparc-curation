import ast
import copy
import json
import types
import inspect
from functools import wraps
import jsonschema

# FIXME these imports should not be here types rules should be set in another way
from pathlib import PurePath
import idlib
import rdflib
import requests
import ontquery as oq
from pysercomb.pyr.types import ProtcurExpression
from sparcur import exceptions as exc
from sparcur.utils import logd, BlackfynnId, PennsieveId
from sparcur.core import JEncode, JApplyRecursive, JPointer, OntCuries
from sparcur.core import get_nested_by_key
from pyontutils.utils import asStr
from pyontutils.namespaces import TEMP, TEMPRAW, sparc, unit, PREFIXES as uPREFIXES, ilxtr


CONTEXT_ROOT = object()  # used to mark where nested context_value statements should be deposited
prefix_endswith = ['/', '#', '_', '-', ':', '=',
                   '/fma', '/sao', 'PTHR', '/chebi#2', '/chebi#3',]

base_context = {
    '@version': 1.1,
    'id': '@id',
    'dataset': {'@id': 'https://api.pennsieve.io/datasets/N:dataset:', '@prefix': True},  # FIXME -> pennsieve
    'package': {'@id': 'https://api.pennsieve.io/packages/N:package:', '@prefix': True},  # FIXME -> pennsieve
    # @base and _bfc are added at runtime and are dataset:{dataset-id-suffix}
    'meta': '@graph',
    #'meta': 'TEMP:hasSubGraph',  #'_bfc:#meta-graph',  # FIXME I think the fragment is the right thing to do here ...
    # the subgraphs arent' actually useful when we condense to triples
    # we can add the predicates on the way back, the issue is that there
    # is a name collision with the prefixes that we would like to use
    # which is something of an annoying design failure I think ...
    #'subjects': 'TEMP:hasSubGraph',  #'_bfc:#subjects-graph',  # FIXME I think the fragment is the right thing to do here ...
    #'samples': 'TEMP:hasSubGraph',  #'_bfc:#samples-graph',
    #'contributors': '_bfc:#contributors-graph',  # FIXME broken I think
    #'contributors': 'TEMP:hasSubGraph',  # FIXME broken I think
    #'N:dataset': {'@id': 'https://api.pennsieve.io/datasets/'},  # darn it multiprefix issues
    'idfall': {"@id": str(TEMP['id-fallthrough/']), "@prefix": True},
    **{p: {'@id': n, '@prefix': True}
       if [c for c in prefix_endswith if n.endswith(c)]
       else n for p, n in {**uPREFIXES, **OntCuries._dict}.items()
       if p != ''},  # FIXME massive data duplication

    # OLD convention
    'unit': {'@id': 'TEMP:hasUnit',
             '@type': '@id',
             '@context': {'@base': str(unit)}},
    'value': {'@id': 'rdf:value'},  # FIXME maybe include this in a dedicated quantity @context?
    # END OLD

    'units': {'@id': 'TEMP:hasUnit',
             '@type': '@id',
             '@context': {'@base': str(unit)}},
    'magnitude': {'@id': 'rdf:value'},  # FIXME maybe include this in a dedicated quantity @context?
    'type': {'@id': 'ilxtr:jsonRecordType',  # FIXME ...
             '@type': '@id',
             '@context': {'@base': str(ilxtr['sparcur/types/'])},},
    'quantity': 'sparc:Measurement',  # for some reason this isn't working, and is captured by @base
    #'identifier': 'owl:Class',
    'system': {'@id': 'ilxtr:identifierType',  # FIXME see what predicate we used for this
               '@type': '@id',
               '@context': {'@base': str(ilxtr['idlib/systems/'])},},
    'OntTerm': 'owl:Class',
    #'uri_api': {'@id': '@id', '@type': '@id'},  # klobbered below, now fixed
}


protcur_context = {  # FIXME how is the protcur -> mis related to a native protcur schema?
    '@version': 1.1,
    #'TEMP:':

    '@base': 'https://uilx.org/tgbugs/u/sparcur-protcur-json-ld/',  # FIXME per subsection?

    'label': 'rdfs:label',

    'spjl': {'@id': 'https://uilx.org/tgbugs/u/sparcur-protcur-json-ld/', '@prefix': True},

    # XXX FIXME annoying context collisions
    'unt': {'@id': str(unit), '@prefix': True},

    #'protc': {'@id': 'https://uilx.org/tgbugs/u/protc/', '@prefix': True},

    #'protcur': {'@id': 'https://uilx.org/tgbugs/u/protcur/', '@prefix': True},
    #'hyp-protcur': {'@id': 'https://uilx.org/tgbugs/u/hypothesis/protcur/', '@prefix': True},

    #'sparcur': {'@id': 'https://uilx.org/tgbugs/u/sparcur/', '@prefix': True},
    #'aspect': {'@id': 'https://uilx.org/tgbugs/u/aspect/', '@prefix': True},

    #'aspect-raw': {'@id': 'https://uilx.org/tgbugs/u/aspect-raw/', '@prefix': True},

    #'verb': {'@id': 'https://uilx.org/tgbugs/u/executor-verb/', '@prefix': True},

    'start': {'@id': 'TEMP:rangeStart', '@type': '@id'},
    'stop': {'@id': 'TEMP:rangeStop', '@type': '@id'},

    'hasDoi': {'@id': 'TEMP:hasDoi', '@type': '@id'},

    'fuzzy': {'@id': 'https://uilx.org/tgbugs/u/fuzzy-quantity/', '@prefix': True},

    'children': {'@id': 'TEMP:protcurChildren', '@type': '@id'},
    'ast_value': {'@id': 'TEMP:hasValue', '@type': '@id'},
    'raw_value': {'@id': 'TEMPRAW:hasValue', '@type': '@id'},
    'original': {'@id': 'TEMPRAW:hasValue'},  # don't want @type @id for this one

    'protcur_anno_count': {'@id': 'TEMP:hasNumberOfProtcurAnnotations'},
    'datasets': {'@id': 'TEMP:priorInformationalConstraintOnProcessThatGenerated', '@type': '@id'},

    'ilxtr_technique': {'@id': 'TEMPRAW:protocolEmploysTechnique'},
    'protocolEmploysTechnique': {'@id': 'TEMP:protocolEmploysTechnique', '@type': '@id'},

    # https://www.w3.org/TR/json-ld11/#example-32-using-vocabularies
    # explains the issues associated with trying to use a context to
    # to identifier remapping directly, you have to use an intermediate
    # context which compacts a form where keys are not curies at all
    'protc_aspect': {'@id': 'TEMPRAW:protocolInvolvesAspect'},
    'protc_implied-aspect': {'@id': 'TEMPRAW:protocolInvolvesAspect'},
    'protc_input': {'@id': 'TEMPRAW:protocolInvolvesInput'},
    'protc_implied-input': {'@id': 'TEMPRAW:protocolInvolvesInput'},
    'protc_input-instance': {'@id': 'TEMPRAW:protocolInvolvesInputInstance'},
    'protc_output': {'@id': 'TEMPRAW:protocolInvolvesOutput'},
    'protc_parameter*': {'@id': 'TEMPRAW:protocolInvolvesParameter'},
    'protc_invariant': {'@id': 'TEMPRAW:protocolInvolvesInvariant'},
    'protc_executor-verb': {'@id': 'TEMPRAW:protocolInvolvesAction'},
    'protc_black-box': {'@id': 'TEMPRAW:protocolInvolvesBlackBox'},
    'protc_black-box-component': {'@id': 'TEMPRAW:protocolInvolvesBlackBoxComponent'},
    'protc_objective*': {'@id': 'TEMPRAW:protocolInvolvesObjective'},

    'protcur_aspect': {'@id': 'TEMP:protocolInvolvesAspect', '@type': '@id'},
    'protcur_implied-aspect': {'@id': 'TEMP:protocolInvolvesAspect', '@type': '@id'},
    'protcur_input': {'@id': 'TEMP:protocolInvolvesInput', '@type': '@id'},
    'protcur_implied-input': {'@id': 'TEMP:protocolInvolvesInput', '@type': '@id'},
    'protcur_input-instance': {'@id': 'TEMP:protocolInvolvesInputInstance', '@type': '@id'},
    'protcur_output': {'@id': 'TEMP:protocolInvolvesOutput', '@type': '@id'},
    'protcur_parameter': {'@id': 'TEMP:protocolInvolvesParameter', '@type': '@id'},
    'protcur_invariant': {'@id': 'TEMP:protocolInvolvesInvariant', '@type': '@id'},
    'protcur_executor-verb': {'@id': 'TEMP:protocolInvolvesAction', '@type': '@id'},
    'protcur_black-box': {'@id': 'TEMP:protocolInvolvesBlackBox', '@type': '@id'},
    'protcur_black-box-component': {'@id': 'TEMP:protocolInvolvesBlackBoxComponent', '@type': '@id'},
    'protcur_objective': {'@id': 'TEMP:protocolInvolvesObjective', '@type': '@id'},

    #TODO aspects of subjects vs reagents: {'@id': primary particiant in the
    #core protocols vs subprotocols

    'sparc_AnatomicalLocation': {'@id': 'TEMPRAW:involvesAnatomicalRegion'},
    'sparc_Measurement': {'@id': 'TEMPRAW:protocolMakesMeasurement'},
    'sparc_Reagent': {'@id': 'TEMPRAW:protocolUsesReagent'},
    'sparc_Tool': {'@id': 'TEMPRAW:protocolUsesTool'},
    'sparc_Sample': {'@id': 'TEMPRAW:protocolInvolvesSampleType'},
    'sparc_OrganismSubject': {'@id': 'TEMPRAW:protocolInvolvesSubjectType'},
    'sparc_Procedure': {'@id': 'TEMPRAW:whatIsThisDoingHere'},

    'sparcur_AnatomicalLocation': {'@id': 'TEMP:involvesAnatomicalRegion', '@type': '@id'},
    'sparcur_Measurement': {'@id': 'TEMP:protocolMakesMeasurement', '@type': '@id'},
    'sparcur_Reagent': {'@id': 'TEMP:protocolUsesReagent', '@type': '@id'},
    'sparcur_Tool': {'@id': 'TEMP:protocolUsesTool', '@type': '@id'},
    'sparcur_Sample': {'@id': 'TEMP:protocolInvolvesSampleType', '@type': '@id'},
    'sparcur_OrganismSubject': {'@id': 'TEMP:protocolInvolvesSubjectType', '@type': '@id'},
    'sparcur_Procedure': {'@id': 'TEMP:whatIsThisDoingHere', '@type': '@id'},
}


def json_version(version):
    """ version should be a branch or a release tag e.g. schema-0.0.1 """
    return {
        '$schema': 'http://json-schema.org/draft-06/schema#',
        '$id': ('https://raw.githubusercontent.com/SciCrunch/sparc-curation/'
                f'{version}/resources/defs.schema.json'),
        'title': 'Common definitions for the SDS json schemas, including mappings to json-ld @context',
        'description': 'JSON-LD mappings are under the `context_value` key by convention',
        '@context': {
            'sparc': str(sparc),
            'TEMP': str(TEMP),
            'TEMPRAW': str(TEMPRAW),
        },
        'definitions': {
            'patterns': {
                'metadata_filename': metadata_filename_pattern,
                'simple_url': simple_url_pattern ,
                #'doi': idlib.Doi._id_class.canonical_regex,
                #'orcid': idlib.Orcid._id_class.canonical_regex,
                #'ror': idlib.Ror._id_class.canonical_regex,
            'iso8601_date': iso8601datepattern,
                'iso8601_datetime': iso8601pattern,
                'whitespace_lead_trail': pattern_whitespace_lead_trail,
                'whitespace_multi': pattern_whitespace_multi,
            },
            'NoLTWhitespace': NoLTWhitespaceSchema.schema,
            'EmbeddedIdentifier': EmbeddedIdentifierSchema.schema,

            'meta': MetaOutExportSchema.schema,
            'prov': ProvSchema.schema,
            'errors': ErrorSchema.schema,
            'contributor': ContributorOutExportSchema.schema,
            'performance': PerformanceExportSchema.schema,
            'subject': SubjectExportSchema.schema,
            'sample': SampleExportSchema.schema,
            'dataset_description_file': DatasetDescriptionSchema.schema,
            'submission_file': SubmissionSchema.schema,
            'subjects_file': SubjectsSchema.schema,
            'samples_file': SamplesFileSchema.schema,
            'dataset': {
                'meta': {'$ref': '#/definitions/meta'},
                'prov': {'$ref': '#/definitions/prov'},
                'errors': {'$ref': '#/definitions/errors'},
                'contributor': {'$ref': '#/definitions/contributor'},
                'subject': {'$ref': '#/definitions/subject'},
                'sample': {'$ref': '#/definitions/sample'},
                'dataaset_description_file': {'$ref': '#/definitions/dataaset_description_file'},
                'submission_file': {'$ref': '#/definitions/submission_file'},
                'subjects_file': {'$ref': '#/definitions/subjects_file'},
                'samples_file': {'$ref': '#/definitions/samples_file'},
                'errors': {'$ref': '#/definitions/errors'},
            }
        }
    }


def get_runtime_context_specs():
    # FIXME not clear we should be embedding context_runtime deeply in
    # the schemas, though it is useful for being able to see things
    # side by side
    from pyontutils.utils import subclasses
    collect = []
    for cls in subclasses(JSONSchema):
        JApplyRecursive(get_nested_by_key,
                        cls.schema,
                        'context_runtime',
                        join_lists=False,
                        collect=collect)

    specs = tuple(set(tuple(l) for l in collect))
    # FIXME convert to JPointer here?
    return specs


def _defunc(obj, *args, path=None, **kwargs):  # FIXME add tests
    if isinstance(obj, types.FunctionType):
        return asStr(ast.parse(inspect.getsource(obj).strip().strip(',')))
    else:
        return obj


def update_include_paths(obj, *args, path=None, key='jsonld_include', moves=tuple(), collect=tuple()):
    """ collect json pointers that need to be updated """
    if isinstance(obj, dict) and key in obj:
        path = list(JPointer.pathFromSchema(path))
        for frm, to in moves:  # sigh python
            lf = len(frm)
            if path[:lf] == frm:
                path = to + path[lf:]
                break

        for key, value in obj[key].items():
            path.append(key)
            collect.append((path, lambda _: value))  # note that collect is really a list
            # note also that lambda _: value is because pipeline adds currently calls into lifters

    return obj  # have to return this otherwise somehow everything is turned to None?


def idtype(id, base=str(TEMP['id-fallthrough/'])):
    # we set base to id-fallthrough so we can easily pull out unmapped
    # cases that should be identifiers
    out = {'@id': id, '@type': '@id'}
    if base and id != '@id':
        # for whatever reason @context for @id @id is not allowed so
        # it should be set in an outer @context
        out['@context'] = {'@base': str(base)}

    return out


def strcont(id):
    return {'type': 'string', 'context_value': id}


def inttype(id):
    return {'@id': id, '@type': 'xsd:integer'}


def booltype(id):
    return {'@id': id, '@type': 'xsd:boolean'}


def cont(id, type):
    return {'type': type, 'context_value': id}


def not_array(schema, in_all=False):
    """ hacked way to determine that the blob in question cannot be an array """
    return ('type' in schema and schema['type'] != 'array' or
            'properties' in schema and 'items' not in schema or
            # FIXME hack hard to maintain
            'allOf' in schema and all(not_array(s, True) for s in schema['allOf']) or
            'oneOf' in schema and all(not_array(s, True) for s in schema['oneOf']) or
            'anyOf' in schema and all(not_array(s, True) for s in schema['anyOf']) or
            'not' in schema and (not not_array(schema['not'], in_all) or in_all)
    )


class hproperty:
    def __init__(self, fget=None, fset=None, fdel=None, doc=None):
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        if doc is None and fget is not None:
            doc = fget.__doc__
        self.__doc__ = doc

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self

        if self.fget is None:
            raise AttributeError("unreadable attribute")

        return self.fget(obj)

    def __set__(self, obj, value):
        if self.fset is None:
            raise AttributeError("can't set attribute")

        self.fset(obj, value)

    def __delete__(self, obj):
        if self.fdel is None:
            raise AttributeError("can't delete attribute")

        self.fdel(obj)

    def getter(self, fget):
        return type(self)(fget, self.fset, self.fdel, self.__doc__)

    def setter(self, fset):
        return type(self)(self.fget, fset, self.fdel, self.__doc__)

    def deleter(self, fdel):
        return type(self)(self.fget, self.fset, fdel, self.__doc__)


class HasSchema:
    """ decorator for classes with methods whose output can be validated by jsonschema """
    def __init__(self, input_schema_class=None, fail=True, normalize=False):
        self.input_schema_class = input_schema_class
        self.fail = fail
        self.normalize = normalize
        self.schema = None  # deprecated output schema ...

    def mark(self, cls):
        """ note that this runs AFTER all the methods """

        if self.input_schema_class is not None:
            # FIXME probably better to do this as types
            # and check that schemas match at class time
            cls._pipeline_start = cls.pipeline_start

            @hproperty
            def pipeline_start(self, isc=self.input_schema_class, fail=self.fail):
                if pipeline_start.schema == isc:
                    pipeline_start.schema = isc()

                schema = pipeline_start.schema
                data = self._pipeline_start
                ok, norm_or_error, data = schema.validate(data)
                if not ok and fail:
                    raise norm_or_error

                return data

            pipeline_start.schema = self.input_schema_class
            cls.pipeline_start = pipeline_start

        return cls

    def f(self, schema_class, fail=False):
        return self(schema_class, fail=fail, _property=False)

    def __call__(self, schema_class, fail=False, _property=True):
        # TODO switch for normalized output if value passes?
        def decorator(function):
            pipeline_stage_name = function.__qualname__
            @wraps(function)
            def schema_wrapped_property(_self, *args, **kwargs):
                if schema_wrapped_property.schema == schema_class:
                    schema_wrapped_property.schema = schema_class()

                schema = schema_wrapped_property.schema
                data = function(_self, *args, **kwargs)
                ok, norm_or_error, data = schema.validate(data)
                if not ok:
                    if fail:
                        logd.error('schema validation has failed and fail=True')
                        logd.critical(f'failing dataset is {data["id"]}')
                        raise norm_or_error

                    # I have no idea why this if statement used to be in
                    # a try except block, it should never fail unless
                    # the definition of __contains__ for a dict was overwritten
                    if 'errors' not in data:
                        data['errors'] = []
                        
                    data['errors'] += norm_or_error.json(pipeline_stage_name)
                    # TODO make sure the step is noted even if the schema is the same

                if self.normalize:
                    return norm_or_error

                return data

            if _property:
                schema_wrapped_property = hproperty(schema_wrapped_property)

            schema_wrapped_property.schema = schema_class
            return schema_wrapped_property

        return decorator


class ConvertingChecker(jsonschema.FormatChecker):
    _enc = JEncode()

    def check(self, instance, format):
        converted = _enc.default(instance)
        return super().check(converted, format)


def _make_st_checker(st):
    def inn(c, i):
        for t in st:
            if isinstance(i, t):
                return True

    return inn


class JSONSchema(object):

    schema = {}

    string_types = [  # add more types here
        str,
        PurePath,
        idlib.Stream,  # FIXME str/dict
        rdflib.URIRef,
        rdflib.Literal,
        oq.OntId,
        oq.OntTerm,  # FIXME str/dict
        ProtcurExpression,
    ]

    type_checker = jsonschema.Draft6Validator.TYPE_CHECKER.redefine_many(
        dict(array=(lambda c, i: isinstance(i, list) or isinstance(i, tuple)),
             string=_make_st_checker(string_types)))

    validator_class = jsonschema.validators.extend(
        jsonschema.Draft6Validator,
        type_checker=type_checker)

    def __init__(self):
        format_checker = jsonschema.FormatChecker()
        #format_checker = ConvertingChecker()
        types = dict(array=(list, tuple),
                     string=tuple(self.string_types))
        self.validator = self.validator_class(self.schema,
                                              format_checker=format_checker)

    @classmethod
    def _add_meta(cls, blob):
        """ In place modification of a schema blob to add schema level metadata """
        # TODO schema metadata, I don't know enough to do this correctly yet
        #template_schema_version = cls.validator_class.ID_OF(cls.validator_class.META_SCHEMA)
        #schema['$schema'] = template_schema_version

        #schema_id = '#/temp/' + cls.__name__ + '.json'  # FIXME version ...
        #schema['$id'] = schema_id

    @classmethod
    def export(cls, base_path):
        """ write schema to file with """
        def pop_errors(dict_):
            if 'errors' in dict_:
                dict_.pop('errors')
            for v in dict_.values():
                if isinstance(v, dict):
                    pop_errors(v)

        schema = copy.deepcopy(cls.schema)
        pop_errors(schema)
        cls._add_meta(schema)

        def debug(obj, *args, path=None, **kwargs):
            if isinstance(obj, types.FunctionType):
                raise TypeError(path + [obj])
            else:
                return obj

        schema = JApplyRecursive(_defunc, schema)
        JApplyRecursive(debug, schema)

        with open((base_path / cls.__name__).with_suffix('.json'), 'wt') as f:
            json.dump(schema, f, sort_keys=True, indent=2)

    def validate_strict(self, data):
        # Take a copy to ensure we don't modify what we were passed.
        #appstruct = copy.deepcopy(data)

        appstruct = json.loads(json.dumps(data, cls=JEncode))  # FIXME figure out converters ...

        errors = list(self.validator.iter_errors(appstruct))
        if errors:
            raise exc.ValidationError(errors)

        return appstruct

    def validate(self, data):
        """ capture errors """
        try:
            ok = self.validate_strict(data)  # validate {} to get better error messages
            return True, ok, data  # FIXME better format

        except exc.ValidationError as e:
            return False, e, data  # FIXME better format

    @classmethod
    def context(cls):
        """ return a json-ld context by extracting annotations from nested schemas
            those annotations are currently placed under the `context_value' key """
        # FIXME last one wins right now
        #context_runtimes = {}
        context = {}
        marker = 'context_value'
        #marker_runtime = 'context_runtime'
        def inner(schema, key=None):
            if isinstance(schema, dict):
                if marker in schema:
                    if key is None:
                        raise ValueError('you probably don\'t want to do this ...')

                    context_value = schema[marker]
                    context[key] = context_value

                #if marker_runtime in schema:
                    #if key is None:
                        #raise ValueError('you probably don\'t want to do this ...')

                    #context_runtimes[key] = schema[marker_runtime]

                for k, v in schema.items():
                    if k == marker:
                        continue
                    inner(v, k)

            elif isinstance(schema, list):
                for ss in schema:
                    inner(ss, key)

            else:
                pass

        inner(cls.schema)

        #if context:  # if there is no context do not add version
            #context.update(base_context)

        #if context_runtimes:
            #def context_runtime(base):
                #return {k: v(base) for k, v in context_runtimes.items()}

        return context


class JsonLdHelperSchema(JSONSchema):
    """ documentation of the additions to the json schema draft validator schema
        that are used for transformation to jsonld """
    schema = {
        'properties': {
            'jsonld_includes': {'type': 'object'},
            'context_value': {'oneOf': [
                # re https://github.com/json-ld/json-ld.org/issues/612
                # https://github.com/json-ld/json-ld.org/blob/master/schemas/jsonld-schema.json
                {'type': 'string'},  # TODO are there any restrictions? string -> @base/string ?
                {'type': 'object'},  # TODO this should conform to the jsonld schema for @context
            ]}
        }
    }


class RuntimeSchema(JSONSchema):
    """ finally a zero (runtime) cost first time only init
    with reasonable debugability and readability """

    schema = None

    def __new__(cls):
        return super().__new__(cls)

    _renew = __new__

    def __new__(cls, *args, **kwargs):
        cls.setup(*args, **kwargs)
        cls.__new__ = cls._renew
        return super().__new__(cls)

    def __init__(self):
        super().__init__()

    _reinit = __init__

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.__class__.__init__ = self.__class__._reinit

    @classmethod
    def setup(cls):
        raise NotImplementedError('subclassit')


class RemoteSchema(RuntimeSchema):
    schema = ''
    # FIXME TODO these need to be cached locally
    @classmethod
    def setup(cls):
        cls.schema = requests.get(cls.schema).json()


class ApiNATOMYSchema(RemoteSchema):
    schema = ('https://raw.githubusercontent.com/open-physiology/'
              'open-physiology-viewer/master/src/model/graphScheme.json')


class ToExport(RuntimeSchema):
    """ version should match the github release tag i.e. schema-0.0.1 """

    @classmethod
    def setup(cls, version=None):
        if version is None:
            version = 'master'
        #cls.schema = JApplyRecursive(_defunc, json_version(version))
        cls.schema = json_version(version)


metadata_filename_pattern = r'^.+\/[A-Za-z-_\/]+\.(xlsx|csv|tsv|json)$'

simple_url_pattern = r'^(https?):\/\/([^\s\/]+)\/([^\s]*)'
simple_file_url_pattern = r'^file:\/\/'

# no whitespace no colons for subject and sample identifiers
# and no forward slashes and no pipes
# also no leading symbols (problematic for utf-8 but deal with it)
# see https://stackoverflow.com/questions/1976007/
fs_safe_identifier_pattern = '^[A-Za-z0-9][^ \n\t*|:\/<>"\\?]+$'

# NOTE don't use builtin date-time format due to , vs . issue
iso8601pattern =     '^[0-9]{4}-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-6][0-9](:[0-6][0-9](,[0-9]{1,9})?)?(Z|[+-][0-2][0-9]:[0-6][0-9])$'
iso8601bothpattern = '^[0-9]{4}-[0-1][0-9]-[0-3][0-9](T[0-2][0-9]:[0-6][0-9](:[0-6][0-9](,[0-9]{1,9})?)?(Z|[+-][0-2][0-9]:[0-6][0-9]))?$'
iso8601datepattern = '^[0-9]{4}-[0-1][0-9]-[0-3][0-9]$'  # XXX people are using this in the timestamp field repeatedly

# https://www.crossref.org/blog/dois-and-matching-regular-expressions/
doi_pattern = idlib.Doi._id_class.canonical_regex

orcid_pattern = idlib.Orcid._id_class.canonical_regex

# for a fun (?) time
# https://en.wikipedia.org/wiki/List_of_family_name_affixes
contributor_name_pattern = r'^((([Vv](an|on))|[Dd][ia]|([Dd]e( [Ll]os)?)) )?[^, ]+, [^,]+$'

ror_pattern = idlib.Ror._id_class.canonical_regex

pattern_whitespace_lead_trail = r'(^[\s]+[^\s].*|.*[^\s][\s]+$)'
pattern_whitespace_multi = r'[\s]{2,}'

award_pattern = '^(OT2OD|OT3OD|U18|TR|U01)'  # really just no whitespace or / ?


class NoLTWhitespaceSchema(JSONSchema):
    schema = {'allOf': [{'type': 'string'},
                        {'not': {'pattern': pattern_whitespace_lead_trail,}},
                        {'not': {'pattern': pattern_whitespace_multi}},
                        ]}


string_noltws = NoLTWhitespaceSchema.schema


class ErrorSchema(JSONSchema):
    schema = {'type':'array',
              'minItems': 1,
              'items': {'type': 'object'},}



def make_id_schema(cls, pattern=None, format=None):
    id_schema = {'type': 'string'}
    if format is not None:
        id_schema['format'] = format

    if pattern is not None:
        id_schema['pattern'] = pattern
    elif hasattr(cls, '_id_class') and hasattr(cls._id_class, 'canonical_regex'):
        id_schema['pattern'] = cls._id_class.canonical_regex

    class _IdSchema(JSONSchema):
        __name__ = cls.__name__ + 'Schema'
        schema = {'properties': {'system': {'type': 'string',
                                            'enum': [cls.__name__],},
                                 'id': id_schema},}

    return _IdSchema


OntTermSchema = make_id_schema(oq.OntTerm, format='iri')
DoiSchema = make_id_schema(idlib.Doi)
RorSchema = make_id_schema(idlib.Ror)
OrcidSchema = make_id_schema(idlib.Orcid)
PioSchema = make_id_schema(idlib.Pio)
RridSchema = make_id_schema(idlib.Rrid)
BlackfynnIdSchema = make_id_schema(BlackfynnId)
PennsieveIdSchema = make_id_schema(PennsieveId)


class EmbeddedIdentifierSchema(JSONSchema):
    schema = {'type': 'object',
              'required': ['type', 'id', 'label'],
              'properties': {
                  'id': {'type': 'string'},
                  'type': {'type': 'string',
                           'enum': ['identifier']},
                  'system': {'type': 'string'},
                  'label': {'type': 'string'},
                  'synonyms': {'type': 'array',
                               'minItems': 1},
                  # NOTE: description is the normalized form name ofr
                  # ontology term definitions
                  'description': {'type': 'string'},},}

    @classmethod
    def _to_pattern(cls, obj, *args, path=None, alternates=None, **kwargs):  # FIXME add tests
        if (isinstance(obj, dict) and
            'allOf' in obj and
            (len(obj) == 1 or len(obj) == 2 and 'context_value' in obj) and  # FIXME extremely brittle
            len(obj['allOf']) == 2 and
            obj['allOf'][0] == cls.schema):
            pattern = obj['allOf'][1]['properties']['id']
            if alternates is not None:
                # FIXME SUPER brittle
                system = obj['allOf'][1]['properties']['system']['enum'][0]
                if system in alternates:
                    pattern['pattern'] = pattern['pattern'] + '|' + alternates[system]

            return pattern

        else:
            return obj

    @classmethod
    def _allOf(cls, schema, context_value=None):
        ao = {'allOf': [cls.schema, schema.schema,]}
        if context_value:
            ao['context_value'] = context_value

        return ao

    @classmethod
    def allOf(cls, schema_):
        class _SCM(JSONSchema):
            schema = cls._allOf(schema_)  # LOL PYTHON

        return _SCM


EIS = EmbeddedIdentifierSchema
EISs = EmbeddedIdentifierSchema.schema


class ProvSchema(JSONSchema):
    schema = {'type': 'object',
              'properties': {'timestamp_export_start': {'type': 'string',
                                                        'pattern': iso8601pattern,},
                             #'timestamp_export_end': {'type': 'string'},  # this isn't really possible
                             'export_system_identifier': {'type': 'string'},
                             'export_system_hostname': {'type': 'string'},
                             'export_project_path': {'type': 'string'},
                             'remote': {'type': 'string'},
                             'sparcur_version': {'type': 'string'},
                             'sparcur_internal_version': {'type': 'integer'},
                             'sparcur_commit': {'type': 'string'},
                             'ontology_mappings': {
                                 'type': 'array',
                                 'minItems': 1,
                                 'items': {
                                     'type': 'object',
                                     'properties': {
                                         'curie': {'type': 'string'},
                                         'label': {'type': 'string'},
                                         'matches': {
                                             'type': 'array',
                                             'minItems': 1,
                                             'items': {
                                                 'type': 'object',
                                                 'properties': {
                                                     'path': {'type': 'array'},
                                                     'input': {'type': 'string'},
                                                 },},},},},},},}


class MbfTracingSchema(JSONSchema):
    schema = {
        'type': 'object',
        'required': ['subject', 'atlas', 'images', 'contours'],
        'properties': {
            'subject': {'type': 'object',
                        'required': ['id'],
                        'properties': {'id': {'type': 'string'},
                                       'species': {'type': 'string'},
                                       'sex': {'type': 'string'},
                                       'age': {'type': 'string'},
                                       },},
            'atlas': {'type': 'object',
                      'properties': {'organ': {'type': 'string'},
                                     'atlas_label': {'type': 'string'},
                                     'atlas_rootid': {'type': 'string'},
                                     },},
            'images': {'type': 'array',
                       'items': {'type': 'object',
                                 'properties':
                                 {'path_mbf': {
                                     'type': 'array',
                                     'minItems': 1,
                                     'items': {'type': 'string'}},
                                  'channels': {
                                      'type': 'array',
                                      'items': {
                                          'type': 'object',
                                          'properties': {
                                              'id': {'type': 'string'},
                                              'source': {'type': 'string'},
                                          },},},},},},
            'contours': {'type': 'array',
                         'items': {'type': 'object',
                                   'properties': {
                                       'name': string_noltws,
                                       'guid': {'type': 'string'},
                                       'id_ontology': {'type': 'string'},},
                                   },},},}


class NeurolucidaSchema(JSONSchema):
    schema = copy.deepcopy(MbfTracingSchema.schema)
    schema['required'] = ['images', 'contours']


class DatasetStructureSchema(JSONSchema):
    __schema = {'type': 'object',
                'required': ['submission_file', 'dataset_description_file'],
                'properties': {
                    'submission_file': {'type': 'string',
                                        'pattern': metadata_filename_pattern},
                    'dataset_description_file': {'type': 'string',
                                                 'pattern': metadata_filename_pattern},
                    'subjects_file': {'type': 'string',
                                      'pattern': metadata_filename_pattern},
                    'samples_file': {'type': 'string',
                                     'pattern': metadata_filename_pattern},
                    'manifest_file': {'type': 'array',
                                      'minItems': 1,
                                      'items': {
                                          'type': 'string',
                                          'pattern': metadata_filename_pattern},
                                      },
                    'dir_structure': {'type': 'array',  # TODO FIXME do not include in primary export
                                      'items': {'type': 'object',}
                                      ,},
                    'dirs': {'type': 'integer',
                             'minimum': 0},
                    'files': {'type': 'integer',
                              'minimum': 0},
                    'size': {'type': 'integer',
                             'minimum': 0},
                    'errors': ErrorSchema.schema,
                }}
    schema = {'allOf': [__schema,
                        {'anyOf': [
                            {'required': ['subjects_file']},
                            {'required': ['samples_file']}]}]}


class FutureValidPaths:
    # maybe we use these in the future?

    folder_patterns_full = (
        '^('
        'part(icipant)?|'
        'pop(ulation)?|'
        'spec(imen)?|'
        'sub(j|ject)?|'
        'sam(p|ple)?|'
        'pool|'
        'perf(ormance)?'
        ')-([0-9A-Za-z][0-9A-Za-z-]*)$'
    )

    folder_patterns = '^(part|pop|spec|subj?|samp?|pool|perf)-([0-9A-Za-z][0-9A-Za-z-]*)$'
    folder_patterns_strict = '^(pop|sub|sam|pool|perf)-([0-9A-Za-z][0-9A-Za-z-]*)$'

    participant_patterns = '^(part|pop|spec|subj?|samp?|pool)-([0-9A-Za-z][0-9A-Za-z-]*)$'
    participant_patterns_strict = '^(pop|sub|sam|pool)-([0-9A-Za-z][0-9A-Za-z-]*)$'

    ['0-9A-Za-z-']
    ['^pool-']
    ['^subj?-']
    ['^samp?-']
    ['^perf-']  # ^ses- ^scan- ^run- roll up here
    ['^spec-']
    ['^pop-']
    ['^part-']


    ['code', '*']
    ['docs', '*']
    ['source', '*']  # but will check for matches and warn on extra
    ['derivative', '*']  # but will check for matches and warn on extra

    # paths
    ['primary', '^pop-']
    ['primary', '^pool-']
    ['primary', '^pool-', '^perf-']
    ['primary', '^subj?-']
    ['primary', '^subj?-', '^perf-']
    ['primary', '^subj?-', '^samp?-']
    ['primary', '^samp?-']
    ['primary', '^samp?-', '^perf-']
    # ['primary', '^perf-']  # these would be perf types which are metadata so we don't allow this pattern

    # data modalities ^mod- in bids
    '^phys(iology)?$'
    '^seq(uencing)?$'
    '^rad(iology)?$'
    '^mic(roscopy)?$'
    '^morph(ology)?$'  # swc

    # XXX we might try to avoid using modalities
    # we are going to use ^perf-type-sub-1 instead or something like that
    # then the protocol should give us the data type, keep the data types out of this
    # OR do we need that information to statically validate allowed file types?

    # the more likely approach is as follows
    # all some-* indicates that the id MUST be a known
    # identifier whose type matches the expected slot

    # primary (or some-subject some-sample some-pool some-perf)
    # this is broken unless the manifest can save us (which it must)
    # otherwise we have to warn on all folders that do not match
    # a known entity
    # a perf- folder may appear once per non-perf node XXX this might be incorrect?
    # some-subject (or some-sample some-perf-no-subject some-manifest-folder)
    # some-sample (or some-sample some-perf-end some-manifest-folder)
    # some-perf-no-subject (or some-sample some-manifest-folder)
    # some-perf (or some-subject-no-perf some-sample-no-perf)
    # HOWEVER this is too much for right now, we may need to completely
    # rework the requirements for the template 2.0 to avoid these tricky cases
    # for now, we are going to find all folders that match an id below the top
    # level and then we are going to warn about all other folder names that are
    # not either contained in a subject where there are no samples or in a sample

    # {'allOf': [
    #     {'type': 'string',},
    #                   {'oneOf': [
    #                       {'pattern': subject_folder_pattern},
    #                       {'pattern': sample_folder_pattern},
    #                       {'pattern': performance_folder_pattern},
    #                       {'pattern': pool_folder_pattern},
    #                   ],},]
    # }


class DatasetPathSchema(JSONSchema):
    schema = {'type': 'array',
              'additionalItems': {'type': 'string'},
              'items': [
                  {'type': 'string',
                   'enum': ['code', 'docs', 'source', 'primary', 'derivative'],
                   },]}


class ContributorExportSchema(JSONSchema):
    schema = {
        'type': 'object',
        #'jsonld_include': {'@type': ['sparc:Person', 'owl:NamedIndividual']},  # FIXME being included THREE times due to references in nested schema # XXX THIS HAS BEEN MOVED AFTER ALL DEEPCOPY OPERATIONS SIGH
        'properties': {
            'id': {'type': 'string',  # FIXME TODO
                   },
            'affiliation': EIS._allOf(RorSchema,
                                      context_value=idtype('TEMP:hasAffiliation')),
            'contributor_name': {'type': 'string',
                                 'pattern': contributor_name_pattern,},
            'first_name': {'type': 'string',
                           'context_value': 'sparc:firstName',},
            'middle_name': strcont('TEMP:middleName'),
            'last_name': {'type': 'string',
                          'context_value': 'sparc:lastName',},
            'contributor_orcid': EIS._allOf(
                OrcidSchema,
                context_value=idtype('sparc:hasORCIDId')),
            #'data_remote_user_id': EIS._allOf(
                #PennsieveIdSchema,
                #context_value=idtype('TEMP:hasDataRemoteUserId')),
            'data_remote_user_id': strcont(idtype('TEMP:hasDataRemoteUserId')),
            #'blackfynn_user_id': EIS._allOf(
                #BlackfynnIdSchema,
                #context_value=idtype('TEMP:hasBlackfynnUserId')),
            'blackfynn_user_id': strcont(idtype('TEMP:hasBlackfynnUserId')),
            'contributor_affiliation': {
                'anyOf': [EIS._allOf(RorSchema),  # FIXME oneOf issues with EIS
                          {'type': 'string'}],
                'context_value': idtype('TEMP:hasAffiliation')
            },
            'contributor_role': {
                'type':'array',
                'context_value': idtype('TEMP:hasRole', base=str(TEMP)),
                'minItems': 1,
                'items': {
                    'type': 'string',
                    'enum': ['CorrespondingAuthor',  # FIXME max card 1
                             'ContactPerson',  # FIXME need version dependence here
                             'Creator',  # allowed here, moved later
                             'DataCollector',
                             'DataCurator',
                             'DataManager', # this is DatasetMaintainer
                             'Distributor',
                             'Editor',
                             'HostingInstitution',
                             'PrincipalInvestigator',  # added for sparc map to ProjectLeader probably?
                             'CoInvestigator', # added for sparc, to distingusih ResponsibleInvestigator
                             'Producer',
                             'ProjectLeader',
                             'ProjectManager',
                             'ProjectMember',
                             'RegistrationAgency',
                             'RegistrationAuthority',
                             'RelatedPerson',
                             'Researcher',
                             'ResearchGroup',
                             'RightsHolder',
                             'Sponsor',
                             'Supervisor',
                             'WorkPackageLeader',
                             'Other',]}
            },
            'is_contact_person': {'type': 'boolean'},  # XXX deprecated
            'errors': ErrorSchema.schema,
        }}


class ContributorSchema(JSONSchema):
    context = lambda : ({}, None)
    __schema = copy.deepcopy(ContributorExportSchema.schema)
    schema = JApplyRecursive(
        EIS._to_pattern, __schema,
        alternates={'Orcid': idlib.Orcid._id_class.local_regex})


class ContributorsExportSchema(JSONSchema):
    schema = {'anyOf':
              [{'type': 'array',
                #'context_runtime': lambda base: {'@id': f'{base}contributors',},
                'contains': {
                    'type': 'object',
                    'required': ['is_contact_person'],
                    'properties': {
                        'is_contact_person': {'type': 'boolean', 'enum': [True]},
                    },
                },
                'items': ContributorExportSchema.schema,
                },
               # 2.0.0
               {'type': 'array',
                'contains': {
                    'type': 'object',
                    'required': ['contributor_role'],
                    'properties': {
                        'contributor_role': {
                            'type': 'array',
                            'contains': {
                                'type': 'string',
                                'enum': ['CorrespondingAuthor', 'ContactPerson']
                            },
                        },
                    },
                },
                'items': ContributorExportSchema.schema,},
               ],
              }


class ContributorsSchema(JSONSchema):
    context = lambda : ({}, None)
    __schema = copy.deepcopy(ContributorsExportSchema.schema)
    schema = JApplyRecursive(
        EIS._to_pattern, __schema,  # watch out for singular vs plural
        # >_< also watch out for non-commutative derivation chains,
        # sigh this is exactly the kind of issue that I was worried
        # about when I originally took this approach and yes, it is a
        # mistake and is why DRY is a good thing
        alternates={'Orcid': idlib.Orcid._id_class.local_regex})


class ContributorOutExportSchema(JSONSchema):
    schema = copy.deepcopy(ContributorExportSchema.schema)
    schema['properties']['contributor_role']['items']['enum'].remove('Creator')


class ContributorsOutExportSchema(JSONSchema):
    schema = copy.deepcopy(ContributorsExportSchema.schema)
    schema['items'] = ContributorOutExportSchema.schema


class CreatorExportSchema(JSONSchema):
    schema = copy.deepcopy(ContributorExportSchema.schema)
    schema['properties'].pop('contributor_role')
    schema['properties'].pop('is_contact_person')


class CreatorSchema(JSONSchema):
    context = lambda : ({}, None)
    __schema = copy.deepcopy(CreatorExportSchema.schema)
    schema = JApplyRecursive(EIS._to_pattern, __schema)



class CreatorsExportSchema(JSONSchema):
    schema = {'type': 'array',
              'minItems': 1,
              'items': CreatorExportSchema.schema}


class CreatorsSchema(JSONSchema):
    context = lambda : ({}, None)
    __schema = copy.deepcopy(CreatorsExportSchema.schema)
    schema = JApplyRecursive(EIS._to_pattern, __schema)


_protocol_url_or_doi_schema = {'anyOf':[EIS._allOf(DoiSchema),
                                        EIS._allOf(PioSchema),
                                        {'type': 'string',
                                         'pattern': simple_url_pattern,}]}


ContributorExportSchema.schema['jsonld_include'] = {
    '@type': ['sparc:Person', 'owl:NamedIndividual']}


class DatasetDescriptionExportSchema(JSONSchema):
    schema = {
        'type': 'object',
        'additionalProperties': False,
        'required': ['template_schema_version',  # missing should fail for this one ...
                     'name',
                     'description',
                     'funding',
                     'protocol_url_or_doi',
                     #'completeness_of_data_set'  # TODO versioned schema validation
                     'contributors',
                     'number_of_subjects',
                     'number_of_samples',],
        # TODO dependency to have title for complete if completeness is is not complete?
        'properties': {
            # 2.x helpers
            'dataset_type': {'type': 'string'},
            # 1.x
            'errors': ErrorSchema.schema,
            'template_schema_version': {'type': 'string'},
            'name': string_noltws,
            'description': {'type': 'string'},
            'keywords': {'type': 'array', 'items': {'type': 'string'}},
            'acknowledgments': {'type': 'string'},
            'funding': {'type': 'array',
                        'minItems': 1,
                        'items': {'type': 'string'}},
            'completeness_of_data_set': {'type': 'string'},
            'prior_batch_number': {'type': 'string'},
            'title_for_complete_data_set': {'type': 'string'},
            'originating_article_doi': {'type': 'array',
                                        'items': EIS._allOf(DoiSchema),
                                        #'context_value': idtype('TEMP:hasDoi'),  # FIXME convert here?
                                        },
            'number_of_subjects': {'type': 'integer'},
            'number_of_samples': {'type': 'integer'},
            'parent_dataset_id': {'type': 'string'},  # blackfynn id
            'protocol_url_or_doi': {'type': 'array',
                                    'minItems': 1,
                                    'items': _protocol_url_or_doi_schema,
                                    #'context_value': idtype('TEMP:hasProtocol'),  # FIXME convert here?
                                    },
            'links': {
                'type': 'array',
                'minItems': 1,
                'items': {
                    'type': 'object',
                    'properties': {
                        'additional_links': {'type': 'string'},
                        'link_description': {'type': 'string'},  # FIXME only if there is an additional link? some using for protocol desc
                    }
                }
            },
            'related_identifiers' : {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'required': [
                        'relation_type',
                        'related_identifier_type',
                        'related_identifier'],
                    'properties': {
                        'related_identifier': {'type': 'string'},
                        'related_identifier_type': {
                            'type': 'string',
                            'enum': [
                                # datacite types
                                'ARK',
                                'arXiv',
                                'bibcode',
                                'DOI',
                                'EAN13',
                                'EISSN',
                                'Handle',
                                'IGSN',
                                'ISBN',
                                'ISSN',
                                'ISTC',
                                'LISSN',
                                'LSID',
                                'PMID',
                                'PURL',
                                'UPC',
                                'URL',
                                'URN',
                                'w3id',
                            ],},
                        'relation_type': {
                            'type': 'string',
                            'enum':
                            [
                                # additional relations that are needed for sparc, specifically for code and protocols
                                'IsProtocolFor',  # like is metadata for
                                'HasProtocol', # hopefully entails WasGeneratedByFollowing
                                'IsSoftwareFor', # includes source code
                                'HasSoftware',
                                # DataCite conflates source and compiled, consider Requires?
                                # Dataset -> Software -> SourceCode

                                # datacite relations as of schema 4.4
                                'IsCitedBy',
                                'Cites',
                                'IsSupplementTo',
                                'IsSupplementedBy',
                                'IsContinuedByContinues',
                                'IsDescribedBy',
                                'Describes',
                                'HasMetadata',
                                'IsMetadataFor',
                                'HasVersion',
                                'IsVersionOf',
                                'IsNewVersionOf',
                                'IsPreviousVersionOf',
                                'IsPartOf',
                                'HasPart',
                                'IsPublishedIn',
                                'IsReferencedBy',
                                'References',
                                'IsDocumentedBy',
                                'Documents',
                                'IsCompiledBy',
                                'Compiles',
                                'IsVariantFormOf',
                                'IsOriginalFormOf',
                                'IsIdenticalTo',
                                'IsReviewedBy',
                                'Reviews',
                                'IsDerivedFrom',
                                'IsSourceOf',
                                'IsRequiredBy',
                                'Requires',
                                'IsObsoletedBy',
                                'Obsoletes',
                            ],
                        },
                        'related_identifier_description': {'type': 'string'},
                        'resource_type_general': {'type': 'string'},  # TODO this isn't in the template to keep the template simple
                    },
                },
            },
            'examples': {
                'type': 'array',
                'minItems': 1,   # this pattern makes things self describing
                'items': {
                    'type': 'object',
                    'properties': {
                        'example_image_filename': {'type': 'string'},
                        'example_image_locator': {'type': 'string'},
                        'example_image_description': {'type': 'string'},
                    }
                }
            },
            'contributors': ContributorsExportSchema.schema,
        }
    }


class DatasetDescription2Schema(JSONSchema):
    # XXX not clear that we need a separate schema for this
    # or whether we handle it in the ingestion side, having
    # a single internal schema should be possible
    schema = {
        'errors': ErrorSchema.schema,
        'template_schema_version': {'type': 'string'},
        # 2.0
        # 'required': []
        'properties': {
            'dataset_type': {
                'type': 'string',
                'enum': ['experimental', 'computational', 'abi-scaffold', 'o2s-simulation'],
            },
            'title': {'type': 'string',},
            'subtitle': {'type': 'string',},
            'keywords': {'type': 'array',},
            'funding': {'type': 'array',},
            'acknowledgements': {'type': 'string',},

            'study': {
                'type': 'object',
                'properties': {
                    'study_purpose': {'type': 'string',},
                    'study_data_collection': {'type': 'string',},
                    'study_conclusion': {'type': 'string',},
                    'study_collection_title': {'type': 'string',},
                },},
            'study_organ_system': {'type': 'array',},
            'study_approach': {'type': 'array',},
            'study_technique': {'type': 'array',},

            'number_of_subjects': {'type': 'integer'},
            'number_of_samples': {'type': 'integer'},
        }
    }


DatasetDescriptionExportSchema.schema['properties'].update(
    DatasetDescription2Schema.schema['properties'])


class DatasetDescriptionSchema(JSONSchema):
    context = lambda : ({}, None)
    __schema = copy.deepcopy(DatasetDescriptionExportSchema.schema)
    schema = JApplyRecursive(
        EIS._to_pattern, __schema,
        alternates={'Orcid': idlib.Orcid._id_class.local_regex})


class SubmissionSchema(JSONSchema):
    context = lambda : ({}, None)
    schema = {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'errors': ErrorSchema.schema,
            'submission': {
                'type': 'object',
                'required': ['sparc_award_number',
                             'milestone_achieved',
                             'milestone_completion_date'],
                # TODO allOf?
                'properties': {'sparc_award_number': {'type': 'string'},
                               'milestone_achieved': {'type': 'string'},
                               'milestone_completion_date': {'type': 'string',
                                                             'context_value': 'TEMP:milestoneCompletionDate',
                                                             'pattern': iso8601datepattern,},}}}}


class UnitSchema(JSONSchema):
    schema = {'oneOf': [{'type': ['number', 'string']},
                        {'type': 'object',
                         # TODO enum on the allowed values here
                         'properties': {'type': {'type': 'string'},
                                        'value': {'type': 'number'},
                                        'unit': {'type': 'string'}}}]}


_software_schema = {'type': 'array',
                    'minItems': 1,
                    'items': {
                        'type': 'object',
                        'properties': {
                            'software_version': {'type': 'string'},
                            'software_vendor': {'type': 'string'},
                            'software_url': {'type': 'string'},
                            'software_rrid': EIS._allOf(RridSchema),
                        },},}


subsam_common_properties = {  # FIXME these should not be in the samples sheet, or if they are they refer to the sample
    'species': {'anyOf': [EIS._allOf(OntTermSchema),
                          {'type': 'string'},],
                'context_value': 'sparc:animalSubjectIsOfSpecies'},
    'strain': {'anyOf': [EIS._allOf(OntTermSchema),
                         {'type': 'string'},],
                'context_value': 'sparc:animalSubjectIsOfStrain'},
    'rrid_for_strain': EIS._allOf(RridSchema, context_value=idtype('TEMP:specimenRRID')),  # XXX CHANGE from sparc:specimenHasIdentifier
    'genus': strcont('sparc:animalSubjectIsOfGenus'),
    'sex': {'anyOf': [EIS._allOf(OntTermSchema),
                      # FIXME oneOf fails when EIS is converted to input type
                      {'type': 'string'}],
            'context_value': idtype('sparc:hasBiologicalSex'),
            },  # sex as a variable ?

    'body_mass':{'context_value': 'sparc:animalSubjectHasWeight',
                 **UnitSchema.schema},
    'body_weight': {'context_value': 'sparc:animalSubjectHasWeight',
                    **UnitSchema.schema},
    'age': {'context_value': 'TEMP:hasAge',
            **UnitSchema.schema},
    'age_years': {'context_value': 'TEMP:hasAge',
                  **UnitSchema.schema},
    'age_category': {'type': 'string',
                     'context_value': idtype('TEMP:hasAgeCategory'),
                     },  # TODO uberon
    'age_range_min': {'context_value': 'TEMP:hasAgeMin',
                      **UnitSchema.schema},
    'age_range_max': {'context_value': 'TEMP:hasAgeMax',
                      **UnitSchema.schema},
    'handedness': strcont('TEMP:hasHandedness'),

    # ambiguous
    # genotype is ambiguous when dealing with tissue from chimeric subjects or experimentally manipulated cell populations
    'genotype': strcont('TEMP:hasGenotype'),
    'mass': {'context_value': 'sparc:animalSubjectHasWeight',
             # XXX NOTE subjects will require their own context
             # since mass can clearly have other meanings elsewhere
             **UnitSchema.schema},
    'weight': {'context_value': 'sparc:animalSubjectHasWeight',
               **UnitSchema.schema},
    'experimental_group': strcont('TEMP:hasAssignedGroup'),
    'group': strcont('TEMP:hasAssignedGroup'),
    'organism_rrid': strcont('TEMP:organismRRID'),

    # fields that require additional information
    'experiment_date': strcont('TEMP:protocolPerformanceDate'),  # FIXME needs to reference a protocol
    'injection_date': strcont('TEMP:protocolPerformanceDate'),  # FIXME needs to reference a protocol
    'date_of_euthanasia': strcont('TEMP:protocolPerformanceDate'),
    'date_of_injection': strcont('TEMP:protocolPerformanceDate'),

    # fields that are actually about files
    'sha1': strcont('TEMPRAW:hasDigitalArtifactThatIsAboutItWithHash'),
    'filename': strcont('TEMPRAW:hasDigitalArtifactThatIsAboutIt'),
    'upload_filename': strcont('TEMPRAW:hasDigitalArtifactThatIsAboutIt'),
    'dataset_id': strcont('TEMP:providerDatasetIdentifier'),  # FIXME need a global convention for this

    # fields that are actually about performances
    'experiment_number': strcont('TEMP:localPerformanceNumber'),  # FIXME TODO
    'session': strcont('TEMP:localPerformanceNumber'),

    # extra
    'comment': strcont('TEMP:providerNote'),
    'note': strcont('TEMP:providerNote'),
}


class PerformanceExportSchema(JSONSchema):
    schema = {
        'type': 'object',
        'jsonld_include': {'@type': ['sparc:Performance', 'owl:NamedIndividual']},
        '': [{'context_runtime': [
                '#/meta/uri_api',
                (lambda uri_api: uri_api + '/performances/'),
                '#/performances/@context/@base']},
             {'context_runtime': [
                 '#/meta/uri_api',
                 (lambda uri_api: {'@id': uri_api + '/performances/',
                                   '@prefix': True}),
                 '#/@context/performances']}],
        'required': ['performance_id',],
        'properties': {
            'performance_id': {'type': 'string',
                               'pattern': fs_safe_identifier_pattern,
                               'context_value': {"@id": "@id",
                                                 "@type": "@id"},
                           },
            'protocol_url_or_doi': _protocol_url_or_doi_schema,  # FIXME ideally this would be true but are we sure it always is?
        },}


class PerformancesExportSchema(JSONSchema):
    schema = {
        'type': 'object',
        #'context_value': {'@id': 'graph-subjects', CONTEXT_ROOT: None},  # not needed, schemas have their own context
        'required': ['performances'],
        'properties': {
            'performances': {
                'type': 'array',
                'minItems': 1,
                # FIXME there is currently no way to require that
                # a key be unique among all objects
                'items': PerformanceExportSchema.schema,},
            'errors': ErrorSchema.schema,}}


class PerformancesSchema(JSONSchema):
    context = lambda : ({}, None)
    __schema = copy.deepcopy(PerformancesExportSchema.schema)
    schema = JApplyRecursive(EIS._to_pattern, __schema)


class SubjectExportSchema(JSONSchema):
    schema = {
        'type': 'object',
        'jsonld_include': {'@type': ['sparc:Subject', 'owl:NamedIndividual']},
        '': [{'context_runtime': [
                '#/meta/uri_api',
                (lambda uri_api: uri_api + '/subjects/'),
                '#/subjects/@context/@base']},
             {'context_runtime': [
                 '#/meta/uri_api',
                 (lambda uri_api: {'@id': uri_api + '/subjects/',
                                   '@prefix': True}),
                 '#/@context/subjects']}],
        'required': ['subject_id', 'species'],
        'properties': {
            'subject_id': {'type': 'string',
                           'pattern': fs_safe_identifier_pattern,
                           'context_value': {"@id": "@id",
                                             "@type": "@id"},
                           #'context_runtime': [  # TODO -> derive after add ?
                               #'#/meta/uri_api',
                               # FIXME the @id mapping seems to break the @context !?
                                #(lambda uri_api: {
                                    #'@id': '@id',
                                    #'@context': {
                                        #'@base': uri_api + '/subjects/'}}),
                               #'#/subjects/@context/subject_id']
                           #'context_runtime':
                           #lambda base: {'@id': '@id',
                                         #'@type': '@id',
                                         #'@context': {'@base': f'{base}subjects/'}}
                           },
            'ear_tag_number': strcont('TEMP:localIdAlt'),  # FIXME also int? or int ids don't support mathematical operations
            'treatment': strcont('TEMP:hadExperimentalTreatmentApplied'),
            'initial_weight': cont('sparc:animalSubjectHasWeight', ['string', 'number']),
            'height_inches': cont('TEMP:subjectHasHeight', ['string', 'number']),  # FIXME units
            'gender': strcont('sparc:hasGender'),
            'body_mass_weight': cont('TEMP:subjectHasWeight', ['string', 'number']),  # FIXME units
            'anesthesia': strcont('TEMP:wasAdministeredAnesthesia'),
            'stimulation_site': strcont('sparc:spatialLocationOfModulator'),  # TODO ontology
            'stimulator': strcont('sparc:stimulatorUtilized'),
            'stimulating_electrode_type': strcont('sparc:stimulatorUtilized'),  # FIXME instance vs type

            'nerve': strcont('TEMPRAW:involvesAnatomicalRegion'),  # FIXME can be more specific than this
            **subsam_common_properties
        },}


class SubjectsExportSchema(JSONSchema):
    schema = {
        'type': 'object',
        #'context_value': {'@id': 'graph-subjects', CONTEXT_ROOT: None},  # not needed, schemas have their own context
        'required': ['subjects'],
        'properties': {
            'subjects': {
                'type': 'array',
                #'context_runtime': lambda base: {'@id': f'{base}subjects',},
                'minItems': 1,
                # FIXME there is currently no way to require that
                # a key be unique among all objects
                'items': SubjectExportSchema.schema,},
            'errors': ErrorSchema.schema,
            'software': _software_schema}}

    # FIXME not implemented
    extras = [['unique', ['subjects', '*', 'subject_id']]]


class SubjectsSchema(JSONSchema):
    context = lambda : ({}, None)
    __schema = copy.deepcopy(SubjectsExportSchema.schema)
    schema = JApplyRecursive(EIS._to_pattern, __schema)


class SampleExportSchema(JSONSchema):
    schema = {
        'type': 'object',
        'jsonld_include': {'@type': ['sparc:Sample', 'owl:NamedIndividual']},
        '': [{'context_runtime': [
                '#/meta/uri_api',
                (lambda uri_api: uri_api + '/samples/'),
                '#/samples/@context/@base']},
             {'context_runtime': [
                 '#/meta/uri_api',
                 (lambda uri_api: {'@id': uri_api + '/samples/',
                                   '@prefix': True}),
                 '#/@context/samples']}],
        'required': ['sample_id', 'subject_id'],
        'properties': {
            'sample_id': {'type': 'string',
                          'pattern': fs_safe_identifier_pattern,
                          },
            'subject_id': {'type': 'string',
                           'pattern': fs_safe_identifier_pattern,
                           },
            'primary_key': {'type': 'string',
                            'context_value': {"@id": "@id",
                                              "@type": "@id"},
                            #'context_runtime': [  # TODO -> derive after add ?
                                #'#/meta/uri_api',
                                #(lambda uri_api: uri_api + '/samples/'),
                                # FIXME the @id mapping seems to break the @context !?
                                # yep, this doesn't work because for some reason the
                                # @id has to use the parent space, but I think this
                                # approach to setting @base is probably safer
                                #(lambda uri_api: {
                                    #'@id': '@id',
                                    #'@context': {
                                        #'@base': uri_api + '/samples/'}}),
                                #'#/samples/@context/primary_key',
                                #'#/samples/@context/@base'],
                            #'context_runtime':
                            #lambda base: {'@id': '@id',
                                          #'@type': '@id',
                                          #'@context': {'@base': f'{base}samples/'}}
                            },
            'specimen': {'type': 'string',
                         },  # what are we expecting here?
            'specimen_anatomical_location': {'oneOf':
                                             [{'type': 'string',},
                                              # FIXME not seeing an easy wawy out of this
                                              # other than switching many of these over to arrays
                                              # by default
                                              {'type': 'array',
                                               'items': {'type': 'string'},},
                                              ],},

                        'disease': {'type': 'string',
                                    },
            'reference_atlas': {'type': 'string',
                                },
            'protocol_title': {'type': 'string',
                               },
            'protocol_url_or_doi': _protocol_url_or_doi_schema 
            ,
            'experimental_log_file_name': {'type': 'string',
                                           },
            **subsam_common_properties
        },}


class SamplesFileExportSchema(JSONSchema):
    schema = {
        'type': 'object',
        'required': ['samples'],
        'properties': {
            'samples': {
                'type': 'array',
                #'context_runtime': lambda base: {'@id': f'{base}samples',},
                'minItems': 1,
                'items': SampleExportSchema.schema,},
            'errors': ErrorSchema.schema,
            'software': _software_schema}}

    # FIXME not implemented
    extras = [['unique', ['samples', '*', 'sample_id']]]


class SamplesFileSchema(JSONSchema):
    context = lambda : ({}, None)
    __schema = copy.deepcopy(SamplesFileExportSchema.schema)
    schema = JApplyRecursive(EIS._to_pattern, __schema)


class ManifestRecordExportSchema(JSONSchema):
    schema = {
        'allOf': [
            {'type': 'object',
             'properties': {
                 'pattern': {
                     'type': 'string',
                 },
                 'filename': {
                     'type': 'string',
                 },
                 'timestamp': {
                     'type': 'string',
                     'pattern': iso8601pattern,
                 },
                 'timestamp_or_date': {  # this is internal for dataset template <= 1.2.3
                     'type': 'string',
                     'pattern': iso8601bothpattern,
                 },
                 'date': {
                     'type': 'string',
                     'pattern': iso8601datepattern,
                 },
                 'description': {
                     'type': 'string',
                 },
                 'file_type': {
                     'type': 'string',
                 },
                 'data_type': {
                     'type': 'string',
                 },
                 'additional_types': {
                     'type': 'string',  # FIXME currently expects a single value, maybe rename?
                     #'type': 'array',
                     #'minItems': 1,
                     #'items': {
                         #'type': 'string',
                         # FIXME need to make a note that if more than one
                         # value is provided then the value provided first
                         # is the one that will be lifted to mimetype for the file
                         # TODO mimetype pattern?
                     #},
                 },
                 # extras duplicated from other sheets
                 'protocl_title': {'type': 'string',},
                 'protocol_url_or_doi': _protocol_url_or_doi_schema,
                 'software_version': {'type': 'string'},
                 'software_vendor': {'type': 'string'},
                 'software_url': {'type': 'string'},
                 'software_rrid': EIS._allOf(RridSchema),
                 'software': {'type': 'string'},
                 # more extras
                 #'organism_RRID': EIS._allOf(RridSchema),
                 'sha1': {'type': 'string'},
             },
             },
            {'oneOf': [
                {'required': ['pattern']},
                {'required': ['filename']},  # timestamp not required for now
            ]},
        ]
    }


class ChecksumSchema(JSONSchema):
    _schema_base = {
        'type': 'object',
        'required': ['type', 'cypher'],
        'properties': {
            'type': {'type': 'string',
                     'enum': ('checksum',),},
            'cypher': {'type': 'string',
                       # more may be supported in the future
                       # but supported checksums must be closed
                       # because every single supported checksum
                       # requires a full implementation
                       'enum': ('sha256',),},
            # other options might be base64 or something like that
            'hex': {'type': 'string',},
        }
    }

    _schema_more = {'oneOf': [
        {'properties': {'cypher': {'type': 'string', 'enum': ('sha256',),},
                        'hex': {'type': 'string', 'minLength': 64, 'maxLength': 64,},},},
    ],}

    schema = {'allOf': [_schema_base,
                        _schema_more,]}


class PathSchema(JSONSchema):
    schema = {
        'type': 'object',
        'jsonld_include': {'@type': ['sparc:Path', 'owl:NamedIndividual']},
        'required': ['type',
                     'dataset_relative_path',
                     'uri_api',
                     'uri_human',
                     ],
        'properties': {
            'type': {'type': 'string',
                     'enum': ['path']},
            'dataset_relative_path': {'type': 'string'},
            'uri_api': {'oneOf': [{'type': 'string',
                                   'pattern': simple_url_pattern},
                                  {'type': 'string',
                                   'pattern': simple_file_url_pattern}]},
            'uri_human': {'oneOf': [{'type': 'string',
                                     'pattern': simple_url_pattern},
                                    {'type': 'string',
                                     'pattern': simple_file_url_pattern}]},
            'dataset_id': {'type': 'string'},
            'remote_id': {'type': 'string'},
            'parent_id': {'type': 'string'},
            'mimetype': {'type': 'string'},
            'magic_mimetype': {'type': 'string'},
            'contents': {'type': 'object'},  # opaque here
            'size_bytes': {'type': 'number'},
            'checksums': {'type': 'array',
                          'minItems': 1,
                          'items': ChecksumSchema.schema,},
            'basename': {'type': 'string',
                         # FIXME TODO this is overly restrictive and
                         # doesn't help in providing good feedback in
                         # cases where people are using periods as
                         # file name separators which messes up suffix
                         # file type detection, it also doesn't warn
                         # if there is no suffix
                         'pattern': fs_safe_identifier_pattern,},
            'errors': ErrorSchema.schema,
        }
    }


class PathTransitiveMetadataSchema(JSONSchema):
    schema = {'type': 'object',
              'properties': {
                  'type': {'type': 'string',
                           'enum': ('path-metadata',),},
                  'records': {'type': 'array',
                              'items': PathSchema.schema,
                              'minItems': 1,},},}


class ManifestFileExportSchema(JSONSchema):  # FIXME TODO FileObjectSchema ??
    schema = {
        'type': 'object',
        'jsonld_include': {'@type': ['sparc:File', 'owl:NamedIndividual']},
        # FIXME handle raw json manifest file structure where contents is the json blob
        # I think we just need to rename metadata -> manifest_records to fix that case
        'allOf': [
            PathSchema.schema,
            {'required': ['contents'],  # TODO uri_api
             'properties': {
                 'errors': ErrorSchema.schema,
                 'contents':  {
                     'type': 'object',
                     'required': ['manifest_records'],
                     'properties': {
                         'manifest_records': {
                             'type': 'array',
                             'minItems': 1,
                             'items': ManifestRecordExportSchema.schema,
                         },
                     },
                 },
             },
             },
        ]
    }


class ManifestFileSchema(JSONSchema):
    __schema = copy.deepcopy(ManifestFileExportSchema.schema)
    schema = JApplyRecursive(EIS._to_pattern, __schema)


class ManifestFilesExportSchema(JSONSchema):
    schema = {
        'type': 'object',
        'required': ['manifest_files'],
        'properties': {
            'manifest_files': {
                'type': 'array',
                'minItems': 1,
                'items': ManifestFileExportSchema.schema,
            },
            'errors': ErrorSchema.schema,
        }
    }


class MetaOutExportSchema(JSONSchema):
    __schema = copy.deepcopy(DatasetDescriptionExportSchema.schema)
    extra_required = ['award_number',
                      'principal_investigator',
                      'species',
                      'organ',
                      'modality',
                      'techniques',
                      'contributor_count',
                      'uri_human',  # from DatasetMetadata
                      'uri_api',  # from DatasetMetadata
                      'sparse',  # tells you whether files/dirs/size is reliable
                      'files',
                      'dirs',
                      'size',
                      'folder_name',  # from DatasetMetadat
                      'title',
                      'template_schema_version',
                      'number_of_subjects',
                      'number_of_samples',
                      'timestamp_created',
                      'timestamp_updated',
                      'timestamp_updated_contents',
                      #'subject_count',
                      #'sample_count',
    ]
    __schema['required'].remove('contributors')
    __schema['required'].remove('name')
    __schema['required'] += extra_required
    __schema['properties'].pop('contributors')
    __schema['properties'].pop('name')
    __schema['properties'].update({
        'errors': ErrorSchema.schema,
        'dirs': {'type': 'integer'},
        'doi': EIS._allOf(DoiSchema, context_value='TEMP:hasDoi'),
        'files': {'type': 'integer',
                  'context_value': inttype('TEMP:numberOfFiles'),
                  },
        'size': {'type': 'integer',
                 'context_value': inttype('TEMP:hasSizeInBytes'),
                 },
        'sparse': {'type': 'boolean',
                   'context_value': booltype('TEMP:sparseClone'),
                   },
        'folder_name': {'context_value': 'rdfs:label',
                        **string_noltws},
        'title': {'context_value': 'dc:title',
                  **string_noltws},
        'timestamp_created': {'type': 'string',
                              'pattern': iso8601pattern,
                              'context_value': 'TEMP:wasCreatedAtTime',
                              },
        'timestamp_updated': {'type': 'string',
                              'pattern': iso8601pattern,
                              'context_value': 'TEMP:wasUpdatedAtTime',
                              },
        'timestamp_updated_contents': {'type': 'string',
                                       'pattern': iso8601pattern,
                                       'context_value': 'TEMP:contentsWereUpdatedAtTime',
                                       },
        'uri_human': {'allOf': [  # FIXME this is absolutely not the right way to solve this problem
            {'type': 'string',
             'context_value': idtype('TEMP:hasUriHuman'),},
            {'oneOf': [
                # FIXME proper regex
                {'pattern': r'^https://app\.pennsieve\.io/N:organization:',},
                {'pattern': simple_file_url_pattern},]}]},
        'uri_api': {'allOf': [  # FIXME this is absolutely not the right way to solve this problem
            {'type': 'string',
             #'context_value': idtype('TEMP:hasUriApi'),
             'context_value': idtype('@id'),},
            {'oneOf': [
                # FIXME proper regex
                {'pattern': r'^https://api\.pennsieve\.io/(datasets|packages)/',},
                {'pattern': simple_file_url_pattern},]}]},
        'award_number': {'type': 'string',
                         'pattern': award_pattern,
                         'context_value': idtype('TEMP:hasAwardNumber', base=TEMP['awards/']),
                         },
        'principal_investigator': {'type': 'string',
                                   'context_value': idtype('TEMP:hasResponsiblePrincipalInvestigator'),
                                   },
        'protocol_url_or_doi': {'type': 'array',
                                'minItems': 1,
                                'items': _protocol_url_or_doi_schema,
                                'context_value': idtype('TEMP:hasProtocol'),
                                },
        'additional_links': {'type': 'array',
                             'minItems': 1,
                             'items': {
                                 'type': 'object',
                                 'properties': {
                                     'link': {'type': 'string'},
                                     'link_description': {'type': 'string'},
                                     },},
                             'context_value': idtype('TEMP:hasAdditionalLinks'),  # TODO
                             },
        'species': {'anyOf': [EIS._allOf(OntTermSchema),
                              {'type': 'string',},],
                    'context_value': idtype('isAbout'),  # FIXME vs has part generated from participant
                    },
        'template_schema_version': {'type': 'string'},
        'organ': {'type': 'array',
                  'minItems': 1,
                  'items': EIS._allOf(OntTermSchema),
                  'context_value': idtype('isAbout'),
                  },
        'modality': {'type': 'array',
                     'minItems': 1,
                     'items': {
                         'type': 'string',
                     },
                    'context_value': idtype('TEMP:hasExperimentalModality'),
        },
        'techniques': {'type': 'array',
                       'minItems': 1,
                       'items': EIS._allOf(OntTermSchema),
                       # FIXME usedProtocolThatEmployedTechniques ...
                       'context_value': idtype('TEMP:protocolEmploysTechnique'),
        },

        # TODO $ref these w/ pointer?
        # in contributor etc?

        'performance_count': {'type': 'integer',
                            'context_value': inttype('TEMP:hasNumberOfPerformances'),
                              },
        'subject_count': {'type': 'integer',
                          'context_value': inttype('TEMP:hasNumberOfSubjects'),
                          },
        'sample_count': {'type': 'integer',
                         'context_value': inttype('TEMP:hasNumberOfSamples'),
                         },
        'contributor_count': {'type': 'integer',
                              'context_value': inttype('TEMP:hasNumberOfSamples'),
                              },})

    schema = {
        'jsonld_include': {'@type': ['sparc:Dataset', 'owl:NamedIndividual']},
        'allOf': [__schema,
                  {'anyOf': [
                      #{'required': ['number_of_subjects']},
                      #{'required': ['number_of_samples']},
                            {'required': ['subject_count']},  # FIXME extract subjects from samples ?
                      {'required': ['sample_count']}
                  ]}]}


class MetaOutSchema(JSONSchema):
    context = lambda : ({}, None)
    __schema = copy.deepcopy(MetaOutExportSchema.schema)
    schema = JApplyRecursive(EIS._to_pattern, __schema)


class DatasetOutExportSchema(JSONSchema):
    """ Schema that adds the id field since investigators shouldn't need to know
        the id to check the integrity of their data. We need it because that is
        a key piece of information that we use to link everything together. """

    __schema = copy.deepcopy(DatasetStructureSchema.schema['allOf'][0])
    __schema['required'] = [
        'id',
        'meta',
        'contributors',
        'path_metadata',  # required for the time being as a sanity check on manifest structure, will move elsewhere in the future
        'prov']
    __schema['properties'] = {
        'id': {'type': 'string',  # ye old multiple meta/bf id issue
               'pattern': '^N:dataset:'},
        'meta': MetaOutExportSchema.schema,  # XXXXXXXXXXX wrong types ??
        #'readme': {'type': 'string',},  # TODO ...
        'rmeta': {'type': 'object',
                  # FIXME TODO not entirely sure how to deal with embedding remote metadata about
                  # datasets some of which also comes from this pipeline
                  # heh, recursive metadata ...
                  'properties': {'readme': {
                      'type': 'string',
                      'context_value': {'@id': 'TEMP:readmeText', '@type': 'ilxtr:Markdown'},
                  },},},
        'path_metadata': {'type': 'array'},
        'prov': ProvSchema.schema,
        'errors': ErrorSchema.schema,
        'contributors': ContributorsOutExportSchema.schema,
        'creators': CreatorsExportSchema.schema,
        'performances': PerformancesExportSchema.schema['properties']['performances'],  # 2.0.0
        # FIXME subjects_file might make the name overlap clearer
        'subjects': SubjectsExportSchema.schema['properties']['subjects'],  # FIXME SubjectsOutSchema
        'samples': SamplesFileExportSchema.schema['properties']['samples'],
        # 'identifier_metadata': {'type': 'array'},  # FIXME temporary
        'resources': {'type': 'array',
                      'items': {'type': 'object'},},
        'submission': {'type': 'object',},
        'specimen_dirs': {'type': 'object',  # FIXME unnest this
                          'required': ['records',],
                          'properties':
                          {'records': {'type': 'array',  # FIXME records vs contents we use both in manifests
                                       'items': {
                                           'type': 'object',
                                           'required': ['specimen_id', 'dirs'],
                                           'properties': {
                                               'type': {'type': 'string',  # FIXME seems wrong
                                                        'enum': ('SampleDirs', 'SubjectDirs'),},
                                               'specimen_id': {'type': 'string'},  # foreign key to subject or sample id
                                               'dirs': {'type': 'array',
                                                        'minItems': 1,},},},},
                           'errors': ErrorSchema.schema,},},
        'inputs': {'type': 'object',
                   # TODO do we need errors at this level?
                   'properties': {'dataset_description_file': DatasetDescriptionSchema.schema,
                                  'submission_file': SubmissionSchema.schema,
                                  'subjects_file': SubjectsSchema.schema,
                                  'samples_file':SamplesFileSchema.schema,
                                  },},}

    # FIXME switch to make samples optional since subject_id will always be there even in samples?
    schema = {
        'jsonld_include': {'@type': ['owl:Ontology']},
        '': [{'context_runtime': [  # induces indentation bug in Eamcs I think
                '#/meta/uri_api',
                (lambda uri_api: {'@id': uri_api + '/', '@prefix': True}),
                '#/@context/_pnc',]},
            {'context_runtime': [
                '#/meta/uri_api',
                (lambda uri_api: uri_api + '/'),
                '#/@context/@base',]}],
        'allOf': [__schema,
                  {'anyOf': [
                      {'required': ['subjects']},  # FIXME extract subjects from samples ?
                      {'required': ['samples']},
                      {'properties': {
                          'meta': {'properties': {
                              'number_of_subjects': {'type': 'integer',
                                                     'minimum': 0,
                                                     'maximum': 0,
                                                     },
                              'number_of_samples': {'type': 'integer',
                                                    'minimum': 0,
                                                    'maximum': 0,
                                                    },
                          }}}},
                  ]}]}


class DatasetOutSchema(JSONSchema):
    context = lambda : ({}, None)
    __schema = copy.deepcopy(DatasetOutExportSchema.schema)
    schema = JApplyRecursive(EIS._to_pattern, __schema)


class StatusSchema(JSONSchema):
    schema = {'type': 'object',
              'required': ['submission_index', 'curation_index', 'error_index',
                           'submission_errors', 'curation_errors'],
              'properties': {
                  'status_on_platform': {'type': 'object',  # NOTE becomes a json literal iirc
                                         'context_value': 'TEMP:remotePlatformStatus',
                                         },
                  'submission_index': {'type': 'integer', 'minimum': 0,
                                       'context_value': inttype('TEMP:submissionIndex'),
                                       },
                  'curation_index': {'type': 'integer', 'minimum': 0,
                                     'context_value': inttype('TEMP:curationIndex'),
                                     },
                  'error_index': {'type': 'integer', 'minimum': 0,
                                  'context_value': inttype('TEMP:errorIndex'),
                                  },
                  'submission_errors':{'type':'array',  # allow these to be empty
                                       'items': {'type': 'object'},
                                       },
                  'curation_errors':{'type':'array',  # allow these to be empty
                                     'items': {'type': 'object'},
                                     },
              }}


class PostSchema(JSONSchema):
    """ This is used to validate only the status schema part that is added to the DatasetOutSchema """
    schema = {
        'type': 'object',
        'required': [
            #'path_metadata',  # issue resolved for now determined to be non-fatal
            'status',
        ],
        'properties': {
            'path_metadata': {'type': 'array'},
            'status': StatusSchema.schema,},
    }


class SummarySchema(JSONSchema):
    # TODO add expected counts
    schema = {'type': 'object',
              'required': ['id', 'meta', 'prov'],
              'properties': {'id': {'type': 'string',
                                    'oneOf': [
                                        {'pattern': '^.*/.*$'},  # a file path based id
                                        {'pattern': '^N:organization:'},
                                    ],},
                             'meta': {'type': 'object',
                                      # folder name cannot be required when combining from existing exports
                                      'required': ['count', 'uri_api', 'uri_human'],
                                      'properties': {'folder_name': {'type': 'string'},
                                                     # FIXME common source for these with MetaOutSchema
                                                     'uri_human': {'type': 'string',
                                                                   'oneOf': [
                                                                       {'pattern': simple_file_url_pattern},
                                                                       {'pattern': r'^https://app\.pennsieve\.io/N:organization:'},
                                                                   ],},
                                                     'uri_api': {'type': 'string',
                                                                 'oneOf': [
                                                                     {'pattern': simple_file_url_pattern},
                                                                     {'pattern': r'^https://api\.pennsieve\.io/organizations/N:organization:'},
                                                                 ],},
                                                     'count': {'type': 'integer'},},},
                             'prov': {'type': 'object'},
                             'datasets': {'type': 'array',
                                          'minItems': 1,
                                          'items': {'type': 'object'},  # DatasetOutSchema already validated
                             }}}


class HeaderSchema(JSONSchema):
    schema = {'type': 'array',
              'items': {'type': 'string'}}


# constraints on MIS classes
class MISSpecimenSchema(JSONSchema):
    schema = {
        'type': 'object',
        'properties': {
            'TEMP:hasAge': {},
            'TEMP:hasAgeCategory': {},  # hasDevelopmentalStage
            'TEMP:hasBiologicalSex': {},  # TEMP because sparc:hasPhenotypicSex
            'TEMP:hasWeight': {},
            'TEMP:hasGenus': {},
            'TEMP:hasSpecies': {},
            'TEMP:hasStrain': {},  # race/ethnicity?
            'TEMP:hasRRID': {},
            'TEMP:hasGeneticVariation': {},

            'sparc:hasBodyMassIndex': {} ,
            'sparc:hasClinicalInformation': {},
            'sparc:hasDiseaseInformation': {},
            'sparc:hasDrugInformation': {},
            'sparc:hasFamilyHistory': {},
            'sparc:hasGender': {},
            'TEMP:hasHandedness': {},
            'TEMP:hasRace': {},
            'TEMP:hasEthnicity': {},
        },
        'oneOf': [
            {'allOf': [
                {'allOf':
                 [{'required': [
                     'TEMP:hasSpecies',
                     'TEMP:hasBiologicalSex',
                     'TEMP:hasWeight',]},
                  {'oneOf': [{'required': ['TEMP:hasAge']},
                             {'required': ['TEMP:hasAgeCategory']}]}]},
                {'oneOf': [
                    {'properties': {'TEMP:hasSpecies': {'enum': ['NCBITaxon:9606']}},
                     'required': [
                         'sparc:hasBodyMassIndex',
                         'sparc:hasClinicalInformation',
                         'sparc:hasDiseaseInformation',
                         'sparc:hasDrugInformation',
                         'sparc:hasFamilyHistory',
                         #'sparc:hasGender',
                         #'TEMP:hasHandedness',
                         'TEMP:hasRace',
                         'TEMP:hasEthnicity',
                     ]},
                    {'allOf': [
                        {'not': {'properties': {'TEMP:hasSpecies': {'enum': ['NCBITaxon:9606']}}}},
                        {'required': [
                            #'TEMP:hasSupplier',
                            #'TEMP:hasStrain',  # RRID? race/ethnicity?
                            'TEMP:hasRRID',
                         ]},
                    ]},
                ]},
            ]},
        ],
    }


class MISDatasetSchema(JSONSchema):
    moves = [  # FIXME this goes somehwere else
        # from datasetoutschema -> mis
        [[''], ['']],
        [[''], ['']],
        [[''], ['']],
        [[''], ['']],
        [[''], ['']],
        [[''], ['']],
        [['resources'], ['']],
        [['meta', 'protocol_url_or_doi'], ['']],
        #[['meta'], ['sparc:Resource']],
        [['subjects'], ['sparc:Specimen']],
        [['contributors'], ['sparc:Researcher']],  # ah the role confusion
    ]
    schema = {'type': 'object',
              'required': ['curies', 'rdf:type'],
              'properties': {
                  'curies': {'type': 'object'},
                  'rdf:type': {'type': 'string',
                               'pattern': 'sparc:Resource'},
                  #'sparc:'
                  'sparc:Specimen': {'type': 'array',
                                     'items': MISSpecimenSchema.schema},
              }
              }


runtime_context_specs = get_runtime_context_specs()
