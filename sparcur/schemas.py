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
from sparcur.utils import logd
from sparcur.core import JEncode, JApplyRecursive, JPointer, OntCuries
from pyontutils.utils import asStr
from pyontutils.namespaces import TEMP, TEMPRAW, sparc, unit, PREFIXES as uPREFIXES, ilxtr


CONTEXT_ROOT = object()  # used to mark where nested context_value statements should be deposited
prefix_endswith = ['/', '#', '_', '-', ':', '=',
                   '/fma', '/sao', 'PTHR', '/chebi#2', '/chebi#3',]

base_context = {
    '@version': 1.1,
    'id': '@id',
    'dataset': {'@id': 'https://api.blackfynn.io/datasets/N:dataset:', '@prefix': True},
    'package': {'@id': 'https://api.blackfynn.io/packages/N:package:', '@prefix': True},
    # @base and _bfc are added at runtime and are dataset:{dataset-id-suffix}
    'meta': '@graph',
    #'meta': 'TEMP:hasSubGraph',  #'_bfc:#meta-graph',  # FIXME I think the fragment is the right thing to do here ...
    'subjects': 'TEMP:hasSubGraph',  #'_bfc:#subjects-graph',  # FIXME I think the fragment is the right thing to do here ...
    'samples': 'TEMP:hasSubGraph',  #'_bfc:#samples-graph',
    'contributors': '_bfc:#contributors-graph',
    #'N:dataset': {'@id': 'https://api.blackfynn.io/datasets/'},  # darn it multiprefix issues
    **{p: {'@id': n, '@prefix': True}
       if [c for c in prefix_endswith if n.endswith(c)]
       else n for p, n in {**uPREFIXES, **OntCuries._dict}.items()
       if p != ''},  # FIXME massive data duplication
    'unit': {'@id': 'TEMP:hasUnit',
             '@type': '@id',
             '@context': {'@base': str(unit)}},
    'value': {'@id': 'rdf:value'},  # FIXME maybe include this in a dedicated quantity @context?
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
            },
            'NoLTWhitespace': NoLTWhitespaceSchema.schema,
            'EmbeddedIdentifier': EmbeddedIdentifierSchema.schema,

        'meta': MetaOutExportSchema.schema,
            'prov': ProvSchema.schema,
            'errors': ErrorSchema.schema,
            'contributor': ContributorOutExportSchema.schema,
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


def idtype(id, base=None):
    out = {'@id': id, '@type': '@id'}
    if base:
        out['@context'] = {'@base': str(base)}

    return out


def strcont(id):
    return {'type': 'string', 'context_value': id}


def inttype(id):
    return {'@id': id, '@type': 'xsd:integer'}


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

        # pretty sure this functionality is no longer used
        if self.schema is not None:
            cls._output = cls.output
            @property
            def output(_self):
                return self.schema.validate(cls._output)

            cls.output = output

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
                        raise norm_or_error

                    try:
                        if 'errors' not in data:
                            data['errors'] = []
                    except BaseException as e:
                        raise exc.SparCurError(
                            f'Error from {_self.__class__.__name__}.'
                            f'{function.__name__}') from e
                        
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


class JSONSchema(object):

    schema = {}

    validator_class = jsonschema.Draft6Validator

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

    def __init__(self):
        format_checker = jsonschema.FormatChecker()
        #format_checker = ConvertingChecker()
        types = dict(array=(list, tuple),
                     string=tuple(self.string_types))
        self.validator = self.validator_class(self.schema,
                                              format_checker=format_checker,
                                              types=types)

    @classmethod
    def _add_meta(cls, blob):
        """ In place modification of a schema blob to add schema level metadata """
        # TODO schema metadata, I don't know enough to do this correctly yet
        #schema_version = cls.validator_class.ID_OF(cls.validator_class.META_SCHEMA)
        #schema['$schema'] = schema_version

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


metadata_filename_pattern = r'^.+\/[a-z_\/]+\.(xlsx|csv|tsv|json)$'

simple_url_pattern = r'^(https?):\/\/([^\s\/]+)\/([^\s]*)'

# NOTE don't use builtin date-time format due to , vs . issue
iso8601pattern = '^[0-9]{4}-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-6][0-9]:[0-6][0-9](,[0-9]{6})*(Z|[-\+[0-2][0-9]:[0-6][0-9]])'
iso8601datepattern = '^[0-9]{4}-[0-1][0-9]-[0-3][0-9]'

# https://www.crossref.org/blog/dois-and-matching-regular-expressions/
doi_pattern = idlib.Doi._id_class.canonical_regex

orcid_pattern = idlib.Orcid._id_class.canonical_regex

ror_pattern = idlib.Ror._id_class.canonical_regex

pattern_whitespace_lead_trail = '(^[\s]+[^\s].*|.*[^\s][\s]+$)'


class NoLTWhitespaceSchema(JSONSchema):
    schema = {'allOf': [{'type': 'string'},
                        {'not': {'type': 'string',
                                 'pattern': pattern_whitespace_lead_trail,
                        }},]}


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
    def _to_pattern(cls, obj, *args, path=None, **kwargs):  # FIXME add tests
        if (isinstance(obj, dict) and
            'allOf' in obj and
            (len(obj) == 1 or len(obj) == 2 and 'context_value' in obj) and  # FIXME extremely brittle
            len(obj['allOf']) == 2 and
            obj['allOf'][0] == cls.schema):
            return obj['allOf'][1]['properties']['id']

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
                             'sparcur_version': {'type': 'string'},
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
                    #'path_structure': {'type': 'array',
                    # 'items': {'type': 'string', 'pattern': ''}}
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


class ContributorExportSchema(JSONSchema):
    schema = {
        'type': 'object',
        'jsonld_include': {'@type': ['sparc:Person', 'owl:NamedIndividual']},
        'properties': {
            'id': {'type': 'string',  # FIXME TODO
                   },
            'affiliation': EIS._allOf(RorSchema,
                                      context_value=idtype('TEMP:hasAffiliation')),
            'contributor_name': {'type': 'string'},
            'first_name': {'type': 'string',
                           'context_value': 'sparc:firstName',},
            'middle_name': strcont('TEMP:middleName'),
            'last_name': {'type': 'string',
                          'context_value': 'sparc:lastName',},
            'contributor_orcid_id': EIS._allOf(OrcidSchema, context_value=idtype('sparc:hasORCIDId')),
            'blackfynn_user_id': strcont(idtype('TEMP:hasBlackfynnUserId')),
            'contributor_affiliation': {'anyOf': [EIS._allOf(RorSchema),
                                                  {'type': 'string'}],
                                        'context_value': idtype('TEMP:hasAffiliation')
                                        },
            'contributor_role': {
                'type':'array',
                'context_value': idtype('TEMP:hasRole', base=str(TEMP)),
                'minItems': 1,
                'items': {
                    'type': 'string',
                    'enum': ['ContactPerson',
                             'Creator',  # allowed here, moved later
                             'DataCollector',
                             'DataCurator',
                             'DataManager',
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
            'is_contact_person': {'type': 'boolean'},
            'errors': ErrorSchema.schema,
        }}


class ContributorSchema(JSONSchema):
    context = lambda : ({}, None)
    __schema = copy.deepcopy(ContributorExportSchema.schema)
    schema = JApplyRecursive(EIS._to_pattern, __schema)


class ContributorsExportSchema(JSONSchema):
    schema = {'type': 'array',
              #'context_runtime': lambda base: {'@id': f'{base}contributors',},
              'contains': {
                  'type': 'object',
                  'required': ['is_contact_person'],
                  'properties': {
                      'is_contact_person': {'type': 'boolean', 'enum': [True]},
                  },
              },
              'items': ContributorExportSchema.schema,
            }

class ContributorsSchema(JSONSchema):
    context = lambda : ({}, None)
    __schema = copy.deepcopy(ContributorsExportSchema.schema)
    schema = JApplyRecursive(EIS._to_pattern, __schema)


class ContributorOutExportSchema(JSONSchema):
    schema = copy.deepcopy(ContributorExportSchema.schema)
    schema['properties']['contributor_role']['items']['enum'].remove('Creator')


class ContributorsOutExportSchema(JSONSchema):
    schema = copy.deepcopy(ContributorsExportSchema.schema)
    schema['items'] = ContributorOutExportSchema.schema


class CreatorExportSchema(JSONSchema):
    schema = copy.deepcopy(ContributorExportSchema.schema)
    schema.pop('jsonld_include')
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


class DatasetDescriptionExportSchema(JSONSchema):
    schema = {
        'type': 'object',
        'additionalProperties': False,
        'required': ['schema_version',  # missing should fail for this one ...
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
            'errors': ErrorSchema.schema,
            'schema_version': {'type': 'string'},
            'name': string_noltws,
            'description': {'type': 'string'},
            'keywords': {'type': 'array', 'items': {'type': 'string'}},
            'acknowledgements': {'type': 'string'},
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


class DatasetDescriptionSchema(JSONSchema):
    context = lambda : ({}, None)
    __schema = copy.deepcopy(DatasetDescriptionExportSchema.schema)
    schema = JApplyRecursive(EIS._to_pattern, __schema)


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
    'species': strcont('sparc:animalSubjectIsOfSpecies'),
    'strain': strcont('sparc:animalSubjectIsOfStrain'),
    'rrid_for_strain': EIS._allOf(RridSchema, context_value=idtype('TEMP:specimenRRID')),  # XXX CHANGE from sparc:specimenHasIdentifier
    'genus': strcont('sparc:animalSubjectIsOfGenus'),
    'sex': {'type': 'string',
            'context_value': 'TEMP:hasBiologicalSex',  # FIXME idtype
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
    'experiment_date': strcont('TEMP:protocolExecutionDate'),  # FIXME needs to reference a protocol
    'injection_date': strcont('TEMP:protocolExecutionDate'),  # FIXME needs to reference a protocol
    'date_of_euthanasia': strcont('TEMP:protocolExecutionDate'),
    'date_of_injection': strcont('TEMP:protocolExecutionDate'),

    # fields that are actually about files
    'sha1': strcont('TEMP:hasDigitalArtifactThatIsAboutItWithHash'),
    'filename': strcont('TEMP:hasDigitalArtifactThatIsAboutIt'),
    'upload_filename': strcont('TEMP:hasDigitalArtifactThatIsAboutIt'),
    'dataset_id': strcont('TEMP:providerDatasetIdentifier'),  # FIXME need a global convention for this

    # fields that are actually about performances
    'experiment_number': strcont('TEMP:localPerformanceNumber'),  # FIXME TODO
    'session': strcont('TEMP:localPerformanceNumber'),

    # extra
    'comment': strcont('TEMP:providerNote'),
    'note': strcont('TEMP:providerNote'),
}


class SubjectExportSchema(JSONSchema):
    schema = {
        'type': 'object',
        'jsonld_include': {'@type': ['sparc:Subject', 'owl:NamedIndividual']},
        'required': ['subject_id', 'species'],
        'properties': {
            'subject_id': {'type': 'string',
                           #'context_runtime':
                           #lambda base: {'@id': '@id',
                                         #'@type': '@id',
                                         #'@context': {'@base': f'{base}subjects/'}}
                           },
            'ear_tag_number': strcont('TEMP:localIdAlt'),
            'treatment': strcont('TEMP:hadExperimentalTreatmentApplied'),
            'initial_weight': strcont('sparc:animalSubjectHasWeight'),
            'height_inches': strcont('TEMP:subjectHasHeight'),
            'gender': strcont('sparc:hasGender'),
            'body_mass_weight': strcont('TEMP:subjectHasWeight'),

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
        'required': ['sample_id', 'subject_id'],
        'properties': {
            'sample_id': {'type': 'string',},
            'subject_id': {'type': 'string',},
            'primary_key': {'type': 'string',
                            'context_runtime': [  # TODO -> derive after add ?
                                '#/meta/uri_api',
                                # FIXME the @id mapping seems to break the @context !?
                                (lambda uri_api: {
                                    '@id': '@id',
                                    '@context': {
                                        '@base': uri_api + '/samples/'}}),
                                '#/samples/@context/primary_key'],
                            #'context_runtime':
                            #lambda base: {'@id': '@id',
                                          #'@type': '@id',
                                          #'@context': {'@base': f'{base}samples/'}}
                            },
            'specimen': {'type': 'string',
                         },  # what are we expecting here?
            'specimen_anatomical_location': {'type': 'string',
                                             },

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
                 'description': {
                     'type': 'string',
                 },
                 'file_type': {
                     'type': 'string',
                 },
                 'data_type': {
                     'type': 'string',
                 },
             },
             },
            {'oneOf': [
                {'required': ['pattern']},
                {'required': ['filename', 'timestamp']},
            ]},
        ]
    }


class ManifestFileExportSchema(JSONSchema):  # FIXME TODO FileObjectSchema ??
    schema = {
        'type': 'object',
        'jsonld_include': {'@type': ['sparc:File', 'owl:NamedIndividual']},
        'required': ['dataset_relative_path', 'manifest_records'],  # TODO uri_api
        'properties': {
            'dataset_relative_path': {'type': 'string'},
            'manifest_records': {
                'type': 'array',
                'minItems': 1,
                'items': ManifestRecordExportSchema.schema,
            },
            'errors': ErrorSchema.schema,
        }
    }


ManifestFileSchema = ManifestFileExportSchema


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
                      'files',
                      'dirs',
                      'size',
                      'folder_name',  # from DatasetMetadat
                      'title',
                      'schema_version',
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
        'uri_human': {'type': 'string',
                      'pattern': r'^https://app\.blackfynn\.io/N:organization:',  # FIXME proper regex
                      'context_value': idtype('TEMP:hasUriHuman'),
        },
        'uri_api': {'type': 'string',
                    'pattern': r'^https://api\.blackfynn\.io/(datasets|packages)/',  # FIXME proper regex
                    #'context_value': idtype('TEMP:hasUriApi'),
                    'context_value': idtype('@id'),
        },
        'award_number': {'type': 'string',
                         'pattern': '^OT|^U18',
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
                             'items': {'type': 'string'},
                             'context_value': idtype('TEMP:hasAdditionalLinks'),  # TODO
                             },
        'species': {'type': 'string',
                    'context_value': idtype('isAbout'),
                    },
        'schema_version': {'type': 'string'},
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

        'subject_count': {'type': 'integer',
                          'context_value': inttype('TEMP:hasNumberOfSubjects'),
                          },
        'sample_count': {'type': 'integer',
                         'context_value': inttype('TEMP:hasNumberOfSamples'),
                         },
        'contributor_count': {'type': 'integer',
                              'context_value': inttype('TEMP:hasNumberOfSamples'),
                              },})

    schema = {'allOf': [__schema,
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
    __schema['required'] = ['id',
                            'meta',
                            'contributors',
                            'prov']
    __schema['properties'] = {
        'id': {'type': 'string',  # ye old multiple meta/bf id issue
               'pattern': '^N:dataset:'},
        'meta': MetaOutExportSchema.schema,
        'prov': ProvSchema.schema,
        'errors': ErrorSchema.schema,
        'contributors': ContributorsOutExportSchema.schema,
        'creators': CreatorsExportSchema.schema,
        # FIXME subjects_file might make the name overlap clearer
        'subjects': SubjectsExportSchema.schema['properties']['subjects'],  # FIXME SubjectsOutSchema
        'samples': SamplesFileExportSchema.schema['properties']['samples'],
        # 'identifier_metadata': {'type': 'array'},  # FIXME temporary
        'resources': {'type': 'array',
                      'items': {'type': 'object'},},
        'submission': {'type': 'object',},
        'inputs': {'type': 'object',
                   # TODO do we need errors at this level?
                   'properties': {'dataset_description_file': DatasetDescriptionSchema.schema,
                                  'submission_file': SubmissionSchema.schema,
                                  'subjects_file': SubjectsSchema.schema,
                                  'samples_file':SamplesFileSchema.schema,
                                  },},}

    # FIXME switch to make samples optional since subject_id will always be there even in samples?
    schema = {
        'jsonld_include': {'@type': ['sparc:Dataset', 'owl:NamedIndividual']},
        'allOf': [__schema,
                  {'anyOf': [
                      {'required': ['subjects']},  # FIXME extract subjects from samples ?
                      {'required': ['samples']}
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
                  'status_on_platform': {'type': 'string',
                                         'context_value': 'TEMP:blackfynnPlatformStatus',
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
        'required': ['status'],
        'properties': {'status': StatusSchema.schema,}
    }


class SummarySchema(JSONSchema):
    # TODO add expected counts
    schema = {'type': 'object',
              'required': ['id', 'meta', 'prov'],
              'properties': {'id': {'type': 'string',
                                    'pattern': '^N:organization:'},
                             'meta': {'type': 'object',
                                      'required': ['folder_name', 'count', 'uri_api', 'uri_human'],
                                      'properties': {'folder_name': {'type': 'string'},
                                                     # FIXME common source for these with MetaOutSchema
                                                     'uri_human': {'type': 'string',
                                                                   'pattern': r'^https://app\.blackfynn\.io/N:organization:',
        },
                                                     'uri_api': {'type': 'string',
                                                                 'pattern': r'^https://api\.blackfynn\.io/organizations/N:organization:',
                                                     },
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
