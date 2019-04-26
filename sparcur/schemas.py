import copy
import jsonschema


class ValidationError(Exception):
    def __init__(self, errors):
        self.errors = errors

    def __repr__(self):
        msg = ', '.join([_format_jsonschema_error(e) for e in self.errors])
        return self.__class__.__name__ + f'({msg})'

    def __str__(self):
        return repr(self)

    def json(self):
        """ update this to change how errors appear in the validation pipeline """
        skip = 'schema', 'instance'
        return [{k:v if k not in skip else k + ' REMOVED'
                 for k, v in e._contents().items()
                 # TODO see if it makes sense to drop these because the parser did know ...
                 if v and k not in skip}
                for e in self.errors]


class JSONSchema(object):

    schema = {}

    def __init__(self):
        format_checker = jsonschema.FormatChecker()
        types = dict(array=(list, tuple))
        self.validator = jsonschema.Draft6Validator(self.schema,
                                                    format_checker=format_checker,
                                                    types=types)

    def validate_strict(self, data):
        # Take a copy to ensure we don't modify what we were passed.
        appstruct = copy.deepcopy(data)

        errors = list(self.validator.iter_errors(appstruct))
        if errors:
            raise ValidationError(errors)

        return appstruct

    def validate(self, data):
        """ capture errors """
        try:
            ok = self.validate_strict(data)  # validate {} to get better error messages
            return True, ok, data  # FIXME better format

        except ValidationError as e:
            return False, e, data  # FIXME better format

    @property
    def total_possible_errors(self):
        # TODO the structure for actual_errors is where we have to recurse
        # this could be applied recursivley now, though using it requires unpacking the output
        total_possible_errors = 0
        if self.schema['type'] == 'object':
            if 'required' in self.schema:
                required = self.schema['required']
            else:
                required = []

            total_possible_errors += len(required)

            # this makes each error matter less the more values you have
            # 
            #for prop, value in self.schema['properties'].items():
                #if prop not in required and len(value) > 1 and prop in instance:

            if 'additionalProperties' in self.schema and not self.schema['additionalProperties']:
                total_possible_errors += 1

        elif self.schema['type'] == 'array' and 'contains' in schema:
            total_possible_errors += 1

        return total_possible_errors
    
        # optional fields means that the total
        # possible errors cannot be known until runtime
        # but what that means is that we assume that optional fields
        # should always be correct and thus that only the static
        # information in the schema will be used to generate the score
        # FIXME the above doesn't actually work because missing optional
        # fields do need to be added at runtime, we'll do that next time

def _format_jsonschema_error(error):
    """Format a :py:class:`jsonschema.ValidationError` as a string."""
    if error.path:
        dotted_path = ".".join([str(c) for c in error.path])
        return "{path}: {message}".format(path=dotted_path, message=error.message)
    return error.message


class ErrorSchema(JSONSchema):
    schema = {'type':'array',
              'minItems': 1,
              'items': {'type': 'object'},}


class DatasetSchema(JSONSchema):
    schema = {'type': 'object',
              'required': ['submission', 'dataset_description', 'subjects'],
              'properties': {'submission': {'type': 'string'},
                             'dataset_description': {'type': 'string'},
                             'subjects': {'type': 'string'},}}


class ContributorSchema(JSONSchema):
    schema = {'type': 'object',
              'properties': {
                  'name': {'type': 'string'},
                  'first_name': {'type': 'string'},
                  'last_name': {'type': 'string'},
                  'contributor_orcid_id': {'type': 'string',
                                           'pattern': ('^https://orcid.org/0000-000(1-[5-9]|2-[0-9]|3-'
                                                       '[0-4])[0-9][0-9][0-9]-[0-9][0-9][0-9]([0-9]|X)$')},
                  'contributor_affiliation': {'type': 'string'},
                  'contributor_role': {
                      'type':'array',
                      'minItems': 1,
                      'items': {
                          'type': 'string',
                          'enum': ['ContactPerson',
                                   'DataCollector',
                                   'DataCurator',
                                   'DataManager',
                                   'Distributor',
                                   'Editor',
                                   'HostingInstitution',
                                   'PrincipalInvestigator',  # added for sparc map to ProjectLeader probably?
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
              }}


class ContributorsSchema(JSONSchema):
    schema = {'type': 'array',
              'contains': {
                  'type': 'object',
                  'required': ['is_contact_person'],
                  'properties': {
                      'is_contact_person': {'type': 'boolean', 'enum': [True]},
                  },
              },
              'items': ContributorSchema.schema
            }


class DatasetDescriptionSchema(JSONSchema):
    schema = {
        'type': 'object',
        'additionalProperties': False,
        'required': ['name', 'description', 'funding', 'protocol_url_or_doi', 'contributors', 'completeness_of_data_set'],
        # TODO dependency to have title for complete if completeness is is not complete?
        'properties': {
            'errors': ErrorSchema.schema,
            'name': {'type': 'string'},
            'description': {'type': 'string'},
            'keywords': {'type': 'array', 'items': {'type': 'string'}},
            'acknowledgements': {'type': 'string'},
            'funding': {'type': 'string'},
            'completeness_of_data_set': {'type': 'string'},
            'prior_batch_number': {'type': 'string'},
            'title_for_complete_data_set': {'type': 'string'},
            'originating_article_doi': {'type': 'array', 'items': {'type': 'string'}},  # TODO doi matching? maybe types?
            'protocol_url_or_doi': {'type': 'array',
                                    'minItems': 1,
                                    'items': {'type': 'string'}},
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
            'contributors': ContributorsSchema.schema
        }
    }


class SubmissionSchema(JSONSchema):
    schema = {
        'type': 'object',
        'additionalProperties': False,
        'properties': { 'submission': {
            'type': 'object',
            'required': ['sparc_award_number', 'milestone_achieved', 'milestone_completion_date'],
            # TODO allOf?
            'properties': {
                'sparc_award_number': {'type': 'string'},
                'milestone_achieved': {'type': 'string'},
                'milestone_completion_date': {'type': 'string'},}}}}


class SubjectsSchema(JSONSchema):
    schema = {
        'type': 'object',
        'required': ['subjects'],
        'properties': {'subjects': {'type': 'array',
                                    'minItems': 1,
                                    'items': {
                                        'type': 'object',
                                        'required': ['subject_id', 'species'],
                                        'properties': {
                                            'subject_id': {'type': 'string'},
                                            'species': {'type': 'string'},
                                            'strain': {'type': 'string'},  # TODO RRID
                                            'sex': {'type': 'string'},  # sex as a variable ?
                                            'mass': {'type': 'string'},  # TODO protc -> json
                                            'weight': {'type': 'string'},  # TODO protc -> json
                                            'age': {'type': 'string'},  # TODO protc -> json
                                            'age_category': {'type': 'string'},  # TODO uberon
                                        },},},
                       'software': {'type': 'array',
                                    'minItems': 1,
                                    'items': {
                                        'type': 'object',
                                        'properties': {
                                            'software_version': {'type': 'string'},
                                            'software_vendor': {'type': 'string'},
                                            'software_url': {'type': 'string'},
                                            'software_rri': {'type': 'string'},
                                        },},}}}


class MetaOutSchema(JSONSchema):
    schema = copy.deepcopy(DatasetDescriptionSchema.schema)
    extra_required = ['award_number',
                      'principal_investigator',
                      'species',
                      'organ',
                      'modality',
                      'contributor_count',
                      'subject_count',
                      'human_uri'
                      #'sample_count',
    ]
    schema['required'].remove('contributors')
    schema['required'] += extra_required
    schema['properties'].pop('contributors')
    schema['properties'].update({
        'errors': ErrorSchema.schema,
        'human_uri': {'type': 'string',
                      'pattern': '^https://',  # FIXME proper regex
        },
        'award_number': {'type': 'string',
                         'pattern': '^OT|^U18',},
        'principal_investigator': {'type': 'string'},
        'protocol_url_or_doi': {'type': 'array',
                                'minItems': 1,
                                'items': {'type': 'string'}},
        'additional_links': {'type': 'array',
                             'minItems': 1,
                             'items': {'type': 'string'}},
        'species': {'type': 'string'},
        'organ': {'type': 'string'},
        'modality': {'type': 'string'},  # FIXME array?

        # TODO $ref these w/ pointer?
        # in contributor etc?

        'subject_count': {'type': 'integer'},
        'sample_count': {'type': 'integer'},
        'contributor_count': {'type': 'integer'},})

    mis_mapping = []
    _schema = {'type': 'object',
              'required': [
                  'award_number',
                  'principal_investigator'
                  'species',
                  'organ',
                  'contributor_count',
                  'subject_count',
                  'sample_count',
              ],
              'properties': {
                  'award_number': {'type': 'string',
                                   'pattern': '^OT|^U18',},
                  'name': {'type': 'string'},
                  'description': {'type': 'string'},
                  'keywords': {'type': 'array',
                               'minItems': 1,
                               'items': {'type': 'string'}},
                  'acknowledgements': {'type': 'string'},
                  'originating_articles': {'type': 'array',
                                           'items': {'type': 'string'}},
                  'principal_investigator': {'type': 'string'},
                  'species': {'type': 'string'},
                  'organ': {'type': 'string'},

                  # TODO $ref these w/ pointer?
                  # in contributor etc?

                  'subject_count': {'type': 'int'},
                  'sample_count': {'type': 'int'},
                  'contributor_count': {'type': 'int'},},}


class DatasetOutSchema(JSONSchema):
    """ Schema that adds the id field since investigators shouldn't need to know
        the id to check the integrity of their data. We need it because that is
        a key piece of information that we use to link everything together. """

    schema = copy.deepcopy(DatasetSchema.schema)
    schema['required'] = ['id', 'meta', 'contributors', 'subjects']
    schema['properties'] = {'id': {'type': 'string',  # ye old multiple meta/bf id issue
                                   'pattern': '^N:dataset:'},
                            'meta': MetaOutSchema.schema,
                            'errors': ErrorSchema.schema,
                            'error_index': {'type': 'integer',
                                            'minimum': 0},
                            'submission_completeness_index': {'type': 'number',
                                                              'minimum': 0,
                                                              'maximum': 1,},
                            'contributors': ContributorsSchema.schema,
                            # FIXME subjects_file might make the name overlap clearer
                            'subjects': SubjectsSchema.schema['properties']['subjects'],  # FIXME SubjectsOutSchema
                            'resources': {'type':'array',
                                          'items': {'type': 'object'},},
                            'protocols': {'type':'array'},
                            'inputs': {'type': 'object',
                                       # TODO do we need errors at this level?
                                       'properties': {'dataset_description': DatasetDescriptionSchema.schema,
                                                      'submission': SubmissionSchema.schema,
                                                      'subjects': SubjectsSchema.schema,},},}


class SummarySchema(JSONSchema):
    # TODO add expected counts
    schema = {'type': 'object',
              'required': ['id', 'meta'],
              'properties': {'id': {'type': 'string',
                                    'pattern': '^N:organization:'},
                             'meta': {'type': 'object',
                                      'properties': {'count': {'type': 'integer'},},},
                             'datasets': {'type': 'array',
                                          'minItems': 1,
                                          'items': DatasetOutSchema.schema,},
                             'errors': ErrorSchema.schema,}}


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


