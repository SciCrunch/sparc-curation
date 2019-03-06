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


class JSONSchema(object):

    schema = {}

    def __init__(self):
        format_checker = jsonschema.FormatChecker()
        types = dict(array=(list, tuple))
        self.validator = jsonschema.Draft6Validator(self.schema,
                                                    format_checker=format_checker,
                                                    types=types)

    def validate(self, data):
        # Take a copy to ensure we don't modify what we were passed.
        appstruct = copy.deepcopy(data)

        errors = list(self.validator.iter_errors(appstruct))
        if errors:
            raise ValidationError(errors)

        return appstruct


def _format_jsonschema_error(error):
    """Format a :py:class:`jsonschema.ValidationError` as a string."""
    if error.path:
        dotted_path = ".".join([str(c) for c in error.path])
        return "{path}: {message}".format(path=dotted_path, message=error.message)
    return error.message


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
                  'contributor_orcid_id': {'type': 'string'},
                  'contributor_affiliation': {'type': 'string'},
                  'contributor_role': {
                      'type':'array',
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
                      #'sample_count',
    ]
    schema['required'].remove('contributors')
    schema['required'] += extra_required
    schema['properties'].update({
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
                            'errors': {'type':'array',
                                       'minItems': 1,
                                       'items': {'type': 'object'},},
                            'contributors': ContributorsSchema.schema,
                            'subjects': SubjectsSchema.schema,  # FIXME SubjectsOutSchema
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
                                          'items': DatasetOutSchema.schema,}}}


class CurationOut(JSONSchema):
    """ dead """
    schema = {
        'type': 'object',
        'properties': {
        'structure': {
            'submission': {
                'award': {},
            },
            'dataset_description': {
                'award': {},
                'protocol_uri': {
                    'protocl': {},
                },
                'name': {},
                'description': {},
                'name': {},
                'name': {},
                'name': {},
            },
            'subjects': {
                'species': {},
                'folder': {},
            },
        },
        'protocol': {
            'annotations': {},
        },
        },
        }


