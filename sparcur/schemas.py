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
            'protocol_url_or_doi': {'type': 'array', 'items': {'type': 'string'}},
            'links': {
                'type': 'array',
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
                'items': {
                    'type': 'object',
                    'properties': {
                        'example_image_filename': {'type': 'string'},
                        'example_image_locator': {'type': 'string'},
                        'example_image_description': {'type': 'string'},
                    }
                }
            },
            'contributors': {
                'type': 'array',
                'contains': {
                    'type': 'object',
                    'required': ['is_contact_person'],
                    'properties': {
                        'is_contact_person': {'type': 'boolean', 'enum': [True]},
                    },
                },
                'items': {
                    'type': 'object',
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
                    }
                }
            }
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
        'properties': {'subjects': {
            'type': 'array',
            'items': {
                'type': 'object',
                'required': ['subject_id', 'species'],
                'properties': {
                    'subject_id': {'type': 'string'},
                    'species': {'type': 'string'},
                    'sex': {'type': 'string'},  # sex as a variable ?
            }}}}}


class CurationOutput(JSONSchema):
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


