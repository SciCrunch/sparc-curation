import unittest

from sparcur import schemas as sc


class OrcidSchema(sc.JSONSchema):
    orcid_pattern = sc.ContributorSchema.schema['properties']['contributor_orcid_id']['pattern']
    schema = {'type': 'object',
              'required': ['orcid'],
              'properties': {
                  'orcid': {
                      'type': 'string',
                      'pattern': orcid_pattern}}}


class TestOrcidRegex(unittest.TestCase):
    def test_positive(self):
        orcids = ('https://orcid.org/0000-0002-1825-0097',
                  'https://orcid.org/0000-0001-5109-3700',
                  'https://orcid.org/0000-0002-1694-233X')
        os = OrcidSchema()
        for o in orcids:
            j = {'orcid': o}
            ok, data_or_error, _  = os.validate(j)
            assert j == data_or_error

    def test_negative(self):
        orcids = ('https://orcid.org/0000-0a02-1825-0097',
                  'https://orcid.org/0000-0001-5109-370',
                  'https://orcid.org/0000-0002-1694-233Y')
        os = OrcidSchema()
        for o in orcids:
            j = {'orcid': o}
            ok, data_or_error, _  = os.validate(j)
            assert not ok and j != data_or_error
