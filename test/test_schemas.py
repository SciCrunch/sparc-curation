import unittest

from sparcur import schemas as sc
from pyld import jsonld


class TestContext(unittest.TestCase):

    def _doit(self, j):
        proc = jsonld.JsonLdProcessor()
        context = j['@context']
        bads = []
        try:
            ctx = proc.process_context(proc._get_initial_context({}),
                                       context, {})
        except jsonld.JsonLdError as e:
            for k, v in context.items():
                c = {k: v, '@version': context['@version']}
                try:
                    ctx = proc.process_context(proc._get_initial_context({}),
                                               c, {})
                except jsonld.JsonLdError as e:
                    bads.append((k, v))

        assert not bads, bads

    def test_base(self):
        j = {'@context': sc.base_context,
             '@graph': []}
        self._doit(j)

    def test_protcur(self):
        j = {'@context': sc.protcur_context,
             '@graph': []}
        self._doit(j)


def make_pattern_schema(key, pattern):
    return {'type': 'object',
            'required': [key],
            'properties': {
                key: {
                    'type': 'string',
                    'pattern': pattern}}}


class OrcidSchema(sc.JSONSchema):
    schema = make_pattern_schema('orcid', sc.orcid_pattern)


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


class TestNoLTWhitespaceRegex(unittest.TestCase):
    schema = sc.NoLTWhitespaceSchema

    def test_positive(self):
        strings = (
            'asdf',
            'asdf asdf',
            'asdfaAdf asZf asd | " f asdf as df 131 23 45 ..as f91891l`1823409`-5',
        )
        schema = self.schema()
        for s in strings:
            ok, data_or_error, _  = schema.validate(s)
            assert s == data_or_error

    def test_negative(self):
        strings = (
            ' asdf',
            'asdf ',
            ' asdf ',
            ' asdf asdf',
            'asdf asdf ',
            ' asdf asdf ',
            'asdfaAdf asZf asd | " f asdf as df  131 23 45 ..as f91891l`1823409`-5',
            ' asdfaAdf asZf asd | " f asdf as df 131 23 45 ..as f91891l`1823409`-5 ',
        )

        schema = self.schema()
        for s in strings:
            ok, data_or_error, _  = schema.validate(s)
            assert not ok and s != data_or_error


class CNPSchema(sc.JSONSchema):
    schema = make_pattern_schema('cname', sc.contributor_name_pattern)


class TestContributorNamePatternRegex(unittest.TestCase):
    schema = CNPSchema

    def test_positive(self):
        strings = (
            'Last, First Middle',
            'Di Last, First Middle',
            'Von Last, First Middle',
            'van Last, First Middle',
            'Last-Last, First-First',
        )
        schema = self.schema()
        for s in strings:
            j = {'cname': s}
            ok, data_or_error, _  = schema.validate(j)
            assert j == data_or_error, s

    def test_negative(self):
        strings = (
            'Space,Missing',
            'Commas, Too, Many',
        )

        schema = self.schema()
        for s in strings:
            j = {'cname': s}
            ok, data_or_error, _  = schema.validate(j)
            assert not ok and j != data_or_error, s


class Iso8601Schema(sc.JSONSchema):
    schema = make_pattern_schema('iso8601', sc.iso8601bothpattern)


class TestIso8601(unittest.TestCase):
    def test_positive(self):
        strings = (
            '1000-01-01',
            '1000-01-01T00:00:00,000000001Z',
            '1000-01-01T00:00:00,000000001-00:00',
            '1000-01-01T00:00:00,000000001+00:00',
        )
        schema = Iso8601Schema()
        for s in strings:
            j = {'iso8601': s}
            ok, data_or_error, _  = schema.validate(j)
            assert j == data_or_error, s


    def test_negative(self):
        schema = Iso8601Schema()
        strings = (
            '01/01/01',
            '1000-01-01T00:00:00,000000001',
            '1000-01-01T00:00:00,000000001Z-00:00',
            '1000-01-01T00:00:00,000000001Z+00:00',
        )
        for s in strings:
            j = {'iso8601': s}
            ok, data_or_error, _  = schema.validate(j)
            assert not ok and j != data_or_error, s
