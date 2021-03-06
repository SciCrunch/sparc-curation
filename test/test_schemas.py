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


class OrcidSchema(sc.JSONSchema):
    orcid_pattern = sc.orcid_pattern
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


class TestNoLTWhitespaceRegex(unittest.TestCase):
    schema = sc.NoLTWhitespaceSchema

    def test_positive(self):
        strings = (
            'asdf',
            'asdf asdf',
            'asdfaAdf asZf asd | " f asdf as df  131 23 45 ..as f91891l`1823409`-5',
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
            ' asdfaAdf asZf asd | " f asdf as df  131 23 45 ..as f91891l`1823409`-5 ',
        )

        schema = self.schema()
        for s in strings:
            ok, data_or_error, _  = schema.validate(s)
            assert not ok and s != data_or_error
