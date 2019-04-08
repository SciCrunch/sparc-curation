import unittest
from sparcur.core import OrcidId


class TestOrcidId(unittest.TestCase):
    def test_validate(self):
        orcids = ('https://orcid.org/0000-0002-1825-0097',
                  'https://orcid.org/0000-0001-5109-3700',
                  'https://orcid.org/0000-0002-1694-233X')
        ids = [OrcidId(orcid) for orcid in orcids]
        bads = [orcid for orcid in ids if not orcid.checksumValid]
        assert not bads, str(bads)
