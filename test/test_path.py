import unittest
from sparcur.core import PathMeta, Path
from .common import project_path

class TestPathMeta(unittest.TestCase):
    prefix = None

    def setUp(self):
        self.path = Path(project_path)

    def test_xattrs_roundtrip(self):
        pm = self.path.meta
        xattrs = pm.as_xattrs(self.prefix)
        print(xattrs)
        # FIXME actually write these to disk as well?
        new_pm = PathMeta.from_xattrs(xattrs, self.prefix)
        assert new_pm == pm

    def test_metastore_roundtrip(self):
        pm = self.path.meta
        ms = pm.as_metastore(self.prefix)
        # FIXME actually write these to disk as well?
        new_pm = PathMeta.from_metastore(ms, self.prefix)
        assert new_pm == pm


class TestPrefix(TestPathMeta):
    prefix = 'prefix'


class TestPrefixEvil(TestPathMeta):
    prefix = 'prefix.'
