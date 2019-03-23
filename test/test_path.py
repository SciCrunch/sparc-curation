import unittest
from sparcur.core import PathMeta, Path
from .common import project_path

class TestPathMeta(unittest.TestCase):
    def setUp(self):
        self.path = Path(project_path)

    def test_xattrs_roundtrip(self):
        pm = self.path.meta
        xattrs = pm.as_xattrs()
        print(xattrs)
        # FIXME actually write these to disk as well?
        new_pm = PathMeta.from_xattrs(xattrs)
        assert new_pm == pm

    def test_metastore_roundtrip(self):
        pm = self.path.meta
        ms = pm.as_metastore()
        # FIXME actually write these to disk as well?
        new_pm = PathMeta.from_metastore(ms)
        assert new_pm == pm
