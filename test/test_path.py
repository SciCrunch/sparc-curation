import unittest
from pathlib import PurePosixPath
from sparcur.paths import Path, SymlinkCache
from sparcur.pathmeta import PathMeta, _PathMetaAsSymlink, _PathMetaAsXattrs
from sparcur.pathmeta import FileSize
from .common import project_path


class TestFileSize(unittest.TestCase):
    def test_getattr(self):
        s = FileSize(1000000000000000)
        getattr(s, 'hr')


class TestPathMeta(unittest.TestCase):
    prefix = None

    def setUp(self):
        self.path = Path(project_path)

        self.test_path = Path('/tmp/testpath')  # FIXME random needed ...
        if self.test_path.is_symlink():
            self.test_path.unlink()

    def _test_getattr_size_hr(self):
        pm = PathMeta(size=1000000000000000)
        woo = getattr(pm, 'size.hr')

    def test_neg__neg__(self):
        pm = PathMeta(id='lol')
        assert pm

    def test___neg__(self):
        pm = PathMeta()
        assert not pm, set(pm.__dict__.values())

    def test_xattrs_roundtrip(self):
        # TODO __kwargs ...
        pm = self.path.meta
        xattrs = pm.as_xattrs(self.prefix)
        print(xattrs)
        # FIXME actually write these to disk as well?
        new_pm = PathMeta.from_xattrs(xattrs, self.prefix)
        msg = '\n'.join([f'{k!r} {v!r} {getattr(new_pm, k)!r}' for k, v in pm.items()])
        assert new_pm == pm, msg
        #'\n'.join([str((getattr(pm, field), getattr(new_pm, field)))
        #for field in _PathMetaAsXattrs.fields])

    def test_metastore_roundtrip(self):
        pm = self.path.meta
        ms = pm.as_metastore(self.prefix)
        # FIXME actually write these to disk as well?
        new_pm = PathMeta.from_metastore(ms, self.prefix)
        assert new_pm == pm, '\n'.join([str((getattr(pm, field), getattr(new_pm, field)))
                                        for field in tuple()])  # TODO

    def test_symlink_roundtrip(self):
        meta = PathMeta(id='N:helloworld:123', size=10)
        path = self.test_path
        path._cache = SymlinkCache(path, meta=meta)
        path.cache.meta = meta
        new_meta = cache.meta
        path.unlink()
        msg = '\n'.join([f'{k!r} {v!r} {getattr(new_meta, k)!r}' for k, v in meta.items()])
        assert meta == new_meta, msg

    def _test_symlink_roundtrip_weird(self):
        path = Path('/tmp/testpath')  # FIXME random needed ...
        meta = PathMeta(id='N:helloworld:123', size=10)
        pure_symlink = PurePosixPath(path.name) / meta.as_symlink()
        path.symlink_to(pure_symlink)
        try:
            cache = SymlinkCache(path)
            new_meta = cache.meta
            msg = '\n'.join([f'{k!r} {v!r} {getattr(new_meta, k)!r}' for k, v in meta.items()])
            assert meta == new_meta, msg
        finally:
            path.unlink()

    def test_pure_symlink_roundtrip(self):
        pmas = _PathMetaAsSymlink()
        lpm = self.path.meta
        bpm = PathMeta(id='N:helloworld:123', size=10)
        bads = []
        for pm in (lpm, bpm):
            symlink = pm.as_symlink()
            print(symlink)
            new_pm = pmas.from_pure_symlink(symlink)
            if new_pm != pm:
                bads += ['\n'.join([str((getattr(pm, field), getattr(new_pm, field)))
                                    for field in _PathMetaAsSymlink.order]),
                         f'{pm.__reduce__()}\n{new_pm.__reduce__()}']

        assert not bads, '\n===========\n'.join(bads)


class TestPrefix(TestPathMeta):
    prefix = 'prefix'


class TestPrefixEvil(TestPathMeta):
    prefix = 'prefix.'
