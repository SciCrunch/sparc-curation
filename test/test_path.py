import shutil
import unittest
from pathlib import PurePosixPath
from sparcur.core import FileSize
from sparcur.paths import AugmentedPath, LocalPath, PrimaryCache, RemotePath
from sparcur.paths import XattrCache, SymlinkCache
from sparcur.pathmeta import PathMeta, _PathMetaAsSymlink, _PathMetaAsXattrs
from .common import project_path

SymlinkCache._local_class = AugmentedPath  # have to set a default

class TestFileSize(unittest.TestCase):
    def test_getattr(self):
        s = FileSize(1000000000000000)
        getattr(s, 'hr')


class TestPathMeta(unittest.TestCase):
    prefix = None

    def setUp(self):
        self.path = TestLocalPath(project_path)

        self.test_path = TestLocalPath('/tmp/testpath')  # FIXME random needed ...
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
        meta = PathMeta(id='N:helloworld:123', size=10, checksum=b'1;o2j\x9912\xffo3ij\x01123,asdf.')
        path = self.test_path
        path._cache = SymlinkCache(path, meta=meta)
        path.cache.meta = meta
        new_meta = path.cache.meta
        path.unlink()
        msg = '\n'.join([f'{k!r} {v!r} {getattr(new_meta, k)!r}' for k, v in meta.items()])
        assert meta == new_meta, msg

    def _test_symlink_roundtrip_weird(self):
        path = TestLocalPath('/tmp/testpath')  # FIXME random needed ...
        meta = PathMeta(id='N:helloworld:123', size=10, checksum=b'1;o2j\x9912\xffo3ij\x01123,asdf.')
        pure_symlink = PurePosixPath(path.name) / meta.as_symlink()
        path.symlink_to(pure_symlink)
        try:
            cache = SymlinkCache(path)
            new_meta = cache.meta
            msg = '\n'.join([f'{k!r} {v!r} {getattr(new_meta, k)!r}' for k, v in meta.items()])
            assert meta == new_meta, msg
        finally:
            path.unlink()

    def test_parts_roundtrip(self):
        pmas = _PathMetaAsSymlink()
        lpm = self.path.meta
        bpm = PathMeta(id='N:helloworld:123', size=10, checksum=b'1;o2j\x9912\xffo3ij\x01123,asdf.')
        bads = []
        for pm in (lpm, bpm):
            symlink = pm.as_symlink()
            print(symlink)
            new_pm = pmas.from_parts(symlink.parts)
            #corrected_new_pm = PurePosixPath()
            if new_pm != pm:
                bads += ['\n'.join([str((getattr(pm, field), getattr(new_pm, field)))
                                    for field in ('id',) + _PathMetaAsSymlink.order
                                    if not (getattr(pm, field) is getattr(new_pm, field) is None)]),
                         f'{pm.__reduce__()}\n{new_pm.__reduce__()}']

        assert not bads, '\n===========\n'.join(bads)


class TestPrefix(TestPathMeta):
    prefix = 'prefix'


class TestPrefixEvil(TestPathMeta):
    prefix = 'prefix.'


class TestContext(unittest.TestCase):
    def test_context(self):
        start = AugmentedPath.cwd()
        target = AugmentedPath('/tmp/')
        distractor = AugmentedPath('/home/')
        with target:
            target_cwd = AugmentedPath.cwd()
            distractor.chdir()
            distractor_cwd = AugmentedPath.cwd()

        end = AugmentedPath.cwd()
        eq = (
            (target, target_cwd),
            (distractor, distractor_cwd),
            (start, end),
        )
        
        bads = [(a, b) for a, b in eq if a != b]
        assert not bads, bads
        assert start != target != distractor



class TestLocalPath(LocalPath):
    def metaAtTime(self, time):
        # we are cheating in order to do this
        return PathMeta(id=self._cache_class._remote_class.invAtTime(self, time))


test_base = TestLocalPath(__file__).parent / 'test-moves'
test_path = test_base / 'test-container'


class TestCachePath(XattrCache, PrimaryCache):
    xattr_prefix = 'test'
    #_backup_cache = SqliteCache
    _not_exists_cache = SymlinkCache
    anchor = test_path

    @property
    def _anchor(self):
        """ a folder based anchor that turns all child folders
            into their own anchor, appropriately throws an error
            if there is no common path """

        # cool but not useful atm
        this_folder = TestLocalPath(__file__).absolute().parent
        return this_folder / self.relative_to(this_folder).parts[0]


class TestRemotePath(RemotePath):
    anchor = test_path
    ids = {0: anchor}  # time invariant
    index_at_time = {1: {1: anchor / 'a',

                         2: anchor / 'c',

                         3: anchor / 'e',
                         4: anchor / 'e/f',
                         5: anchor / 'e/f/g',

                         8: anchor / 'i',
                         9: anchor / 'i/j',
                         10: anchor / 'i/j/k',

                         13: anchor / 'n',
                         14: anchor / 'n/o',
                         15: anchor / 'n/o/p',},
                     2: {1: anchor / 'b',

                         2: anchor / 'd',

                         3: anchor / 'h/',
                         4: anchor / 'h/f',
                         5: anchor / 'h/f/g',

                         11: anchor / 'l',
                         12: anchor / 'l/m',
                         10: anchor / 'l/m/k',

                         16: anchor / 'q',
                         17: anchor / 'q/r',
                         18: anchor / 'q/r/s',}}

    def __init__(self, id, cache=None, is_dir=True):
        super().__init__(id, cache)
        self._is_dir = is_dir

    def is_dir(self):
        return self._is_dir

    @classmethod
    def invAtTime(cls, path, index):
        return {p:i for i, p in cls.index_at_time[index].items()}[path]


TestLocalPath._cache_class = TestCachePath
TestCachePath._local_class = TestLocalPath
TestCachePath._remote_class = TestRemotePath
TestRemotePath._cache_class = TestCachePath


class TestMoveBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if cls.test_base.exists():
            shutil.rmtree(cls.test_base)

        cls.test_base.mkdir()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_base)

    def setUp(self):
        self.test_path.mkdir()

    def tearDown(self):
        shutil.rmtree(self.test_path)


TestMoveBase.test_base = test_base
TestMoveBase.test_path = test_path


class TestMove(TestMoveBase):
    def _mkpath(self, path, time, is_dir):
        if not path.parent.exists():
            yield from self._mkpath(path.parent, time, True)

        if is_dir:
            path.mkdir()
        else:
            path.touch()

        yield path.cache_init(path.metaAtTime(time))

    def _test_move(self, source, target, is_dir=False, target_exists=False):
        s = self.test_path / source
        t = self.test_path / target
        caches = list(self._mkpath(s, 1, is_dir))
        if target_exists:  # FIXME and same id vs and different id
            target_caches = list(self._mkpath(t, is_dir))

        cache = caches[-1]
        meta = t.metaAtTime(2)
        print(f'{source} -> {target} {cache.meta} {meta}')
        cache.move(target=t, meta=meta)

    def test_dir_moved(self):
        source = 'a'
        target = 'b'
        self._test_move(source, target, is_dir=True)

    def test_file_moved(self):
        source = 'c'
        target = 'd'
        self._test_move(source, target)

    def test_parent_moved(self):
        source = 'e/f/g'
        target = 'h/f/g'
        self._test_move(source, target)

    def test_parents_moved(self):
        source = 'i/j/k'
        target = 'l/m/k'
        self._test_move(source, target)

    def test_all_moved(self):
        source = 'n/o/p'
        target = 'q/r/s'
        self._test_move(source, target)


class TestMoveTargetExists(TestMoveBase):
    def _test_move(self, source, target, is_dir=False):
        super()._test_move(self, source, target, is_dir, target_exists=True)
