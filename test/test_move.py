import shutil
import unittest
from sparcur import exceptions as exc
from .common import TestPathHelper
from .common import TestLocalPath, TestCachePath, TestRemotePath


class TestMove(TestPathHelper, unittest.TestCase):
    def _mkpath(self, path, time, is_dir):
        if path.exists():
            return

        if not path.parent.exists():
            yield from self._mkpath(path.parent, time, True)

        if is_dir:
            path.mkdir()
        else:
            path.touch()

        yield path.cache_init(path.metaAtTime(time))

    def _test_move(self, source, target, target_exists=False):
        s = self.test_path / source
        t = self.test_path / target
        #remote = TestRemotePath.invAtTime(1)
        caches = list(self._mkpath(s, 1, int(s.metaAtTime(1).id) in TestRemotePath.dirs))
        if target_exists:  # FIXME and same id vs and different id
            target_caches = list(self._mkpath(t, 2, int(t.metaAtTime(2).id) in TestRemotePath.dirs))

        cache = caches[-1]
        meta = t.metaAtTime(2)
        print(f'{source} -> {target} {cache.meta} {meta}')
        cache.move(target=t, meta=meta)
        assert t.cache.id == TestRemotePath.invAtTime(t, 2)

    def test_0_dir_moved(self):
        source = 'a'
        target = 'b'
        self._test_move(source, target)

    def test_1_file_moved(self):
        source = 'c'
        target = 'd'
        self._test_move(source, target)

    def test_2_parent_moved(self):
        source = 'e/f/g'
        target = 'h/f/g'
        self._test_move(source, target)

    def test_3_parents_moved(self):
        source = 'i/j/k'
        target = 'l/m/k'
        self._test_move(source, target)

    def test_4_all_moved(self):
        source = 'n/o/p'
        target = 'q/r/s'
        self._test_move(source, target)

    def test_5_onto_self(self):
        source = 't'
        target = 't'
        self._test_move(source, target)

    def test_6_onto_different(self):
        source = 'a'
        target = 't'
        try:
            self._test_move(source, target)
            raise AssertionError('should have failed')
        except exc.PathExistsError:
            pass


class TestMoveTargetExists(TestMove):
    def _test_move(self, source, target):
        super()._test_move(source, target, target_exists=True)
        # since all the ids match this should work ...
        #try:
            #raise AssertionError('should have failed')
        #except exc.PathExistsError:
            #pass
