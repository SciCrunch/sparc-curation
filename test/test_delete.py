import os
import shutil
import unittest
from sparcur.utils import FileSize
from sparcur.paths import AugmentedPath, BlackfynnCache as BFC, LocalPath
from sparcur.backends import BlackfynnRemote
from sparcur.blackfynn_api import BFLocal
import pytest

test_organization = 'N:organization:ba06d66e-9b03-4e3d-95a8-649c30682d2d'
test_dataset = 'N:dataset:5d167ba6-b918-4f21-b23d-cdb124780da1'


@pytest.mark.skip()
class TestOperation(unittest.TestCase):
    def setUp(self):
        class BlackfynnCache(BFC):
            pass

        base = AugmentedPath(__file__).parent / 'test-operation'

        if base.exists():
            shutil.rmtree(base)

        base.mkdir()
        base.chdir()

        self.BlackfynnRemote = BlackfynnRemote._new(LocalPath, BlackfynnCache)
        self.BlackfynnRemote.init(test_organization)
        self.anchor = self.BlackfynnRemote.dropAnchor(base)
        self.root = self.anchor.remote
        self.project_path = self.anchor.local
        list(self.root.children)  # populate datasets
        self.test_base = [p for p in self.project_path.children if p.cache.id == test_dataset][0]
        asdf = self.root / 'lol' / 'lol' / 'lol (1)'

        class Fun(os.PathLike):
            name = 'hohohohohoho'

            def __fspath__(self):
                return ''

            @property
            def size(self):
                return FileSize(len(b''.join(self.data)))

            @property
            def data(self):
                for i in range(100):
                    yield b'0' * 1000

        wat = asdf.bfobject.upload(Fun())
        #breakpoint()


@pytest.mark.skipif('CI' in os.environ, reason='Requires access to data')
class TestDelete(TestOperation):

    def test(self):
        pass

    def test_1_case(self):
        # this is an old scenario that happens because of how the old system worked
        # local working directory | x
        # local cache directory | o
        # remote | o
        pass

@pytest.mark.skipif('CI' in os.environ, reason='Requires access to data')
class TestUpdate(TestOperation):

    def test(self):
        test_file = self.test_base / 'dataset_description.csv'
        test_file.data = iter(('lol'.encode(),))
        test_file.remote.data = test_file.data
        pass
