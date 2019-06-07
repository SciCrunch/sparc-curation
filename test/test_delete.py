import os
import shutil
import unittest
from sparcur.paths import AugmentedPath, BlackfynnCache as BFC, LocalPath
from sparcur.backends import BlackfynnRemote
from sparcur.blackfynn_api import BFLocal
import pytest

test_organization = 'N:organization:ba06d66e-9b03-4e3d-95a8-649c30682d2d'
test_dataset = 'N:dataset:5d167ba6-b918-4f21-b23d-cdb124780da1'


@pytest.mark.skipif('CI' in os.environ, reason='Requires access to data')
class TestDelete(unittest.TestCase):
    def setUp(self):
        class BlackfynnCache(BFC):
            pass

        base = AugmentedPath(__file__).parent / 'test-delete'

        if base.exists():
            shutil.rmtree(base)

        base.mkdir()
        base.chdir()

        self.BlackfynnRemote = BlackfynnRemote._new(LocalPath, BlackfynnCache)
        self.BlackfynnRemote.init(test_organization)
        self.BlackfynnRemote.dropAnchor(base)
        root = self.BlackfynnRemote(self.BlackfynnRemote.root)
        #breakpoint()

    def test(self):
        pass
