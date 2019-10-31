import os
import unittest
from augpathlib import AugmentedPath, FileSize
from sparcur import exceptions as exc
from sparcur.paths import BlackfynnCache as BFC, LocalPath
from sparcur.backends import BlackfynnRemote
from sparcur.blackfynn_api import BFLocal
from .common import test_organization, test_dataset
import pytest


class _TestOperation:
    def setUp(self):
        class BlackfynnCache(BFC):
            pass

        BlackfynnCache._bind_flavours(auto=True)

        base = AugmentedPath(__file__).parent / 'test-operation'

        if base.exists():
            base.popd()  # in case we were inside it pop back out first
            base.rmtree()

        base.mkdir()
        base.pushd()

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

        wat = asdf.bfobject.upload(Fun(), use_agent=False)
        #breakpoint()


@pytest.mark.skipif('CI' in os.environ, reason='Requires access to data')
class TestDelete(_TestOperation):

    def test(self):
        pass

    def test_1_case(self):
        # this is an old scenario that happens because of how the old system worked
        # local working directory | x
        # local cache directory | o
        # remote | o
        pass

@pytest.mark.skipif('CI' in os.environ, reason='Requires access to data')
class TestUpdate(_TestOperation, unittest.TestCase):

    def test(self):
        test_file = self.test_base / 'dataset_description.csv'
        test_file.data = iter(('lol'.encode(),))
        test_file.remote.data = test_file.data
        pass


@pytest.mark.skipif('CI' in os.environ, reason='Requires access to data')
class TestClone(_TestOperation, unittest.TestCase):
    # TODO test a variety of clone scenarios
    # and consider whether testing for and
    # existing root should be done in dropAnchor

    def setUp(self):
        super().setUp()
        self.alt_project_path = self.project_path.parent / 'alt' / self.project_path.name
        if self.alt_project_path.parent.exists():
            self.alt_project_path.parent.rmtree()

        self.alt_project_path.mkdir(parents=True)

    def _do_target(self, target, expect_error_type=None):
        class BlackfynnCache(BFC):
            pass

        BlackfynnCache._bind_flavours(auto=True)

        BFR = BlackfynnRemote._new(LocalPath, BlackfynnCache)
        BFR.init(test_organization)

        if expect_error_type:
            try:
                anchor = BFR.dropAnchor(target.parent)
                raise AssertionError(f'should have failed with a {expect_error_type}')
            except expect_error_type as e:
                pass

        else:
            anchor = BFR.dropAnchor(target.parent)

    def test_1_in_project(self):
        target = self.project_path / 'some-new-folder'
        target.mkdir()
        self._do_target(target)  # FIXME succeeds for now, but probably should not?

    def test_2_project_top_level(self):
        target = self.project_path
        self._do_target(target, exc.DirectoryNotEmptyError)

    def test_3_existing_empty(self):
        target = self.alt_project_path
        self._do_target(target)

    def test_4_existing_has_folder(self):
        target = self.alt_project_path
        child = target / 'a-folder'
        child.mkdir(parents=True)
        self._do_target(target, exc.DirectoryNotEmptyError)

    def test_5_existing_has_file(self):
        target = self.alt_project_path
        child = target / 'a-file'
        child.touch()
        self._do_target(target, exc.DirectoryNotEmptyError)

    def test_6_existing_has_local_data_dir(self):
        target = self.alt_project_path
        child = target / self.anchor._local_data_dir
        child.mkdir()
        self._do_target(target, exc.DirectoryNotEmptyError)
