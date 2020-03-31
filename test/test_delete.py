import os
import unittest
import augpathlib as aug
from sparcur import exceptions as exc
from sparcur.utils import GetTimeNow
from sparcur.paths import BlackfynnCache as BFC, LocalPath, Path
from sparcur.backends import BlackfynnRemote
from sparcur.blackfynn_api import BFLocal
from .common import test_organization, test_dataset
import pytest


class _TestOperation:
    def setUp(self):
        class BlackfynnCache(BFC):
            pass

        BlackfynnCache._bind_flavours()

        base = aug.AugmentedPath(__file__).parent / 'test-operation'

        if base.exists():
            base.popd()  # in case we were inside it pop back out first
            base.rmtree()

        base.mkdir()
        base.pushd()

        self.BlackfynnRemote = BlackfynnRemote._new(LocalPath, BlackfynnCache)
        self.BlackfynnRemote.init(test_organization)
        self.anchor = self.BlackfynnRemote.dropAnchor(base)

        # FIXME this needs to be implemented as part of local
        self.anchor.local_data_dir.mkdir()
        self.anchor.local_objects_dir.mkdir()
        self.anchor.trash.mkdir()

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
                return aug.FileSize(len(b''.join(self.data)))

            @property
            def data(self):
                for i in range(100):
                    yield b'0' * 1000

        #wat = asdf.bfobject.upload(Fun(), use_agent=False)
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
        # FIXME temp sandboxing for upload until naming gets sorted
        test_file.__class__.upload = Path.upload
        test_file.upload()
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

        BlackfynnCache._bind_flavours()

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


class TestMkdirRemote(_TestOperation, unittest.TestCase):

    def test_mkdir_remote_parents_false(self):
        now = GetTimeNow()
        local = self.project_path / f'test-dataset-{now.START_TIMESTAMP}' / 'some-folder'
        try:
            remote = local.mkdir_remote()
            raise AssertionError('Should have failed since parents=False')
        except FileNotFoundError:
            pass

    def test_0_mkdir_remote_will_be_dataset(self):
        now = GetTimeNow()
        local = self.project_path / f'test-dataset-{now.START_TIMESTAMP}'
        remote = local.mkdir_remote()
        remote.rmdir()
        remote.cache.refresh()  # reminder that remotes are a snapshot in time, NOT dynamic
        assert not local.exists(), f'should have been deleted {remote}'

    def test_1_mkdir_remote_will_be_collection(self):
        now = GetTimeNow()
        local = self.project_path / f'test-dataset-{now.START_TIMESTAMP}' / 'some-folder'
        remote = local.mkdir_remote(parents=True)
        parent = remote.parent
        try:
            parent.rmdir()  # should fail here
            try:
                remote.rmdir()  # insurance
            except BaseException as e:
                log.exception(e)
            finally:
                raise AssertionError('remote parent should NOT have rmdired {parent}')
        except exc.PathNotEmptyError:
            pass

        try:
            remote.rmdir()
            remote.cache.refresh()
            assert not local.exists(), f'should have been deleted {remote}'
        finally:
            lparent = parent.local
            parent.cache.refresh()  # we just removed the child so the parent is stale
            parent.rmdir()
            parent.cache.refresh()  # and THIS is the error we have been trying to handle all night!
            assert not lparent.exists(), f'should have been deleted {parent}'


class TestRemote(_TestOperation, unittest.TestCase):
    def test_remote_path_does_not_exist(self):
        new_thing = self.root / 'does not exist'

    def test_cache_path_does_not_exist(self):
        new_thing = self.anchor / 'does not exist'

    def __test_cache_path_fake_id(self):
        # FIXME the right way to do this is
        np = self.project_path / 'new-path'
        np.mkdir()
        npm = np.meta
        
        # bad way
        class FakeBase(aug.RemotePath):
            def __init__(self, id, name, cache=None):
                super().__init__(id, cache)
                self.name = name
                now = GetTimeNow()
                self.created = now._start_time
                self.updated = now._start_time
                self.checksum = 'lolnone'
                self.chunksize = 4096
                self.file_id = 'asdfasdfasdf'

            @property
            def meta(self):
                return PathMeta(size=self.size,
                                created=self.created,
                                updated=self.updated,
                                checksum=self.checksum,
                                chunksize=self.chunksize,
                                id=self.id,
                                file_id=self.file_id)


            @property
            def parent(self):
                return None

            def _parts_relative_to(self, remote, cache_parent=None):
                return [self.name]


        # This takes way too many steps :/
        Fake = FakeBase._new(LocalPath, aug.CachePath)

        fake = Fake('lol', 'double lol')

        self.anchor / fake


