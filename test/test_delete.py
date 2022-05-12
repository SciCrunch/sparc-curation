import os
import secrets
import unittest
import augpathlib as aug
from sparcur import exceptions as exc
from sparcur.utils import GetTimeNow
from sparcur.paths import PennsieveCache, LocalPath, Path
from sparcur.backends import PennsieveRemote
from .common import test_organization, test_dataset, _pid
from .common import skipif_ci, skipif_no_net
import pytest


class _TestOperation:

    _cache_class = PennsieveCache
    _remote_class = PennsieveRemote

    @classmethod
    def tearDownClass(cls):
        base = aug.AugmentedPath(__file__).parent / f'test-operation-{_pid}'
        if base.exists():
            base.popd()  # in case we were inside it pop back out first
            base.rmtree()

    def setUp(self):
        class Cache(self._cache_class):
            pass

        Cache._bind_flavours()

        base = aug.AugmentedPath(__file__).parent / f'test-operation-{_pid}'

        if base.exists():
            base.popd()  # in case we were inside it pop back out first
            base.rmtree()

        base.mkdir()
        base.pushd()

        self.Remote = self._remote_class._new(LocalPath, Cache)
        self.Remote.init(test_organization)
        self.anchor = self.Remote.dropAnchor(base)

        self.root = self.anchor.remote
        self.project_path = self.anchor.local
        list(self.root.children)  # populate datasets
        self.test_base = [
            p for p in self.project_path.children
            if p.cache.id == test_dataset][0]
        list(self.test_base.rchildren)  # populate test dataset

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

@skipif_ci
@skipif_no_net
class TestDelete(_TestOperation, unittest.TestCase):

    def test_0(self):
        assert True

    def test_1_case(self):
        # this is an old scenario that happens because of how the old system worked
        # local working directory | x
        # local cache directory | o
        # remote | o
        pass


def make_rand(n, width=80):
    lw = width + 1
    hex_lw = lw // 2
    n_lines = n // lw
    n_accounted = n_lines * lw
    h_lines = n // (hex_lw * 2)
    h_accounted = h_lines * (hex_lw * 2)
    ldiff = n_lines - h_lines
    adiff = n_accounted - h_accounted
    accounted = n_accounted
    missing = n - accounted
    hex_missing = (missing + 1) // 2
    diff = hex_missing * 2 - missing
    hexstart = width % 2  # almost there fails on 71
    # also fails len(make_rand(102, 101)) - 102

    print(adiff, ldiff, missing, diff)
    string = '\n'.join([secrets.token_hex(hex_lw)[hexstart:]
                        for i in range(n_lines)]
                       + [secrets.token_hex(hex_missing)[diff:-1] + '\n'])
    return string.encode()


@skipif_ci
@skipif_no_net
@pytest.mark.skip('VERY SLOW')
class TestFilenames(_TestOperation, unittest.TestCase):

    _evil_names = (
        # '......................',  # this breaks the agent with infinite timeout
        '!@#$%^&*()[]{}`~;:,',
        '(╯°□°）╯︵ ┻━┻)',
        'הָיְתָהtestالصفحات التّحول',
        'הָיְתָהtestالصفحا تالتّحول',  # check bucket names
        '᚛ᚄᚓᚐᚋᚒᚄ ᚑᚄᚂᚑᚏᚅ᚜',
        'Z̮̞̠͙͔ͅḀ̗̞͈̻̗Ḷ͙͎̯̹̞͓G̻O̭̗̮',
        # '𝕿𝖍𝖊 𝖖𝖚𝖎𝖈𝖐 𝖇𝖗𝖔𝖜𝖓 𝖋𝖔𝖝 𝖏𝖚𝖒𝖕𝖘 𝖔𝖛𝖊𝖗 𝖙𝖍𝖊 𝖑𝖆𝖟𝖞 𝖉𝖔𝖌',  # this breaks the agent with ERRORED
        'evil file space',
        'evil_file underscore',
        'evil-file dash',
        'evil%20file percent 20',
        'hello%20world%20%60~%21%40%23%24%25%5E%26%2A%28%29%5B%5D%7B%7D%27',
        # the problem is that we don't know whether we can actually
        # decode a file name, and wthe database stores the encoded filename
        'hello%20world',
        'hello%20world~',
    )

    @property
    def _more_evil_names(self):
        # class scope strikes back! LOL PYTHON
        return [
            name
          for char in
            ('\x07',
             #'/',  # have to do this in a different way on unix
             '\\',
             '|',
             '!',
             '@',
             '#',
             '$',
             '%',
             '^',
             '&',
             '*',
             '(',
             ')',
             '[',
             ']',
             '{',
             '}',
             "'",
             '`',
             '~',
             ';',
             ':',
             ',',
             '"',
             '?',
             '<',
             '>',
             )
          for name in
            (f'prefix{char}',
             f'prefix{char}suffix',
             f'{char}suffix',)]

    @staticmethod
    def _op(test_folder, fsize, name):
        test_file_a = test_folder / (name + '.ext')
        test_file_b = test_folder / (name + '.txe')
        test_folder_i = test_folder / name

        for _f in (test_file_a, test_file_b):
            if _f.exists() or _f.is_broken_symlink():
                msg = (f'bad test environment: file/link already exists: {_f}')
                raise FileExistsError(msg)

        # FIXME maybe don't straight fail here, but instead
        # don't upload and just compare the existing name?
        # the fact that we get an error is a sign that the
        # name matches actually ... so not getting an error
        # in subsequent runs is bad ... for test_base at least

        test_file_a.data = iter((make_rand(fsize),))
        test_file_b.data = iter((make_rand(fsize),))

        try:
            remote_a = test_file_a.upload()
            name_a = remote_a.bfobject.name
        except Exception as e:
            name_a = e

        try:
            remote_b = test_file_b.upload()
            name_b = remote_b.bfobject.name
        except Exception as e:
            name_b = e

        try:
            remote_i = test_folder_i.mkdir_remote()
            name_i = remote_i.bfobject.name
        except Exception as e:
            name_i = e

        return name_a, name_b, name_i

    def test_filenames_more_evil(self):
        return self.test_filenames_evil(self._more_evil_names)

    def test_filenames_evil(self, names=_evil_names):
        # XXX warning slow!
        now = GetTimeNow()
        local = self.project_path / f'test-dataset-{now.START_TIMESTAMP_LOCAL_FRIENDLY}'
        remote = local.mkdir_remote()

        try:
            # FIXME consider going back to self.test_base instead of local here
            test_folder = local / 'pandora'
            test_folder.mkdir_remote()
            test_folder.__class__.upload = Path.upload
            results = []
            fsize = 1024  # needed for uniqueish hashes colloisions will still happen
            # FIXME this pretty clearly reveals a need for
            # batching to multiplex the fetch ... SIGH
            for name in names:
                name_a, name_b, name_i = self._op(test_folder, fsize, name)
                results.append((name_a, name_b, name_i))
        finally:
            remote.rmdir()
            remote.cache.refresh()


@skipif_ci
@skipif_no_net
class TestUpdate(_TestOperation, unittest.TestCase):

    @pytest.mark.skip('the question has been answered')
    def test_process_filesize_limit(self):
        # so the 1 mb size file works, eventually, something else is wrong
        test_folder = self.test_base / 'hrm'
        test_folder.mkdir_remote()
        test_folder.__class__.upload = Path.upload
        for i in range(1024 ** 2, 5 * 1024 ** 2, 1024 ** 2):
            test_file = test_folder / f'size-{i}'
            if test_file.is_broken_symlink():
                test_file.remote.bfobject.package.delete()
                test_file.unlink()
                test_file = test_folder / f'size-{i}'  # remove stale cache

            test_file.data = iter((make_rand(i),))
            remote = test_file.upload()

    def test_upload_noreplace(self):
        for i in range(2):
            test_file = self.test_base / 'dataset_description.csv'
            test_file.data = iter((make_rand(100),))
            # FIXME temp sandboxing for upload until naming gets sorted
            test_file.__class__.upload = Path.upload
            # create some noise
            remote = test_file.upload(replace=False)
            print(remote.bfobject.package.name)

    def test_upload_noreplace_fail(self):
        # some persistent state from other tests is causing this to fail
        test_file = self.test_base / 'dataset_description.csv'
        test_file.data = iter((make_rand(100),))
        # FIXME temp sandboxing for upload until naming gets sorted
        test_file.__class__.upload = Path.upload
        test_file.upload(replace=False)
        try:
            test_file.upload(replace=False)
            assert False, 'should have failed'
        except exc.FileHasNotChangedError:
            pass

    def test_upload_replace(self):
        test_file = self.test_base / 'dataset_description.csv'
        test_file.data = iter((make_rand(100),))
        # FIXME temp sandboxing for upload until naming gets sorted
        test_file.__class__.upload = Path.upload
        test_file.upload()


@skipif_ci
@skipif_no_net
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
        class Cache(self._cache_class):
            pass

        Cache._bind_flavours()

        BFR = self._remote_class._new(LocalPath, Cache)
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


@skipif_ci
@skipif_no_net
class TestMkdirRemote(_TestOperation, unittest.TestCase):

    def test_mkdir_remote_parents_false(self):
        now = GetTimeNow()
        local = self.project_path / f'test-dataset-{now.START_TIMESTAMP_LOCAL_FRIENDLY}' / 'some-folder'
        try:
            remote = local.mkdir_remote()
            raise AssertionError('Should have failed since parents=False')
        except FileNotFoundError:
            pass

    def test_0_mkdir_remote_will_be_dataset(self):
        now = GetTimeNow()
        local = self.project_path / f'test-dataset-{now.START_TIMESTAMP_LOCAL_FRIENDLY}'
        remote = local.mkdir_remote()
        remote.rmdir()
        remote.cache.refresh()  # reminder that remotes are a snapshot in time, NOT dynamic
        assert not local.exists(), f'should have been deleted {remote}'

    def test_1_mkdir_remote_will_be_collection(self):
        now = GetTimeNow()
        local = self.project_path / f'test-dataset-{now.START_TIMESTAMP_LOCAL_FRIENDLY}' / 'some-folder'
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
            parent.cache.refresh()
            assert not lparent.exists(), f'should have been deleted {parent}'



class TestRenameFolder:
    # TODO
    pass


class TestMoveFolder:
    # TODO
    pass


class TestRemote(_TestOperation, unittest.TestCase):
    def test_remote_path_does_not_exist(self):
        new_thing = self.root / 'does not exist'

    @pytest.mark.skip('Not ready.')
    def test_cache_path_does_not_exist(self):
        """ This should not produce an error.
            Path objects should be able to be instantiated without
            po.exists() -> True at a point in time prior to instantiation.
        """
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


