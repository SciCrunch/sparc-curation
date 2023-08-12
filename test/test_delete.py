import os
import atexit
import secrets
import unittest
from functools import wraps
import augpathlib as aug
from sxpyr import sxpyr
from pyontutils.utils import Async, deferred  # TODO -> asyncd in future
from pyontutils.utils_fast import isoformat
from sparcur import exceptions as exc
from sparcur.utils import GetTimeNow, log
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

        self.Remote = self._remote_class._new(Cache._local_class, Cache)
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

    log.debug((adiff, ldiff, missing, diff))
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
            fsize = 1024  # needed for uniqueish hashes colloisions will still happen
            # FIXME this pretty clearly reveals a need for
            # batching to multiplex the fetch ... SIGH
            results = Async(rate=10)(deferred(self._op)(test_folder, fsize, name) for name in names)
            #results = []
            #for name in names:
            #    name_a, name_b, name_i = self._op(test_folder, fsize, name)
            #    results.append((name_a, name_b, name_i))
        finally:
            remote.rmdir(force=True)
            # FIXME crumple fails in refresh since we use rmdir
            # instead of rmtree (for safety)
            #remote.cache.refresh()  # FIXME


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


class TestMoveFolder(_TestOperation, unittest.TestCase):

    def test_reparent(self):
        # TODO this is nowhere near complete with respect to synchronization
        # but it is sufficient to test the components needed for sync
        now = GetTimeNow()
        local = self.project_path / f'test-dataset-{now.START_TIMESTAMP_LOCAL_FRIENDLY}'
        remote = local.mkdir_remote()

        try:
            # FIXME consider going back to self.test_base instead of local here
            test_folder_1 = local / 'dire-1'
            test_folder_1.mkdir_remote()
            test_folder_2 = local / 'dire-2'
            test_folder_2.mkdir_remote()
            list(remote.cache.children)  # XXX populate with remote data

            test_folder_2.remote.reparent(test_folder_1.cache_id)

            test_folder_1.__class__.upload = Path.upload
            fsize = 1024
            test_file_1 = test_folder_1 / 'file-1.ext'
            test_file_1.data = iter((make_rand(fsize),))
            test_file_1.upload()
            test_file_1.remote.reparent(test_folder_2.cache_id)

        finally:
            remote.rmdir(force=True)
            # FIXME crumple fails in refresh since we use rmdir
            # instead of rmtree (for safety)
            #remote.cache.refresh()  # FIXME


class _ChangesHelper:
    _local_only = True

    def _make_ops(self):
        ops = tuple()
        # dirs
        dataset = 'project/dataset'
        ops += (
        #(0, 'mkdir', 'project'),  # don't actually need this since we replace it when building paths
        (0, 'mkdir', dataset),
        (0, 'mkdir', 'project/dataset/dire-1'),
        (0, 'mkdir', 'project/dataset/dire-2'),
        (0, 'mkdir', 'project/dataset/dire-6'),
        )

        # sources
        d3_1 = 'project/dataset/dire-1/dire-3-1-rn'
        d3_2 = 'project/dataset/dire-1/dire-3-2-rp'
        d3_3 = 'project/dataset/dire-1/dire-3-3-np'

        f1_0 = 'project/dataset/dire-1/file-1-0.ext'
        f1_1 = 'project/dataset/dire-1/file-1-1-rn.ext'
        f1_2 = 'project/dataset/dire-1/file-1-2-rp.ext'
        f1_3 = 'project/dataset/dire-1/file-1-3-np.ext'

        l1_0 = 'project/dataset/dire-1/link-1-0.ext'
        l1_1 = 'project/dataset/dire-1/link-1-1-rn.ext'
        l1_2 = 'project/dataset/dire-1/link-1-2-rp.ext'
        l1_3 = 'project/dataset/dire-1/link-1-3-np.ext'

        # targets
        # TODO need variants of all of these where we lose the metadata probably?

        ops += (
        (0, 'mkdir', d3_1),
        (0, 'mkdir', d3_2),
        (0, 'mkdir', d3_3),

        (0, 'mkfile', f1_0),  # nochange
        (0, 'mkfile', f1_1),
        (0, 'mkfile', f1_2),
        (0, 'mkfile', f1_3),

        (0, 'mklink', l1_0),  # nochange
        (0, 'mklink', l1_1),
        (0, 'mklink', l1_2),
        (0, 'mklink', l1_3),

        # moves: renames, reparents, rename_reparent
        (1, 'rename', d3_1, 'project/dataset/dire-1/dire-3-1-rn-r'),  # rn
        (1, 'rename', d3_2, 'project/dataset/dire-2/dire-3-2-rp'),  # rp
        (1, 'rename', d3_3, 'project/dataset/dire-2/dire-3-3-np-r'),  # rnp

        (1, 'rename', f1_1, 'project/dataset/dire-1/file-1-1-rn-r.ext'),  # rn
        (1, 'rename', f1_2, 'project/dataset/dire-2/file-1-2-rp.ext'),  # rp
        (1, 'rename', f1_3, 'project/dataset/dire-2/file-1-3-np-r.ext'),  # rnp

        (1, 'rename', l1_1, 'project/dataset/dire-1/link-1-1-rn-r.ext'),  # rn
        (1, 'rename', l1_2, 'project/dataset/dire-2/link-1-2-rp.ext'),  # rp
        (1, 'rename', l1_3, 'project/dataset/dire-2/link-1-3-np-r.ext'),  # rnp

        # add
        (1, 'mkdir',  'project/dataset/dire-6/dire-7-add'),
        (1, 'mkfile', 'project/dataset/dire-6/file-4-add.ext'),
        (2, 'mklink', 'project/dataset/dire-6/link-4-add.ext'),  # XXX this causes an error because it looks like the index is out of synx
        )

        # change (only applies to files)
        f5_1 = 'project/dataset/dire-6/file-5-1-cd_.ext'
        f5_2 = 'project/dataset/dire-6/file-5-2-c_m.ext'
        f5_3 = 'project/dataset/dire-6/file-5-3-c_x.ext'
        f5_4 = 'project/dataset/dire-6/file-5-4-cdm.ext'
        f5_5 = 'project/dataset/dire-6/file-5-5-cdx.ext'
        # file_id change ? should be impossible ...
        ops += (
        (0, 'mkfile', f5_1),
        (0, 'mkfile', f5_2),
        (0, 'mkfile', f5_3),
        (0, 'mkfile', f5_4),
        (0, 'mkfile', f5_5),

        # TODO probably also change size
        (1, 'change', f5_1, True, False),  # data
        (1, 'change', f5_2, False, True),  # metadata
        (1, 'change', f5_3, False, None),  # no metadata # can handle this from objects cache
        (1, 'change', f5_4, True, True),  # data metadata
        (1, 'change', f5_5, True, None),  # data no metadata
        )

        # remove
        d9 = 'project/dataset/dire-6/dire-9-rem'
        f6 = 'project/dataset/dire-6/file-6-rem.ext'
        l6 = 'project/dataset/dire-6/link-6-rem.ext'
        ops += (
        (0, 'mkdir', d9),
        (0, 'mkfile', f6),
        (0, 'mklink', l6),

        (1, 'remove', d9),
        (1, 'remove', f6),
        (1, 'remove', l6),
        )

        # build the indexes so we can do the diff
        ops += (
            (0.5, 'index', dataset),
        )
        return ops

    def setUp(self):
        # TODO construct the template we need
        super().setUp()
        self.Path = self.Remote._local_class

        #sigh = list(self.project_path.remote.children)
        #[s.rmdir(force=True) for s in sigh if '2023' in s.name]
        #breakpoint()
        #raise ValueError('cleanup tearDown failure mess')

        # TODO expected outcome after stage probably

        if self._local_only:
            def populate_cache(p, change=False):
                # FIXME TODO change=True case may need special handling
                # and probably represents a strange case of some kind
                return p._cache_class.fromLocal(p)

            def norm_path(p):
                return self.project_path / p.replace('project/', '')

        else:
            # XXX WARNING extremely slow due to sequentially creating each remote file
            now = GetTimeNow()
            local_dataset = self.project_path / f'test-dataset-{now.START_TIMESTAMP_LOCAL_FRIENDLY}'
            remote_dataset = local_dataset.mkdir_remote()
            #self._test_dataset = remote_dataset
            # tearDown fails to trigger if failure happens in setUp which is useless
            # so use atexit instead
            atexit.register(lambda : remote_dataset.rmdir(force=True))
            def populate_cache(p, change=False):
                # FIXME TODO change=True case may need special handling
                # and probably represents a strange case of some kind
                if change:
                    return

                remote = p.create_remote()
                return remote.cache

            def norm_path(p):
                return local_dataset / p.replace('project/dataset', '').strip('/')

        def mkdir(d, add=False):
            if not self._local_only and d == local_dataset:
                # FIXME HACK
                return

            d.mkdir()
            if not add:
                cache = populate_cache(d)
                d._cache = cache
            #d._cache_class.fromLocal(d)

        def mkfile(f, add=False):
            f.data = iter((make_rand(100),))
            if not add:
                cache = populate_cache(f)
                f._cache = cache
            #f._cache_class.fromLocal(f)

        def mklink(l):
            try:
                l.data = iter((make_rand(100),))
                # issue with id and parent_id not being set so use fromLocal since it does it correctly
                #meta = f.meta  # TODO checksum probably?
                #symlink = meta.as_symlink(local_name=l.name)
                #cache = l._cache_class.fromLocal(l)
                cache = populate_cache(l)
                symlink = cache.meta.as_symlink(local_name=l.name)
            finally:
                l.unlink()

            l.symlink_to(symlink)

        def rename(path, target):
            path.rename(target)

        def change(f, data, metadata):
            if data:
                f.data = iter((make_rand(100),))

            if metadata is None or metadata:
                # must set xattrs to nothing if
                # we want to change metadata otherwise
                # PrimaryCache._meta_updater will go haywire and
                # ... try to delete the file ... and it will
                # actually delete it instead of crumpling it
                # so definitely a FIXME very dangerous lose your work
                # kind of scenario between that _meta_updater and
                # BFPNCache._actually_crumple and change of BFPNCache.crumple
                [f.delxattr(k) for k in f.xattrs()]
                if f.xattrs():
                    breakpoint()

            if metadata:
                if not f.exists():
                    raise FileNotFoundError(f)

                try:
                    populate_cache(f, change=True)
                    #f._cache_class.fromLocal(f)
                except Exception as e:
                    breakpoint()
                    raise e

        def remove(path):
            if path.is_dir():
                path.rmdir()
            else:
                path.unlink()

        def index(ds):
            if self._local_only:
                caches = [l.cache for l in ds.rchildren]  # XXX reminder, NEVER use ds.cache.rchildren that will pull
                class fakeremote:
                    def __init__(self, id, name, parent_id, file_id, updated, local):
                        self.id = id
                        self.name = name
                        self._name = name
                        self.parent_id = parent_id
                        self.updated = updated
                        self._lol_local = local

                        if file_id is not None:
                            self.file_id = file_id

                    def is_dir(self):
                        return self._lol_local.is_dir()

                for c in caches:
                    # FIXME causes other issues ... even while trying to avoid init issues
                    # we should not have to do this
                    cmeta = c.meta
                    c._remote = fakeremote(
                        cmeta.id, cmeta.name, cmeta.parent_id, cmeta.file_id,
                        cmeta.updated, c.local)
            else:
                # this is safe at this stage since everything should match upstream
                caches = [c.cache for c in local_dataset.rchildren]

            ds._generate_pull_index(ds, caches)

        fops = {
            'mkdir': mkdir,
            'mkfile': mkfile,
            'mklink': mklink,
            'rename': rename,
            'change': change,
            'remove': remove,
            'index': index,
        }

        def make_closure(stage, op, obj, args):
            f = fops[op]

            if stage > 0 and op in ('mkdir', 'mkfile'):
                kwargs=dict(add=True)
            else:
                kwargs = {}

            @wraps(f)
            def inner():
                f(path, *args, **kwargs)

            return inner

        def cargs(args):
            for a in args:
                if isinstance(a, str) and a.startswith('project/'):
                    yield norm_path(a)
                else:
                    yield a

        ops = self._make_ops()
        pops = [(stage, op, norm_path(s), *cargs(args)) for stage, op, s, *args in ops]
        init = set([path for stage, op, path, *args in pops if stage == 0])
        test = set([p for stage, op, path, *args in pops if stage >= 1 for p in (path, *args) if isinstance(p, self.project_path.__class__)])
        nochange = init - test
        add_rename_reparent = test - init
        change_remove = test - add_rename_reparent

        cs = [(stage, path, make_closure(stage, op, path, args)) for stage, op, path, *args in pops]
        scs = sorted(cs, key=(lambda abc: (abc[0], len(abc[1].parts))))
        will_fails = []
        for stage, path, fun in scs:
            if stage > 1:
                will_fails.append(fun)
            else:
                fun()

        self._will_fails = will_fails
        self.dataset = pops[0][-1]


class TestChanges(_ChangesHelper, _TestOperation, unittest.TestCase):

    def test_changes(self):
        from dateutil import parser as dateparser
        dataset = self.dataset
        dataset_id, id_name, parent_children, name_id, updated_transitive = dataset._read_indexes()
        # XXX updated_transitive from _read_indexes is a string because that is what
        # _transitive_changes needs internally and then transforms to a datetime object
        # when it returns, therefore we don't fiddle with the types here

        #tc = dataset._transitive_changes()
        # XXX see sparcur.simple.utils
        dataset_id, updated_cache_transitive, diff = dataset.diff()
        blob = {
            'dataset-id': dataset_id.id,
            'updated-transitive': updated_transitive,
            'diff': diff,
        }
        pl = sxpyr.python_to_sxpr(blob, str_as_string=True)
        sxpr = pl._print(sxpyr.configure_print_plist(newline_keyword=False))
        print(sxpr)
        #pl = sxpyr.python_to_sxpr(diff, str_as_string=True)
        #sxpr = pl._print(sxpyr.configure_print_plist(newline_keyword=False))
        breakpoint()


class _WorkflowHelper:

    def _do_workflow(self, paths_to_add):
        # push button, receive bacon

        # 0. asumme there are changes to a dataset

        # 1. click upload button in main window (python get diff)
        #    argv-simple-diff -> sparcur.simple.utils for-racket diff -> path_dataset.diff()

        # 2. select specific paths for upload (python nothing)
        #    racket side selects the list of files to push (push_list) that goes into paths.sxpr
        #    which the python side then reads in the next step

        # 3. click confirm selection checkbox (python generate manifest)
        #    (ensure-directory! (push-dir)) -> updated_transitive push_id
        #    write-push-paths -> {:user-cache-path}/{dataset-uuid}/{updated-transitive}/{push-id}/paths.sxpr -> push_list
        #    argv-simple-make-push-manifest -> sparcur.simple.utils for-racket make-push-manifest -> path_dataset.make_mush_manifest()

        # 4. click push selected to remote (python push from manifest)
        #    argv-simple-push -> sparcur.simple.utils for-racket push -> path_dataset.push_from_manifest()

        # 5. TODO I think that after remote changes are made we probably want to create
        #    a modified index file that notes the changes so that incremental changes
        #    do not have to be pulled again ... of course if upstream has changed we are
        #    back in the usual world of pain ...

        path_dataset = self.dataset  # given
        dataset_id = path_dataset.cache.identifier
        # 1
        __dataset_id, updated_transitive, diff = path_dataset.diff()
        # 2
        # write the push_list to paths.sxpr
        push_id = path_dataset._write_push_list(dataset_id, updated_transitive, diff, paths_to_add)
        # 3
        path_dataset.make_push_manifest(dataset_id, updated_transitive, push_id)
        # 4
        if not self._local_only:  # this fails without remote
            path_dataset.push_from_manifest(dataset_id, updated_transitive, push_id)


class TestWorkflow(_ChangesHelper, _WorkflowHelper, _TestOperation, unittest.TestCase):

    def test_workflow(self):
        # splitting d r l lets us test incremental changes
        paths_to_add_d = [
            'dire-1/dire-3-1-rn-r',  # rn
            'dire-2/dire-3-2-rp',  # rp
            'dire-2/dire-3-3-np-r',  # rnp
        ]
        paths_to_add_r = [
            'dire-1/file-1-1-rn-r.ext',  # rn
            'dire-2/file-1-2-rp.ext',  # rp
            'dire-2/file-1-3-np-r.ext',  # rnp
        ]
        paths_to_add_l = [
            'dire-1/link-1-1-rn-r.ext',  # rn
            'dire-2/link-1-2-rp.ext',  # rp
            'dire-2/link-1-3-np-r.ext',  # rnp
        ]

        paths_to_add_2 = [
            'dire-6/file-4-add.ext',  # should error for now
        ]
        self._do_workflow(paths_to_add_d)
        self._do_workflow(paths_to_add_r)
        self._do_workflow(paths_to_add_l)
        try:
            self._do_workflow(paths_to_add_2)
            raise AssertionError('should have failed due to forbidden ops')
        except ValueError:
            pass


class TestWithRemoteWorkflow(TestWorkflow):

    _local_only = False



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
