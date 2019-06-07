import shutil
from pathlib import PurePosixPath
from datetime import datetime
from sparcur import config
from sparcur import exceptions as exc
from sparcur.paths import Path
from sparcur.paths import LocalPath, PrimaryCache, RemotePath
from sparcur.paths import XattrCache, SymlinkCache
from sparcur.state import State
from sparcur.pathmeta import PathMeta
from sparcur.datasets import Version1Header
from sparcur.curation import PathData, Integrator
from sparcur.blackfynn_api import FakeBFLocal
this_file = Path(__file__)
template_root = this_file.parent.parent / 'resources/DatasetTemplate'
print(template_root)
project_path = this_file.parent / 'test_local/test_project'
test_organization = 'N:organization:ba06d66e-9b03-4e3d-95a8-649c30682d2d'
test_dataset = 'N:dataset:5d167ba6-b918-4f21-b23d-cdb124780da1'

PathData.project_path = project_path

osk = Version1Header.skip_cols  # save original skips
Version1Header.skip_cols = tuple(_ for _ in osk if _ != 'example')  # use the example values for tests

ds_folders = 'ds1', 'ds2', 'ds3', 'ds4'
ds_roots = (
    'ds1',
    'ds2/ds2',
    'ds3/oops',
    'ds3/too',
    'ds3/many',
    'ds4/really/deep',
    'ds5',
    'ds5/multi',
    'ds5/level/wat',
)

def mk_fldr_meta(fldr_path, ftype='collection', id=None):
    _meta = fldr_path.meta
    kwargs = {**_meta}
    kwargs['id'] = id if id is not None else f'N:{ftype}:' + fldr_path.as_posix()
    meta = PathMeta(**kwargs)
    # NOTE st_mtime -> modified time of the file contents (data)
    # NOTE st_ctime -> changed time of the file status (metadata)
    # linux does not have a unified way to bet st_btime aka st_crtime which is birth time or created time
    return meta.as_xattrs(prefix='bf')
    return {'bf.id': id if id is not None else f'N:{ftype}:' + fldr_path.as_posix(),
            'bf.created': meta.created.isoformat().replace('.', ',') if meta.created is not None else None,
            'bf.updated': meta.updated.isoformat().replace('.', ',')}


def mk_file_meta(fp):
    meta = fp.meta
    return meta.as_xattrs(prefix='bf')
    return {'bf.id': 'N:package:' + fp.as_posix(),
            'bf.file_id': 0,
            'bf.size': meta.size,
            # FIXME timezone
            'bf.created': meta.created.isoformat().replace('.', ',') if meta.created is not None else None,
            'bf.updated': meta.updated.isoformat().replace('.', ','),
            'bf.checksum': fp.checksum(),
            # 'bf.old_id': None  # TODO
    }


def mk_required_files(path, suffix='.csv'):
    # TODO samples.* ?!??!
    # FIXME because empty files get treated as non-existent
    # we either need to treat them as a different category of error
    # OR we need to fill in some fake values when testing
    for globpath in ('submission.*', 'dataset_description.*',  'subjects.*'):
        for template_file in template_root.glob(globpath):
            file_name = template_file.name
            file_path = path / file_name
            shutil.copy(template_file, file_path)
            file_path.touch()
            attrs = mk_file_meta(file_path)
            file_path.setxattrs(attrs)


if not project_path.exists() or not list(project_path.iterdir()):
    project_path.mkdir(parents=True, exist_ok=True)
    attrs = mk_fldr_meta(project_path, 'organization', id=test_organization)
    project_path.setxattrs(attrs)
    for ds in ds_folders:
        dsp = project_path / ds
        dsp.mkdir()
        dsp.setxattrs(mk_fldr_meta(dsp, 'dataset'))

    for root in ds_roots:
        rp = project_path / root
        if not rp.exists():
            current_parent = rp
            to_reverse = []
            while not current_parent.exists():
                to_reverse.append(current_parent)
                current_parent = current_parent.parent

            for folder in reversed(to_reverse):
                folder.mkdir()
                folder.setxattrs(mk_fldr_meta(folder))

        print(rp)
        mk_required_files(rp)  # TODO all variants of missing files


fbfl = FakeBFLocal(project_path.cache.id, project_path.cache)
State.bind_blackfynn(fbfl)
Integrator.setup(fbfl)


class TestLocalPath(LocalPath):
    def metaAtTime(self, time):
        # we are cheating in order to do this
        return PathMeta(id=self._cache_class._remote_class.invAtTime(self, time))


test_base = TestLocalPath(__file__).parent / 'test-base'
test_path = test_base / 'test-container'


class TestCachePath(PrimaryCache, XattrCache):
    xattr_prefix = 'test'
    #_backup_cache = SqliteCache
    _not_exists_cache = SymlinkCache


class TestRemotePath(RemotePath):
    anchor = test_path
    ids = {0: anchor}  # time invariant
    dirs = {2, 3, 4, 8, 9, 11, 12, 13, 14, 16, 17, 18}
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
                         15: anchor / 'n/o/p',

                         18: anchor / 't',},
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
                         15: anchor / 'q/r/s',

                         18: anchor / 't',

                         19: anchor / 'u',}}

    for ind in index_at_time:
        index_at_time[ind].update(ids)

    test_time = 2

    def __init__(self, thing_with_id, cache=None):
        if isinstance(thing_with_id, int):
            thing_with_id = str(thing_with_id)

        super().__init__(thing_with_id, cache)
        self._errors = []

    def is_dir(self):
        return int(self.id) in self.dirs

    def is_file(self):
        return not self.is_dir()

    def as_path(self):
        return PurePosixPath(self.index_at_time[self.test_time][int(self.id)].relative_to(self.anchor))

    @classmethod
    def invAtTime(cls, path, index):
        path = cls.anchor / path
        return str({p:i for i, p in cls.index_at_time[index].items()}[path])

    @property
    def name(self):
        return self.as_path().name

    @property
    def parent(self):
        if int(self.id) == 0:
            return None

        rlu = self.as_path().parent
        return self.__class__(self.invAtTime(rlu, self.test_time))

    @property
    def meta(self):
        return PathMeta(id=self.id)

    def __repr__(self):
        p = self.as_path()
        return f'{self.__class__.__name__} <{self.id!r} {p!r}>'

# set up cache hierarchy
TestLocalPath._cache_class = TestCachePath
TestCachePath._local_class = TestLocalPath
TestCachePath._remote_class = TestRemotePath
TestRemotePath._cache_class = TestCachePath

# set up testing anchor (must come after the hierarchy)
TestCachePath.anchor = test_path
TestCachePath.anchor = TestCachePath(test_path, meta=PathMeta(id='0'))


class TestPathHelper:
    @classmethod
    def setUpClass(cls):
        if cls.test_base.exists():
            shutil.rmtree(cls.test_base)

        cls.test_base.mkdir()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_base)

    def setUp(self, init_cache=True):
        if self.test_path.exists():  # in case something went wrong with a previous test
            shutil.rmtree(self.test_path)

        self.test_path.mkdir()
        if init_cache:
            self.test_path.cache_init('0')

    def tearDown(self):
        shutil.rmtree(self.test_path)


TestPathHelper.test_base = test_base
TestPathHelper.test_path = test_path
