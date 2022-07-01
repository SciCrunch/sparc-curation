import os
import uuid
import atexit
import shutil
from tempfile import gettempdir
from pathlib import PurePosixPath
import pytest
from augpathlib import PathMeta
from augpathlib.utils import onerror_windows_readwrite_remove
from sparcur import config
from sparcur import exceptions as exc
from sparcur.paths import Path, PathL
from sparcur.paths import LocalPath, PrimaryCache
from sparcur.paths import SymlinkCache
from sparcur.state import State
from sparcur.utils import PennsieveId, log
from sparcur.datasets import DatasetDescriptionFile
from sparcur.curation import PathData, Integrator
from sparcur.pennsieve_api import FakeBFLocal
this_file = Path(__file__).resolve()  # ARGH PYTHON ARGH NO LOL BAD PYTHON
examples_root = this_file.parent / 'examples'
template_root = this_file.parent.parent / 'resources/DatasetTemplate'
print(template_root)
_pid = os.getpid()
path_project_container = this_file.parent / f'test_local-{_pid}'
project_path = PathL(path_project_container) / 'test_project'
fake_org_uuid4 = '0000dead-f00d-4bad-beef-00f000000000'
fake_organization = f'N:organization:{fake_org_uuid4}'
PennsieveId(fake_organization)  # canary
project_path_real = path_project_container / 'UCSD'
test_organization = 'N:organization:ba06d66e-9b03-4e3d-95a8-649c30682d2d'
test_dataset = 'N:dataset:aa859fe9-02d0-4518-981b-012ef8f35c34'
temp_path = Path(gettempdir(), f'.sparcur-testing-base-{_pid}')

onerror = onerror_windows_readwrite_remove if os.name == 'nt' else None

SKIP_NETWORK = ('SKIP_NETWORK' in os.environ or
                'FEATURES' in os.environ and 'network-sandbox' in os.environ['FEATURES'])
skipif_no_net = pytest.mark.skipif(SKIP_NETWORK, reason='Skipping due to network requirement')
skipif_ci = pytest.mark.skipif('CI' in os.environ, reason='Requires access to data')

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
ds_folders = set(Path(d).parts[0] for d in ds_roots)

DIVISION_SLASH = '\u2215'  # what have we done XXX evil


def mk_fldr_meta(fldr_path, ftype='collection', id=None):
    tuuid = 'deadbeef' + str(uuid.uuid4())[8:]
    altid = f'N:{ftype}:{tuuid}'  # ok, we validate without remote in a different way
    _id = id if id is not None else altid
    _id = _id.replace('/', DIVISION_SLASH)  # ARGH there must be a way to ensure file system safe ids :/ XXX evil
    _meta = fldr_path.meta
    kwargs = {**_meta}
    kwargs['id'] = _id
    meta = PathMeta(**kwargs)
    # NOTE st_mtime -> modified time of the file contents (data)
    # NOTE st_ctime -> changed time of the file status (metadata)
    # linux does not have a unified way to bet st_btime aka st_crtime which is birth time or created time
    return meta.as_xattrs(prefix='bf')
    #return {'bf.id': kwargs['id'],
            #'bf.created': meta.created.isoformat().replace('.', ',') if meta.created is not None else None,
            #'bf.updated': meta.updated.isoformat().replace('.', ',')}


def mk_file_meta(fp):
    meta = fp.meta
    return meta.as_xattrs(prefix='bf')
    #return {'bf.id': 'N:package:' + fp.as_posix(),
            #'bf.file_id': 0,
            #'bf.size': meta.size,
            # FIXME timezone
            #'bf.created': meta.created.isoformat().replace('.', ',') if meta.created is not None else None,
            #'bf.updated': meta.updated.isoformat().replace('.', ','),
            #'bf.checksum': fp.checksum(),
            # 'bf.old_id': None  # TODO
    #}


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


if project_path.exists():
    # too much time wasted over stale test data
    # like ACSF just make it fresh every time
    project_path.rmtree(onerror=onerror)

project_path.mkdir(parents=True)
atexit.register(lambda : path_project_container.rmtree(onerror=onerror))
attrs = mk_fldr_meta(project_path, 'organization', id=fake_organization)
project_path.setxattrs(attrs)
# FIXME I know why lddi is on cache but I still don't like it
# because it should be possible to call lddi without forcing the cache to exist
# file under CachPath and RemotePath implementations are broken because instances
# can only exist if the underlying thing exists, which defeats the point of paths
project_path._cache_class._asserted_anchor = project_path.cache  # XXX sigh, here we are again
project_path.cache.local_data_dir_init()

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
#Integrator.setup()  # not needed for tests it seems


@pytest.mark.skipif('CI' in os.environ, reason='Requires access to data')
class RealDataHelper:

    _fetched = False
    _cache_class = None
    _remote_class = None

    @classmethod
    def setUpClass(cls):
        # child classes need to set _cache_class and _remote_class and then super
        from sparcur.paths import Path
        from sparcur.config import auth
        from sparcur.simple import pull, fetch_metadata_files, fetch_files
        test_datasets_real = auth.get_list('datasets-test')
        nosparse = [
            'N:dataset:bec4d335-9377-4863-9017-ecd01170f354',  # mbf headers
        ]
        test_datasets_real.extend(nosparse)
        slot = auth._pathit('{:user-cache-path}/sparcur/objects-temp')
        # FIXME slot needs to be handled transparently as an LRU cache
        # that has multiple folder levels and stores only by uuid
        # and probably lives in '{:user-cache-path}/sparcur/objects'
        slot = slot if slot.exists() else None
        cls.organization_id = auth.get('remote-organization')
        cls.Remote = cls._remote_class._new(Path, cls._cache_class)
        if (hasattr(cls.Remote, '_api') and
            not isinstance(cls.Remote._api, cls.Remote._api_class)):
            log.warning(f'stale _api on remote {cls.Remote._api}')
            for cls in self.Remote.mro():
                if hasattr(cls, '_api'):
                    try:
                        del cls._api
                    except AttributeError as e:
                        pass

        cls.Remote.init(cls.organization_id)
        cls.anchor = cls.Remote.smartAnchor(path_project_container)
        cls.anchor.local_data_dir_init(symlink_objects_to=slot)
        cls.project_path = cls.anchor.local
        list(cls.anchor.children)  # side effect to retrieve top level folders
        datasets = list(cls.project_path.children)
        cls.test_datasets = [d for d in datasets if d.cache_id in test_datasets_real]
        [d.rmdir() for d in datasets if d.cache_id not in test_datasets_real]  # for sanity
        if not RealDataHelper._fetched:
            RealDataHelper._fetched = True  # if we fail we aren't going to try again
            [d._mark_sparse() for d in cls.test_datasets if d.cache_id not in nosparse]  # keep pulls fastish
            pull.from_path_dataset_file_structure_all(cls.project_path, paths=cls.test_datasets)
            fetch_metadata_files.main(cls.project_path)
            fetch_files.main(cls.project_path)


class RDHPN(RealDataHelper):

    @classmethod
    def setUpClass(cls):
        from sparcur.paths import PennsieveCache
        from sparcur.backends import PennsieveRemote
        cls._cache_class = PennsieveCache
        cls._remote_class = PennsieveRemote
        super().setUpClass()
