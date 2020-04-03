import os
import atexit
import shutil
from tempfile import gettempdir
from pathlib import PurePosixPath
from datetime import datetime
import pytest
from augpathlib import PathMeta
from augpathlib.utils import onerror_windows_readwrite_remove
from sparcur import config
from sparcur import exceptions as exc
from sparcur.paths import Path
from sparcur.paths import LocalPath, PrimaryCache, RemotePath
from sparcur.paths import SymlinkCache
from sparcur.state import State
from sparcur.datasets import DatasetDescriptionFile
from sparcur.curation import PathData, Integrator
from sparcur.blackfynn_api import FakeBFLocal
this_file = Path(__file__)
examples_root = this_file.parent / 'examples'
template_root = this_file.parent.parent / 'resources/DatasetTemplate'
print(template_root)
_pid = os.getpid()
path_project_container = this_file.parent / f'test_local-{_pid}'
project_path = path_project_container / 'test_project'
fake_organization = 'N:organization:fake-organization-id'
project_path_real = path_project_container / 'UCSD'
test_organization = 'N:organization:ba06d66e-9b03-4e3d-95a8-649c30682d2d'
test_dataset = 'N:dataset:5d167ba6-b918-4f21-b23d-cdb124780da1'
temp_path = Path(gettempdir(), f'.sparcur-testing-base-{_pid}')

onerror = onerror_windows_readwrite_remove if os.name == 'nt' else None

SKIP_NETWORK = ('SKIP_NETWORK' in os.environ or
                'FEATURES' in os.environ and 'network-sandbox' in os.environ['FEATURES'])
skipif_no_net = pytest.mark.skipif(SKIP_NETWORK, reason='Skipping due to network requirement')

ddih = DatasetDescriptionFile.ignore_header  # save original skips
DatasetDescriptionFile.ignore_header = tuple(_ for _ in ddih if _ != 'example')  # use the example values for tests

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

DIVISION_SLASH = '\u2215'  # what have we done


def mk_fldr_meta(fldr_path, ftype='collection', id=None):
    altid = f'N:{ftype}:' + fldr_path.as_posix()
    _id = id if id is not None else altid
    _id = _id.replace('/', DIVISION_SLASH)  # ARGH there must be a way to ensure file system safe ids :/
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
