import shutil
from pathlib import PurePosixPath
from datetime import datetime
from augpathlib import PathMeta
from sparcur import config
from sparcur import exceptions as exc
from sparcur.paths import Path
from sparcur.paths import LocalPath, PrimaryCache, RemotePath
from sparcur.paths import XattrCache, SymlinkCache
from sparcur.state import State
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
