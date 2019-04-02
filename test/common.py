import shutil
from datetime import datetime
from sparcur import config
from sparcur.paths import Path
from sparcur.curation import Version1Header
this_file = Path(__file__)
template_root = this_file.parent.parent / 'resources/DatasetTemplate'
print(template_root)
project_path = this_file.parent / 'test_local/test_project'

config.local_storage_prefix = project_path.parent

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

def mk_fldr_meta(fldr_path, ftype='collection'):
    st = fldr_path.stat()
    # NOTE st_mtime -> modified time of the file contents (data)
    # NOTE st_ctime -> changed time of the file status (metadata)
    # linux does not have a unified way to bet st_btime aka st_crtime which is birth time or created time
    return {'bf.id': f'N:{ftype}:' + fldr_path.as_posix(),
            'bf.created_at': datetime.fromtimestamp(st.st_mtime).isoformat(),
            'bf.updated_at': datetime.fromtimestamp(st.st_mtime).isoformat()}


def mk_file_meta(fp):
    st = fp.stat()
    return {'bf.id': 'N:package:' + fp.as_posix(),
            'bf.file_id': 0,
            'bf.size': st.st_size,
            # FIXME timezone
            'bf.created_at': datetime.fromtimestamp(st.st_mtime).isoformat(),
            'bf.updated_at': datetime.fromtimestamp(st.st_mtime).isoformat(),
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
    attrs = mk_fldr_meta(project_path, 'organization')
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


