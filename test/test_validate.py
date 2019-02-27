import unittest
from datetime import datetime
from sparcur.core import Path

project_path = Path(__file__).parent / 'test_local/test_project'
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
    return {'bf.id': f'N:{ftype}:' + fldr_path.as_posix(),
            'bf.created_at': datetime.fromtimestamp(st.st_ctime).isoformat(),
            'bf.updated_at': datetime.fromtimestamp(st.st_mtime).isoformat()}


def mk_file_meta(fp):
    st = fp.stat()
    return {'bf.id': 'N:package:' + fp.as_posix(),
            'bf.file_id': 0,
            'bf.size': st.st_size,
            # FIXME timezone
            'bf.created_at': datetime.fromtimestamp(st.st_ctime).isoformat(),
            'bf.updated_at': datetime.fromtimestamp(st.st_mtime).isoformat(),
            'bf.checksum': fp.checksum(),
            # 'bf.old_id': None  # TODO
    }


def mk_required_files(path, suffix='.csv'):
    for file_name in ('submission', 'dataset_description',  'subjects'):
        file_path = path / (file_name + suffix)
        file_path.touch()  # TODO
        file_path.touch()  # TODO
        attrs = mk_file_meta(file_path)
        file_path.setxattrs(attrs)


if not project_path.exists():
    project_path.mkdir(parents=True)
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


class TestHierarchy(unittest.TestCase):
    def setup(self):
        pass

    def teardown(self):
        pass

    def test_create(self):
        ppattrs = project_path.xattrs()
        for pthing in project_path.rglob('*'):
            ptattrs = pthing.xattrs()

    def test_dataset(self):
        pass

    def test_submission(self):
        pass

    def test_dataset_description(self):
        pass

    def test_subjects(self):
        pass
