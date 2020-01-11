import unittest
import augpathlib as aug
from sparcur.datasets import Tabular, DatasetDescriptionFile
from .common import template_root, project_path, temp_path

template_root = aug.RepoPath(template_root)


class TestTabular(unittest.TestCase):
    def setUp(self):
        self.pp = probject_path


class TestDatasetDescription(unittest.TestCase):
    template = 'dataset_description.xlsx'
    urg = DatasetDescriptionFile
    
    refs = ('d8a6aa5f83021b3b9ea208c295a19051ffe83cd9',  # located in working dir not resources
            'dataset-template-1.1',
            'dataset-template-1.2',
            'dataset-template-1.2.1',
            'dataset-template-1.2.2',
            'dataset-template-1.2.3',) 

    def setUp(self):
        if temp_path.exists():
            temp_path.rmtree()

        temp_path.mkdir()

        self.file = template_root / self.template

    def tearDown(self):
        temp_path.rmtree()

    def test_dataset_description(self):
        pass

    def test_versions(self):
        tf = temp_path / 'test-file.xlsx'
        for ref in self.refs:
            if ref.startswith('d8a'):
                file = template_root.working_dir / template_root.name / self.template
                version = None
            else:
                file = self.file
                version = ref.rsplit('-', 1)[-1]

            with open(tf, 'wb') as f:
                f.write(file.show(ref))

            obj = self.urg(tf)
            obj.data
