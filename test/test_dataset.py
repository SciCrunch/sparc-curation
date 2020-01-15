import pprint
import unittest
import augpathlib as aug
from sparcur.datasets import (Tabular,
                              DatasetDescriptionFile,
                              SubmissionFile,
                              SubjectsFile,
                              SamplesFile,
                              remove_rule,)
from .common import examples_root, template_root, project_path, temp_path

template_root = aug.RepoPath(template_root)


class TestTabular(unittest.TestCase):
    def setUp(self):
        self.pp = probject_path


class Helper:
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

    def _versions(self):
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


class TestSubmissionFile(Helper, unittest.TestCase):
    template = 'submission.xlsx'
    urg = SubmissionFile
    def test_sm_ot(self):
        tf = examples_root / 'sm-ot.csv'
        obj = self.urg(tf)
        value = obj.data
        pprint.pprint(value)

    def test_versions(self):
        self._versions()


class TestDatasetDescription(Helper, unittest.TestCase):
    template = 'dataset_description.xlsx'
    urg = DatasetDescriptionFile
    
    def test_dataset_description(self):
        pass

    def test_dd_pie(self):
        tf = examples_root / 'dd-pie.csv'
        obj = self.urg(tf)
        value = obj.data
        pprint.pprint(value)

    def test_versions(self):
        self._versions()


class TestSubjectsFile(Helper, unittest.TestCase):
    template = 'subjects.xlsx'
    urg = SubjectsFile

    def test_su_pie(self):
        tf = examples_root / 'su-pie.csv'
        obj = self.urg(tf)
        value = obj.data
        pprint.pprint(value)
        breakpoint()

    def test_versions(self):
        self._versions()


class TestSamplesFile(Helper, unittest.TestCase):
    template = 'samples.xlsx'
    urg = SamplesFile

    def test_sa_pie(self):
        tf = examples_root / 'sa-pie.csv'
        obj = self.urg(tf)
        value = obj.data
        pprint.pprint(value)

    def test_versions(self):
        self._versions()
