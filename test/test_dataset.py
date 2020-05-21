import pprint
import unittest
import pytest
import augpathlib as aug
from sparcur.datasets import (Tabular,
                              DatasetDescriptionFile,
                              SubmissionFile,
                              SubjectsFile,
                              SamplesFile,
                              remove_rule,)
from sparcur import pipelines as pipes
from sparcur import exceptions as exc
from .common import examples_root, template_root, project_path, temp_path, ddih

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
        self._ddih = DatasetDescriptionFile.ignore_header
        DatasetDescriptionFile.ignore_header = ddih

    def tearDown(self):
        DatasetDescriptionFile.ignore_header = self._ddih
        temp_path.rmtree()

    def _versions(self):
        tf = temp_path / 'test-file.xlsx'
        bads = []
        for ref in self.refs:
            if ref.startswith('d8a'):
                wd = template_root.working_dir
                if wd is None:
                    pytest.skip('not in repo')

                file = wd / template_root.name / self.template
                version = None
            else:
                file = self.file
                version = ref.rsplit('-', 1)[-1]

            with open(tf, 'wb') as f:
                f.write(file.show(ref))

            obj = self.metadata_file_class(tf, schema_version=version)
            try:
                obj.data
            except exc.MalformedHeaderError as e:
                if version == '1.1':
                    # known bad
                    pass
                else:
                    bads.append((ref, e))
            except BaseException as e:
                bads.append((ref, e))

        assert not bads, bads


class TestSubmissionFile(Helper, unittest.TestCase):
    template = 'submission.xlsx'
    metadata_file_class = SubmissionFile
    pipe = pipes.SubmissionFilePipeline
    def test_sm_ot(self):
        tf = examples_root / 'sm-ot.csv'
        obj = self.metadata_file_class(tf)
        value = obj.data
        pprint.pprint(value)
        assert not [k for k in value if not isinstance(k, str)]

    def test_versions(self):
        self._versions()


class TestDatasetDescription(Helper, unittest.TestCase):
    template = 'dataset_description.xlsx'
    metadata_file_class = DatasetDescriptionFile
    pipe = pipes.DatasetDescriptionFilePipeline
    
    def test_dataset_description(self):
        pass

    def test_dd_pie(self):
        tf = examples_root / 'dd-pie.csv'
        obj = self.metadata_file_class(tf)
        value = obj.data
        pprint.pprint(value)

    def test_dd_pie_pipeline(self):
        tf = examples_root / 'dd-pie.csv'
        pipeline = self.pipe(tf, None, None)
        data = pipeline.data
        pprint.pprint(data)

    def test_versions(self):
        self._versions()


class TestSubjectsFile(Helper, unittest.TestCase):
    template = 'subjects.xlsx'
    metadata_file_class = SubjectsFile
    pipe = pipes.SubjectsFilePipeline

    def test_su_pie(self):
        tf = examples_root / 'su-pie.csv'
        obj = self.metadata_file_class(tf)
        value = obj.data
        pprint.pprint(value)
        assert [s for s in value['subjects'] if 'subject_id' in s]

    def test_versions(self):
        self._versions()


class TestSamplesFile(Helper, unittest.TestCase):
    template = 'samples.xlsx'
    metadata_file_class = SamplesFile
    pipe = pipes.SamplesFilePipeline

    def test_sa_pie(self):
        tf = examples_root / 'sa-pie.csv'
        obj = self.metadata_file_class(tf)
        value = obj.data
        pprint.pprint(value)

    def test_versions(self):
        self._versions()
