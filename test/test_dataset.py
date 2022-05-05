import csv
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
from .common import examples_root, template_root, project_path, temp_path

template_root = aug.RepoPath(template_root)


class TestTabular(unittest.TestCase):
    def setUp(self):
        self.pp = probject_path


class Helper:
    use_examples = False
    canary_key = False

    refs = ('d8a6aa5f83021b3b9ea208c295a19051ffe83cd9',  # located in working dir not resources
            'dataset-template-1.1',
            'dataset-template-1.2',
            'dataset-template-1.2.1',
            'dataset-template-1.2.2',
            'dataset-template-1.2.3',
            'dataset-template-2.0.0',
            'HEAD',)

    def setUp(self):
        if temp_path.exists():
            temp_path.rmtree()

        temp_path.mkdir()

        self.file = template_root / self.template

    def tearDown(self):
        temp_path.rmtree()

    def _versions(self):
        tf = temp_path / 'test-file.xlsx'
        tfc = temp_path / 'test-file.csv'
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

            obj = self.metadata_file_class(tf, template_schema_version=version)
            if self.use_examples:
                recs = list(obj._t())
                test_values = ['test value'] + [r[self.use_examples] for r in recs[1:]]
                rows = [r + [tv] for r, tv in zip(recs, test_values)]
                with open(tfc, 'wt') as f:
                    writer = csv.writer(f)
                    writer.writerows(rows)

                obj = self.metadata_file_class(tfc, template_schema_version=version)

            try:
                data = obj.data
                # for dataset_description_file
                # we expect template_schema_version to be duplicated
                # because it is present in value and test_value
                # columns, which is ok for this stage
                if self.canary_key and self.canary_key not in data:
                    raise ValueError('canary missing, something is wrong with the test')
                if ref == 'HEAD':
                    # breakpoint()  # XXX testing here
                    'sigh'
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

    def test_submission_multi_column_extra_row(self):
        tf = examples_root / 'submission-multi-column-extra-row.csv'
        obj = self.metadata_file_class(tf)
        value = obj.data
        pprint.pprint(value)
        assert not [k for k in value if not isinstance(k, str)]

    def test_submission_multi_row_error_no_values(self):
        tf = examples_root / 'submission-multi-row-error-no-values.csv'
        obj = self.metadata_file_class(tf)
        value = obj.data
        pprint.pprint(value)
        assert not [k for k in value if not isinstance(k, str)]

    def test_submission_data_in_definition(self):
        tf = examples_root / 'submission-data-in-definition.csv'
        obj = self.metadata_file_class(tf)
        value = obj.data
        pprint.pprint(value)
        assert not [k for k in value if not isinstance(k, str)]

    def test_submission_matched_at_header(self):
        tf = examples_root / 'submission-matched-alt-header.csv'
        obj = self.metadata_file_class(tf)
        try:
            value = obj.data
            assert False, "Should have failed."
        except exc.MalformedHeaderError:
            pass

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
    use_examples = 2
    canary_key = 'contributors'
    
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

    def test_su_cry(self):
        tf = examples_root / 'su-cry.csv'
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

    def test_samples_duplicate_ids(self):
        tf = examples_root / 'samples-duplicate-ids.csv'
        obj = self.metadata_file_class(tf)
        try:
            value = obj.data
            assert False, 'should have failed due to duplicate keys'
        except:
            pass

    def test_sa_pie(self):
        tf = examples_root / 'sa-pie.csv'
        obj = self.metadata_file_class(tf)
        value = obj.data
        pprint.pprint(value)
        derp = list(value['samples'][0])
        assert 'same_header' in derp, derp

    def test_versions(self):
        self._versions()
