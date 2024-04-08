import csv
import json
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
from sparcur.core import JEncode
from sparcur.utils import fromJson, change_rcs
from sparcur.paths import PathL
from .common import examples_root, template_root, project_path, temp_path, dpie_path
from .test_pipelines import PipelineHelper as PH

template_root = aug.RepoPath(template_root)


def ser_deser(blob):
    # use to test roundtrip to and from json
    s = json.dumps(blob, cls=JEncode)
    reblob = json.loads(s)
    ir = fromJson(reblob)


class PipelineHelper(PH):

    def ser_deser(self, blob):
        ser_deser(blob)

    @classmethod
    def setUpClass(cls):
        cls.test_datasets = []


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
            'dataset-template-2.1.0',
            'HEAD',)

    _cry_base = None

    def setUp(self):
        if temp_path.exists():
            temp_path.rmtree()

        temp_path.mkdir()

        self.file = template_root / self.template

    def tearDown(self):
        temp_path.rmtree()

    def _cry(self, tfn, rcfs, should_embed=True):
        base = examples_root / self._cry_base
        tf = temp_path / tfn
        change_rcs(base, tf, rcfs)
        obj = self.metadata_file_class(tf)
        try:
            value = obj.data
            if should_embed:
                assert 'errors' in value, 'should have embedded errors'
                blobs = [eb for eb in value['errors'] if 'error_type' in eb and eb['error_type'] == exc.MalformedHeaderError]
                assert blobs, 'embedded errors should be MalformedHeaderError'
            else:
                assert False, 'should have failed'
        except exc.MalformedHeaderError as e:
            print(e)

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
        ser_deser(value)
        assert not [k for k in value if not isinstance(k, str)]

    def test_submission_multi_row_error_no_values(self):
        tf = examples_root / 'submission-multi-row-error-no-values.csv'
        obj = self.metadata_file_class(tf)
        value = obj.data
        pprint.pprint(value)
        ser_deser(value)
        assert not [k for k in value if not isinstance(k, str)]

    def test_submission_data_in_definition(self):
        tf = examples_root / 'submission-data-in-definition.csv'
        obj = self.metadata_file_class(tf)
        value = obj.data
        pprint.pprint(value)
        ser_deser(value)
        assert not [k for k in value if not isinstance(k, str)]

    def test_submission_matched_at_header(self):
        tf = examples_root / 'submission-matched-alt-header.csv'
        obj = self.metadata_file_class(tf)
        try:
            value = obj.data
            assert 'errors' in value, 'should have embedded errors'
            blobs = [eb for eb in value['errors'] if 'error_type' in eb and eb['error_type'] == exc.MalformedHeaderError]
            assert blobs, 'embedded errors should be MalformedHeaderError'
            assert 'submission' not in value, 'this file should be empty'
        except exc.MalformedHeaderError:
            pass

    def test_sm_ot(self):
        tf = examples_root / 'sm-ot.csv'
        obj = self.metadata_file_class(tf)
        value = obj.data
        pprint.pprint(value)
        ser_deser(value)
        assert not [k for k in value if not isinstance(k, str)]

    def test_sm_210(self):
        expect_error = (
            'sm-210-sparc-na.csv',
        )
        tfs = list(examples_root.glob('sm-210*.csv'))
        out = []
        bads = []
        for tf in tfs:
            obj = self.metadata_file_class(tf)
            value = obj.data
            pipeline = self.pipe(tf, None, None)
            data = pipeline.data
            # TODO ser_deser
            out.append((tf, value, data))
            if ('errors' in data and tf.name not in expect_error or
                'errors' not in data and tf.name in expect_error):
                bads.append((tf, value, data))

        assert not bads, bads

    def test_versions(self):
        self._versions()


class TestDatasetDescription(Helper, unittest.TestCase):
    template = 'dataset_description.xlsx'
    metadata_file_class = DatasetDescriptionFile
    pipe = pipes.DatasetDescriptionFilePipeline
    use_examples = 2
    canary_key = 'contributors'
    _cry_base = 'dd-pie.csv'
    
    def test_dataset_description(self):
        pass

    def test_dd_pie(self):
        tf = examples_root / 'dd-pie.csv'
        obj = self.metadata_file_class(tf)
        value = obj.data
        pprint.pprint(value)
        ser_deser(value)

    def test_dd_cry_null_alt(self):
        self._cry('dd-cry-null-alt.csv', [[0, 3, lambda _: '']])

    def test_dd_cry_null_header(self):
        self._cry('dd-cry-null-header.csv', [[3, 0, lambda _: '']])

    def test_dd_cry_dupe_header(self):
        self._cry('dd-cry-dupe-header.csv', [[6, 0, lambda _: 'Contributor ORCID ID']])

    def test_dd_cry_multi(self):
        self._cry(
            'dd-cry-multi.csv',
            [[0, 4, lambda _: ''],  # null_alt
             [3, 0, lambda _: ''],  # null_header
             [6, 0, lambda _: 'Contributor ORCID ID'],  # dupe_header
             # dupe_alt not fatal here
             ])

    def test_dd_pie_pipeline(self):
        tf = examples_root / 'dd-pie.csv'
        pipeline = self.pipe(tf, None, None)
        data = pipeline.data
        pprint.pprint(data)
        ser_deser(data)

    def test_versions(self):
        self._versions()


class TestSubjectsFile(Helper, unittest.TestCase):
    template = 'subjects.xlsx'
    metadata_file_class = SubjectsFile
    pipe = pipes.SubjectsFilePipeline
    _cry_base = 'su-pie.csv'

    def test_su_pie(self):
        tf = examples_root / 'su-pie.csv'
        obj = self.metadata_file_class(tf)
        value = obj.data
        pprint.pprint(value)
        ser_deser(value)
        assert [s for s in value['subjects'] if 'subject_id' in s]

    def test_su_cry(self):
        tf = examples_root / 'su-cry.csv'
        obj = self.metadata_file_class(tf)
        value = obj.data
        pprint.pprint(value)
        ser_deser(value)
        assert [s for s in value['subjects'] if 'subject_id' in s]

    def test_su_cry_alt(self):
        self._cry('su-cry-null-alt.csv', [[0, 4, lambda _: '']])

    def test_su_cry_header(self):
        self._cry('su-cry-null-header.csv', [[4, 0, lambda _: '']])

    def test_su_cry_multi(self):
        self._cry(
            'su-cry-multi.csv',
            [[0, 3, lambda _: ''],  # null_alt
             [4, 0, lambda _: ''],  # null_header
             [0, 5, lambda _: 'strain'],  # dupe_alt  # not fatal
             [6, 0, lambda _: 'bb-2'],  # dupe_header
             ])

    def test_versions(self):
        self._versions()


class TestSamplesFile(Helper, unittest.TestCase):
    template = 'samples.xlsx'
    metadata_file_class = SamplesFile
    pipe = pipes.SamplesFilePipeline
    _cry_base = 'sa-pie.csv'

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
        ser_deser(value)
        derp = list(value['samples'][0])

    def test_sa_pie_no_overwrite(self):
        base = examples_root / 'sa-pie.csv'
        tf = temp_path / 'sa-cry-no-overwrite-on-dupe.csv'
        change_rcs(base, tf, [[0, 23, lambda _: 'header 1']])
        obj = self.metadata_file_class(tf)
        # print([(i, c) for i, c in enumerate(list(obj._t())[0])])
        value = obj.data
        derp = list(value['samples'][0])
        assert 'header_1' in derp, derp
        assert len([h for h in derp if h.startswith('header_1')]) > 1, 'silent overwrite happening'

    def test_sa_cry_null_pk_0(self):
        self._cry('sa-cry-null-pk-0.csv', [[4, 0, lambda _: '']])

    def test_sa_cry_null_pk_1(self):
        self._cry('sa-cry-null-pk-1.csv', [[4, 1, lambda _: '']])

    def test_sa_cry_dupe_pk(self):
        self._cry('sa-cry-dupe-pk.csv', [[6, 1, lambda _: 'slice-3']])

    def test_sa_cry_dupe_alt(self):
        # alt dupes are warnings instead of fatal?
        # should this be the case? probably not, it should be fatal
        self._cry('sa-cry-dupe-alt.csv', [[0, 5, lambda _: 'specimen anatomical location']])

    def test_sa_cry_multi(self):
        self._cry(
            'sa-cry-multi.csv',
            [[0, 3, lambda _: ''],  # null_alt
             [4, 0, lambda _: ''],  # null_header 0
             [8, 1, lambda _: ''],  # null_header 1
             [0, 5, lambda _: 'specimen anatomical location'],  # dupe_alt  # not fatal atm
             [6, 1, lambda _: 'slice-3'],  # dupe_header
             ])

    def test_versions(self):
        self._versions()


class TestDatasetPie(PipelineHelper, unittest.TestCase):

    def setUp(self):
        self.dataset = PathL(dpie_path)
        self.test_datasets = [self.dataset]
