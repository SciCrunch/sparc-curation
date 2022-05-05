import unittest
from pathlib import Path
import pytest
from sparcur import pipelines as pipes
from .common import (examples_root,
                     project_path,
                     RDHPN,
                     )


class TestDatasetDescription(unittest.TestCase):
    source = examples_root / 'dd-pie.csv'

    def test_dd_pie_p(self):
        p = pipes.DatasetDescriptionFilePipeline(self.source, None, None)
        data = p.data
        # TODO test as subpipeline ?


class PipelineHelper:

    @classmethod
    def setUpClass(cls):
        cls.project_path = project_path
        cls.datasets = list(cls.project_path.children)

    def _path_to_pipe(self, dataset_path):
        """ FIXME TODO this needs to be simplified """
        class context:
            path = dataset_path.resolve()
            id = path.id
            uri_api = path.as_uri()
            uri_human = path.as_uri()

        class lifters:
            # minimal set
            id = context.id
            remote = context.path._cache_class._remote_class._remote_type
            folder_name = context.path.name
            uri_api = context.uri_api
            uri_human = context.uri_human
            timestamp_export_start = None

            # extended requirements (annoying)
            # FIXME these need to be removed
            techniques = 'FAKE TECHNIQUE'
            award_manual = 'FAKE TOTALLY NOT AN AWARD'
            modality = 'THE MODALITY THE HAS BECOME ONE WITH NOTHINGNESS'
            organ_term = 'ilxtr:NOGGIN'  # expects a curie or iri
            protocol_uris = ('https://example.org/TOTALLY-NOT-A-REAL-URI',)
            affiliations = lambda _: None

        pipe = pipes.PipelineEnd(dataset_path, lifters, context)
        return pipe

    def test_pipeline_end(self):
        pipelines = []
        for dataset_path in self.test_datasets:
            pipe = self._path_to_pipe(dataset_path)
            pipelines.append(pipe)

        bads = []
        fails = []
        errors = []
        for p in pipelines:
            try:
                d = p.data
                if 'errors' in d:
                    errors.append(d.pop('errors'))
                    fails.append(d)
                    if 'submission_errors' in d['status']:
                        d['status'].pop('submission_errors')
                    if 'curation_errors' in d['status']:
                        d['status'].pop('curation_errors')
                    if 'errors' in d['inputs']:
                        d['inputs'].pop('errors')
            except Exception as e:
                raise e
                bads.append((e, p))

        assert not bads, bads


class TestPipelines(unittest.TestCase):
    pass


class TestPipelinesRealPN(RDHPN, PipelineHelper, unittest.TestCase):
    # RealDataHelper needs to resolve first to get correct setUpClass
    pass
