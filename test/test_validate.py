import unittest
import pytest
from .common import project_path
from sparcur import schemas as sc
from sparcur import datasets as dat


class TestHierarchy(unittest.TestCase):

    def setUp(self):
        self.ds =  [dat.DatasetStructure(p) for p in project_path.children]

    def tearDown(self):
        pass

    def test_create(self):
        ppattrs = project_path.cache.xattrs()
        for pthing in project_path.rglob('*'):
            if not pthing.skip_cache:
                ptattrs = pthing.cache.xattrs()

    def test_paths(self):
        for d in self.ds:
            for mp in d.meta_paths:
                print(mp)

        pytest.skip('TODO look at the lists here and figure out where they should go.')
        # for example if they are buried many levels too low how do we deal with that?

    def test_dataset(self):
        dsc = sc.DatasetStructureSchema()
        for d in self.ds:
            print(d.data)
            dsc.validate(d.data)

        pytest.skip('TODO look at the lists here and figure out where they should go.')

    def test_tables(self):
        for d in self.ds:
            for p in d.meta_paths:
                for row in dat.Tabular(p):
                    print(row)

    def test_submission(self):
        pass

    def test_dataset_description(self):
        pass

    def test_subjects(self):
        pass
