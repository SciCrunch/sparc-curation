import unittest
import pytest
from .common import project_path

from sparcur import schemas as sc
from sparcur import datasets as dat
from sparcur import validate as vldt
from sparcur.paths import Path


class TestHierarchy(unittest.TestCase):
    def setUp(self):
        self.ds =  [dat.DatasetStructure(p) for p in Path(project_path).children]

    def tearDown(self):
        pass

    def test_create(self):
        ppattrs = project_path.cache.xattrs()
        for pthing in project_path.rglob('*'):
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


class TestLax(TestHierarchy):
    def setUp(self):
        self.ds =  [dat.DatasetStructureLax(p) for p in Path(project_path).children]


class TestStage(unittest.TestCase):
    def test_simple(self):
        i = 'hello world'
        s = vldt.Stage(i)
        o = s.output
        assert o == i

class TestHeader(unittest.TestCase):
    def test_simple(self):
        i = ['a', 'b', 'c', 'd']
        s = vldt.Header(i)
        o = s.output
        assert o == i
