import unittest
import pytest
from .common import project_path

from sparcur import validate as vldt
from sparcur.paths import Path
from sparcur.curation import FTLax, FThing


class TestHierarchy(unittest.TestCase):
    def setUp(self):
        self.ds =  [FThing(p) for p in Path(project_path).children]

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

    def test_dataset(self):
        for d in self.ds:
            print(d.data)
            d.schema.validate(d.data)

        pytest.skip('TODO look at the lists here and figure out where they should go.')

    def test_tables(self):
        for d in self.ds:
            for table in d._meta_tables:
                for row in table:
                    print(row)

    def test_things(self):
        for d in self.ds:
            for thing in d.meta_sections:
                print(thing.__class__.__name__, thing.data)

        pytest.skip('TODO look at the lists here and figure out where they should go.')

    def test_submission(self):
        pass

    def test_dataset_description(self):
        pass

    def test_subjects(self):
        pass


class TestLax(TestHierarchy):
    def setUp(self):
        self.ds =  [FTLax(p) for p in Path(project_path).children]


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
