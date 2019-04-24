import unittest
import pytest
from .common import project_path

from sparcur.curation import get_datasets, FTLax

from sparcur import validate as vldt


class TestHierarchy(unittest.TestCase):
    def setUp(self):
        self.ds, self.dsd = get_datasets(project_path)

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
        self.ds, self.dsd = get_datasets(project_path, FTC=FTLax)


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
