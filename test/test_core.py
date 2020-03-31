import unittest
from sparcur.core import adops, DictTransformer
from sparcur.derives import Derives as De


class Examples:
    # yield expect, data, args
    @property
    def add(self):
        yield {'hello': 'world'}, {}, (['hello'], 'world')
        yield {'hello': {'there': {'general': 'world'}}}, {}, (['hello', 'there', 'general'], 'world')
        yield {'hello': {'there': {'general': 'world'}}}, {'hello': {}}, (['hello', 'there', 'general'], 'world')

    @property
    def add_error(self):
        yield {'hello':'1'}, {'hello':'1'}, (['hello'], 'there')
        yield {'hello':'2'}, {'hello':'2'}, ([], 'there')
        yield {'hello':'3'}, {'hello':'3'}, ('lolol', 'there')  # this might succeed :x indeed it did
        yield {'hello': {'there': {'general': 'world'}}}, {'hello': 'OH NO'}, (['hello', 'there', 'general'], 'world')

    @property
    def get(self):
        yield {'hello': 'world'}, {'hello','world'}, (['hello'],)
        yield {'hello': {'there': {'general': 'world'}}}, {}, (['hello', 'there', 'general'],)

    @property
    def get_error(self):
        yield {'hello': 'world'}, {'hello','world'}, (['another'],)

    @property
    def delete(self):
        yield {}, {'hello','world'}, (['hello'],)
        yield ({'hello': {'there': {'keep': 'MEEE'}}},
               {'hello': {'there': {'general': 'world', 'keep': 'MEEE'}}},
               (['hello', 'there', 'general'],))

    @property
    def delete_error(self):
        yield {'hello': 'world'}, {'hello','world'}, (['hello', 'world'],)

    @property
    def copy(self):
        yield {'another':'world', 'hello': 'world'}, {'hello': 'world'}, (['hello'], ['another'])

    @property
    def copy_error(self):
        yield {'another':'world', 'hello': 'world'}, {'another':'world', 'hello': 'world'}, (['hello'], ['another'])
        yield {'another':'world', 'hello': 'world'}, {'hello': 'world'}, ([], ['another'])
        yield {'another':'world', 'hello': 'world'}, {'hello': 'world'}, (['hello'], [])  # dicts are closed world can't break on through
        yield {'another':'world', 'hello': 'world'}, {'hello': 'world'}, (['no-source'], ['oops'])

    @property
    def move(self):
        yield {'another':'world'}, {'hello': 'world'}, (['hello'], ['another'])

    @property
    def move_error(self):
        yield {'another':'1'}, {}, (['hello'], ['another'])
        yield {'another':'2'}, {'another':0, 'hello':'world'}, (['hello'], ['another'])


class TestDer(unittest.TestCase):
    def test_der(self):
        assert De.contributor_name('b, a') == ('a', 'b')


class ExamplesDT:

    @property
    def derive(self):
        yield ({'a': 2, 'b': 3, 'c': 5, 'd': 6, 'e': 1},
               {'a': 2, 'b': 3}, [[[['a'], ['b']],
                                   lambda a, b: (a + b, a * b),
                                   [['c'], ['d']]],
                                  # look you can chain them!
                                  [[['c'], ['d']],
                                   lambda c, d: (d - c,),
                                   [['e']]]])
        yield ({'name': 'b, a', 'first_name': 'a', 'last_name': 'b'},
               {'name': 'b, a'}, [[[['name']],
                                   De.contributor_name,
                                   [['first_name'], ['last_name']]]])

        # this is no longer an error because we allow TypeError and
        # catch the issue using the schema
        yield ({'a': 1}, {'a': 1}, [[[['a']],
                                     # TODO is there any way to catch this information early ?
                                     lambda _: None,  # previously None would include a TypeError
                                     [['b']]]])

    @property
    def lift(self):
        yield {'hello':'there'}, {'hello': 'world'}, [[['hello'], lambda v: 'there']]


def failed_to_error(function, data, args):
    try:
        function(data, *args)
        print(function, data, args)
        return True
    except BaseException as e:
        return False


class Populator:
    apply = True
    @classmethod
    def populate(cls):
        for name in cls.functions:
            name_error = name + '_error'
            name_negative = name + '_negative'

            test_name = 'test_' + name
            function_to_test = getattr(cls.to_test, name)
            def fun(self, name=name, function=function_to_test):
                bads = [(expect, data, args)
                        for expect, data, args in getattr(self, name)
                        if not function(data, *(args if cls.apply else (args,))) and data != expect]
                assert not bads, bads

            setattr(cls, test_name, fun)

            if hasattr(cls, name_error):
                test_name_error = 'test_' + name_error
                def efun(self, name=name_error, function=function_to_test):
                    bads = [(expect, data, args)
                            for expect, data, args in getattr(self, name)
                            if failed_to_error(function, data, (args if cls.apply else (args,)))]
                    assert not bads, bads

                setattr(cls, test_name_error, efun)

            if hasattr(cls, name_negative):
                test_name_negative = 'test_' + name_negative
                def nfun(self, name=name_negative, function=function_to_test):
                    bads = [(expect, data, args)
                            for expect, data, args in getattr(self, name)
                            if not function(data, *(args if cls.apply else (args,))) and data == expect]
                    assert not bads, bads

                setattr(cls, test_name_negative, nfun)


class TestAdops(Examples, Populator, unittest.TestCase):
    functions = 'add', 'copy', 'move'
    to_test = adops
TestAdops.populate()


class TestDictTransformer(ExamplesDT, Populator, unittest.TestCase):
    functions = 'lift', 'derive'
    to_test = DictTransformer
    apply = False
TestDictTransformer.populate()
