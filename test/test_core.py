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
        yield {'world': [{'hello': 'there'},
                         {'hello': 'there'}]}, {'world': [{},
                                                          {}]}, (['world', int, 'hello'], 'there')
        yield {'world': {'oh': 'no'}}, {'world': {'oh': 'no'}}, (['world', 'hello', int, 'mr'], 'there')

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
        yield {'source': {},
               'target': {'b': 1}}, {'source': {'a': 1},
                                     'target': {}}, (['source', 'a'], ['target', 'b'])

    @property
    def move_error(self):
        yield {'another':'1'}, {}, (['hello'], ['another'])
        yield {'another':'2'}, {'another': 0, 'hello':'world'}, (['hello'], ['another'])

    @property
    def update(self):
        yield [0, 2, 3, 4, 5, 6], [1, 2, 3, 4, 5, 6], ([0], 0)

    @property
    def update_error(self):
        # tuple object does not support item assignment
        yield (0, 2, 3, 4, 5, 6), (1, 2, 3, 4, 5, 6), ([0], 0)


class TestDer(unittest.TestCase):
    def test_der(self):
        assert De.contributor_name('b, a') == ('a', 'b')


class ExamplesDT:

    @property
    def move(self):
        # after before command
        sko = dict(source_key_optional=True)

        # simple moves
        yield ({'other-key': 'something'},
               {'other-key': 'something'},
               [[['not-here'], ['not-here', 'over-there']]],
               sko)

        yield ({'not-here': {'over-there': {}}},
               {'not-here': {}},
               [[['not-here'], ['not-here', 'over-there']]],
               sko)

        yield ({'not-here': {'over-there': []}},
               {'not-here': []},
               [[['not-here'], ['not-here', 'over-there']]],
               sko)

        yield ({'not-here': {'over-there': None}},
               {'not-here': None},
               [[['not-here'], ['not-here', 'over-there']]],
               sko)

        # pattern matching with holes
        yield ({'a': [{'c': 1}, {'c': 2}]},

               {'a': [{'b': 1}, {'b': 2}]},

               [[['a', int, 'b'],
                 ['a', int, 'c']]],
               sko)

        yield ({'a': {'x': [{'c': 1}, {'c': 2}]}},

               {'a': {'x': [{'b': 1}, {'b': 2}]}},

               [[['a', 'x', int, 'b'],
                 ['a', 'x', int, 'c']]],
               sko)

        return  # things not implemented yet
        yield (
            {'d': [{'g': [{'h': 1}, {'h': 2}]},
                   {'g': [{'h': 3}, {'h': 4}]}]},

            {'d': [{'e': [{'f': 1}, {'f': 2}]},
                   {'e': [{'f': 3}, {'f': 4}]}]},

            [[['d', int, 'e', int, 'f'],
              ['d', int, 'g', int, 'h']]],

            sko)

        # TODO it is not clear whether the examples below are moves

        yield ({'d': [{'e': [{}, {}],
                       'k': [1, 2]},
                      {'e': [{}, {}],
                       'k': [3, 4]}]},
               {'d': [{'e': [{'f': 1}, {'f': 2}]},
                      {'e': [{'f': 3}, {'f': 4}]}]},
               [[['d', int, 'e', int, 'f'],
                 ['d', int, 'k', int]]],
               sko)

        yield ({'z': [{'e': [{}, {}],
                       'g': [1, 2]},
                      {'e': [{}, {}],
                       'g': [3, 4]}]},
               {'d': [{'e': [{'f': 1}, {'f': 2}]},
                      {'e': [{'f': 3}, {'f': 4}]}]},
               [[['d', int, 'e', int, 'f'],
                 ['z', int, 'g', int]]],
               sko)

        # note that moving individual elements of a list i.e. ending
        # the source-path with int does not make sense because that
        # would change the structure of the source along the way

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

    @property
    def update(self):
        yield {'hello':[
            'there',
            'there',]}, {'hello': [
                'general',
                'kenobi',
            ]}, [[['hello', int], lambda v: 'there']]

        yield {'hello':[
            {'n': 1},
            {'n': 1}]}, {'hello': [
                {'n': 0},
                {'n': 0}]}, [[['hello', int, 'n'], lambda v: v + 1]]


def failed_to_error(function, data, args, kwargs):
    try:
        function(data, *args, **kwargs)
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
                        for expect, data, args, *kwargs in getattr(self, name)
                        if not function(data,
                                        *(args if cls.apply else (args,)),
                                        **{k:v for kw in kwargs for k, v in kw.items()})
                        and data != expect]
                assert not bads, bads

            setattr(cls, test_name, fun)

            if hasattr(cls, name_error):
                test_name_error = 'test_' + name_error
                def efun(self, name=name_error, function=function_to_test):
                    bads = [(expect, data, args)
                            for expect, data, args, *kwargs in getattr(self, name)
                            if failed_to_error(function,
                                               data,
                                               (args if cls.apply else (args,)),
                                               {k:v for kw in kwargs for k, v in kw.items()})]
                    assert not bads, bads

                setattr(cls, test_name_error, efun)

            if hasattr(cls, name_negative):
                test_name_negative = 'test_' + name_negative
                def nfun(self, name=name_negative, function=function_to_test):
                    bads = [(expect, data, args)
                            for expect, data, args, *kwargs in getattr(self, name)
                            if not function(data,
                                            *(args if cls.apply else (args,)),
                                            **{k:v for kw in kwargs for k, v in kw.items()})
                            and data == expect]
                    assert not bads, bads

                setattr(cls, test_name_negative, nfun)


class TestAdops(Examples, Populator, unittest.TestCase):
    functions = 'add', 'copy', 'move', 'update'
    to_test = adops
TestAdops.populate()


class TestDictTransformer(ExamplesDT, Populator, unittest.TestCase):
    functions = 'lift', 'derive', 'update', 'move',
    to_test = DictTransformer
    apply = False
TestDictTransformer.populate()
