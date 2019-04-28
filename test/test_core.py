import unittest
from sparcur.core import OrcidId
from sparcur.core import cache_hash, argspector
from sparcur.core import adops, DictTransformer


class TestOrcidId(unittest.TestCase):
    def test_validate(self):
        orcids = ('https://orcid.org/0000-0002-1825-0097',
                  'https://orcid.org/0000-0001-5109-3700',
                  'https://orcid.org/0000-0002-1694-233X')
        ids = [OrcidId(orcid) for orcid in orcids]
        bads = [orcid for orcid in ids if not orcid.checksumValid]
        assert not bads, str(bads)


class TestCacheHash(unittest.TestCase):
    def test_argspector(self):
        class c:
            def f(self, value): pass
            def g(self, value=None): pass
            def h(self, a, *b, c=None, **d): pass

        def f(value): pass
        def g(value=None): pass
        def h(a, *b, c=None, **d): pass

        ic = c()

        spectors = [
            [argspector(c.f), [((ic, 'f'), {}),]],
            [argspector(c.g), [((ic, 'g'), {}),
                               ((ic,), {'value':None}),]],
            [argspector(c.h), [((ic, 'h'), {}),
                               ((ic, 'a', 'b', 'oops'), dict(c='c', d='d', e='e')),]],
            [argspector(f), [(('f',), {}),]],
            [argspector(g), [(('g',), {}),
                             ([], {'value':None}),]],
            [argspector(h), [(('h',), {}),
                             (('a', 'b', 'oops'), dict(c='c', d='d', e='e')),]],
            ]


        for spector, arg_sets in spectors:
            for args, kwargs in arg_sets:
                pairs = list(spector(*args, **kwargs))
                cache_hash(pairs)


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


class ExamplesDT:
    @property
    def lift(self):
        yield {'hello':'there'}, {'hello', 'world'}, (['hello'], lambda v: 'there')


def failed_to_error(function, data, args):
    try:
        function(data, *args)
        print(function, data, args)
        return True
    except BaseException as e:
        return False


class TestAdops(Examples, unittest.TestCase):
    functions = 'add', 'copy', 'move'
    to_test = adops
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
                        if not function(data, *args) and data != expect]
                assert not bads, bads

            setattr(cls, test_name, fun)

            if hasattr(cls, name_error):
                test_name_error = 'test_' + name_error
                def efun(self, name=name_error, function=function_to_test):
                    bads = [(expect, data, args)
                            for expect, data, args in getattr(self, name)
                            if failed_to_error(function, data, args)]
                    assert not bads, bads

                setattr(cls, test_name_error, efun)

            if hasattr(cls, name_negative):
                test_name_negative = 'test_' + name_negative
                def nfun(self, name=name_negative, function=function_to_test):
                    bads = [(expect, data, args)
                            for expect, data, args in getattr(self, name)
                            if not function(data, *args) and data == expect]
                    assert not bads, bads

                setattr(cls, test_name_negative, nfun)


TestAdops.populate()


class TestDictTransformer(Examples, unittest.TestCase):
    pass
