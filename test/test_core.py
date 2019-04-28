import unittest
from sparcur.core import OrcidId
from sparcur.core import cache_hash, argspector


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
