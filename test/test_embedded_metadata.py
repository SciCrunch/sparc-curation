import os
import json
import unittest
import pytest
from pyontutils.utils import Async, deferred
from sparcur.core import JEncode
from sparcur.extract import xml as exml
from .common import examples_root, RDHPN

export = False


class TestExtractMetadata(unittest.TestCase):

    def test_new_mbf_format(self):
        x = examples_root / 'mbf-example.xml'
        embf = exml.ExtractXml(x)
        d = embf.asDict()
        errors = d.pop('errors') if 'errors' in d else tuple()
        error_types = set(e['validator'] for es in errors for e in es)
        assert error_types == {'not'} or not error_types, f'unexpected error type! {error_types}'


class ExtractMetadataReal:

    def test_mbf_header(self):
        test_id = 'N:dataset:bec4d335-9377-4863-9017-ecd01170f354'
        test_dataset = [d.cache for d in self.test_datasets if d.cache.id == test_id][0]
        if not list(test_dataset.local.children):
            rchilds = list(test_dataset.rchildren)
            xmls = [c for c in rchilds if c.suffix == '.xml']
            Async(rate=5)(deferred(x.fetch)() for x in xmls if not x.exists())
            #[x.fetch() for x in xmls if not x.exists()]
            local_xmls = [x.local for x in xmls]
        else:
            local_xmls = list(test_dataset.local.rglob('*.xml'))
            if any(p for p in local_xmls if not p.exists()):
                raise BaseException('unfetched children')

        embfs = [exml.ExtractXml(x) for x in local_xmls]
        d = embfs[0].asDict()
        blob = [e.asDict() for e in embfs]
        errors = [b.pop('errors') for b in blob if 'errors' in b]
        error_types = set(e['validator'] for es in errors for e in es)
        if export:
            with open('mbf-test.json', 'wt') as f:
                json.dump(blob, f, indent=2, cls=JEncode)
            with open('mbf-errors.json', 'wt') as f:
                json.dump(errors, f, indent=2, cls=JEncode)

        assert error_types == {'not'} or not error_types, f'unexpected error type! {error_types}'


class TestExtractMetadataRealPN(RDHPN, ExtractMetadataReal, unittest.TestCase):
    pass
