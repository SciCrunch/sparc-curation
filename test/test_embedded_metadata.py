import os
import unittest
import pytest
from pyontutils.utils import Async, deferred
from sparcur.paths import BlackfynnCache, Path
from sparcur.backends import BlackfynnRemote
from sparcur.config import auth
from sparcur import mbf
from sparcur import schemas as sc
from .common import path_project_container


@pytest.mark.skipif('CI' in os.environ, reason='Requires access to data')
class TestExtractMetadata(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.organization_id = auth.get('blackfynn-organization')
        cls.BlackfynnRemote = BlackfynnRemote._new(Path, BlackfynnCache)
        cls.BlackfynnRemote.init(cls.organization_id)
        cls.anchor = cls.BlackfynnRemote.smartAnchor(path_project_container)
        cls.anchor.local_data_dir_init()
        cls.datasets = list(cls.anchor.children)

    def test_mbf_header(self):
        test_id = 'N:dataset:bec4d335-9377-4863-9017-ecd01170f354'
        test_dataset = [d for d in self.datasets if d.id == test_id][0]
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

        embfs = [mbf.ExtractMBF(x) for x in local_xmls]
        d = embfs[0].asDict()
        blob = [e.asDict() for e in embfs]
        sc
