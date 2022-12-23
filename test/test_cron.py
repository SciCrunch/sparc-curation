import unittest
from .common import skipif_no_net, skipif_ci


@skipif_ci
@skipif_no_net
class TestCron(unittest.TestCase):

    def test_import(self):
        from sparcur import sparcron
        from sparcur.sparcron import core

    def test_sheet_update(self):
        from sparcur.sparcron import core as sparcron
        sparcron.check_sheet_updates()
