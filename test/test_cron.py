import unittest


class TestCron(unittest.TestCase):

    def test_import(self):
        from sparcur import sparcron

    def test_sheet_update(self):
        from sparcur import sparcron
        sparcron.check_sheet_updates()
