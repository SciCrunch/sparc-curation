import copy
import pickle
import unittest
import pytest
import idlib
from sparcur.utils import BlackfynnId, PennsieveId
from idlib.streams import HelpTestStreams


class TestBlackfynnId(unittest.TestCase):

    _id_class = BlackfynnId
    uuids = (('e4d16d59-c963-4d9c-af2f-2e40853881c3', 'package'),)
    cases = (
                     'package:e4d16d59-c963-4d9c-af2f-2e40853881c3',
                    'N:package:e4d16d59-c963-4d9c-af2f-2e40853881c3',
        'https://api.blackfynn.io/packages/N:package:e4d16d59-c963-4d9c-af2f-2e40853881c3',
        'https://api.blackfynn.io/packages/N:package:e4d16d59-c963-4d9c-af2f-2e40853881c3/',
        'https://api.blackfynn.io/packages/N:package:e4d16d59-c963-4d9c-af2f-2e40853881c3/files/1222508',
        'https://api.blackfynn.io/packages/N:package:e4d16d59-c963-4d9c-af2f-2e40853881c3/files/1222508/',
'https://app.blackfynn.io/N:organization:618e8dd9-f8d2-4dc4-9abb-c6aaab2e78a0/datasets/N:dataset:fce3f57f-18ea-4453-887e-58a885e90e7e/overview',
'https://app.blackfynn.io/N:organization:618e8dd9-f8d2-4dc4-9abb-c6aaab2e78a0/datasets/N:dataset:834e182d-b52c-4389-ad09-6ec9467f3b55/viewer/N:package:a44040e7-5d30-4930-aaac-3aa238ea9081',
'https://app.blackfynn.io/N:organization:618e8dd9-f8d2-4dc4-9abb-c6aaab2e78a0/datasets/N:dataset:fce3f57f-18ea-4453-887e-58a885e90e7e/files/N:collection:5bf942a5-10e4-414e-bba6-1f41b053675e',
'https://app.blackfynn.io/N:organization:618e8dd9-f8d2-4dc4-9abb-c6aaab2e78a0/datasets/N:dataset:fce3f57f-18ea-4453-887e-58a885e90e7e/files/lol/N:package:457b1339-ac9c-4232-a73e-6c39b1cc1572',
'https://app.blackfynn.io/N:organization:618e8dd9-f8d2-4dc4-9abb-c6aaab2e78a0/teams/N:team:d296053d-91db-46ae-ac80-3c137ea144e4',
'https://app.blackfynn.io/N:organization:618e8dd9-f8d2-4dc4-9abb-c6aaab2e78a0/teams/N:team:d296053d-91db-46ae-ac80-3c137ea144e4/',
    )

    def test_regex(self):
        compiled = self._id_class.compiled
        [x.match(u).groups()
           for x, u in ((compiled[x][0], i)
                        for x, i in zip((0,1,3,3,3,3,4,4,4,4,4,4,),
                                        self.cases))
         if not print(u) and not print(x.match(u).groups())]

    def test_uuid(self):
        ids = []
        for uuid, type in self.uuids:
            id = self._id_class(uuid, type=type)
            ids.append(id)

    def test_id(self):
        ids = []
        for string in self.cases:
            id = self._id_class(string)
            ids.append(id)

    @pytest.mark.skip('TODO')
    def test_roundtrip(self):
        # TODO need some way to get/store other component identifiers
        # but tricky when there are 3 identifiers in a single uri
        humans = [case for case in self.cases if 'app.' in case]
        for id_str in humans:
            id = self._id_class(id_str)
            assert id.id in id.uri_human()

    def test_fail_rx(self):
        # TODO bads with edge cases
        try:
            self._id_class('lol not an bfid')
            assert False, 'should have failed'
        except idlib.exc.MalformedIdentifierError as e:  # FIXME malformed id error?
            pass

    def test_pickle(self):
        thing = self._id_class(self.cases[0])
        hrm = pickle.dumps(thing)
        tv = pickle.loads(hrm)
        assert tv == thing

    def test_copy(self):
        thing = self._id_class(self.cases[0])
        thing_prime = copy.deepcopy(thing)
        assert thing_prime == thing

    def test_asCell(self):
        thing = self._id_class(self.cases[0])
        ac = thing.asCell()


class TestPennsieveId(TestBlackfynnId):

    _id_class = PennsieveId
    cases = tuple([c.replace('blackfynn', 'pennsieve') for c in TestBlackfynnId.cases])


@pytest.mark.skip('TODO, need merge of idlib and augpathlib')
class TestIdlibPennsieveId(HelpTestStreams, unittest.TestCase):
    stream = PennsieveId
    ids = TestPennsieveId.cases
