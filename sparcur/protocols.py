from hyputils.hypothesis import HypothesisHelper, group_to_memfile
from pyontutils.config import devconfig
from protcur.analysis import Hybrid, protc
from protcur.core import annoSync
from protcur import namespace_mappings as nm

class ProtcurSource:

    @staticmethod
    def populate_annos(group_name='sparc-curation'):
        group = devconfig.secrets('hypothesis', 'group', group_name)
        get_annos, annos, stream_thread, exit_loop = annoSync(group_to_memfile(group + 'sparcur'),
                                                              helpers=(HypothesisHelper, Hybrid, protc),
                                                              group=group)

        [protc(a, annos) for a in annos]
        [Hybrid(a, annos) for a in annos]

    def __lol__(self, protocol_uri):
        self.protocol_uri = protocol_uri
