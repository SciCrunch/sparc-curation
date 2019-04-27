from hyputils.hypothesis import HypothesisHelper, group_to_memfile
from protcur.analysis import Hybrid, protc
from protcur.core import annoSync
from protcur import namespace_mappings as nm

class ProtcurSource:

    group_name='sparc-curation'

    def __init__(self, protocol_uri):
        self.protocol_uri = protocol_uri

    def populate_annos(self):
        group = devconfig.secrets('hypothesis', 'group', self.group_name)
        get_annos, annos, stream_thread, exit_loop = annoSync(group_to_memfile(group),  # FIXME name collision?
                                                              helpers=(HypothesisHelper, Hybrid, protc),
                                                              group=group)

        [protc(a, annos) for a in annos]
        [Hybrid(a, annos) for a in annos]
