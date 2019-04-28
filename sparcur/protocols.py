import requests
from scibot.utils import resolution_chain
from pyontutils.config import devconfig
from hyputils.hypothesis import HypothesisHelper, group_to_memfile
from protcur import namespace_mappings as nm
from protcur.core import annoSync
from protcur.analysis import Hybrid, protc
from sparcur.protocols_io_api import get_protocols_io_auth
from sparcur.core import log, cache
from sparcur.paths import Path
from sparcur.config import config

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


class ProtocolData:

    @classmethod
    def setup(cls):
        creds_file = devconfig.secrets('protocols-io', 'api', 'creds-file')
        _pio_creds = get_protocols_io_auth(creds_file)
        cls._pio_header = {'Authorization': 'Bearer ' + _pio_creds.access_token}

    @classmethod
    def cache_path(cls):
        return config.protocol_cache_path

    @property
    def protocol_uris(self):
        raise NotImplementedError('your class needs to have a way of producing protocol uris '
                                  'otherwise we can\'t help you')

    @property
    def protocol_uris_resolved(self):
        if not hasattr(self, '_c_protocol_uris_resolved'):
            self._c_protocol_uris_resolved = list(self._protocol_uris_resolved)

        return self._c_protocol_uris_resolved

    @property
    def _protocol_uris_resolved(self):
        # FIXME quite slow ...
        for start_uri in self.protocol_uris:
            for end_uri in resolution_chain(start_uri):
                pass
            else:
                yield end_uri

    @property
    def protocol_annotations(self):
        for uri in self.protocol_uris_resolved:
            yield from protc.byIri(uri, prefix=True)

    @property
    def protocol_jsons(self):
        for uri in self.protocol_uris_resolved:
            yield self._get_protocol_json(uri)

    @cache(Path(config.cache_dir, 'protocol_json'))
    def _get_protocol_json(self, uri):
        #juri = uri + '.json'
        uri_path = uri.rsplit('/', 1)[-1]
        apiuri = 'https://protocols.io/api/v3/protocols/' + uri_path
        #'https://www.protocols.io/api/v3/groups/sparc/protocols'
        #apiuri = 'https://www.protocols.io/api/v3/filemanager/folders?top'
        #print(apiuri, header)
        log.debug('going to network for protocols')
        resp = requests.get(apiuri, headers=self._pio_header)
        #log.info(str(resp.request.headers))
        j = resp.json()  # the api is reasonably consistent
        if resp.ok:
            return j
        else:
            log.error(f"protocol no access {uri} '{self.dataset.id}'")

