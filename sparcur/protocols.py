import requests
import rdflib
from scibot.utils import resolution_chain
from pyontutils import combinators as cmb
from pyontutils.config import devconfig, QuietDict
from hyputils.hypothesis import HypothesisHelper, group_to_memfile
from pysercomb.pyr import units as pyru
from protcur import namespace_mappings as nm
from protcur.core import annoSync
from protcur.analysis import Hybrid, protc
from sparcur import datasets as dat
from sparcur.protocols_io_api import get_protocols_io_auth
from sparcur.utils import log, logd, cache
from sparcur.paths import Path
from sparcur.config import config
from sparcur.core import log, logd, OntTerm, OntId, OrcidId, PioId, sparc
from sparcur.core import get_right_id, DoiId, DoiInst, PioInst

from pyontutils.namespaces import OntCuries, makeNamespaces, TEMP, isAbout, ilxtr
from pyontutils.closed_namespaces import rdf, rdfs, owl, skos, dc


class ProtcurData:

    def __init__(self, *args, **kwargs):
        super().__init__()  # this is the last in the chain atm

    def __call__(self, protocol_uri):
        """ can't use __call__ in subclasses where it is overwritten ... """
        yield from self.protcur_nodes(protocol_uri)

    def _protcur(self, protocol_uri, filter=lambda p: True):
        self.lazy_setup()
        protocol_uri = get_right_id(protocol_uri)
        gen = (p for p in protc if p.uri.startswith(protocol_uri) and filter(p))

        try:
            p = next(gen)
            yield p
            yield from gen
        except StopIteration:
            log.error(f'could not find annotations for {protocol_uri}')
            return

        if p.document.otherVersionUri:  # FIXME also maybe check /abstract?
            other_uri = p.document.otherVersionUri
            yield from (p for p in protc if p.uri.startswith(other_uri) and filter(p))

    @property
    def protcur(self):
        """ protcur nodes without parents """
        for protocol_uri in self.protocol_uris:
            yield from self._protcur(protocol_uri, filter=lambda p: not p.hasAstParent)

    @property
    def protcur_all(self):
        for protocol_uri in self.protocol_uris:
            yield from self._protcur(protocol_uri)

    def triples_protcur(self, protocol_subject):
        ps = list(self._protcur(str(protocol_subject)))
        anatomy = [(p, OntId('UBERON:' + str(p).split('UBERON:', 1)[-1].split(' ', 1)[0])) for p in ps
                   if p.astType == 'protc:input' and '(protc:input (term UBERON' in str(p)]
        #breakpoint()
        dataset_subject = rdflib.URIRef(self.uri_api)
        yield protocol_subject, TEMP.hasNumberOfProtcurAnnotations, rdflib.Literal(len(ps))
        done = set()
        for anno, term in anatomy:
            if term in done:
                continue

            done.add(term)
            o = term.u
            t = dataset_subject, TEMP.involvesAnatomicalRegion, o
            sl = rdflib.URIRef(anno.shareLink)
            av = (((ilxtr.annotationValue, rdflib.Literal(anno.value)),)
                  if anno.value != o else tuple())
            notes = [(ilxtr.curatorNote, rdflib.Literal(n)) for n in anno.curatorNotes]
            prov = [(ilxtr.hasAnnotation, sl)]
            yield t
            yield from cmb.annotation(t, *av, *notes, *prov)()

    @classmethod
    def lazy_setup(cls):
        """ Do not use setup since it is always run by Integrator
            We want lazy loading, and should probably conver all
            setup style things that hit the network to be lazy. """
        if not hasattr(cls, '_setup_ok'):
            cls.populate_annos()
            cls._setup_ok = True

    @staticmethod
    def populate_annos(group_name='sparc-curation'):
        from hyputils import hypothesis as hyp
        if hyp.api_token == 'TOKEN':  # FIXME does not work
            hyp.api_token = devconfig.secrets('hypothesis', 'api', devconfig.hypothesis_api_user)

        group = devconfig.secrets('hypothesis', 'group', group_name)
        get_annos, annos, stream_thread, exit_loop = annoSync(group_to_memfile(group + 'sparcur'),
                                                              helpers=(HypothesisHelper, Hybrid, protc),
                                                              group=group,
                                                              sync=False)

        [protc(a, annos) for a in annos]
        [Hybrid(a, annos) for a in annos]

    def __lol__(self, protocol_uri):
        self.protocol_uri = protocol_uri


class ProtocolData(dat.HasErrors):
    # this class is best used as a helper class not as a __call__ class

    _instance_wanted_by = PioInst,

    def __init__(self, id=None):  # FIXME lots of ways to use this class ...
        self.id = id  # still needed for the converters use case :/
        # FIXME protocol data shouldn't need do know anything about
        # what dataset is using it, >_<
        super().__init__(pipeline_stage=self.__class__)

    def protocol(self, uri):
        return self._get_protocol_json(uri)

    __call__ = protocol

    @classmethod
    def setup(cls, creds_file=None):
        if creds_file is None:
            try:
                creds_file = devconfig.secrets('protocols-io', 'api', 'creds-file')
            except KeyError as e:
                raise TypeError('creds_file is a required argument'
                                ' unless you have it in secrets') from e
        _pio_creds = get_protocols_io_auth(creds_file)
        cls._pio_header = QuietDict({'Authorization': 'Bearer ' + _pio_creds.access_token})
        _inst = cls()
        for wants in cls._instance_wanted_by:
            wants._protocol_data = _inst

    @classmethod
    def cache_path(cls):
        return config.protocol_cache_path

    @property
    def protocol_uris_resolved(self):
        if not hasattr(self, '_c_protocol_uris_resolved'):
            self._c_protocol_uris_resolved = list(self._protocol_uris_resolved)

        return self._c_protocol_uris_resolved

    @property
    def _protocol_uris_resolved(self):
        # FIXME quite slow ...
        for start_uri in self.protocol_uris:
            log.debug(start_uri)
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
        logd.info(uri)
        pi = get_right_id(uri)
        if 'protocols.io' in pi:
            pioid = pi.slug  # FIXME normalize before we ever get here ...
            log.info(pioid)
        else:
            msg = f'protocol uri is not from protocols.io {pi} {self.id}'
            logd.error(msg)
            self.addError(msg)
            return

        #uri_path = uri.rsplit('/', 1)[-1]
        apiuri = 'https://protocols.io/api/v3/protocols/' + pioid
        #'https://www.protocols.io/api/v3/groups/sparc/protocols'
        #apiuri = 'https://www.protocols.io/api/v3/filemanager/folders?top'
        #print(apiuri, header)
        log.debug('going to network for protocols')
        resp = requests.get(apiuri, headers=self._pio_header)
        #log.info(str(resp.request.headers))
        if resp.ok:
            try:
                j = resp.json()  # the api is reasonably consistent
            except BaseException as e:
                log.exception(e)
                breakpoint()
                raise e
            return j
        else:
            try:
                j = resp.json()
                sc = j['status_code']
                em = j['error_message']
                msg = f'protocol issue {uri} {resp.status_code} {sc} {em} {self.id!r}'
                logd.error(msg)
                self.addError(msg)
                # can't return here because of the cache
            except BaseException as e:
                log.exception(e)

            logd.error(f'protocol no access {uri} {self.id!r}')
