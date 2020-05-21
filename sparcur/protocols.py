import idlib
import rdflib
import requests
from idlib.cache import cache, COOLDOWN
from idlib.utils import resolution_chain
from orthauth.utils import QuietDict
from pyontutils import combinators as cmb
from pyontutils.namespaces import OntCuries, makeNamespaces, TEMP, isAbout, ilxtr
from pyontutils.namespaces import rdf, rdfs, owl, skos, dc
from sparcur import datasets as dat
from sparcur.core import log, logd, OntTerm, OntId, sparc
from sparcur.paths import Path
from sparcur.utils import log, logd
from sparcur.config import config, auth


class ProtcurData:

    def __init__(self, *args, **kwargs):
        super().__init__()  # this is the last in the chain atm

    def __call__(self, protocol_uri):
        """ can't use __call__ in subclasses where it is overwritten ... """
        yield from self.protcur_nodes(protocol_uri)

    def _protcur(self, protocol_uri, filter=lambda p: True):
        self.lazy_setup()
        protocol_uri = idlib.get_right_id(protocol_uri)
        gen = (p for p in self.protc if p.uri.startswith(protocol_uri.identifier) and filter(p))

        try:
            p = next(gen)
            yield p
            yield from gen
        except StopIteration:
            log.error(f'could not find annotations for {protocol_uri.identifier}')
            return

        if p.document.otherVersionUri:  # FIXME also maybe check /abstract?
            other_uri = p.document.otherVersionUri
            yield from (p for p in self.protc if p.uri.startswith(other_uri) and filter(p))

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
            yield from OntTerm(term).triples_simple
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
    def populate_annos():
        from protcur.core import annoSync
        from protcur.analysis import Hybrid, protc
        from hyputils.hypothesis import HypothesisHelper, group_to_memfile
        from hyputils import hypothesis as hyp
        ProtcurData.protc = protc
        group = auth.get('hypothesis-group')
        get_annos, annos, stream_thread, exit_loop = annoSync(group_to_memfile(group + 'sparcur'),
                                                              helpers=(HypothesisHelper, Hybrid, protc),
                                                              group=group,
                                                              sync=False)

        # FIXME hack to workaround bad api key init for hyutils until we can integrate orthauth
        get_annos.api_token = auth.get('hypothesis-api-key')
        annos.clear()
        annos.extend(get_annos())

        # reset classes in case some other class has populated them
        # (e.g. during testing) FIXME this is a bad hack
        protc.reset()
        Hybrid.reset()

        # FIXME this is expensive and slow to continually recompute
        [protc(a, annos) for a in annos]
        [Hybrid(a, annos) for a in annos]

    def __lol__(self, protocol_uri):
        self.protocol_uri = protocol_uri


class ProtocolData(dat.HasErrors):
    # this class is best used as a helper class not as a __call__ class

    _instance_wanted_by = idlib.Pio, idlib.PioUser

    def __init__(self, id=None):  # FIXME lots of ways to use this class ...
        self.id = id  # still needed for the converters use case :/
        # FIXME protocol data shouldn't need do know anything about
        # what dataset is using it, >_<
        super().__init__(pipeline_stage=self.__class__)

    def protocol(self, uri):
        return self._get_protocol_json(uri)

    __call__ = protocol

    @property
    def protocol_uris(self):
        raise NotImplementedError('implement in subclass')
        yield

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
            try:
                if not hasattr(start_uri, 'dereference'):
                    start_uri = idlib.StreamUri(start_uri)

                end_uri = start_uri.dereference()
                yield end_uri
                sc = end_uri.progenitor.status_code
                if sc > 400:
                    msg = f'error accessing {end_uri} {sc}'
                    if self.addError(msg, blame='submission'):
                        logd.error(msg)

            except idlib.exceptions.ResolutionError as e:
                pass  # FIXME I think we already log this error?
            except requests.exceptions.MissingSchema as e:
                if self.addError(e, blame='submission'):
                    logd.error(e)
            except OntId.BadCurieError as e:
                if self.addError(e, blame='submission'):
                    logd.error(e)
            except BaseException as e:
                #breakpoint()
                log.exception(e)
                log.critical('see exception above')

    @property
    def protocol_annotations(self):
        for uri in self.protocol_uris_resolved:
            yield from self.protc.byIri(uri, prefix=True)

    @property
    def protocol_jsons(self):
        for uri in self.protocol_uris_resolved:
            j = self._get_protocol_json(uri)
            if j:
                yield j

    def _get_protocol_json(self, uri):
        #juri = uri + '.json'
        logd.info(uri.identifier if isinstance(uri, idlib.Stream) else uri)  # FIXME
        pi = idlib.get_right_id(uri)
        if 'protocols.io' in pi:
            out = pi.data()
            if out is None and hasattr(pi, '_failure_message'):
                msg = pi._failure_message  # FIXME HACK use progenitor
                msg += ' {self.id!r}'
                if self.addError(msg):
                    logd.error(msg)

            return out
        else:
            msg = f'protocol uri is not from protocols.io {pi} {self.id}'
            if self.addError(msg):
                logd.error(msg)
