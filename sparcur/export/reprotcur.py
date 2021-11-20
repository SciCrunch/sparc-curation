""" Split protcur.ttl into multiple files with one file per protocol.
"""

import tempfile
import idlib
import rdflib
from pyontutils.core import OntResIri, OntGraph
from pyontutils.namespaces import sparc, rdf, owl, ilxtr, TEMP
from sparcur.core import OntId
from sparcur.utils import GetTimeNow
from sparcur.paths import Path

errorns = rdflib.Namespace(str(ilxtr.error) + '/')
pio_onts = rdflib.Namespace('https://uilx.org/tgbugs/u/protocols.io/protocol/')

ph_prefix = 'https://uilx.org/tgbugs/u/hypothesis/protcur/'

bnodes = {}


def fix(e):
    if e.startswith(ph_prefix):
        if e not in bnodes:
            bnodes[e] = rdflib.BNode()
        return bnodes[e]
    else:
        return e

def tobn(gen):
    """convert hypothesis ids to blank nodes so that values serialize locally"""
    for s, p, o in gen:
        ns = fix(s)
        no = fix(o)
        if p == TEMP.protcurChildren:
            yield ns, p, no
        elif s != ns:
            yield ns, p, o
            yield ns, ilxtr.hasId, s
            yield ns, TEMP.hasUriHumanContext, rdflib.URIRef(s.replace(ph_prefix, 'https://hyp.is/'))
        else:
            yield s, p, o

        if o == sparc.Protocol:
            try:
                pid = idlib.Pio(s)
                os = pio_onts[pid.identifier.suffix]
                yield os, rdf.type, owl.Ontology
                yield os, TEMP.hasUriApi, s
                for _s in (s, os):
                    yield _s, TEMP.hasUriHuman, pid.uri_human.asType(rdflib.URIRef)
            except (idlib.exc.NotAuthorizedError) as e:
                tn = GetTimeNow()
                yield s, errorns.NotAuthorized, rdflib.Literal(tn._start_time_local)
            except (idlib.exc.IdDoesNotExistError) as e:
                tn = GetTimeNow()
                yield s, errorns.IdDoesNotExist, rdflib.Literal(tn._start_time_local)
            except (idlib.exc.MalformedIdentifierError) as e:
                pass


def make_graphs(pids):
    sgs = []
    for i in pids:
        ng = OntGraph()
        ng.namespace_manager.populate_from(g)
        ng.populate_from_triples(tobn(g.subjectGraphClosure(i)))
        sgs.append(ng)


def write_graphs(sgs, path=None)
    if path is None:
        path = Path(tempfile.tempdir) / 'protcur-individual'

    if not path.exists():
        path.mkdir()

    for wg in sgs:
        u = next(wg[:rdf.type:sparc.Protocol])
        try:
            pid = idlib.Pio(u)
            base = 'pio-' + pid.identifier.suffix
        except idlib.exc.IdlibError as e:
            base = (u
                    .replace('http://', '')
                    .replace('https://', '')
                    .replace('/', '_')
                    .replace('.', '_'))

        name = base + '.ttl'
        wg.write(path / name)


def main():
    # FIXME TODO generalize
    ori = OntResIri("https://cassava.ucsd.edu/sparc/preview/exports/protcur.ttl")
    g = ori.graph
    pids = list(g[:rdf.type:sparc.Protocol])
    graphs = make_graphs(pids)
    write_graphs(graphs, path=None)


if __name__ == '__main__':
    main()
