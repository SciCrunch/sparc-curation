""" Split protcur.ttl into multiple files with one file per protocol.
"""

import tempfile
import idlib
import rdflib
import htmlfn as hfn
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


def tobn(gen, published):
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
                    doi = pid.doi
                    if doi is not None:
                        yield _s, TEMP.hasDoi, pid.doi.asType(rdflib.URIRef)
                    if s in published:
                        yield _s, TEMP.datasetPublishedDoi, published[s]
            except (idlib.exc.NotAuthorizedError) as e:
                tn = GetTimeNow()
                yield s, errorns.NotAuthorized, rdflib.Literal(tn._start_time_local)
            except (idlib.exc.IdDoesNotExistError) as e:
                tn = GetTimeNow()
                yield s, errorns.IdDoesNotExist, rdflib.Literal(tn._start_time_local)
            except (idlib.exc.MalformedIdentifierError) as e:
                pass


def make_graphs(g, pids, published):
    sgs = []
    for i in pids:
        ng = OntGraph()
        ng.namespace_manager.populate_from(g)
        ng.namespace_manager.bind(
            'spjl', 'https://uilx.org/tgbugs/u/sparcur-protcur-json-ld/')
        ng.populate_from_triples(tobn(g.subjectGraphClosure(i), published))
        sgs.append(ng)
    return sgs


def write_html(graph, path):
    body = graph.asMimetype('text/turtle+html').decode()
    html = hfn.htmldoc(
        body,
        styles=(hfn.ttl_html_style,),
        title=f'Protocol {path.name}',)
    with open(path, 'wt') as f:
        f.write(html)


def write_graphs(sgs, path=None):
    if path is None:
        path = Path(tempfile.tempdir) / 'protcur-individual'

    if not path.exists():
        path.mkdir()

    pp = path / 'published'
    if not pp.exists():
        pp.mkdir()

    hpath = path / 'html'
    if not hpath.exists():
        hpath.mkdir()

    hpp = hpath / 'published'
    if not hpp.exists():
        hpp.mkdir()

    opath = path / 'org'
    if not opath.exists():
        opath.mkdir()

    opp = opath / 'published'
    if not opp.exists():
        opp.mkdir()

    for wg in sgs:
        u = next(wg[:rdf.type:sparc.Protocol])
        published = bool(list(wg[u:TEMP.datasetPublishedDoi:]))
        try:
            pid = idlib.Pio(u)
            base = 'pio-' + pid.identifier.suffix
        except idlib.exc.IdlibError as e:
            pid = None
            base = (u
                    .replace('http://', '')
                    .replace('https://', '')
                    .replace('/', '_')
                    .replace('.', '_'))

        name = base + '.ttl'
        hname = base + '.html'
        oname = base + '.org'

        if published:
            wt_path = pp / name
            wh_path = hpp / hname
            wo_path = opp / oname
        else:
            wt_path = path / name
            wh_path = hpath / hname
            wo_path = opath / oname

        wg.write(wt_path)
        write_html(wg, wh_path)

        if pid is None:
            org = None
        else:
            #if wo_path.exists(): continue  # XXX remove after testing complete
            try:
                org = pid.asOrg()
            except idlib.exc.IdlibError as e:
                org = None

        if org is not None:
            with open(wo_path, 'wt') as f:
                f.write(org)


def main(g=None, ce_g=None, protcur_export_path=None, curation_export_path=None):

    if g is None:
        if not protcur_export_path:
            ori = OntResIri('https://cassava.ucsd.edu/sparc/preview/exports/protcur.ttl')
            g = ori.graph
        else:
            g = OntGraph().parse(protcur_export_path)

    pids = list(g[:rdf.type:sparc.Protocol])

    if ce_g is None:
        if not curation_export_path:
            ce_ori = OntResIri('https://cassava.ucsd.edu/sparc/preview/exports/curation-export.ttl')
            ce_g = ce_ori.graph
        else:
            ce_g = OntGraph().parse(curation_export_path)

    ce_pids = list(ce_g[:rdf.type:sparc.Protocol])
    ap = [(p, d, list(ce_g[d:TEMP.hasDoi:]))
          for p in ce_pids for d in ce_g[:TEMP.hasProtocol:p]
          if list(ce_g[d:TEMP.hasDoi:])]
    with_published_dataset = {p:dois[0] for p, d, dois in ap}
    graphs = make_graphs(g, pids, with_published_dataset)
    write_graphs(graphs, path=None)


if __name__ == '__main__':
    main()
