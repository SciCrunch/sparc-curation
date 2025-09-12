import rdflib
from pathlib import Path
from pyontutils.core import OntGraph, OntId, OntResPath, OntConjunctiveGraph, log
from pyontutils.namespaces import rdf, ilxtr, PREFIXES
from neurondm import orders as nord

b = Path('/tmp/build')
rp = sorted(b.glob('release-*-sckan'))[-1]
orp = OntResPath(rp / "data/sparc-data.ttl")
apinat_imports = [
    i for i in orp.imports if 'ApiNATOMY' in i.identifier
]
local = False
if local:
    apinat_imports = [
        OntResPath(Path(f'~/git/apinatomy-models/models/{model_id}/derived/{model_id}.ttl').expanduser())
        for i in apinat_imports
        if (model_id := i.identifier.rsplit('/', 1)[-1].split('.')[0])]

# FIXME conjunctive graph has to conjoin by sharing a store ??? wat
apinat_graphs = [i.graph for i in apinat_imports]
ocg = OntConjunctiveGraph()
_ = [ocg.add((*t, g.boundIdentifier)) for g in apinat_graphs for t in g]
set(ocg.predicates())
apinat = rdflib.Namespace('https://apinatomy.org/uris/readable/')
elements = rdflib.Namespace('https://apinatomy.org/uris/elements/')

ancsl = list(ocg[:apinat.nextChainStartLevels:])
anext = list(ocg[:apinat.next:])  # vast majority of partial order info is in anext

# XXX if the housing lyphs that correspond to a merging levelTargets
# are not the same then apinatomy does not correctly generate next
# there is only currently one case where that happens in aacar-5 not
# all of the pairs pulled in here seem to be needed possibly because
# the wind up in the same housing lyph
_cands = [n for _, n in anext if
          not list(ocg[n:apinat.next:]) and
          not list(ocg[n:apinat.nextChainStartLevels:]) and
          list(ocg[n:apinat.target])]
alevtar = [(l, sof_link)
           for l in _cands
           for node in ocg[l:apinat.target]
           for sof_link in ocg[node:apinat.sourceOf:]]

_soma = OntId('NLX:154731').u
somas = [s for s in ocg[:apinat.ontologyTerms:_soma] if (s, rdf.type, elements.Lyph) in ocg]
soma_links = set()


def soma_link_edges(soma):
    # XXX uses internalIn instead of housingLyph
    for link in ocg[soma:apinat.conveys:]:
        soma_links.add(link)
        for node_s in ocg[link:apinat.source:]:
            for other_link in ocg[node_s:apinat.sourceOf:]:  # FIXME only by convention that the direction matches
                if other_link != link and (other_link, apinat.collapsible, rdflib.Literal(True)) not in ocg:
                    yield link, other_link

        for node_t in ocg[link:apinat.target:]:
            for other_link in ocg[node_t:apinat.sourceOf:]:  # FIXME only by convention that the direction matches
                if other_link != link and (other_link, apinat.collapsible, rdflib.Literal(True)) not in ocg:
                    yield link, other_link


soma_edges = [pair for s in somas for pair in soma_link_edges(s)]


def get_neuron(link):
    # FIXME this requires somas
    for lyph in ocg[link:apinat.conveyingLyph:]:
        for group in ocg[lyph:apinat.seedIn:]:
            for ot in ocg[group:apinat.ontologyTerms:]:
                return ot

        # multi-soma case (lol fixed everything except aacar 10a and 10v (and one other))
        for group in ocg[:apinat.lyphs:lyph]:
            if ((group, rdf.type, elements.Group) not in ocg or
                (group, apinat.description, rdflib.Literal('dynamic')) not in ocg):
                continue

            for glyph in ocg[group:apinat.lyphs]:
                for ggroup in ocg[glyph:apinat.seedIn:]:
                    for ot in ocg[ggroup:apinat.ontologyTerms:]:
                        return ot

    log.error(f'no connection found for {link}')


link_lookup = {}
other_lookup = {}
def link_to_ont_region_layer(link):
    # FIXME this doesn not quite match what we did in cypher
    # FIXME most importantly it is missing the soma links
    if link in soma_links:
        hlii = apinat.internalIn
    else:
        hlii = apinat.housingLyph  # XXX NOTE THE DIFFERENCE! apinat:housingLyphs is for chains, and applies down so we shouldn't need it here at all

    cl = None
    for cl in ocg[link:apinat.conveyingLyph:]:
        hl = None
        for hl in ocg[cl:hlii:]:
            if hl in other_lookup:
                link_lookup[link] = other_lookup[hl]
                continue

            i = None
            for i, ot in enumerate(ocg[hl:apinat.ontologyTerms:]):
                pass

            if i is None:
                for i, ot in enumerate(ocg[hl:apinat.inheritedOntologyTerms:]):
                    pass

                if i is None:
                    for co in ocg[hl:apinat.cloneOf:]:
                        for i, ot in enumerate(ocg[co:apinat.ontologyTerms:]):
                            pass

                        if i is None:
                            for i, ot in enumerate(ocg[co:apinat.inheritedOntologyTerms:]):
                                pass

            if i is None:
                log.error(('rol', hl))
                continue
            elif i > 0:
                log.warning(f'multiple 0 ontology terms {i + 1} for {hl}')

            li = None
            for co in ocg[hl:apinat.cloneOf:]:
                pass
            for li in ocg[hl:apinat.layerIn:]:
                j = None
                for j, liot in enumerate(ocg[li:apinat.ontologyTerms:]):
                    pass

                if j is None:
                    for co in ocg[li:apinat.cloneOf:]:
                        for j, liot in enumerate(ocg[co:apinat.ontologyTerms:]):
                            pass

                if j is None:
                    log.error(('r', li))
                    pass
                elif j > 0:
                    log.warning(f'multiple 1 ontology terms {j + 1} {li}')

            if li is None:
                other_lookup[hl] = link_lookup[link] = nord.rl(region=ot)
            else:
                other_lookup[hl] = link_lookup[link] = nord.rl(region=liot, layer=ot)  # til that -liot -> rdflib.NegatedPath

        if hl is None:
            log.error(f'conveying lyph for link has no housing lyph??? {cl} {link}')
            # FIXME axon-chain-gastric-duodenum-neuron-4 has housingLyphs on the chain
            # and housingLayers on the chain, but those are not being materialized to the
            # generated lyphs, which they need to be, also wbkg terms are not being lifted
            # so they are not appearing in the chain housingLyphs, also reordering of these
            # on the chain during serialization to ttl is another reason why they need to be
            # materialized down to the generated lyphs
            link_lookup[link] = nord.rl(region=rdflib.URIRef(f'ERROR-{link}'))

    if cl is None:
        log.error(f'something has gone very wrong {link}')
        link_lookup[link] = nord.rl(region=rdflib.URIRef(f'EXTREME-ERROR-{link}'))


neuron_parts = {
OntId('SAO:1770195789').u,  # axon
OntId('SAO:1211023249').u,  # dendrite or sensory axon
OntId('SAO:280355188').u,  # regional part of axon
OntId('SAO:420754792').u,  # regional part of dendrite
_soma,
}
def filter_for_neuron_parts(link):
    for cl in ocg[link:apinat.conveyingLyph:]:
        for ot in ocg[cl:apinat.ontologyTerms:]:
            if ot in neuron_parts:
                return ot

        for iot in ocg[cl:apinat.inheritedOntologyTerms:]:
            if iot in neuron_parts:
                return iot


_l_adj = soma_edges + ancsl + anext + alevtar
l_adj = [(a, b) for (a, b) in _l_adj if filter_for_neuron_parts(a) or filter_for_neuron_parts(b)]
l_skipped = set(_l_adj) - set(l_adj)  # TODO review to make sure there aren't lurking issues (looks like we aren't skipping anything we want, mostly spinal cord and circulatory chains ...)
torep = set(e for es in l_adj for e in es)
_ = [link_to_ont_region_layer(link) for link in torep]

seeds = {l: neuron for l in (soma_links | set(e for es in l_adj for e in es)) if (neuron := get_neuron(l))}

l_nst = nord.adj_to_nst(l_adj)  # this is the ord over the links we may have to do this first and then replace because different neurons may converge, or do this first, get the distinct graphs per neuron, separate, replace and then h_adj -> h_nst
l_split_nst = l_nst[1:]
l_split_adj = [nord.nst_to_adj(n) for n in l_split_nst]

nrns = [set(e for es in al for e in es) for al in l_split_adj]
def get_seed_neuron(ns):
    key = ns & set(seeds)
    knrns = set(seeds[k] for k in key)
    assert len(knrns) <= 1, (len(knrns), knrns)
    if key:
        return next(iter(knrns))

    return rdflib.BNode()  # FIXME obvs an issue

nrn_index = [get_seed_neuron(ns) for ns in nrns]

_l_split_adj_merge = {}
for n, sadj in zip(nrn_index, l_split_adj):
    if n not in _l_split_adj_merge:
        _l_split_adj_merge[n] = tuple()
    _l_split_adj_merge[n] += sadj

h_nrn_index, l_split_adj_merge = zip(*_l_split_adj_merge.items())


hardcoded_fixes = True
def fixes(n, adj):
    if 'bolew-unbranched' in n:
        intid = int(n.rsplit('-', 1)[-1])
        if intid >= 13:
            # neural tissue is implicit in the neurondm representation for these populations
            neural_tissue = rdflib.URIRef('http://purl.obolibrary.org/obo/UBERON_0003714')
            for es in adj:
                for e in es:
                    if isinstance(e, nord.rl) and e.layer == neural_tissue:
                        e.layer = None

    if 'pancr-5' in n:
        cns_par = rdflib.URIRef('http://purl.obolibrary.org/obo/UBERON_0005158')
        for es in adj:
            for e in es:
                if isinstance(e, nord.rl) and e.layer == cns_par:
                    e.layer = None

    return adj


def self_only_all_self(s, n):
    adj = set((link_lookup[a], link_lookup[b]) for a, b in s if link_lookup[a] != link_lookup[b])
    if hardcoded_fixes:
        adj = fixes(n, adj)
        adj = set((a, b) for a, b in adj if a != b)  # refilter since mutated nodes

    if not adj:
        adj = set((link_lookup[a], link_lookup[b]) for a, b in s)


    return adj


h_split_adj = [sorted(self_only_all_self(s, n)) for s, n in zip(l_split_adj_merge, h_nrn_index)]
h_split_nst = [nord.adj_to_nst(a) for a in h_split_adj]
# h_adj = [(link_lookup[a], link_lookup[b]) for a, b in l_adj]  # can't do this only produces 52 distinct graphs
# h_nst = nord.adj_to_nst(h_adj)

results = list(zip(h_nrn_index, h_split_nst))

to_rdf, from_rdf = nord.bind_rdflib()

g = OntGraph()
g.namespace_manager.populate_from(PREFIXES)
for n, nst in results:
    bn = to_rdf(g, nst)
    g.add(((rdflib.BNode() if n is None else n), ilxtr.neuronPartialOrder, bn))

g.write(Path('~/git/NIF-Ontology/ttl/generated/neurons/apinat-partial-orders.ttl').expanduser())
