""" Create a file with only the published subset curation-export.ttl

Usage:
    pushd path/to/export/root; python -m sparcur.export.published; popd

"""

import rdflib
from sparcur.paths import Path
from pyontutils.core import OntResPath, OntGraph
from pyontutils.namespaces import rdf, sparc, TEMP


def curation_export_published(export_path, out_base=None):
    p = Path(export_path).expanduser().resolve()
    ce = OntResPath(p / 'curation-export.ttl')
    orps = [OntResPath(_) for _ in (p / 'datasets').children if _.suffix == '.ttl']
    graphs = [o.graph for o in orps]

    merged = _populate_published(ce, graphs)

    op = p if out_base is None else Path(out_base)
    merged.write(op / 'curation-export-published.ttl')


def _merge_graphs(graphs):
    merged = OntGraph()
    for g in graphs:
        merged.namespace_manager.populate_from(
            {k:v for k, v in dict(g.namespace_manager).items()
            if k not in ('contributor', 'sample', 'subject')})
        merged.populate_from_triples(g.data)  # g.data excludes the owl:Ontology section
        # TODO switch the rdf:type of metadata section on combination to preserve export related metadata
    return merged


def _populate_published(curation_export, graphs):

    # datasets = [list(g[:rdf.type:sparc.Dataset]) for g in graphs]
    published_graphs = [
        g for g, doi in [(g, list(g[ds:TEMP.hasDoi]))
                        for g in graphs for ds in g[:rdf.type:sparc.Dataset]]
        if doi]

    merged = _merge_graphs(published_graphs)
    _fix_for_pub(curation_export, merged)
    return merged


def _fix_for_pub(curation_export, merged):
    mg = curation_export.metadata().graph
    mg.namespace_manager.populate(merged)

    new_bi = rdflib.URIRef(mg.boundIdentifier
                           .replace('ontologies/', 'ontologies/published/'))
    new_vi = rdflib.URIRef(mg.versionIdentifier
                           .replace('ontologies/', 'ontologies/published/'))
    replace_pairs = (
        (rdflib.Literal("SPARC Consortium curation export published graph"),
         rdflib.Literal("SPARC Consortium curation export graph")),
        (new_bi, mg.boundIdentifier),
        (new_vi, mg.versionIdentifier))

    new_meta = mg.replaceIdentifiers(replace_pairs)
    new_meta.populate(merged)
    return replace_pairs


def main():
    export_path = Path.cwd()
    curation_export_published(export_path)


if __name__ == '__main__':
    main()
