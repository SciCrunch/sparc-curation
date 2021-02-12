""" convert dandi terms yaml to ttl """

import yaml
import rdflib
import augpathlib as aug
from pyontutils.core import populateFromJsonLd, OntGraph
from pyontutils.namespaces import rdfs, rdf

# pushd ~/git/NOFORK/dandi-schema/context
# python -m http.server 0 --bind 127.0.0.1
# get the tcp port from the python server (used as ${PORT} below)
# export PORT=
# sed -i "s/\.\.\/context\/base\.json/http:\/\/localhost:${PORT}\/base.json/" *.yaml

dandi = rdflib.Namespace('http://schema.dandiarchive.org/')
schema = rdflib.Namespace('http://schema.org/')


def path_yaml(string):
    with open(string, 'rb') as f:
        return yaml.safe_load(f)


def main():
    dandi_terms_path = aug.LocalPath.cwd()
    g = OntGraph()

    _ = [populateFromJsonLd(g, path_yaml(p))
         for p in dandi_terms_path.rglob('*.yaml')]
    g.write('dandi-raw.ttl')
    remove = [(s, p, o)
              for p in (schema.domainIncludes, schema.rangeIncludes, rdfs.subClassOf, rdf.type)
              for s, o in g[:p:]]
    add = [(s, p, (g.namespace_manager.expand(o.toPython()) if isinstance(o, rdflib.Literal) else o))
           for s, p, o in remove]
    _ = [g.remove(t) for t in remove]
    _ = [g.add(t) for t in add]
    # TODO ontology metadata header section
    g.write('dandi.ttl')


if __name__ == '__main__':
    main()
