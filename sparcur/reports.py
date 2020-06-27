import re
import sys
import json
import types
import pprint
from itertools import chain
from collections import Counter, defaultdict
import idlib
import htmlfn as hfn
import augpathlib as aug
from hyputils import hypothesis as hyp
from protcur import document as ptcdoc
from pyontutils.core import OntGraph, OntRes, OntResIri, OntResPath
from pyontutils.utils import UTCNOWISO, anyMembers
from sparcur import sheets
from sparcur.core import (OntId,
                          OntTerm,
                          get_all_errors,
                          adops,
                          register_type,
                          fromJson)
from sparcur.utils import want_prefixes, log as _log
from sparcur.paths import Path
from sparcur.config import auth
from sparcur.curation import ExporterSummarizer
from sparcur.curation import Integrator, DatasetObject  # FIXME

log = _log.getChild('reports')


def normalize_exact(e, *, lower=False):
    if e is not None:
        e = e.replace('\n', ' ')  # sigh
        e = e.replace('\t', ' ')
        e = e.replace('\xA0', ' ')  # nbsp
        e = e.strip().rstrip()
        if lower:
            e = e.lower()
        return e

    return ''


def normtext(t, *, lower=False):
    if 'https://' in t:
        return ''
    else:
        t = t.strip().rstrip()
        if lower:
            t = t.lower()

        return t


class SparqlQueries:
    """ Creates SPARQL query templates. """

    def __init__(self, nsm=None):
        from rdflib.plugins import sparql
        self.sparql = sparql

        self.nsm = nsm if nsm else OntGraph().namespace_manager
        self.prefixes = dict(self.nsm)

    def dataset_about(self):
        # FIXME this will return any resource matching isAbout:
        query = """
            SELECT ?dataset
            WHERE {
                ?dataset rdf:type sparc:Resource .
                ?dataset isAbout: ?about .
            }
        """
        return self.sparql.prepareQuery(query, initNs=self.prefixes)

    def dataset_subjects(self) -> str:
        """ Get all subject groups and dataset associated with subject input.

        :returns: list of tuples containing: subject, subjects group, and subjects dataset.
        """
        query = """
            SELECT ?dataset ?subj
            WHERE {
                ?startsubj TEMP:hasDerivedInformationAsParticipant ?dataset .
                ?subj  TEMP:hasDerivedInformationAsParticipant ?dataset .
            }
        """
        return self.sparql.prepareQuery(query, initNs=self.prefixes)

    def dataset_groups(self) -> str:
        """ Get all subject groups and dataset associated with subject input.

        :returns: list of tuples containing: subject, subjects group, and subjects dataset.
        """
        query = """
            SELECT ?dataset ?group ?subj
            WHERE {
                ?startsubj TEMP:hasDerivedInformationAsParticipant ?dataset .
                ?subj  TEMP:hasDerivedInformationAsParticipant ?dataset .
                ?subj  TEMP:hasAssignedGroup ?group .
            }
        """
        return self.sparql.prepareQuery(query, initNs=self.prefixes)

    def dataset_bundle(self) -> str:
        """ Get all related datasets of subject.

        :returns: list of tuples containing: subject & subjects shared dataset.
        """
        query = """
            SELECT ?dataset
            WHERE {
                ?startdataset TEMP:collectionTitle ?string .
                ?dataset  TEMP:collectionTitle ?string .
            }
        """
        return self.sparql.prepareQuery(query, initNs=self.prefixes)

    def dataset_subject_species(self):
        # FIXME how to correctly init bindings to multiple values ...
        query = """
            SELECT DISTINCT ?dataset
            WHERE {
                VALUES ?species { "human" "homo sapiens" } .
                ?dataset TEMP:isAboutParticipant ?subject .
                ?subject sparc:animalSubjectIsOfSpecies ?species .
            }
        """
        return self.sparql.prepareQuery(query, initNs=self.prefixes)

    def award_affiliations(self):
        query = """
            SELECT DISTINCT ?award ?affiliation
            WHERE {
                ?dataset TEMP:hasAwardNumber ?award .
                ?contributor TEMP:contributorTo ?dataset .
                ?contributor TEMP:hasAffiliation ?affiliation .
            }
        """
        return self.sparql.prepareQuery(query, initNs=self.prefixes)

    def dataset_milestone_completion_date(self):
        query = """
            SELECT DISTINCT ?dataset ?date
            WHERE {
                ?dataset TEMP:milestoneCompletionDate ?date .
            }
        """
        return self.sparql.prepareQuery(query, initNs=self.prefixes)

    def protocol_techniques(self):
        query = """
            SELECT DISTINCT ?protocol ?technique
            WHERE {
                ?protocol rdf:type sparc:Protocol .
                ?protocol TEMP:protocolEmploysTechinque ?technique .
            }
        """
        return self.sparql.prepareQuery(query, initNs=self.prefixes)

    def protocol_aspects(self):
        query = """
            SELECT DISTINCT ?protocol ?aspect
            WHERE {
                ?protocol rdf:type sparc:Protocol .
                ?protocol TEMP:protocolInvolvesAspect ?aspect .
            }
        """
        return self.sparql.prepareQuery(query, initNs=self.prefixes)



class TtlFile:

    def __init__(self, ontres, ontres_compare_to=None):
        import rdflib
        from pyontutils import namespaces as ns
        self.rdflib = rdflib
        self.ns = ns

        self.ontres = ontres

        if ontres_compare_to is None:
            self.ontres_compare_to = ontres
        else:
            self.ontres_compare_to = ontres_compare_to

        self.graph = self.ontres.graph
        self.graph_compare_to = self.ontres_compare_to.graph

        self.queries = SparqlQueries(nsm=self.graph.namespace_manager)

    def _to_human(self, row):
        return [self.graph.namespace_manager.qname(cell)
                if isinstance(cell, rdflib.URIRef) else
                cell.toPython() for cell in row]

    def changes(self):
        added, removed, changed = self.graph.subjectsChanges(self.graph_compare_to)

    def milestones(self):
        query = self.queries.dataset_milestone_completion_date()
        res = list(self.graph.query(query))
        rows = sorted(self._to_human(row) for row in res)
        header = [['dataset', 'milestone comp date']]
        return header + rows
        return self._print_table(header + rows,
                                 title=f'Dataset milestone completion dates')

    def mis(self, ext=None):
        rdflib = self.rdflib
        ns = self.ns
        ontres = self.ontres

        counts = Counter(ontres.graph.predicates())
        g = OntGraph(namespace_manager=ontres.graph.namespace_manager)

        [(g.add((s, ns.ilxtr.numberOfOccurrences, rdflib.Literal(count))),
          g.add((s, ns.rdf.type, ns.ilxtr.MISPredicate)))
         for s, count in counts.items()
         if g.namespace_manager.qname(s).split(':')[0]
         not in ('rdf', 'rdfs', 'owl')]
        g.debug()

        rows = [[g.namespace_manager.qname(s), o.toPython()]
                for s, o in g[:ns.ilxtr.numberOfOccurrences:]]
        header = [['curie', 'count']]
        return header + sorted(rows, key=lambda r:-r[-1])  # FIXME only Report has access to -C :/


class Report:

    @property
    def _sort_key(self):
        if self.options.sort_count_desc:
            return lambda kv: -kv[-1]
        else:
            return lambda kv: kv

    def all(self):
        #self.access()  # must be on live data
        # TODO add a prov report for when this was last run etc.
        self.contributors()
        self.filetypes()
        self.samples()
        self.subjects()
        self.completeness()
        self.keywords()
        #self.size(dirs=list(self.datasets_local))  # TODO needs to be reworked to use latest_ir
        self.overview()
        #self.test()  # just a test
        #self.errors()  # TODO too much for now
        #self.pathids()  # too big/slow
        if hasattr(self, 'anchor'):  # FIXME hack
            # branch used to detect if we have a chance of finding the xml export
            # xml ir is not exported with all the rest at the moment
            self.mbf()

        self.terms()
        self.mis()
        for tag in (
                'protc:input',  # this is slower now
                'protc:input-instance',
                'protc:implied-input',
                'protc:aspect',
                'protc:implied-aspect',
                'protc:executor-verb',
                'protc:parameter*',
                'protc:invariant',
                'protc:output',):
            # FIXME hack around broken anno_tags impl
            self.anno_tags(tag)

    def access(self, ext=None):
        """ Report on datasets that are in the master sheet but which the
            automated pipelines do not have access to. """
        if self.anchor.id == 'N:organization:fake-organization-id':
            # FIXME need a way to generalize/fix this so that organization
            # can vary ...
            print('Not in correct organization for running access report.')
            sys.exit(9999)

        if self.options.server:
            def fmt(d): return hfn.atag(d.uri_human, d.id, new_tab=True)
        else:
            def fmt(d): return d.uri_human

        if self.options.server:
            uri_human = sheets.Master._uri_human()
            def fmtmc(s): return hfn.atag(uri_human, s, new_tab=True)
        else:
            def fmtmc(s): return s

        o = sheets.Overview()
        remote = self.anchor.remote
        remote_datasets = remote.children

        master_sheet_ids = o.dataset_ids()
        bf_api_ids = set(d.id for d in remote_datasets)

        missing_from_api = sorted(master_sheet_ids - bf_api_ids)
        missing_datasets = [self.BlackfynnRemote(i, local_only=True)
                            for i in missing_from_api]
        missing_uris = [fmt(d) for d in missing_datasets]
        rows = [['', ''],
                [fmtmc('Master Count'), len(master_sheet_ids)],
                ['BF API Count', len(bf_api_ids)],
                ['No API Count', len(missing_from_api)],
                *[[i, u] for i, u in zip(missing_from_api, missing_uris)]
        ]
        return self._print_table(rows, title='Access Report', ext=ext)

    _pyru_loaded = False
    def _data_ir(self):
        if self.options.raw:
            data = self.summary.data_for_export(UTCNOWISO())
        elif self.options.export_file:
            if not self.__class__._pyru_loaded:
                self.__class__._pyru_loaded = True
                from pysercomb.pyr import units as pyru
                [register_type(c, c.tag) for c in (pyru._Quant, pyru.Range)]

            with open(self.options.export_file, 'rt') as f:
                data = fromJson(json.load(f))
        else:
            from sparcur import export as ex
            data = self._export(ex.Export).latest_ir
        return data

    @sheets.Reports.makeReportSheet('id')
    def contributors(self, ext=None):
        data = self._data_ir()

        datasets = data['datasets']
        unique = {c['id']:c for d in datasets
                  if 'contributors' in d
                  for c in d['contributors']}
        contribs = sorted(unique.values(),
                          key=lambda c: (c['last_name']  # pYthON dOEsNt hAEV mUlTiLiNE lAmBDaS
                                         if 'last_name' in c else
                                         (c['contributor_name'] if
                                          'contributor_name' in c else
                                          'zzzzzzzzzzzzzzzzzzzzzzzzzzzz')))
        #contribs = sorted((dict(c) for c in
                           #set(frozenset({k:tuple(v) if isinstance(v, list) else
                                          #(frozenset(v.items()) if isinstance(v, dict) else v)
                                          #for k, v in c.items()}.items())
                               #for d in datasets
                               #if 'contributors' in d
                               #for c in d['contributors']
                               #if not log.info(lj(c)))),
                          #key=lambda c: c['last_name'] if 'last_name' in c else c['name'])

        if self.options.debug:
            breakpoint()

        rows = [['id', 'last', 'first', 'PI', 'No Orcid']] + [[
            c['id'],
            c['last_name'] if 'last_name' in c else '',
            c['first_name'] if 'first_name' in c else '',
            'x' if 'contributor_role' in c and 'PrincipalInvestigator' in c['contributor_role'] else '',
            'x' if 'orcid' not in c['id'] else '']
            for c in contribs]

        return self._print_table(rows,
                                 title='Contributors Report',
                                 ext=ext)

    def tofetch(self, dirs=None, ext=None):
        if dirs is None:
            dirs = self.options.directory
            if not dirs:
                dirs.append(self.cwd.as_posix())

        data = []

        def dead(p):
            raise ValueError(p)

        for d in dirs:
            if not Path(d).is_dir():
                continue  # helper files at the top level, and the symlinks that destory python
            path = Path(d).resolve()
            paths = path.rchildren #list(path.rglob('*'))
            path_meta = {p:p.cache.meta if p.cache else dead(p) for p in paths
                         if p.suffix not in ('.swp',)}
            outstanding = 0
            total = 0
            tf = 0
            ff = 0
            td = 0
            uncertain = False  # TODO
            for p, m in path_meta.items():
                #if p.is_file() and not any(p.stem.startswith(pf) for pf in self.spcignore):
                if p.is_file() or p.is_broken_symlink():
                    s = m.size
                    if s is None:
                        uncertain = True
                        continue

                    tf += 1
                    if s:
                        total += s

                    #if '.fake' in p.suffixes:
                    if p.is_broken_symlink():
                        ff += 1
                        if s:
                            outstanding += s

                elif p.is_dir():
                    td += 1

            data.append([path.name,
                         aug.FileSize(total - outstanding),
                         aug.FileSize(outstanding),
                         aug.FileSize(total),
                         uncertain,
                         (tf - ff),
                         ff,
                         tf,
                         td])

        formatted = [[n, l.hr, o.hr, t.hr if not u else '??', lf, ff, tf, td]
                     for n, l, o, t, u, lf, ff, tf, td in
                     sorted(data, key=lambda r: (r[4], -r[3]))]
        rows = [['Folder', 'Local', 'To Retrieve', 'Total', 'L', 'R', 'T', 'TD'],
                *formatted]

        return self._print_table(rows, title='File size counts', ext=ext)

    # TODO generator issue
    #@sheets.Reports.makeReportSheet('suffix', 'mimetype', 'magic_mimetype')
    def filetypes(self, ext=None):
        key = self._sort_key
        paths = self.paths if self.paths else (self.cwd,)
        paths = [c for p in paths for c in p.rchildren if not c.is_dir()]
        rex = re.compile('^\.[0-9][0-9][0-9A-Z]$')
        rex_paths = [p for p in paths if re.match(rex, p.suffix)]
        paths = [p for p in paths if not re.match(rex, p.suffix)]

        def count(thing):
            return sorted([(k if k else '', v) for k, v in
                            Counter([getattr(f, thing)
                                     for f in paths]).items()], key=key)

        each = {t:count(t) for t in ('suffix', 'mimetype', '_magic_mimetype')}
        each['suffix'].append((rex.pattern, len(rex_paths)))

        for title, rows in each.items():
            yield self._print_table(((title, 'count'), *rows),
                                    title=title.replace('_', ' ').strip())

        all_counts = sorted([(*[m if m else '' for m in k], v) for k, v in
                             Counter([(f.suffix, f.mimetype, f._magic_mimetype)
                                      for f in paths]).items()], key=key)

        header = ['suffix', 'mimetype', 'magic mimetype', 'count']
        return self._print_table((header, *all_counts),
                                 title='All types aligned (has duplicates)',
                                 ext=ext)

    @sheets.Reports.makeReportSheet('column_name')
    def samples(self, ext=None):
        data = self._data_ir()
        datasets = data['datasets']
        key = self._sort_key
        # FIXME we need the blob wrapper in addition to the blob generator
        # FIXME these are the normalized ones ...
        samples_headers = tuple(k for dataset_blob in datasets
                                 if 'samples' in dataset_blob  # FIXME inputs?
                                 for samples_blob in dataset_blob['samples']
                                 for k in samples_blob)
        counts = tuple(kv for kv in sorted(Counter(samples_headers).items(),
                                            key=key))

        index_col_name = 'Column Name'
        if ext is None:
            index_col_name += f' unique = {len(counts)}'
        rows = ((index_col_name, '#'), *counts)
        return self._print_table(rows,
                                 title='Samples Report',
                                 ext=ext)

    @sheets.Reports.makeReportSheet('column_name')
    def subjects(self, ext=None):
        data = self._data_ir()
        datasets = data['datasets']
        key = self._sort_key
        # FIXME we need the blob wrapper in addition to the blob generator
        # FIXME these are the normalized ones ...
        subjects_headers = tuple(k for dataset_blob in datasets
                                 if 'subjects' in dataset_blob  # FIXME inputs?
                                 for subject_blob in dataset_blob['subjects']
                                 for k in subject_blob)
        counts = tuple(kv for kv in sorted(Counter(subjects_headers).items(),
                                            key=key))

        index_col_name = 'Column Name'
        if ext is None:
            index_col_name += f' unique = {len(counts)}'
        rows = ((index_col_name, '#'), *counts)
        return self._print_table(rows,
                                 title='Subjects Report',
                                 ext=ext)

    @sheets.Reports.makeReportSheet('id')  # FIXME vs path
    def completeness(self, ext=None):
        if self.options.raw:
            raw = self.summary.completeness
        else:
            from sparcur import export as ex
            datasets = self._data_ir()['datasets']
            raw = [ExporterSummarizer._completeness(data) for data in datasets]

        def rformat(i, si, ci, ei, name, id, award, organ):
            if self.options.server and isinstance(ext, types.FunctionType):
                # rsurl = 'https://projectreporter.nih.gov/reporter_searchresults.cfm'
                dataset_dash_url = self.url_for('route_datasets_id', id=id)
                errors_url = self.url_for('route_reports_errors_id', id=id)
                si = hfn.atag(errors_url + '#submission', si)
                ci = hfn.atag(errors_url + '#curation', ci)
                ei = hfn.atag(errors_url + '#total', ei)
                name = hfn.atag(dataset_dash_url, name)
                id = hfn.atag(dataset_dash_url, id[:10] + '...')
                award = (
                    hfn.atag(('https://scicrunch.org/scicrunch/data/source/'
                              f'nif-0000-10319-1/search?q={award}'), award)
                    if award else 'MISSING')
                organ = organ if organ else ''
                if isinstance(organ, list) or isinstance(organ, tuple):
                    organ = ' '.join([o.atag() for o in organ])
                if isinstance(organ, OntTerm):
                    organ = organ.atag()
            else:
                award = award if award else ''
                organ = ((repr(organ) if isinstance(organ, OntTerm) else organ)
                         if organ else '')
                if isinstance(organ, list):
                    organ = ' '.join([repr(o) for o in organ])

            return (i + 1, si, ci, ei, name, id, award, organ)

        rows = [('', 'SI', 'CI', 'EI', 'name', 'id', 'award', 'organ')]
        rows += [rformat(i, *cols) for i, cols in enumerate(sorted(
            raw, key=lambda t: (t[0], t[1], t[5] if t[5] else 'z' * 999, t[3])))]

        return self._print_table(rows,
                                 title='Completeness Report',
                                 ext=ext)

    @sheets.Reports.makeReportSheet()
    def keywords(self, ext=None):
        data = self._data_ir()
        datasets = data['datasets']
        _rows = [sorted(set(dataset_blob.get('meta', {}).get('keywords', [])),
                        key=lambda v: -len(v))
                    for dataset_blob in datasets]
        rows = [list(r) for r in sorted(
            set(tuple(r) for r in _rows if r),
            key = lambda r: (len(r), tuple(len(c) for c in r if c), r))]
        header = [[f'{i + 1}' for i, _ in enumerate(rows[-1])]]
        rows = header + rows
        return self._print_table(rows,
                                 title='Keywords Report',
                                 ext=ext)


    @sheets.Reports.makeReportSheet('id')
    def size(self, dirs=None, ext=None):
        if dirs is None:
            dirs = self.paths
        intrs = [Integrator(p) for p in dirs]
        if not intrs:
            intrs = self.cwdintr,

        rows = [['path', 'id', 'sparse', 'dirs', 'files', 'size', 'hr'],
                *sorted([[d.name,
                          d.id,
                          'x' if d.path.cache.is_sparse() else '',
                          c['dirs'], c['files'], c['size'], c['size'].hr]
                         for d in intrs
                         for c in (d.datasetdata.counts,)],
                        key=lambda r: -r[-2])]

        return self._print_table(rows, title='Size Report',
                                 align=['l', 'l', 'r', 'r', 'r', 'r', 'r'],
                                 ext=ext)

    @sheets.Reports.makeReportSheet('id')
    def overview(self, ext=None):
        data = self._data_ir()
        datasets = data['datasets']
        hrm = {'award': ['meta', 'award_number'],
               'id': ['id'],
               'name': ['meta', 'folder_name'],
               'errors': ['status', 'error_index'],
               'updated': ['status', 'updated'],
               'published': ['meta', 'doi'],
               'milestone_completion_date': ['submission',
                                             'milestone_completion_date'],
               # TODO
               # subject_count
               # sample_count
               # contributor_count
               }
        header = tuple(hrm)  # TODO counts ?
        paths = tuple(hrm.values())
        def getl(d):
            return [adops.get(d, path, on_failure='') for path in paths]
        _rows = sorted([getl(d) for d in datasets],
                       key=lambda r: (r[-2].asStr()
                                      if isinstance(r[-2], idlib.Stream) else
                                      r[-2],
                                      print(r),
                                      r[0],
                                      r[-3],
                                      r[-4],
                                      ))
        rows = [header] + _rows
        return self._print_table(rows, title='Dataset Report',
                                 align=['l', 'l', 'l', 'r', 'r', 'r', 'r'],
                                 ext=ext)

    def test(self, ext=None):
        rows = [['hello', 'world'], [1, 2]]
        return self._print_table(rows, title='Report Test', ext=ext)


    #@sheets.Reports.makeReportSheet('id')  # TODO bad return format right now
    def errors(self, *, id=None, ext=None):
        data = self._data_ir()
        datasets = data['datasets']

        if self.cwd != self.anchor:
            id = self.cwd.cache.dataset.id

        if id is not None:
            if not id.startswith('N:dataset:'):
                return []

            def pt(rendered_table, title=None):
                """ passthrough """
                return rendered_table

            import htmlfn as hfn
            for dataset_blob in datasets:
                if dataset_blob['id'] == id:
                    dso = DatasetObject.from_json(dataset_blob)
                    title = f'Errors for {id}'
                    urih = dataset_blob['meta']['uri_human']
                    formatted_title = (
                        hfn.h2tag(f'Errors for {hfn.atag(urih, id)}<br>\n') +
                        (hfn.h3tag(dataset_blob['meta']['title']
                                   if 'title' in dataset_blob['meta'] else
                                   dataset_blob['meta']['folder_name'])))
                    log.info(list(dataset_blob.keys()))
                    errors = list(dso.errors)
                    return [(self._print_table(e.as_table(), ext=pt))
                            for e in errors], formatted_title, title
        else:
            pprint.pprint(
                sorted([(d['meta']['folder_name'],
                         [e['message'] for e in get_all_errors(d)])
                        for d in datasets],
                       key=lambda ab: -len(ab[-1])))

    def pathids(self, ext=None):
        base = self.project_path.parent
        rows = [['path', 'id']] + sorted([c.relative_to(base), c.cache.id]#, c.cache.uri_api, c.cache.uri_human]
                                         # slower to include the uris
                                         for c in chain((self.cwd,),
                                                        self.cwd.rchildren)
        )
        return self._print_table(rows, title='Path identifiers', ext=ext)

    #@sheets.Reports.makeReportSheet('id')  # TODO bad return format right now
    def mbf(self, ext=None):
        et = tuple()
        from sparcur.extract import xml as exml

        def settype(mimetype):
            return {exml.ExtractMBF.mimetype: 'MBF Metadata',
                    exml.ExtractNeurolucida.mimetype: 'Neurolucida',}[mimetype]

        if self.options.raw:
            blob_ir = self.parent.export()
        else:
            from sparcur import export as ex
            blob_ir = self._export(ex.ExportXml).latest_ir  # FIXME need to load?

        mbf_types = tuple(c.mimetype for c in
                          (exml.ExtractMBF, exml.ExtractNeurolucida))
        if self.options.unique:
            key = lambda p: (p[2], not p[0], p[1].lower(), p[1])
        else:
            key = lambda p: (p[2], p[3], not p[0], p[1].lower(), p[1])

        all_conts = sorted(set(((OntId(c['id_ontology'])
                                 if 'id_ontology' in c else
                                 ''),
                                c['name'],
                                settype(metadata_blob['type']),
                                *((dataset_xml['dataset_id'],)
                                  if not self.options.unique else et))
                               for dataset_xml in blob_ir.values()
                               if dataset_xml['type'] == 'all-xml-files'  # FIXME
                               for metadata_blob in dataset_xml['xml']
                               if metadata_blob['type'] in mbf_types and
                               'contours' in metadata_blob['extracted']
                               for c in metadata_blob['extracted']['contours']),
                           key=key)
        header = [['id', 'name', 'metadata source', 'dataset']]
        if self.options.unique:
            header[0] = header[0][:-1]

        return self._print_table(header + all_conts,
                                 title='Unique MBF contours',
                                 ext=ext)

    def terms(self, ext=None):
        # anatomy
        # cells
        # subcelluar
        import rdflib
        # FIXME cache these results and only recompute if latest changes?
        if self.options.raw:
            graph = self.summary.triples_exporter.graph
        else:
            from sparcur import export as ex  # FIXME very slow to import
            graph = OntGraph()
            self._export(ex.Export).latest_export_ttl_populate(graph)

        objects = set()
        skipped_prefixes = set()
        for t in graph:
            for e in t:
                if (isinstance(e, rdflib.URIRef) and
                    not e.startswith('info:') and
                    not e.startswith('doi:')):
                    oid = OntId(e)
                    if oid.prefix in want_prefixes:
                        objects.add(oid)
                    else:
                        skipped_prefixes.add(oid.prefix)

        if self.options.server and isinstance(ext, types.FunctionType):
            def reformat(ot):
                return [ot.label
                        if hasattr(ot, 'label') and ot.label else
                        '',
                        ot.atag(curie=True)]

        else:
            def reformat(ot):
                return [ot.label
                        if hasattr(ot, 'label') and ot.label else
                        '',
                        ot.curie]

        log.info(' '.join(sorted(skipped_prefixes)))
        objs = [OntTerm(o) if o.prefix not in ('TEMP', 'sparc') or
                o.prefix == 'TEMP' and o.suffix.isdigit() else
                o for o in objects]
        term_sets = {title:[o for o in objs if o.prefix == prefix]
                     for prefix, title in
                     (('NCBITaxon', 'Species'),
                      ('UBERON', 'Anatomy and age category'),  # FIXME
                      ('FMA', 'Anatomy (FMA)'),
                      ('PATO', 'Qualities'),
                      ('tech', 'Techniques'),
                      ('unit', 'Units'),
                      ('sparc', 'MIS terms'),
                      ('TEMP', 'Suggested terms'),
                     )}

        term_sets['Other'] = set(objs) - set(ot for v in term_sets.values()
                                             for ot in v)

        for title, terms in term_sets.items():
            header = [['Label', 'CURIE']]
            rows = header + [reformat(ot) for ot in
                            sorted(terms,
                                   key=lambda ot: (ot.prefix, ot.label.lower()
                                                   if hasattr(ot, 'label') and ot.label else ''))]

            yield self._print_table(rows, title=title, ext=ext)

    @idlib.utils.cache_result
    def _annos(self):
        group_id = auth.get('hypothesis-group')
        cache_file = Path(hyp.group_to_memfile(group_id + 'sparcur'))
        get_annos = hyp.Memoizer(memoization_file=cache_file, group=group_id)
        get_annos.api_token = auth.get('hypothesis-api-key')
        return get_annos()

    @idlib.utils.cache_result
    def _helpers(self):
        annos = self._annos()
        return [hyp.HypothesisHelper(a, annos) for a in annos]

    @idlib.utils.cache_result
    def _protc(self):
        from pysercomb.parsers import racket
        from protcur.analysis import protc
        annos = self._annos()
        return [protc(a, annos) for a in annos]

    def _protc_input(self, hrmd, tags, protc, ext):
        from pysercomb import exceptions as pexc
        from pysercomb.parsers import racket
        import pysercomb.pyr.units as pyru
        pyru.Hyp.bindImpl(None, HypothesisAnno=protc.byId)
        protcur_interpreter = pyru.Protc()
        def more(k, v):  # FIXME these can actually be parsed in parallel
            count = len(v)
            link = v[0].shareLink if count == 1 else ''
            facet = 'TODO'
            autos = set()
            for protc_helper in v:
                ok, expr, rest = racket.exp(repr(protc_helper))
                #log.critical(v)  # FIXME this induces an infinite loop I think due to a missing child
                #log.critical(expr)
                if expr is None or isinstance(expr, str):
                    continue
                try:
                    hrm = protcur_interpreter(expr)
                except pexc.ParseFailure as e:
                    log.error(e)
                    continue

                if isinstance(hrm.black_box, pyru.Term):
                    autos.add(hrm.black_box)

                continue

            auto_mapping = ';'.join([f'{bb.curie}|{bb.label}' for bb in autos])
            return count, link, facet, auto_mapping

        header = [['tag', 'value', 'text', 'exact', 'count', 'link', 'facet', 'auto-mapping']]
        #rows = [[(*k, ' '.join([a.shareLink for a in v]))] for k, v in sorted(hrm.items())]
        rows = [[*k, *more(k, v)]
                for k, v in sorted(list(hrmd.items()))]

        if self.options.sort_count_desc:
            rows = sorted(rows, key=lambda r: -r[-4])

        return self._print_table(header + rows,
                                 title=f'Annos for {" ".join(tags)}',
                                 ext=ext)

    def _process_protc(self, tags, matches, ext):
        from protcur.analysis import protc
        from pysercomb.pyr import units as pyru
        from pysercomb.parsers import racket

        lowernorm = ('protc:aspect',
                     'protc:implied-aspect',
                     'protc:executor-verb',
                     'protc:parameter*',
                     'protc:invariant',
                     )

        self._protc()
        # FIXME cannot breakpoint with this protc instances around ...
        pm = [protc.byId(a.id) for _, a in matches]

        # testing
        if 'protc:input' in tags:
            input = pm[13]
            invar = next(next(input.children).children)
            paramparser = pyru.ParamParser()
            tv = racket.sexp(invar.parameter())[1]
            tp = invar._parameter[1]
            assert tv == tp
            hrm = paramparser(tv)

        hrm = defaultdict(list)
        def denone(t): return tuple('' if e is None else e for e in t)
        for p in pm:
            pt = [t for t in p.tags if t in tags]
            if pt:
                hrm[denone((
                    pt[0],
                    normalize_exact(
                        p.value,
                        lower=anyMembers(tags, *lowernorm)),
                    normtext(
                        p.text,
                        lower=anyMembers(tags, *lowernorm)),
                    normalize_exact(
                        p.exact,
                        lower=anyMembers(tags,
                                            *lowernorm))))].append(p)
            else:
                log.warning(f'WAT {p.tags}')

        hrmd = dict(hrm)
        if 'protc:executor-verb' in tags:  # woo past tense
            ends_with_e = {h[1]:h for h, p in hrmd.items() if h[1].endswith('e')}
            others = {h[1]:h for h, p in hrmd.items()
                    if not h[1].endswith('ed') and not h[1].endswith('e')}
            for k, p in list(hrmd.items()):
                _, v, _, _ = k
                if v.endswith('ed'):
                    ewe = v[:-1]
                    if ewe in ends_with_e:
                        nk = ends_with_e[ewe]
                    elif v[:-2] in others:
                        nk = others[v[:-2]]
                    else:
                        continue

                    hrmd[nk].extend(hrmd.pop(k))

            # this has to come after due to remapping that happens above first
            wev = sheets.WorkingExecVerb()
            value_to_key, create = wev.condense()
            for _key in create:
                if _key not in hrmd:
                    hrmd[_key] = []

            for k, p in list(hrmd.items()):
                _, v, _, _ = k
                if v in value_to_key:
                    nk = value_to_key[v]
                    hrmd[nk].extend(hrmd.pop(k))

        if 'protc:input' in tags:
            return self._protc_input(hrmd, tags, protc, ext)

        def more(k, v):
            count = len(v)
            link = v[0].shareLink if count == 1 else ''
            facet = 'TODO'
            return count, link, facet

        header = [['tag', 'value', 'text', 'exact', 'count', 'link', 'facet']]
        #rows = [[(*k, ' '.join([a.shareLink for a in v]))] for k, v in sorted(hrm.items())]
        rows = [[*k, *more(k, v)]
                for k, v in sorted(hrmd.items())]

        if self.options.sort_count_desc:
            rows = sorted(rows, key=lambda r: -r[-3])

        return self._print_table(header + rows,
                                    title=f'Annos for {" ".join(tags)}',
                                    ext=ext)

    def _by_tags(self, *tags, links=True, ext=None):
        annos = self._annos()
        bannos = [ptcdoc.Annotation(a) for a in annos]  # better annos
        pool = ptcdoc.Pool(bannos)
        anno_counts = ptcdoc.AnnoCounts(pool)

        self._helpers()
        process_protc = anyMembers(tags,
                                   'protc:executor-verb',
                                   'protc:input',
                                   'protc:aspect',
                                   'protc:input-instance',
                                   'protc:implied-aspect',
                                   'protc:implied-input',
                                   'protc:parameter*',
                                   'protc:invariant',
                                   'protc:output',
                                   )

        # note that HypothesisHelper.byTags is an AND search not and OR search
        matches = [(normalize_exact(a.exact, lower=True), a)
                   for tag in tags for a in hyp.HypothesisHelper.byTags(tag)]

        if process_protc:
            return self._process_protc(tags, matches, ext)

        md = defaultdict(list)
        for e, a in matches:
            md[e].append(a)

        def gtag(e):  # FIXME memoize/cache?
            return ' '.join(sorted(set(t for a in md[e] for t in a.tags
                                       if t in tags)))

        def link(e): pass
        rows = sorted([[gtag(e),  # tag
                        e,        # key
                        c,        # count
                        (' '.join([a.htmlLink  # links
                                   if self.options.uri_api else
                                   a.shareLink for a in md[e]]
                                  if links else '')),
                        '',       # docs
                        '',       # exacts
                        ]
                       for e, c in Counter([normalize_exact(e)
                                            for e, a in matches]).most_common()],
                      key=lambda ab: (ab[0], -ab[2], ab[1].lower()))

        header = [['tag', 'key', 'count', 'links', 'docs', 'exacts']]
        return self._print_table(header + rows,
                                 title=f'Annos for {" ".join(tags)}',
                                 ext=ext)

    @sheets.AnnoTags.makeReportSheet()
    def anno_tags(self, *tags, links=False, ext=None):
        if not tags:
            tags = self.options.tag

        links = links or self.options.uri or self.options.uri_api

        return self._by_tags(*tags, links=links, ext=ext)

    @property
    @idlib.utils.cache_result
    def _ttlfile(self):
        # TODO uri/path reconciliation 
        if self.options.ttl_file:
            ontres = OntRes.fromStr(self.options.ttl_file)
        else:
            # FIXME make this go away and derive it from git
            iri = 'https://cassava.ucsd.edu/sparc/exports/curation-export.ttl'
            ontres = OntResIri(iri)

        if self.options.ttl_compare:
            ontres_compare_to = OntRes.fromStr(self.options.ttl_compare)
        else:
            ontres_compare_to = None

        return TtlFile(ontres, ontres_compare_to)

    def changes(self):
        tout = self._ttlfile.changes()

    @sheets.Reports.makeReportSheet('curies')
    def mis(self, ext=None):
        tout = self._ttlfile.mis(ext=ext)
        return self._print_table(tout,
                                 title=f'MIS predicates',
                                 ext=ext)
