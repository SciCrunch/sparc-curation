#!/usr/bin/env python3
import copy
import json
import math
import hashlib
from types import GeneratorType
from datetime import datetime
from functools import wraps
from itertools import chain
from collections import defaultdict, deque
import rdflib
import dicttoxml
from pyontutils.config import devconfig
from pyontutils.namespaces import OntCuries, makeNamespaces, TEMP, isAbout
from pyontutils.closed_namespaces import rdf, rdfs, owl, skos, dc
from nifstd_tools.methods.core import prot, proc, tech, asp, dim, unit
from protcur.analysis import parameter_expression
from protcur.core import annoSync
from protcur.analysis import protc, Hybrid
from terminaltables import AsciiTable
from hyputils.hypothesis import group_to_memfile, HypothesisHelper
from sparcur import config
from sparcur import exceptions as exc
from sparcur import datasets as dat
from sparcur.core import JT, JEncode, log, logd, lj
from sparcur.core import sparc, DictTransformer, adops, OntId, OntTerm
from sparcur.paths import Path
from sparcur import schemas as sc
from sparcur import converters as conv
from sparcur.datasources import OrganData, OntologyData, MembersData
from sparcur.protocols import ProtocolData, ProtcurData
from sparcur.schemas import SummarySchema, MetaOutSchema  # XXX deprecate
from sparcur import schemas as sc
from sparcur import pipelines as pipes
from sparcur import sheets
from ttlser import CustomTurtleSerializer
from pysercomb.pyr.units import Expr, _Quant as Quantity
from pyontutils.utils import byCol as _byCol

xsd = rdflib.XSD
a = rdf.type

po = CustomTurtleSerializer.predicateOrder
po.extend((sparc.firstName,
           sparc.lastName,
           xsd.minInclusive,
           xsd.maxInclusive,
           TEMP.hasValue,
           TEMP.hasUnit,))

OntCuries({'orcid':'https://orcid.org/',
           'ORCID':'https://orcid.org/',
           'DOI':'https://doi.org/',
           'dataset':'https://api.blackfynn.io/datasets/N:dataset:',
           'package':'https://api.blackfynn.io/packages/N:package:',
           'user':'https://api.blackfynn.io/users/N:user:',
           'unit': str(unit),
           'dim': str(dim),
           'asp': str(asp),
           'tech': str(tech),
           'awards':str(TEMP['awards/']),
           'sparc':str(sparc),})


class CurationStatusStrict:
    def __init__(self, fthing):
        self.fthing = fthing
        # metadata  # own class?

        # protocol  # own class?

    @property
    def dataset(self):
        if self.fthing.is_dataset:
            return self.fthing
        else:
            return DatasetStructureH(self.bids_root)

    @property
    def miss_paths(self):
        yield from self.dataset.submission_paths

    @property
    def dd_paths(self):
        yield from self.dataset.dataset_description_paths

    @property
    def ject_paths(self):
        yield from self.dataset.subjects_paths

    @property
    def overall(self):
        parts = (self.structure,
                 self.metadata,
                 self.protocol)

        n_parts, n_done = 0, 0
        for n_subparts, n_subdone in parts:
            n_parts += n_subparts
            n_done += n_subdone

        return n_parts, n_done

    @property
    def structure(self):
        # in the right place
        parts = (self.has_submission,
                 self.has_dataset_description,
                 self.has_subjects)
        done = [p for p in parts if p]
        return len(parts), len(done)

    @property
    def has_submission(self):
        # FIXME len 1?
        return bool(list(self.miss_paths))

    @property
    def has_dataset_description(self):
        # FIXME len 1?
        return bool(list(self.dd_paths))

    @property
    def has_subjects(self):
        # FIXME len 1?
        return bool(list(self.ject_paths))

    @property
    def metadata(self):
        parts = (self.has_award,
                 self.has_protocol_uri)
        done = [p for p in parts if p]
        n_parts, n_done = len(parts), len(done)
        # TODO accumulate all the other metadata that we need from the files
        return n_parts, n_done

    @property
    def has_award(self):
        # FIXME len 1?
        return bool(list(self.dataset.award))

    @property
    def has_protocol_uri(self):
        return bool(list(self.dataset.protocol_uris))

    @property
    def has_protocol(self):
        return bool(list(self.dataset.protocol_jsons))

    @property
    def has_protocol_annotations(self):
        return bool(list(self.dataset.protocol_jsons))

    @property
    def protocol(self):
        # TODO n_parts = n_total_fields_based_on_context - n_already_filled_out
        # we will try to get as many as we can from the protocol
        parts = (self.has_protocol)
        return 0, 0

    def steps_done(self, stage_name):
        n_parts, n_done = getattr(self, stage_name)
        return f'{stage_name} {n_done}/{n_parts}'

    @property
    def report(self):
        # FIXME this format is awful, need an itemized version
        return (self.steps_done('overall'),
                self.steps_done('structure'),
                self.steps_done('metadata'),
                self.steps_done('protocol'),)

    @property
    def row(self):
        """ tabular representation of the row """
        for column in self.report_header:
            yield getattr(self, column)

    def __repr__(self):
        return self.dataset.dataset_name_proper + '\n' + '\n'.join(self.report)


class CurationStatusLax(CurationStatusStrict):
    # FIXME not quite working right ...
    @property
    def miss_paths(self):
        #yield from self.dataset.path.rglob('submission*.*')
        #yield from self.dataset.path.rglob('Submission*.*')
        for path in self.dataset.path.rglob('*'):
            if path.is_file() and path.name.lower().startswith('submission'):
                yield path

    @property
    def dd_paths(self):
        #yield from self.dataset.path.rglob('dataset_description*.*')
        #yield from self.dataset.path.rglob('Dataset_description*.*')
        for path in self.dataset.path.rglob('*'):
            if path.is_file() and path.name.lower().startswith('dataset_description'):
                yield path

    @property
    def jetc_paths(self):
        #yield from self.dataset.path.rglob('subjects*.*')
        #yield from self.dataset.path.rglob('Subjects*.*')
        for path in self.dataset.path.rglob('*'):
            if path.is_file() and path.name.lower().startswith('subjects'):
                yield path


class MetaMaker:
    """ FIXME this is a bad pattern :/ """
    @staticmethod
    def _generic(gen):
        l = list(gen)
        if len(l) == 1:
            return l[0]
        else:
            return l

class PathData:
    def __new__(cls, path, cypher=hashlib.sha256):
        #cls.schema = cls.schema_class()
        #cls.schema_out = cls.schema_out_class()
        return super().__new__(cls)

    def __init__(self, path, cypher=hashlib.sha256):
        self._errors = []
        if isinstance(path, str):
            path = Path(path)

        if not hasattr(self, 'path'):
            self.path = path

        self.cypher = cypher

    @property
    def id(self):
        return self.path.cache.id

    @property
    def uri_human(self):
        return self.path.cache.uri_human

    @property
    def uri_api(self):
        return self.path.cache.uri_api

    #@property
    #def name(self):
        #return self.path.name

    @property
    def _meta(self):
        """ pathmeta NOT json meta (confusingly) """
        cache = self.path.cache
        if cache is not None:
            return self.path.cache.meta

    """
    @property
    def parent(self):
        if self == self.anchor:
            return None

        #log.debug(f'{self} {self.anchor}')
        if self.path.parent.cache:
            pp = self.__class__(self.path.parent)
            if pp.id is not None:
                return pp

    @property
    def parents(self):
        parent = self.parent
        parents = []
        while parent is not None:
            parents.append(parent)
            parent = parent.parent

        yield from reversed(parents)

    @property
    def children(self):
        for child in self.path.children:
            yield self.__class__(child)

    """
    @property
    def anchor(self):
        if hasattr(self, 'project_path'):
            if self.path == self.project_path:
                return self

            return self.__class__(self.project_path)


class DatasetStructureH(PathData, dat.DatasetStructure):
    """ a homogenous representation """

    @property
    def path(self):
        return self

    @property
    def is_organization(self):
        if self.anchor is not None:
            if self == self.anchor:
                return True

            return self.id.startswith('N:organization:')

    @property
    def organization(self):
        """ organization represents a permissioning boundary
            for blackfynn, so above this we would have to know
            in advance the id and have api keys for it and the
            containing folder would have some non-blackfynn notes
            also it seems likely that the same data could appear in
            multiple different orgs, so that would mean linking locally
        """

        # FIXME in reality files can have more than one org ...
        if self.is_organization:
            return self
        elif self.parent:
            return self.parent.organization

    @property
    def is_dataset(self):
        return self.id.startswith('N:dataset:')

    @property
    def dataset(self):
        if self.is_dataset:
            return self
        elif self.parent:
            return self.parent.dataset

    @property
    def id(self):
        if not hasattr(self, '_id'):
            if self._meta is not None:
                self._id = self._meta.id
            else:
                self._id = None
                # give that fthing should be sitting atop already fetched data, this seems ok
                # we can kill these more frequently anyway? they live for the time it takes them
                # to produce their data and then they move on?

        return self._id

    @property
    def dataset_name_proper(self):
        try:
            award = next(iter(set(self.award)))  # FIXME len1? testing?
        except StopIteration:
            award = '?-no-award-?'
        pis = list(self.PI)
        if not pis:
            pis = '?-no-pi-?',
        PI = ' '.join(pis)

        species = list(self.species)
        if not species:
            species = '?-no-species-?',
        species = ' '.join(s.label if isinstance(s, OntTerm) else s for s in species)

        return f'{award} {PI} {species} {self.organ} {self.modality}'

    @property
    def bf_size(self):
        size = self._meta.size
        if size:
            return size
        elif self.path.is_dir():
            size = 0
            for path in self.path.rglob('*'):
                if path.is_file():
                    try:
                        size += path.cache.meta.size
                    except OSError as e:
                        log.warning(f'No cached file size. Assuming it is not tracked. {path}')

            return size

        else:
            log.warning(f'unknown thing at path {self.path}')

    @property
    def bf_checksum(self):
        return self._meta.checksum

    def checksum(self):
        if self.path.is_file():
            return self.path.checksum(cypher=self.cypher)

        elif self.path.is_dir():
            # TODO need to determine the hashing rule for folders
            pass

    @property
    def _submission_objects(self):
        for p in self.submission_paths:
            try:
                miss = dat.SubmissionFile(p)
                if miss.data:
                    yield miss
            except dat.SubmissionFile.NoDataError as e:
                self._errors.append(e)  # NOTE we treat empty file as no file
            except AttributeError as e:
                log.warning(f'unhandled metadata type {e!r}')
                self._errors.append(e)

    @property
    def submission(self):
        if not hasattr(self, '_subm_cache'):
            self._subm_cache = list(self._submission_objects)

        yield from self._subm_cache

    @property
    def _dd(self):
        for p in self.dataset_description_paths:
            yield dat.DatasetDescriptionFile(p)

    @property
    def _dataset_description_objects(self):
        for p in self.dataset_description_paths:
            #yield from DatasetDescription(t)
            # TODO export adapters for this ... how to recombine and reuse ...
            try:
                dd = dat.DatasetDescriptionFile(p)
                if dd.data:
                    yield dd
            except dat.DatasetDescriptionFile.NoDataError as e:
                self._errors.append(e)  # NOTE we treat empty file as no file
            except AttributeError as e:
                log.warning(f'unhandled metadata type {e!r}')
                self._errors.append(e)

    @property
    def dataset_description(self):
        if not hasattr(self, '_dd_cache'):
            self._dd_cache = list(self._dataset_description_objects)

        yield from self._dd_cache

    @property
    def _subjects_objects(self):
        """ really subjects_file """
        for path in self.subjects_paths:
            try:
                sf = dat.SubjectsFile(path)
                if sf.data:
                    yield sf
            except dat.DatasetDescriptionFile.NoDataError as e:
                self._errors.append(e)  # NOTE we treat empty file as no file
            except AttributeError as e:
                log.warning(f'unhandled metadata type {e!r}')
                self._errors.append(e)

    @property
    def subjects(self):
        if not hasattr(self, '_subj_cache'):
            self._subj_cache = list(self._subjects_objects)

        yield from self._subj_cache

    @property
    def data_lift(self):
        """ used to validate repo structure """

        out = {}

        for section_name in ('submission', 'dataset_description', 'subjects'):
            #path_prop = section_name + '_paths'
            for section in getattr(self, section_name):
                section_name += '_file'
                tp = section.path.as_posix()
                if section_name in out:
                    ot = out[section_name]
                    # doing it this way will cause the schema to fail loudly :)
                    if isinstance(ot, str):
                        out[section_name] = [ot, tp]
                    else:
                        ot.append(tp)
                else:
                    out[section_name] = tp

        return out

    @property
    def meta_paths(self):
        """ All metadata related paths. """
        #yield self.path
        yield from self.submission_paths
        yield from self.dataset_description_paths
        yield from self.subjects_paths

    @property
    def _meta_tables(self):
        yield from (s.t for s in self.submission)
        yield from (dd.t for dd in self.dataset_description)
        yield from (s.t for s in self.subjects)

    @property
    def meta_sections(self):
        """ All metadata related objects. """
        yield self
        yield from self.submission
        yield from self.dataset_description
        yield from self.subjects

    @property
    def __errors(self):  # XXX deprecated
        gen = self.meta_sections
        next(gen)  # skip self
        for thing in gen:
            yield from thing.errors

        yield from self._errors

    @property
    def report(self):

        r = '\n'.join([f'{s:>20}{l:>20}' for s, l in zip(self.status.report, self.lax.report)])
        return self.name + f"\n'{self.id}'\n\n" + r


    def _with_errors(self, ovd, schema):
        # FIXME this needs to be split into in and out
        # and really all this section should do is add the errors
        # on failure and it should to it in place in the pipeline

        # data in -> check -> with errors -> normalize/correct/map ->
        # -> augment and transform -> data out check -> out with errors

        # this flow splits at normalize/correct/map -> export back to user

        ok, valid, data = ovd
        out = {}  # {'raw':{}, 'normalized':{},}
        # use the schema to auto fill things that we are responsible for
        # FIXME possibly separate this to our own step?
        try:
            if 'id' in schema.schema['required']:
                out['id'] = self.id
        except KeyError:
            pass

        if not ok:
            # FIXME this will dump the whole schema, go smaller
            if 'errors' not in out:
                out['errors'] = []

            #out['errors'] += [{k:v if k != 'schema' else k
                               #for k, v in e._contents().items()}
                              #for e in data.errors]
            out['errors'] += valid.json()

        if schema == self.schema:  # FIXME icky hack
            for section_name, path in data.items():
                if section_name == 'id':
                    # FIXME MORE ICK
                    continue
                # output sections don't all have python representations at the moment
                if hasattr(self, section_name):
                    section = next(getattr(self, section_name))  # FIXME non-homogenous
                    # we can safely call next here because missing or multi
                    # sections will be kicked out
                    # TODO n-ary cases may need to be handled separately
                    sec_data = section.data_with_errors
                    #out.update(sec_data)  # the magic of being self describing
                    out[section_name] = sec_data  # FIXME

        else:
            out['errors'] += data.pop('errors', [])

        if self.is_organization:  # FIXME ICK
            # can't run update in cases where there might be bad data
            # in the original data
            out.update(data)

        return out

    @property
    def data_with_errors(self):
        # FIXME remove this
        # this still ok, dwe is very simple for this case
        return self._with_errors(self.schema.validate(copy.deepcopy(self.data)), self.schema)

    def dump_all(self):
        return {attr: express_or_return(getattr(self, attr))
                for attr in dir(self) if not attr.startswith('_')}

    def __eq__(self, other):
        # TODO order by status
        if self.path is self:
            return super().__eq__(other.path)

        return self.path == other.path

    def __gt__(self, other):
        if self.path is self:
            return super().__gt__(other.path)

        return self.path > other.path

    def __lt__(self, other):
        if self.path is self:
            return super().__lt__(other.path)

        return self.path < other.path

    def __hash__(self):
        return hash((hash(self.__class__), self.id))  # FIXME checksum? (expensive!)

    def __repr__(self):
        return f'{self.__class__.__name__}(\'{self.path}\')'


class __Old:

    @property
    def __protocol_uris(self):
        """ property needed for protocol helper to help us """
        #if not hasattr(self, '_puri_cache'):
        p = 'protocol_url_or_doi'
        for dd in self.dataset_description:
            dwe = dd.data_with_errors
            if p in dwe:
                for uri in dwe[p]:
                    if uri.startswith('http'):
                        # TODO normalize
                        yield uri
                    else:
                        log.warning(f"protocol not uri {uri} '{self.id}'")

    @property
    def __keywords(self):
        for dd in self.dataset_description:
            # FIXME this pattern leads to continually recomputing values
            # we need to be deriving all of this from a checkpoint on the fully
            # normalized and transformed data
            data = dd.data_with_errors
            if 'keywords' in data:  # already logged error ...
                yield from data['keywords']

    @property
    def __species(self):
        """ generate this value from the underlying data """
        if not hasattr(self, '_species'):
            out = set()
            for subject_file in self.subjects:
                data = subject_file.data_with_errors
                if 'subjects' in data:
                    subjects = data['subjects']
                    for subject in subjects:
                        if 'species' in subject:
                            out.add(subject['species'])

            self._species = tuple(out)

        return self._species

    @property
    def __organ(self):
        # yield 'Unknown'
        return
        organs = ('Lung',
                  'Heart',
                  'Liver',
                  'Pancreas',
                  'Kidney',
                  'Stomach',
                  'Spleen',
                  'Colon',
                  'Large intestine',
                  'Small intestine',
                  'Urinary bladder',
                  'Lower urinary tract',
                  'Spinal cord',)
        org_lower = (o.lower() for o in organs)
        for kw in self.keywords:
            if kw.lower() in organs:
                yield kw


class LThing:
    def __init__(self, lst):
        self._lst = lst


class TriplesExport:

    def __init__(self, path, *args, **kwargs):
        super().__init__(path, *args, **kwargs)

    def ddt(self, data):
        dsid = self.uri_api
        s = rdflib.URIRef(dsid)
        if 'meta' in data:
            dmap = {'acknowledgements': '',
                    'additional_links': '',
                    'award_number': '',
                    'completeness_of_data_set': '',
                    'contributor_count': '',
                    #'contributors': '',
                    'description': dc.description,
                    'errors': '',
                    'examples': '',
                    'funding': '',
                    'uri_human': '',
                    'uri_api': '',
                    'keywords': '',
                    'links': '',
                    'modality': '',
                    'name': '',
                    'organ': '',
                    'originating_article_doi': '',
                    'principal_investigator': '',
                    'prior_batch_number': '',
                    'protocol_url_or_doi': '',
                    'sample_count': '',
                    'species': '',
                    'subject_count': '',
                    'title_for_complete_data_set': ''}

            for k, _v in data['meta'].items():
                # FIXME how is temp sneeking through?
                if k in dmap:
                    p = dmap[k]
                    if p:
                        for v in (_v if  # FIXME write a function for list vs atom
                                    isinstance(_v, tuple) or
                                    isinstance(_v, list) else
                                    (_v,)):
                            o = rdflib.Literal(v)
                            yield s, p, o

                else:
                    log.error(f'wtf error {k}')
        if 'contributors' in data:
            if 'creators' in data:
                creators = data['creators']
            else:
                creators = []
            for c in data['contributors']:
                creator = ('name' in c and
                           [cr for cr in creators if 'name' in cr and cr['name'] == c['name']])
                yield from self.triples_contributors(c, creator=creator)

    def triples_contributors(self, contributor, creator=False):
        try:
            dsid = self.uri_api
        except BaseException as e:  # FIXME ...
            log.error(e)
            return

        # get member if we can find them
        if 'name' in contributor and 'first_name' in contributor:
            fn = contributor['first_name']
            ln = contributor['last_name']
            if ' ' in fn:
                fn, mn = fn.split(' ', 1)

            failover = f'{fn}-{ln}'
            member = self.member(fn, ln)

            if member is not None:
                userid = rdflib.URIRef('https://api.blackfynn.io/users/' + member.id)

        else:
            member = None
            failover = 'no-orcid-no-name'
            log.warning(f'No name!' + lj(contributor))

        if 'contributor_orcid_id' in contributor:
            s = rdflib.URIRef(contributor['contributor_orcid_id'])
            if member is not None:
                yield s, TEMP.hasBlackfynnUserId, userid
        else:
            if member is not None:
                s = userid
            else:
                log.debug(lj(contributor))
                s = rdflib.URIRef(dsid + '/contributors/' + failover)

        yield s, a, owl.NamedIndividual
        yield s, a, sparc.Researcher
        yield s, TEMP.contributorTo, rdflib.URIRef(dsid)
        converter = conv.ContributorConverter(contributor)
        yield from converter.triples_gen(s)
        if creator:
            yield s, TEMP.creatorOf, rdflib.URIRef(dsid)

        return
        for field, value in contributor.items():
            convert = getattr(converter, field, None)
            for v in (value if isinstance(value, tuple) or isinstance(value, list) else (value,)):
                if convert is not None:
                    yield (s, *convert(v))
                elif field == 'name':  # skip because we do first/last
                    pass
                else:
                    log.warning(f'Unhandled contributor field: {field}')

    @property
    def triples(self):
        # FIXME ick
        data = self.data
        try:
            dsid = self.uri_api
        except BaseException as e:  # FIXME ...
            raise e
            return

        if 'meta' in data:
            meta_converter = conv.MetaConverter(data['meta'], self)
            yield from meta_converter.triples_gen(dsid)
        else:
            log.warning(f'{self} has no meta!')  # FIXME split logs into their problems, and our problems

        yield from conv.StatusConverter(data['status'], self).triples_gen(dsid)

        #converter = conv.DatasetConverter(data)
        #yield from converter.triples_gen(dsid)

        def id_(v):
            s = rdflib.URIRef(dsid)
            yield s, a, owl.NamedIndividual
            yield s, a, sparc.Resource
            yield s, rdfs.label, rdflib.Literal(self.name)

        def subject_id(v, species=None):  # TODO species for human/animal
            v = v.replace(' ', '%20')  # FIXME use quote urlencode
            s = rdflib.URIRef(dsid + '/subjects/' + v)
            return s

        yield from id_(self.id)

        def triples_gen(prefix_func, subjects):

            for i, subject in enumerate(subjects):
                converter = conv.SubjectConverter(subject)
                if 'subject_id' in subject:
                    s_local = subject['subject_id']
                else:
                    s_local = f'local-{i + 1}'  # sigh

                s = prefix_func(s_local)
                yield s, a, owl.NamedIndividual
                yield s, a, sparc.Subject
                yield from converter.triples_gen(s)
                continue
                for field, value in subject.items():
                    convert = getattr(converter, field, None)
                    if convert is not None:
                        yield (s, *convert(value))
                    elif field not in converter.known_skipped:
                        log.warning(f'Unhandled subject field: {field}')

        yield from triples_gen(subject_id, self.subjects)

        #for subjects in self.subjects:
            #for s, p, o in subjects.triples_gen(subject_id):
                #if type(s) == str:
                    #breakpoint()
                #yield s, p, o

        yield from self.ddt(data)

    @property
    def ontid(self):
        return rdflib.URIRef(f'https://sparc.olympiangods.org/sparc/ontologies/{self.id}')

    @property
    def header_graph_description(self):
        return rdflib.Literal(f'SPARC single dataset graph for {self.id}')

    @property
    def triples_header(self):
        ontid = self.ontid
        nowish = datetime.utcnow()  # request doesn't have this
        epoch = nowish.timestamp()
        iso = nowish.isoformat().replace('.', ',')  # sakes fist at iso for non standard
        ver_ontid = rdflib.URIRef(ontid + f'/version/{epoch}/{self.id}')
        sparc_methods = rdflib.URIRef('https://raw.githubusercontent.com/SciCrunch/'
                                      'NIF-Ontology/sparc/ttl/sparc-methods.ttl')

        pos = (
            (a, owl.Ontology),
            (owl.versionIRI, ver_ontid),
            (owl.versionInfo, rdflib.Literal(iso)),
            (isAbout, rdflib.URIRef(self.path.cache.uri_api)),
            (TEMP.hasHumanUri, rdflib.URIRef(self.path.cache.uri_human)),
            (rdfs.label, rdflib.Literal(f'{self.name} curation export graph')),
            (rdfs.comment, self.header_graph_description),
            (owl.imports, sparc_methods),
        )
        for p, o in pos:
            yield ontid, p, o

    @property
    def ttl(self):
        graph = rdflib.Graph()
        self.populate(graph)
        return graph.serialize(format='nifttl')

    def populate(self, graph):
        OntCuries.populate(graph)  # ah smalltalk thinking
        [graph.add(t) for t in self.triples_header]
        [graph.add(t) for t in self.triples
         if not (lambda: (log.error(t)
                          if any(isinstance(_, Expr) or isinstance(_, Quantity)
                                 or (hasattr(_, '_value') and isinstance(_._value, Expr))
                                 for _ in t)
                          else None))()]


def _wrap_path_gen(prop):
    @property
    @wraps(prop.__get__)
    def impostor(self):
        for v in prop.__get__(self.path):
            yield self.__class__(v)

    return impostor


class Integrator(TriplesExport, PathData, ProtocolData, OntologyData, ProtcurData):
    """ pull everything together anchored to the DatasetStructure """
    # note that mro means that we have to run methods backwards
    # so any class that uses something produced by a function
    # of an earlier class needs to be further down the mro so
    # that the producer masks its NotImplementedErrors

    # class level helpers, instance level helpers go as mixins

    no_google = None
    rate = 8

    parent = _wrap_path_gen(Path.parent)
    parents = _wrap_path_gen(Path.parents)
    children = _wrap_path_gen(Path.children)
    rchildren = _wrap_path_gen(Path.rchildren)

    @classmethod
    def setup(cls, blackfynn_local_instance):
        """ make sure we have all datasources
            calling this again will refresh helpers
        """
        for _cls in cls.mro():
            if _cls != cls:
                if hasattr(_cls, 'setup'):
                    _cls.setup()

        dat.DatasetStructure.rate = cls.rate

        # unanchored helpers
        if cls.no_google:
            log.critical('no google no organ data')
            class asdf:
                modality = lambda v: None
                organ_term = lambda v: None
                award_manual = lambda v: None
                byCol = _byCol([['award', 'award_manual', 'organ_term'], []])
            cls.organs_sheet = asdf
        else:
            cls.organs_sheet = sheets.Organs()  # ipv6 resolution issues :/

        cls.organ = OrganData(organs_sheet=cls.organs_sheet)
        cls.organ = OrganData()
        cls.member = MembersData(blackfynn_local_instance)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datasetdata = dat.DatasetStructure(self.path)
        #self.protcur = ProtcurData(self)

    @property
    def name(self):
        return self.path.name

    @property
    def data(self):
        if hasattr(self, '_data'):
            return self._data

        datasetdata = self.datasetdata
        dsc = datasetdata.cache.dataset
        dataset = dat.DatasetStructure(dsc)  # FIXME should be able to go back
        # FIXME except for the adds, everything here should be a pipeline
        class Lifters:  # do this to prevent accidental data leaks
            # context
            id = dsc.id  # in case we are somewhere else
            name = dsc.name
            uri_api = dsc.uri_api
            uri_human = dsc.uri_human
            # dataset metadata
            #submission = property(lambda s: (_ for _ in dataset.submission))
            #dataset_description = property(lambda s: (_ for _ in dataset.dataset_description))
            #subjects = property(lambda s: (_ for _ in dataset.subjects))

            # protocols
            protocol = self.protocol
            #protcur = self.protcur

            # aux
            organ = self.organ
            member = self.member
            modality = self.organs_sheet.modality(id)
            award_manual = self.organs_sheet.award_manual(id)

            #sheets
            organ_term = self.organs_sheet.organ_term(id)

        class RuntimeContext:
            """ utility for logging etc. """
            path = self.path

        helpers = {}
        helper_key = object()
        lifters = Lifters()
        #sources = dataset.data
        # helper key was bad because it means that pipelines have to
        # know the structure of their helper pipelines and maintain them
        # across changes, better to use the lifters as other pipelines that
        # can start from a single piece of data and return a formatted result
        # and track their own provenance, the concept of a helper that holds
        # all the prior existing data is not a bad one, it just just that it
        # is better to create objects that can take the information already
        # in the data for the current pipeline and return an expanded verion
        self.pipeline = pipes.PipelineEnd(dataset, lifters, RuntimeContext())

        self._data = self.pipeline.data
        return self._data

    @property
    def protocol_uris(self):
        try:
            yield from adops.get(self.data, ['meta', 'protocol_url_or_doi'])
        except exc.NoSourcePathError:
            pass

    @property
    def keywords(self):
        try:
            yield from adops.get(self.data, ['meta', 'keywords'])
        except exc.NoSourcePathError:
            pass

    ## sandboxed for triple export

    @property
    def subjects(self):  # XXX
        if 'subjects' in self.data:
            yield from self.data['subjects']


hasSchema = sc.HasSchema()
@hasSchema.mark
class Summary(Integrator):
    """ A class that summarizes members of its __base__ class """
    #schema_class = MetaOutSchema  # FIXME clearly incorrect
    schema_class = SummarySchema
    schema_out_class = SummarySchema

    def __new__(cls, path, cypher=hashlib.sha256):
        #cls.schema = cls.schema_class()
        cls.schema_out = cls.schema_out_class()
        return super().__new__(cls, path, cypher)

    def __init__(self, path, cypher=hashlib.sha256):
        super().__init__(path, cypher)
        # not sure if this is kosher ... but it works
        super().__init__(self.anchor.path, self.cypher)
 
    def __iter__(self):
        """ Return the list of datasets for the organization
            of the current Integrator regardless of whether that
            Integrator is itself an organization node """

        # when going up or down the tree _ALWAYS_
        # use the filesystem as the source of truth
        # do not cache in here
        for i, path in enumerate(self.anchor.path.iterdir()):
            # FIXME non homogenous, need to find a better way ...
            yield self.__class__.__base__(path)
            #if i > 4:
                #break

    def _completeness(self, data):
        accessor = JT(data)  # can go direct if elements are always present
        #organ = accessor.query('meta', 'organ')
        try:
            organ = adops.get(data, ['meta', 'organ'])
        except:
            organ = None

        if isinstance(organ, list) or isinstance(organ, tuple):
            if len(organ) == 1:
                organ, = organ
                organ = (OntTerm(organ))
            else:
                organ = [(OntTerm(o)) for o in organ]

        elif organ == 'othertargets':
            pass
        elif organ:
            organ = OntTerm(organ)

        return (accessor.status.submission_index,
                accessor.status.curation_index,
                accessor.status.error_index,
                #accessor.submission_completeness_index,
                #dataset.name,  # from filename (do we not have that in meta!?)
                accessor.query('meta', 'name'),
                accessor.id, #if 'id' in dowe else None,
                accessor.query('meta', 'award_number'),
                organ,
            )

    @property
    def completeness(self):
        """ completeness, name, and id for all datasets """
        for dataset in self:
            yield self._completeness(dataset.data)

    def make_json(self, gen):
        # FIXME this and the datasets is kind of confusing ...
        # might be worth moving those into a reporting class
        # that always works at the top level regardless of which
        # file it is give?

        # TODO parallelize and stream this!
        ds = list(gen)
        count = len(ds)
        meta = {'count': count}
        return {'id': self.id,
                'meta': meta,
                'datasets': ds}

    @property
    def pipeline_end(self):
        if not hasattr(self, '_data_cache'):
            # FIXME validating in vs out ...
            # return self.make_json(d.validate_out() for d in self)
            self._data_cache = self.make_json(d.data for d in self)

        return self._data_cache

    @hasSchema(sc.SummarySchema, fail=True)
    def data(self):
        data = self.pipeline_end
        return data

    @property
    def foundary(self):
        pass

    @property
    def header_graph_description(self):
        return rdflib.Literal(f'SPARC organization graph for {self.id}')

    @property
    def triples(self):
        for d in self:
            yield from d.triples

    @property
    def xml(self):
        #datasets = []
        #contributors = []
        subjects = []
        errors = []
        resources = []

        def normv(v):
            if isinstance(v, rdflib.URIRef):  # FIXME why is this getting converted early?
                return OntId(v).curie
            if isinstance(v, Expr):
                return str(v)  # FIXME for xml?
            if isinstance(v, Quantity):
                return str(v)
            else:
                #log.debug(repr(v))
                return v

        for dataset in self:
            id = dataset.id
            dowe = dataset.data
            if 'subjects' in dowe:
                for subject in dowe['subjects']:
                    subject['dataset_id'] = id
                    subject = {k:normv(v) for k, v in subject.items()}
                    subjects.append(subject)

            if 'resources' in dowe:
                for res in dowe['resources']:
                    res['dataset_id'] = id
                    res = {k:normv(v) for k, v in res.items()}
                    resources.append(res)

            if 'errors' in dowe:
                ers = dowe['errors']
                for er in ers:
                    er['dataset_id'] = id
                    er = {k:normv(v) for k, v in er.items()}
                    errors.append(er)

        xs = dicttoxml.dicttoxml({'subjects': subjects})
        xr = dicttoxml.dicttoxml({'resources': resources})
        xe = dicttoxml.dicttoxml({'errors': errors})
        return (('subjects', xs),
                ('resources', xr),
                ('errors', xe))
    @property
    def disco(self):
        dsh = sorted(MetaOutSchema.schema['allOf'][0]['properties'])
        chs = sorted(('name',
                      'contributor_orcid_id',
                      'is_contact_person',
                      'contributor_affiliation',
                      'contributor_role'))

        datasets = [['id', 'submission_index', 'curation_index'] + dsh]
        contributors = [['id'] + chs]
        subjects = [['id', 'blob']]
        errors = [['id', 'blob']]
        resources = [['id', 'blob']]

        #cje = JEncode()
        def normv(v):
            if isinstance(v, list) or isinstance(v, tuple):
                v = ','.join(json.dumps(_, cls=JEncode)
                             if isinstance(_, dict) else
                             str(_) for _ in v)
                v = v.replace('\n', ' ').replace('\t', ' ')
            elif any(isinstance(v, c) for c in
                     (int, float, str)):
                v = str(v)
                v = v.replace('\n', ' ').replace('\t', ' ')  # FIXME tests to catch this

            elif isinstance(v, dict):
                v = json.dumps(v, cls=JEncode)

            return v

        for dataset in self:
            id = dataset.id
            dowe = dataset.data

            #row = [id, dowe['error_index'], dowe['submission_completeness_index']]  # FIXME this doubles up on the row
            row = [id, dowe['status']['submission_index'], dowe['status']['curation_index']]  # FIXME this doubles up on the row
            if 'meta' in dowe:
                meta = dowe['meta']
                for k in dsh:
                    if k in meta:
                        v = meta[k]
                        v = normv(v)
                    else:
                        v = None

                    row.append(v)

            else:
                row += [None for k in sc.MetaOutSchema.schema['properties']]

            datasets.append(row)

            # contribs 
            if 'contributors' in dowe:
                cs = dowe['contributors']
                for c in cs:
                    row = [id]
                    for k in chs:
                        if k in c:
                            v = c[k]
                            v = normv(v)
                            row.append(v)
                        else:
                            row.append(None)

                    contributors.append(row)

            if 'subjects' in dowe:
                for subject in dowe['subjects']:
                    row = [id]
                    row.append(json.dumps(subject, cls=JEncode))
                    subjects.append(row)

                # moved to resources if exists already
                #if 'software' in sbs:
                    #for software in sbs['software']:
                        #row = [id]
                        #row.append(json.dumps(software, cls=JEncode))
                        #resources.append(row)

            if 'resources' in dowe:
                for res in dowe['resources']:
                    row = [id]
                    row.append(json.dumps(res, cls=JEncode))
                    resources.append(row)

            if 'errors' in dowe:
                ers = dowe['errors']
                for er in ers:
                    row = [id]
                    row.append(json.dumps(er, cls=JEncode))
                    errors.append(row)

        # TODO samples resources
        return (('datasets', datasets),
                ('contributors', contributors),
                ('subjects', subjects),
                ('resources', resources),
                ('errors', errors))



def express_or_return(thing):
    return list(thing) if isinstance(thing, GeneratorType) else thing


def main():
    pass


if __name__ == '__main__':
    main()
