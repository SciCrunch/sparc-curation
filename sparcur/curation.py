#!/usr/bin/env python3
import copy
import json
import math
import hashlib
from types import GeneratorType
from urllib.parse import urlparse, parse_qs
from datetime import datetime
from itertools import chain
from collections import defaultdict, deque
import rdflib
import dicttoxml
from pyontutils.config import devconfig
from pyontutils.namespaces import OntCuries, makeNamespaces, TEMP, isAbout
from pyontutils.closed_namespaces import rdf, rdfs, owl, skos, dc
from protcur.analysis import parameter_expression
from protcur.core import annoSync
from protcur.analysis import protc, Hybrid
from terminaltables import AsciiTable
from hyputils.hypothesis import group_to_memfile, HypothesisHelper
from sparcur import config
from sparcur import exceptions as exc
from sparcur.core import JT, JEncode, log, logd, lj
from sparcur.core import sparc, memory, DictTransformer, adops, OntId, OntTerm
from sparcur.paths import Path
from sparcur import schemas as sc
from sparcur import converters as conv
from sparcur.datasources import OrganData, OntologyData, MembersData
from sparcur.protocols import ProtocolData, ProtcurData
from sparcur.datasets import SubmissionFile, DatasetDescriptionFile, SubjectsFile
from sparcur.schemas import (JSONSchema, ValidationError,
                             DatasetSchema, SubmissionSchema,
                             DatasetDescriptionSchema, SubjectsSchema,
                             SummarySchema, DatasetOutSchema, MetaOutSchema)
from sparcur import schemas as sc
from sparcur import validate as vldt
from sparcur import sheets
from sparcur.derives import Derives
from ttlser import CustomTurtleSerializer
import RDFClosure as rdfc


DT = DictTransformer
De = Derives
a = rdf.type

po = CustomTurtleSerializer.predicateOrder
po.extend((sparc.firstName,
           sparc.lastName))

OntCuries({'orcid':'https://orcid.org/',
           'ORCID':'https://orcid.org/',
           'dataset':'https://api.blackfynn.io/datasets/N:dataset:',
           'package':'https://api.blackfynn.io/packages/N:package:',
           'user':'https://api.blackfynn.io/users/N:user:',
           'awards':str(TEMP['awards/']),
           'sparc':str(sparc),})


def extract_errors(dict_):
    for k, v in dict_.items():
        if k == 'errors':
            yield from v
        elif isinstance(v, dict):
            yield from extract_errors(v)


def get_all_errors(_with_errors):
    """ A better and easier to interpret measure of completeness. """
    # TODO deduplicate by tracing causes
    # TODO if due to a missing required report expected value of missing steps
    return list(extract_errors(_with_errors))


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
            return DatasetData(self.bids_root)

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
        cls.schema = cls.schema_class()
        cls.schema_out = cls.schema_out_class()
        return super().__new__(cls)

    def __init__(self, path, cypher=hashlib.sha256):
        self._errors = []
        if isinstance(path, str):
            path = Path(path)

        self.path = path
        self.status = CurationStatusStrict(self)
        self.lax = CurationStatusLax(self)

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

    @property
    def name(self):
        return self.path.name

    @property
    def _meta(self):
        """ pathmeta NOT json meta (confusingly) """
        cache = self.path.cache
        if cache is not None:
            return self.path.cache.meta

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

    @property
    def anchor(self):
        if hasattr(self, 'project_path'):
            if self.path == self.project_path:
                return self

            return self.__class__(self.project_path)


class DatasetData(PathData):
    """ a homogenous representation """
    schema_class = DatasetSchema
    schema_out_class = DatasetOutSchema  # FIXME not sure if good idea ...

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
    def bids_root(self):
        # FIXME this will find the first dataset description file at any depth!
        # this is incorrect behavior!
        """ Sometimes there is an intervening folder. """
        if self.is_dataset:
            def check_fordd(paths, level=0, stop=3):
                if not paths:  # apparently the empty case recurses forever
                    return

                if len(paths) > 20:
                    log.warning('Not globing in a folder with > 20 children!')
                    return
                dd_paths_all = []
                children = []
                for path in paths:
                    dd_paths = list(path.glob('[Dd]ataset_description*.*'))
                    if dd_paths:
                        dd_paths_all.extend(dd_paths)
                    elif not dd_paths_all:
                        children.extend([p for p in path.children if p.is_dir()])

                if dd_paths_all:
                    return dd_paths_all
                else:
                    return check_fordd(children, level + 1)

            dd_paths = check_fordd((self.path,))

            if not dd_paths:
                #log.warning(f'No bids root for {self.name} {self.id}')  # logging in a property -> logspam
                return

            elif len(dd_paths) > 1:
                #log.warning(f'More than one submission for {self.name} {self.id} {dd_paths}')
                pass

            return dd_paths[0].parent  # FIXME choose shorter version? if there are multiple?

        elif self.parent:  # organization has no parent
            return self.parent.bids_root

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

    def _abstracted_paths(self, name_prefix, glob_type='glob'):
        """ A bottom up search for the closest file in the parent directory.
            For datasets, if the bids root and path do not match, use the bids root.
            In the future this needs to be normalized because the extra code required
            for dealing with the intervening node is quite annoying to maintain.
        """
        path = self.path
        if self.is_dataset and self.bids_root is not None and self.bids_root != self.path:
            path = self.bids_root
        else:
            path = self.path

        first = name_prefix[0]
        cased_np = '[' + first.upper() + first + ']' + name_prefix[1:]  # FIXME warn and normalize
        glob = getattr(path, glob_type)
        gen = glob(cased_np + '*.*')

        try:
            path = next(gen)
            if not path.is_broken_symlink():
                if path.name[0].isupper():
                    #breakpoint()
                    log.warning(f"path has bad casing '{path}'")
                yield path

            else:
                log.error(f"path has not been retrieved '{path}'")

            for path in gen:
                if not path.is_broken_symlink():
                    if path.name[0].isupper():
                        log.warning(f"path has bad casing '{path}'")

                    yield path

                else:
                    log.warning(f"path has not been retrieved '{path}'")

        except StopIteration:
            if self.parent is not None and self.parent != self:
                yield from getattr(self.parent, name_prefix + '_paths')

    @property
    def manifest_paths(self):
        gen = self.path.glob('manifest*.*')
        try:
            yield next(gen)
            yield from gen
        except StopIteration:
            if self.parent is not None:
                yield from self.parent.manifest_paths

    @property
    def submission_paths(self):
        yield from self._abstracted_paths('submission')
        return
        gen = self.path.rglob('submission*.*')
        try:
            yield next(gen)
            yield from gen
        except StopIteration:
            if self.parent is not None:
                yield from self.parent.submission_paths

    @property
    def _submission_objects(self):
        for p in self.submission_paths:
            try:
                miss = SubmissionFile(p)
                if miss.data:
                    yield miss
            except SubmissionFile.NoDataError as e:
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
    def dataset_description_paths(self):
        yield from self._abstracted_paths('dataset_description')
        #yield from self.path.glob('dataset_description*.*')

    @property
    def _dd(self):
        for p in self.dataset_description_paths:
            yield DatasetDescriptionFile(p)

    @property
    def _dataset_description_objects(self):
        for p in self.dataset_description_paths:
            #yield from DatasetDescription(t)
            # TODO export adapters for this ... how to recombine and reuse ...
            try:
                dd = DatasetDescriptionFile(p)
                if dd.data:
                    yield dd
            except DatasetDescriptionFile.NoDataError as e:
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
    def subjects_paths(self):
        yield from self._abstracted_paths('subjects')
        #yield from self.path.glob('subjects*.*')

    @property
    def _subjects_objects(self):
        """ really subjects_file """
        for path in self.subjects_paths:
            try:
                sf = SubjectsFile(path)
                if sf.data:
                    yield sf
            except DatasetDescriptionFile.NoDataError as e:
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
    def data(self):
        """ used to validate repo structure """

        out = {}

        for section_name in ('submission', 'dataset_description', 'subjects'):
            #path_prop = section_name + '_paths'
            for section in getattr(self, section_name):
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
        yield from (dd.t for dd in self._dataset_description_tables)
        yield from (s.t for s in self._subjects_tables)

    @property
    def meta_sections(self):
        """ All metadata related objects. """
        yield self
        yield from self.submission
        yield from self.dataset_description
        yield from self.subjects

    @property
    def errors(self):
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
        return self.path == other.path

    def __gt__(self, other):
        return self.path > other.path

    def __lt__(self, other):
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


class FTLax(DatasetData):
    def _abstracted_paths(self, name_prefix, glob_type='rglob'):
        yield from super()._abstracted_paths(name_prefix, glob_type)


hasSchema = vldt.HasSchema()
@hasSchema.mark
class Pipeline:
    # we can't quite do this yet, but these pipelines are best composed
    # by subclassing and then using super().previous_step to insert another
    # step along the way

    class SkipPipelineError(Exception):
        """ go directly to the end we are done here """
        def __init__(self, data):
            self.data = data

    def __init__(self, sources, lifters, helper_key,
                 runtime_context):
        """ sources stay, helpers are tracked for prov and then disappear """
        #log.debug(lj(sources))
        self.helper_key = helper_key
        self.lifters = lifters
        self.pipeline_start = sources
        self.runtime_context = runtime_context

    @property
    def data_lifted(self):
        """ doing this pipelines this way for now :/ """

        def section_binder(section):
            def lift_function(_, section=section):
                # throw 'em in a list and let the schema sort 'em out!
                dwe = [section.data_with_errors
                       for section in getattr(self.lifters, section)]
                if dwe:
                    dwe, *oops = dwe
                    if oops:
                        logd.error(f'{self.runtime_context.path} '
                                   f'has too may {section} files!')
                    return dwe

            return lift_function

        lifts = [[[section], func]
                 for section in ['submission', 'dataset_description', 'subjects']
                 for func in (section_binder(section),) if func]

        data = self.pipeline_start
        if 'errors' in data and len(data) == 1:
            raise self.SkipPipelineError(data)

        DictTransformer.lift(data, lifts)
        return data

    @property
    def data_derived_pre(self):
        # FIXME do we really need this ??
        # which copy steps actually depend on it??
        # in this case it is just easier to get some values
        # before moving everything around, provides
        # a way to operate on either side of an impenance mismatch
        # so if something changes, you just added another layer
        derives = [[[['submission', 'submission', 'sparc_award_number'],
                     ['dataset_description', 'funding']],
                    Derives.award_number,
                    [['meta', 'award_number']]],

                   [[['dataset_description', 'contributors']],
                    lambda cs: [DT.derive(c, [[[['name']],  # FIXME [['name]] as missing a nesting level
                                               De.contributor_name,  # and we got no warning ... :/
                                               [['first_name'], ['last_name']]]])
                                for c in cs],
                    []],
        ]
        data = self.data_lifted
        DictTransformer.derive(data, derives, source_key_optional=True)
        return data

    @property
    def data_copied(self):
        copies = ([['dataset_description', 'contributors'], ['contributors']],
                  [['subjects',], ['inputs', 'subjects']],)
        # meta copies
        copies += tuple([['dataset_description', source], ['meta', target]]
                        for source, target in
                        [[direct, direct] for direct in
                          ['principal_investigator',
                           'species',
                           'organ',
                           'modality',
                           'protocol_url_or_doi',

                           'completeness_of_dataset',
                           'funding',
                           'description',
                           'additional_links',
                           'keywords',
                           'acknowledgements',
                           'originating_article_doi',
                          ]] + [])

        data = self.data_derived_pre
        DictTransformer.copy(data, copies, source_key_optional=True)
        return data

    @property
    def data_moved(self):
        moves = ([['dataset_description',], ['inputs', 'dataset_description']],
                 [['subjects', 'software'], ['resources']],  # FIXME update vs replace
                 [['subjects'], ['subjects_file']],  # first step in ending the confusion
                 [['subjects_file', 'subjects'], ['subjects']],
                 [['submission',], ['inputs', 'submission']],)
        # contributor derives
        data = self.data_copied
        DictTransformer.move(data, moves, source_key_optional=True)
        return data

    @property
    def data_lifted_post(self):
        # essentially stateful injection of new data
        # In theory we could include ALL the prior data
        # in one big blob at the start and then never use lifts
        def pi(contributor):
            if 'contributor_orcid_id' in contributor:
                return contributor['contributor_orcid_id']

            # get member if we can find them
            elif 'name' in contributor and 'first_name' in contributor:
                fn = contributor['first_name']
                ln = contributor['last_name']
                if ' ' in fn:
                    fn, mn = fn.split(' ', 1)

                failover = f'{fn}-{ln}'
                member = self.lifters.member(fn, ln)

                if member is not None:
                    userid = rdflib.URIRef('https://api.blackfynn.io/users/' + member.id)

                return userid

            else:
                member = None
                failover = 'no-orcid-no-name'
                log.warning(f'No name!' + lj(contributor))
                return rdflib.URIRef(dsid + '/contributors/' + failover)

            
        lifts = [[['meta', 'principal_investigator'], pi],]
        data = self.data_moved
        DictTransformer.lift(data, lifts, source_key_optional=True)
        return data

    @property
    def data_derived_post(self):
        DT = DictTransformer
        De = Derives
        derives = ([[['contributors']],
                    Derives.creators,
                    [['creators']]],

                   [[['contributors']],
                    len,
                    ['meta', 'contributor_count']],

                   [[['subjects']],
                    Derives.dataset_species,
                    ['meta', 'species']],

                   [[['subjects']],
                    len,
                    ['meta', 'subject_count']],

                   )

        todo = [[[['samples']],
                 len,
                 [['meta', 'sample_count']]] ,
        ]

        data = self.data_lifted_post
        DictTransformer.derive(data, derives)
        return data

    @property
    def data_cleaned(self):
        """ clean up any cruft left over from transformations """


        pops = [['subjects_file'],
        ]
        data = self.data_derived_post
        removed = list(DictTransformer.pop(data, pops, source_key_optional=True))
        log.debug(f'cleaned the following values from {self}' + lj(removed))

        # FIXME post manual fixes ...

        # remote Creator since we lift it out
        if 'contributors' in data:
            for c in data['contributors']:
                if 'contributor_role' in c:
                    cr = c['contributor_role']
                    if 'Creator' in cr:
                        c['contributor_role'] = tuple(r for r in cr if cr != 'Creator')
                        

        return data

    @property
    def data_added(self):
        # FIXME this would seem to be where the Integration class comes in?
        # the schemas make it a bit of a dance
        adds = [[['id'], self.lifters.id],
                [['meta', 'uri_human'], self.lifters.uri_human]]
        data = self.data_cleaned
        DictTransformer.add(data, adds)
        return data

    @hasSchema(sc.DatasetOutSchema)
    def data_out(self):
        """ this part adds the meta bits we need after _with_errors
            and rearranges anything that needs to be moved about """

        try:
            data = self.data_added
        except self.SkipPipelineError as e:
            data = e.data
            data['id'] = self.lifters.id
            data['meta'] = {}
            data['meta']['uri_human'] = self.lifters.uri_human
            data['meta']['uri_api'] = self.lifters.uri_api
            data['error_index'] = 9999
            data['submission_completeness_index'] = 0
            log.debug('pipeline skipped to end due to errors')

        return data

    @property
    def data_post(self):
        # FIXME a bit of a hack
        data = self.data_out

        if 'error_index' not in data:
            ei = len(get_all_errors(data))
            if 'inputs' in data:
                #schema = self.__class__.data_out.schema
                schema = sc.DatasetSchema()
                subschemas = {'dataset_description':sc.DatasetDescriptionSchema(),
                            'submission':sc.SubmissionSchema(),
                            'subjects':sc.SubjectsSchema(),}
                sci = Derives.submission_completeness_index(schema, subschemas, data['inputs'])
            else:
                ei = 9999  # datasets without metadata can have between 0 and +inf.0 so 9999 seems charitable :)
                sci = 0

            data['error_index'] = ei
            data['submission_completeness_index'] = sci

        return data

    @property
    def __data_out_with_errors(self):
        if hasattr(self, '_dowe'):
            return self._dowe

        data = self.data_out
        ok, val, data = self.schema_out.validate(data)
        if not ok:
            if 'errors' not in data:
                data['errors'] = []
            data['errors'] += val.json()

        ei = len(get_all_errors(data))
        if 'inputs' in data:
            sci = self.submission_completeness_index(data['inputs'])
        else:
            ei = 9999  # datasets without metadata can have between 0 and +inf.0 so 9999 seems charitable :)
            sci = 0

        data['error_index'] = ei
        data['submission_completeness_index'] = sci

        self._dowe = data
        return data

    @property
    def data(self):
        return self.data_post


class PipelineExtras(Pipeline):

    @property
    def data_derived_post(self):
        derives = [[[['meta', 'award_number']],
                    lambda an: (self.lifters.organ(an),),
                    [['meta', 'organ']]]]
        data = super().data_derived_post
        DictTransformer.derive(data, derives)
        return data

    @property
    def data_added(self):
        adds = []
        todo = [[['meta', 'modality'], 'TODO should come from sheets maybe?']]
        data = super().data_added
        DictTransformer.add(data, adds)
        return data

   
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
                    log.error('wtf error {k}')
        if 'contributors' in data:
            for c in data['contributors']:
                yield from self.triples_contributors(c)

    def triples_contributors(self, contributor):
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

        return

        for k, p in kps:  # FIXME backwards
            try:
                _v = contributor[k]
                for v in (_v if isinstance(_v, tuple) or isinstance(_v, list) else (_v,)):

                    convert = getattr(converter, field, None)
                    yield s, p, rdflib.Literal(v)
            except KeyError:
                continue

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
            meta_converter = conv.MetaConverter(data['meta'])
            yield from meta_converter.triples_gen(dsid)
        else:
            log.warning(f'{self} has no meta!')  # FIXME split logs into their problems, and our problems

        converter = conv.DatasetConverter(data)
        yield from converter.triples_gen(dsid)

        def id_(v):
            yield rdflib.URIRef(dsid), a, owl.NamedIndividual
            yield rdflib.URIRef(dsid), a, sparc.Resource

        def subject_id(v, species=None):  # TODO species for human/animal
            v = v.replace(' ', '%20')  # FIXME use quote urlencode
            s = rdflib.URIRef(dsid + '/subjects/' + v)
            return s

        yield from id_(self.id)
        for subjects in self.subjects:
            for s, p, o in subjects.triples_gen(subject_id):
                if type(s) == str:
                    breakpoint()
                yield s, p, o

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
        [graph.add(t) for t in self.triples]


class Integrator(TriplesExport, PathData, ProtocolData, OntologyData):
    """ pull everything together anchored to the DatasetData """
    # note that mro means that we have to run methods backwards
    # so any class that uses something produced by a function
    # of an earlier class needs to be further down the mro so
    # that the producer masks its NotImplementedErrors

    # FIXME remove??
    schema_class = DatasetSchema
    schema_out_class = DatasetOutSchema  # FIXME not sure if good idea ...

    # class level helpers, instance level helpers go as mixins
    helpers = (
        ('protcur', ProtcurData),
        # TODO sheets
    )


    @classmethod
    def setup(cls, blackfynn_local_instance):
        """ make sure we have all datasources
            calling this again will refresh helpers
        """
        for _cls in cls.mro():
            if _cls != cls:
                if hasattr(_cls, 'setup'):
                    _cls.setup()

        # unanchored helpers
        cls.organs_sheet = sheets.Organs()
        cls.organ = OrganData(organs_sheet=cls.organs_sheet)
        cls.member = MembersData(blackfynn_local_instance)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datasetdata = DatasetData(self.path)

    @property
    def data(self):
        if hasattr(self, '_data'):
            return self._data

        datasetdata = self.datasetdata
        dataset = datasetdata.dataset
        class Lifters:  # do this to prevent accidental data leaks
            # context
            id = dataset.id  # in case we are somewhere else
            uri_api = dataset.uri_human
            uri_human = dataset.uri_human
            # dataset metadata
            submission = property(lambda s: (_ for _ in dataset.submission))
            dataset_description = property(lambda s: (_ for _ in dataset.dataset_description))
            subjects = property(lambda s: (_ for _ in dataset.subjects))

            # protocols
            protocol = self.protocol
            #protcur = self.protcur

            # aux
            organ = self.organ
            member = self.member
            modality = self.organs_sheet.modality

            #sheets

        class RuntimeContext:
            """ utility for logging etc. """
            path = self.path

        helpers = {}
        helper_key = object()
        lifters = Lifters()
        sources = dataset.data_with_errors
        sources[helper_key] = helpers
        pipeline = PipelineExtras(sources, lifters, helper_key, RuntimeContext())
        sources.pop(helper_key)

        self._data = pipeline.data
        return self._data

    @property
    def protocol_uris(self):
        yield from adops.get(self.data, ['meta', 'protocol_url_or_doi'])

    @property
    def keywords(self):
        yield from adops.get(self.data, ['meta', 'keywords'])

    ## sandboxed for triple export

    @property
    def subjects(self):  # XXX
        yield from self.datasetdata.subjects


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
            of the current DatasetData regardless of whether that
            DatasetData is itself an organization node """

        # when going up or down the tree _ALWAYS_
        # use the filesystem as the source of truth
        # do not cache in here
        for path in self.anchor.path.iterdir():
            # FIXME non homogenous, need to find a better way ...
            yield self.__class__.__base__(path)

    @property
    def completeness(self):
        """ completeness, name, and id for all datasets """
        for dataset in self:
            # FIXME metamaker was a bad, bad idea
            dowe = dataset.data
            accessor = JT(dowe)  # can go direct if elements are always present
            yield (accessor.error_index,
                   accessor.submission_completeness_index,
                   dataset.name,  # from filename (do we not have that in meta!?)
                   accessor.id if 'id' in dowe else None,
                   accessor.query('meta', 'award_number'),
                   accessor.query('meta', 'organ'),
            )

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
    def data(self):
        if not hasattr(self, '_data_cache'):
            # FIXME validating in vs out ...
            # return self.make_json(d.validate_out() for d in self)
            self._data_cache = self.make_json(d.data for d in self)

        return self._data_cache

    @property
    def data_out(self):
        data = self.data_with_errors
        #return self.make_json(d.data_out_with_errors for d in self)
        # TODO transform

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
        dsh = sorted(MetaOutSchema.schema['properties'])
        chs = sorted(('name',
                      'contributor_orcid_id',
                      'is_contact_person',
                      'contributor_affiliation',
                      'contributor_role'))

        datasets = [['id', 'error_index', 'dataset_completeness_index'] + dsh]
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

            row = [id, dowe['error_index'], dowe['submission_completeness_index']]  # FIXME this doubles up on the row
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
