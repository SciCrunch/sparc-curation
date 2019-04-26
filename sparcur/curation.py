#!/usr/bin/env python3
import io
import csv
import copy
import json
import math
import hashlib
import mimetypes
from types import GeneratorType
from urllib.parse import urlparse, parse_qs
from datetime import datetime
from itertools import chain
from collections import defaultdict, deque
import magic  # from sys-apps/file consider python-magic ?
import rdflib
import requests
import dicttoxml
from xlsx2csv import Xlsx2csv, SheetNotFoundException
from pyontutils.core import OntTerm, OntId, cull_prefixes, makeGraph
from pyontutils.utils import byCol
from pyontutils.config import devconfig
from pyontutils.namespaces import OntCuries, makeNamespaces, TEMP, isAbout
from pyontutils.closed_namespaces import rdf, rdfs, owl, skos, dc
from protcur.analysis import parameter_expression
from protcur.core import annoSync
from protcur.analysis import protc, Hybrid
from pysercomb.pyr.units import ProtcParameterParser
from scibot.utils import resolution_chain
from terminaltables import AsciiTable
from hyputils.hypothesis import group_to_memfile, HypothesisHelper
from sparcur import exceptions as exc
from sparcur.core import JEncode, OrcidId, log, lj
from sparcur.paths import Path
from sparcur.config import local_storage_prefix, organ_html_path
from sparcur.protocols_io_api import get_protocols_io_auth
from sparcur import schemas as sc
from sparcur.schemas import (JSONSchema, ValidationError,
                             DatasetSchema, SubmissionSchema,
                             DatasetDescriptionSchema, SubjectsSchema,
                             SummarySchema, DatasetOutSchema, MetaOutSchema)
from sparcur import validate as vldt
from ttlser import CustomTurtleSerializer

sparc = rdflib.Namespace('http://uri.interlex.org/tgbugs/uris/readable/sparc/')
a = rdf.type

po = CustomTurtleSerializer.predicateOrder
po.extend((sparc.firstName,
           sparc.lastName))

OntCuries({'orcid':'https://orcid.org/',
           'ORCID':'https://orcid.org/',
           'dataset':'https://api.blackfynn.io/datasets/N:dataset:',
           'package':'https://api.blackfynn.io/packages/N:package:',
           'user':'https://api.blackfynn.io/users/N:user:',
           'sparc':str(sparc),})

# FIXME this is an awful way to do this ...
if False:  # TODO create a protocols.io class for dealing with fetching that can be initialized with auth stuff
    creds_file = devconfig.secrets('protocols-io', 'api', 'creds-file')
    _pio_creds = get_protocols_io_auth(creds_file)
    _pio_header = {'Authentication': 'Bearer ' + _pio_creds.access_token}
    protocol_jsons = {}


class EncodingError(Exception):
    """ Some encoding error has occured in a file """


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


def normalize_tabular_format(project_path):
    kwargs = {
        'delimiter' : '\t',
        'skip_empty_lines' : True,
        'outputencoding': 'utf-8',
    }
    sheetid = 0
    for xf in project_path.rglob('*.xlsx'):
        xlsx2csv = Xlsx2csv(xf, **kwargs)
        with open(xf.with_suffix('.tsv'), 'wt') as f:
            try:
                xlsx2csv.convert(f, sheetid)
            except SheetNotFoundException as e:
                print('Sheet weirdness in', xf)
                print(e)


class NormAward(str):
    def __new__(cls, value):
        return str.__new__(cls, cls.normalize(value))

    @classmethod
    def normalize(cls, value):
        if 'OT2' in value and 'OD' not in value:
            # one is missing the OD >_<
            log.warning(value)
            value = value.replace('-', '-OD')  # hack

        n = (value
             .strip()
             .replace('-', '-')  # can you spot the difference?
             .replace('(', '')
             .replace(')', '')
             .replace('-01S1', '')
             .replace('-01', '')
             .replace('-02S2', '')
             .replace('-02', '')
             .replace('SPARC', '')
             .replace('NIH-1', '')
             .replace('NIH-', '')
             .replace('-', '')
             .replace('NIH ', '')
             .replace(' ', ''))
        if n[0] in ('1', '3', '5'):
            n = n[1:]

        return n


class NormFileSuffix(str):
    data = {
        'jpeg':'jpg',
        'tif':'tiff',
    }

    def __new__(cls, value):
        return str.__new__(cls, cls.normalize(value))

    @classmethod
    def normalize(cls, value):
        v = value.lower()
        ext = v[1:]
        if ext in cls.data:
            return '.' + data[ext]
        else:
            return v


class NormSimple(str):
    def __new__(cls, value):
        return str.__new__(cls, cls.normalize(value))

    @classmethod
    def normalize(cls, value):
        v = value.lower()
        if v in cls.data:
            return cls.data[v]
        else:
            return v

class NormSpecies(NormSimple):
    data = {
        'cat':'Felis catus',
        'rat':'Rattus norvegicus',
        'mouse':'Mus musculus',
    }

class NormSex(NormSimple):
    data = {
        'm':'male',
        'f':'female',
    }


class NormContributorRole(str):
    values = ('ContactPerson',
              'DataCollector',
              'DataCurator',
              'DataManager',
              'Distributor',
              'Editor',
              'HostingInstitution',
              'PrincipalInvestigator',  # added for sparc map to ProjectLeader probably?
              'Producer',
              'ProjectLeader',
              'ProjectManager',
              'ProjectMember',
              'RegistrationAgency',
              'RegistrationAuthority',
              'RelatedPerson',
              'Researcher',
              'ResearchGroup',
              'RightsHolder',
              'Sponsor',
              'Supervisor',
              'WorkPackageLeader',
              'Other',)

    def __new__(cls, value):
        return str.__new__(cls, cls.normalize(value))

    @staticmethod
    def levenshteinDistance(s1, s2):
        if len(s1) > len(s2):
            s1, s2 = s2, s1

        distances = range(len(s1) + 1)
        for i2, c2 in enumerate(s2):
            distances_ = [i2+1]
            for i1, c1 in enumerate(s1):
                if c1 == c2:
                    distances_.append(distances[i1])
                else:
                    distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])))
            distances = distances_
        return distances[-1]

    @classmethod
    def normalize(cls, value):
        # a hilariously slow way to do this
        # also not really normalization ... more, best guess for what people were shooting for
        if value:
            return sorted((cls.levenshteinDistance(value, v), v) for v in cls.values)[0][1]

class Tabular:
    def __init__(self, path):
        self._errors = []
        self.path = path

    @property
    def file_extension(self):
        if self.path.suffixes:
            ext = self.path.suffixes[0]  # FIXME filenames with dots in them ...
            if ext != '.fake':
                return NormFileSuffix(ext).strip('.')

    def tsv(self):
        return self.csv(delimiter='\t')

    def csv(self, delimiter=','):
        for encoding in ('utf-8', 'latin-1'):
            try:
                with open(self.path, 'rt', encoding=encoding) as f:
                    yield from csv.reader(f, delimiter=delimiter)
                if encoding != 'utf-8':
                    message = f"encoding bad '{encoding}' '{self.path}'"
                    log.error(message)
                    self._errors.append(EncodingError(message))
                return
            except UnicodeDecodeError:
                continue

    def xlsx(self):
        kwargs = {
            'delimiter' : '\t',
            'skip_empty_lines' : True,
            'outputencoding': 'utf-8',
        }
        sheetid = 0
        xlsx2csv = Xlsx2csv(self.path, **kwargs)

        f = io.StringIO()
        try:
            xlsx2csv.convert(f, sheetid)
            f.seek(0)
            gen = csv.reader(f, delimiter='\t')
            # avoid first row sheet line
            next(gen)
            yield from gen
        except SheetNotFoundException as e:
            log.warning(f'Sheet weirdness in{self.path}')
            log.warning(str(e))

    def normalize(self, rows):
        # FIXME need to detect changes
        # this removes any columns that are all dead

        #if any(not(c) for c in rows[0]):  # doesn't work because generators

        error = EncodingError(f"encoding feff error in '{self.path}'")
        cleaned_rows = zip(*(t for t in zip(*rows) if not all(not(e) for e in t)))  # TODO check perf here
        for row in cleaned_rows:
            n_row = [c.strip().replace('\ufeff', '') for c in row
                     if (not self._errors.append(error)  # FIXME will probably append multiple ...
                         if '\ufeff' in c else True)]
            if not all(not(c) for c in n_row):  # skip totally empty rows
                yield n_row

    def __iter__(self):
        try:
            yield from self.normalize(getattr(self, self.file_extension)())
        except UnicodeDecodeError as e:
            log.error(f'\'{self.path}\' {e}')

    def __repr__(self):
        limit = 30
        ft = FThing(self.path)
        title = f'{self.path.name:>40}' + ft.dataset.id + ' ' + ft.dataset.name
        return AsciiTable([[c[:limit] + ' ...' if isinstance(c, str)
                            and len(c) > limit else c
                            for c in r] for r in self],
                          title=title).table


class Version1Header:
    to_index = tuple()  # first element indexes row based data
    skip_cols = 'metadata_element', 'description', 'example'
    max_one = tuple()
    verticals = dict()  # FIXME should really be immutable
    schema_class = JSONSchema

    class NoDataError(Exception):
        """ FIXME HACK workaround for bad handling of empty sheets in byCol """

    def __new__(cls, tabular):
        cls.schema = cls.schema_class()
        return super().__new__(cls)

    def __init__(self, tabular):
        self._errors = []
        self.skip_rows = tuple(key for keys in self.verticals.values() for key in keys)
        self.t = tabular
        l = list(tabular)
        if not l:
            # FIXME bad design, this try block is a workaround for bad handling of empty lists
            raise self.NoDataError(self.path)

        orig_header, *rest = l
        header = vldt.Header(orig_header).output
        #print(header)
        #print(f"'{tabular.path}'", header)
        self.fail = False
        if self.to_index:
            for head in self.to_index:
                if head not in header:
                    log.error(f'\'{self.t.path}\' malformed header!')
                    self.fail = True

        if self.fail:
            self.bc = byCol(rest, header)
        else:
            self.bc = byCol(rest, header, to_index=self.to_index)

    @property
    def path(self):
        return self.t.path

    def xopen(self):
        """ open file using xdg-open """
        self.path.xopen()

    @property
    def errors(self):
        yield from self.t._errors
        yield from self._errors

    @staticmethod
    def normalize_header(orig_header):
        header = []
        for i, c in enumerate(orig_header):
            if c:
                c = (c.strip()
                     .replace('(', '')
                     .replace(')', '')
                     .replace(' ', '_')
                     .replace('+', '')
                     .replace('…','')
                     .replace('.','_')
                     .replace(',','_')
                     .replace('/', '_')
                     .replace('?', '_')
                     .replace('#', 'number')
                     .replace('-', '_')
                     .lower()  # sigh
                )
                if any(c.startswith(str(n)) for n in range(10)):
                    c = 'n_' + c

            if not c:
                c = f'TEMP_{i}'

            if c in header:
                c = c + f'_TEMP_{i}'

            header.append(c)

        return header

    @staticmethod
    def query(value, prefix):
        for query_type in ('term', 'search'):
            terms = [q.OntTerm for q in OntTerm.query(prefix=prefix, **{query_type:value})]
            if terms:
                #print('matching', terms[0], value)
                #print('extra terms for', value, terms[1:])
                return terms[0]
            else:
                continue

        else:
            return value

    def default(self, value):
        yield value

    def normalize(self, key, value):
        v = value.replace('\ufeff', '')  # FIXME utf-16 issue
        if v != value:  # TODO can we decouple encoding from value normalization?
            message = f"encoding feff error in '{self.path}'"
            log.error(message)
            self._errors.append(EncodingError(message))

        if v.lower().strip() not in ('n/a', 'na', 'no'):  # FIXME explicit null vs remove from structure
            yield from getattr(self, key, self.default)(v)

    def rename_key(self, key, *parent_keys):
        """ modify this in your class if you need to rename a key """
        # TODO parent key lists
        return key

    @property
    def inverse(self):
        """ back to the original format """
        # self.to_index -> [(new_col, orig_col) ...]
        # put all the required data in the second column
        # align any data from verticals -> additional columns
        # align any data from horizontals -> additional overlapping rows

    @property
    def data(self):
        if hasattr(self, '_data_cache'):
            return self._data_cache

        index_col, *_ = self.to_index
        out = {}
        if not hasattr(self.bc, index_col):
            log.error(f'\'{self.t.path}\' malformed header!')
            self._data_cache = out
            return out

        ic = list(getattr(self.bc, index_col))
        nme = Version1Header.normalize_header(ic)  # TODO make sure we can recover the original values for these
        nmed = {v:normk for normk, v in zip(nme, ic)}

        for v, nt in self.bc._byCol__indexes[index_col].items():
            if v != index_col:
                normk = nmed[v]
                if normk not in self.skip_rows:
                    _value = tuple(normv for key, value in zip(nt._fields, nt)
                                   if key not in self.skip_cols and value
                                   for normv in self.normalize(normk, value)
                                   if normv)
                    value = tuple(set(_value))
                    if len(value) != len(_value):
                        # TODO counter to show the duplicate values
                        log.warning(f"duplicate values in {normk} TODO '{self.t.path}'")

                    if normk in self.max_one:  # schema will handle this ..
                        if not value:
                            #log.warning(f"missing value for {normk} '{self.t.path}'")
                            pass
                        elif len(value) > 1:
                            log.warning(f"too many values for {normk} {value} '{self.t.path}'")
                            # FIXME not selecting the zeroth element here breaks the schema assumptions
                            #value = 'AAAAAAAAAAA' + '\n|AAAAAAAAAAA|\n'.join(value)
                            #value = 'ERROR>>>' + ','.join(value)
                            # just leave it
                        else:
                            value = value[0]  # FIXME error handling etc.

                    if value:
                        out[normk] = value

        def merge(tup):
            out = {}
            for a, b in tup:
                if a not in out:
                    out[a] = b
                elif a and not isinstance(b, tuple):
                    out[a] = out[a], b
                else:
                    out[a] += b,

            self._data_cache = out
            return out

        for key, keys in self.verticals.items():
            gen = (merge([(self.rename_key(k, key), normv)
                          for k, value in zip(nme, values)
                          if k in keys and value
                          for normv in self.normalize(k, value)
                          if normv])
                   for head, *values in self.bc.cols
                   if head not in self.skip_cols)
            value = tuple(_ for _ in gen if _)
            if value:
                out[key] = value

        self._data_cache = out
        return out

    @property
    def data_with_errors(self):
        """ data with errors added under the 'errors' key """
        # FIXME TODO regularize this with the FThing version
        ok, valid, data = self.schema.validate(copy.deepcopy(self.data))
        if not ok:
            # FIXME this will dump the whole schema, go smaller
            error = valid
            if 'errors' in data:
                class WatError(Exception):
                    """ WAT """

                raise WatError('wat')

            data['errors'] = error.json()
            #data['errors'] = [{k:v if k != 'schema' else k
                               #for k, v in e._contents().items()}
                              #for e in error.errors]

        return data

        # FIXME this seems like a much better way to collect things
        # than the crazy way that I have been doing it ...
        # this will let us go as deep as we want ...

        for section_name, path in data.items():
            section = getattr(self, section_name)
            sec_data = section.data_with_errors
            out[section_name] = sec_data

    @property
    def submission_completeness_index(self):
        """ (/ (- total-possible-errors number-of-errors) total-possible-errors)
            A naieve implementation that requires a recursive algorithem to actually
            count the number of potential errors in a given context. """
        # FIXME the normalized version of this actually isn't helpful
        # because you could have 1000 errors for one substep of a substep
        # and you would appear to be almost done ...
        # what we really want is the total number of non-double-counted errors
        # so if I have an error that causes an error in a later step
        # then I would have only 1 not two errors ...
        # note of course that this just puts the problem off because if
        # I don't have any of the three spreadsheets then each one of them
        # could be completely incorrect ... the only reasonable way to do
        # this is to return the expected value of the number of errors
        # for the missing value, I don't think there is any other reasonable appraoch
        # IF you are allowed a second number then you can communicate the uncertainty
        # TODO this is an augment step in the new pipelined version

        dwe = self.data_with_errors
        if 'errors' not in dwe:
            return 1

        else:
            schema = self.schema.schema
            total_possible_errors = self.schema.total_possible_errors
            number_of_errors = len(dwe['errors'])
            return (total_possible_errors - number_of_errors) / total_possible_errors


class SubmissionFile(Version1Header):
    to_index = 'submission_item',  # FIXME normalized in version 2
    skip_cols = 'submission_item', 'definition'  # FIXME normalized in version 2

    verticals = {'submission':('sparc_award_number', 'milestone_achieved', 'milestone_completion_date')}
    schema_class = SubmissionSchema

    @property
    def data(self):
        """ lift list with single element to object """

        d = copy.deepcopy(super().data)
        if d:
            if d['submission']:
                d['submission'] = d['submission'][0]
            else:
                d['submission'] = {}

        return d


class DatasetDescription(Version1Header):
    to_index = 'metadata_element',
    skip_cols = 'metadata_element', 'description', 'example'
    max_one = (  # FIXME we probably want to write this as json schema or something ...
        'name',
        'description',
        'acknowledgements',
        'funding',
        #'originating_article_doi',  # and sometimes they comma separate them! >_< derp
        #'protocol_url_or_doi',

        #'additional_links',
        #'link_description',

        #'example_image_filename',
        #'example_image_locator',
        #'example_image_description',

        'completeness_of_data_set',
        'prior_batch_number',
        'title_for_complete_data_set')
    verticals = {'contributors': ('contributors',
                                  'contributor_orcid_id',
                                  'contributor_affiliation',
                                  'contributor_role',
                                  'is_contact_person',),
                 'links': ('additional_links', 'link_description'),
                 'examples': ('example_image_filename',
                              'example_image_locator',
                              'example_image_description'),
    }
    schema_class = DatasetDescriptionSchema

    def contributor_orcid_id(self, value):
        # FIXME use schema
        v = value.replace(' ', '')
        if not v:
            return
        if v.startswith('http:'):
            v = v.replace('http:', 'https:', 1)

        if not (v.startswith('ORCID:') or v.startswith('https:')):
            v = v.strip()
            if not len(v):
                return
            elif v == '0':  # FIXME ? someone using strange conventions ...
                return
            elif len(v) != 19:
                log.error(f"orcid wrong length '{value}' '{self.t.path}'")
                return

            v = 'ORCID:' + v

        else:
            if v.startswith('https:'):
                _, numeric = v.rsplit('/', 1)
            elif v.startswith('ORCID:'):
                _, numeric = v.rsplit(':', 1)

            if not len(numeric):
                return
            elif len(numeric) != 19:
                log.error(f"orcid wrong length '{value}' '{self.t.path}'")
                return

        try:
            #log.debug(f"{v} '{self.t.path}'")
            orcid = OrcidId(v)
            if not orcid.checksumValid:
                # FIXME json schema can't do this ...
                log.error(f"orcid failed checksum '{value}' '{self.t.path}'")
                return

            yield orcid

        except (OntId.BadCurieError, OrcidId.MalformedOrcidError) as e:
            log.error(f"orcid malformed '{value}' '{self.t.path}'")
            yield value

    def contributor_role(self, value):
        # FIXME normalizing here momentarily to squash annoying errors
        yield tuple(sorted(set(NormContributorRole(e.strip()) for e in value.split(','))))

    def is_contact_person(self, value):
        yield value.lower() == 'yes'

    def keywords(self, value):
        if ';' in value:
            # FIXME error for this
            values = [v.strip() for v in value.split(';')]
        elif ',' in value:
            # FIXME error for this
            values = [v.strip() for v in value.split(',')]
        else:
            values = value,

        for value in values:
            match = self.query(value, prefix=None)
            if match and False:  # this is incredibly broken at the moment
                yield match
            else:
                yield value

    def rename_key(self, key, *parent_keys):
        # FIXME multiple parent keys...
        if parent_keys == ('contributors',):
            if key == 'contributors':
                return 'name'

        return key


class SubjectsFile(Version1Header):
    to_index = 'subject_id',  # the zeroth is what is used for unique rows by default  # FIXME doesn't work
    # subject id varies, so we have to do something a bit different here
    skip_cols = tuple()
    horizontals = {'software':('software', 'software_version', 'software_vendor', 'software_url', 'software_rrid')}
    schema_class = SubjectsSchema

    def __init__(self, tabular):
        super().__init__(tabular)

        # units merging
        # TODO pull the units in the parens out
        self.h_unit = [k for k in self.bc.header if '_units' in k]
        h_value = [k.replace('_units', '') for k in self.h_unit]
        no_unit = [k for k in self.bc.header if '_units' not in k]
        #self.h_value = [k for k in self.bc.header if '_units' not in k and any(k.startswith(hv) for hv in h_value)]
        self.h_value = [k for hv in h_value
                        for k in no_unit
                        if k.startswith(hv)]
        err = f'Problem! {self.h_unit} {self.h_value} {self.bc.header} \'{self.t.path}\''
        #assert all(v in self.bc.header for v in self.h_value), err
        assert len(self.h_unit) == len(self.h_value), err
        self.skip = self.h_unit + self.h_value

        self.skip_cols += tuple(set(_ for v in self.horizontals.values() for _ in v))

    def species(self, value):
        nv = NormSpecies(value)
        yield self.query(nv, 'NCBITaxon')

    def sex(self, value):
        nv = NormSex(value)
        yield self.query(nv, 'PATO')

    def gender(self, value):
        # FIXME gender -> sex for animals, requires two pass normalization ...
        yield from self.sex(value)

    def _param(self, value):
        pv = ProtcParameterParser(value)
        if not pv._tuple[0] == 'param:parse-failure':
            yield str(pv.for_text)
        else:
            # TODO warn
            yield value

    def age(self, value):
        yield from self._param(value)

    def mass(self, value):
        yield from self._param(value)
        
    def weight(self, value):
        yield from self._param(value)
        
    def rrid_for_strain(self, value):
        yield value

    def protocol_io_location(self, value):  # FIXME need to normalize this with dataset_description
        yield value

    def process_dict(self, dict_):
        """ deal with multiple fields """
        out = {k:v for k, v in dict_.items() if k not in self.skip}
        for h_unit, h_value in zip(self.h_unit, self.h_value):
            compose = dict_[h_value] + dict_[h_unit]
            #_, v, rest = parameter_expression(compose)
            out[h_value] = str(ProtcParameterParser(compose).for_text)  # FIXME sparc repr

        if 'gender' in out and 'species' in out:
            if out['species'] != OntTerm('NCBITaxon:9606'):
                out['sex'] = out.pop('gender')

        return out

    @property
    def _data(self):
        return super().data

    @property
    def data(self):
        out = {'subjects': list(self)}
        for k, heads in self.horizontals.items():
            # TODO make sure we actually check that the horizontal
            # isn't used by someone else already ... shouldn't be
            # it should be skipped but maybe not?
            tups = sorted(set(_ for _ in zip(*(getattr(self.bc, head, [])
                                               # FIXME one [] drops whole horiz group ...
                                               for head in heads))
                              if any(_)))
            if tups:
                out[k] = [{k:v for k, v in zip(heads, t) if v} for t in tups]

        return out

    def __iter__(self):
        """ this is still used """
        yield from (self.process_dict({k:nv for k, v in zip(r._fields, r) if v
                                       and k not in self.skip_cols
                                       for nv in self.normalize(k, v) if nv})
                    for r in self.bc.rows)

    @property
    def triples_local(self):
        """ NOTE the subject is LOCAL """
        for i, subject in enumerate(self):
            converter = SubjectConverter(subject)
            if 'subject_id' in subject:
                s_local = subject['subject_id']
            else:
                s_local = f'local-{i + 1}'  # sigh

            yield s_local, a, owl.NamedIndividual
            yield s_local, a, sparc.Subject
            for field, value in subject.items():
                convert = getattr(converter, field, None)
                if convert is not None:
                    yield (s_local, *convert(value))
                else:
                    log.warning(f'Unhandled subject field: {field}')


class TripleConverter:
    # TODO consider putting mappings in a dict hierarchy
    # that reflects where they are in the schema??
    mapping = tuple()

    @classmethod
    def setup(cls):
        for attr, predicate in cls.mapping:
            def _func(self, value, p=predicate): return p, self.l(value)
            setattr(cls, attr, _func)

    def __init__(self, json_source):
        """ in case we want to do contextual things here """
        self._source = json_source

    def l(self, value):
        if isinstance(value, OntId):
            return value.u
        elif isinstance(value, str) and value.startswith('http'):
            return OntId(value).u
        else:
            return rdflib.Literal(value)

    def triples_gen(self, subject):
        if not isinstance(subject, rdflib.URIRef):
            subject = rdflib.URIRef(subject)

        for field, value in self._source.items():
            log.debug(f'{field} {value}')
            convert = getattr(self, field, None)
            if convert is not None:
                if isinstance(value, tuple) or isinstance(value, list):
                    values = value
                else:
                    values = value,
                
                for v in values:
                    yield (subject, *convert(v))

            else:
                log.warning(f'Unhandled subject field: {field}')



class ContributorConverter(TripleConverter):
    mapping = (
            ('first_name', sparc.firstName),
            ('last_name', sparc.lastName),
            #('contributor_role', TEMP.hasRole),
            ('contributor_affiliation', TEMP.hasAffiliation),
            ('is_contact_person', sparc.isContactPerson),
            ('is_responsible_pi', sparc.isContactPerson),
        )
 
    def contributor_role(self, value):
        return TEMP.hasRole, TEMP[value]

class MetaConverter(TripleConverter):
    mapping = [
        ['principal_investigator', TEMP.hasResponsiblePrincialInvestigator],
        ['protocol_url_or_doi', TEMP.hasProtocol],
        ['award_number', TEMP.hasAwardNumber],
        ['species', isAbout],
        ['organ', isAbout],
        ['subject_count', TEMP.hasNumberOfSubjects],
        ['keywords', isAbout],
    ]


class DatasetConverter(TripleConverter):
    mapping = [
        ['error_index', TEMP.errorIndex],
        ['submission_completeness_index', TEMP.submissionCompletenessIndex],
        ]


class SubjectConverter(TripleConverter):
    mapping = [
        ['age_cateogry', TEMP.hasAgeCategory],
        ['species', sparc.animalSubjectIsOfSpecies],
    ]

    def genus(self, value): return sparc.animalSubjectIsOfGenus, self.l(value)
    def species(self, value): return sparc.animalSubjectIsOfSpecies, self.l(value)
    def strain(self, value): return sparc.animalSubjectIsOfStrain, self.l(value)
    def weight(self, value): return sparc.animalSubjectHasWeight, self.l(value)
    def mass(self, value): return self.weight(value)
    def sex(self, value): return TEMP.hasBiologicalSex, self.l(value)
    def gender(self, value): return sparc.hasGender, self.l(value)
    def age(self, value): return TEMP.hasAge, self.l(value)


ContributorConverter.setup()
MetaConverter.setup()
SubjectConverter.setup()


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
            return FThing(self.bids_root)

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


class OrganData:
    organ_lookup = {'bladder': OntId('FMA:15900'),
                    'brain': OntId('UBERON:0000955'),
                    #'computer': OntId(''),
                    'heart': OntId('FMA:7088'),
                    'kidneys': OntId('FMA:7203'),
                    'largeintestine': OntId('FMA:7201'),
                    'liver': OntId('FMA:7197'),
                    'lung': OntId('FMA:7195'),
                    'malerepro': OntId('UBERON:0000079'),
                    #'othertargets': OntId(''),
                    'pancreas': OntId('FMA:7198'),
                    'smallintestine': OntId('FMA:7200'),
                    'spleen': OntId('FMA:7196'),
                    'stomach': OntId('FMA:7148'),
                    #'uterus': OntId('')
    }
    cache = Path('/tmp/sparc-award-by-organ.json')
    old_cache = Path('/tmp/award-mappings-old-to-new.json')

    def __init__(self, path=organ_html_path):
        from bs4 import BeautifulSoup
        self.path = path
        self.BeautifulSoup = BeautifulSoup
        if not self.cache.exists():
            self.overview()
            with open(self.cache, 'wt') as f:
                json.dump(self.normalized, f)

            with open(self.old_cache, 'wt') as f:
                json.dump(self.former_to_current, f)
        else:
            with open(self.cache, 'rt') as f:
                self.normalized = json.load(f)

            with open(self.old_cache, 'rt') as f:
                self.former_to_current = json.load(f)

    def overview(self):
        with open(self.path, 'rb') as f:
            soup = self.BeautifulSoup(f.read(), 'lxml')

        self.raw = {}
        self.former_to_current = {}
        for bsoup in soup.find_all('div', {'id':lambda v: v and v.endswith('-bubble')}):
            organ, _ = bsoup['id'].split('-')
            award_list = self.raw[organ] = []
            for asoup in bsoup.find_all('a'):
                href = asoup['href']
                log.debug(href)
                parts = urlparse(href)
                query = parse_qs(parts.query)
                if 'projectnumber' in query:
                    award_list.extend(query['projectnumber'])
                elif 'aid' in query:
                    #aid = [int(a) for a in query['aid']]
                    #json = self.reporter(aid)
                    award, former = self.reporter(href)
                    award_list.append(award)
                    if former is not None:
                        award_list.append(former)  # for this usecase this is ok
                        self.former_to_current[former] = award
                elif query:
                    log.debug(lj(query))
            
        self.former_to_current = {NormAward(NormAward(k)):NormAward(NormAward(v))
                                  for k, v in self.former_to_current.items()}
        self._normalized = {}
        self.normalized = {}
        for frm, to in ((self.raw, self._normalized), (self._normalized, self.normalized)):
            for organ, awards in frm.items():
                if organ in self.organ_lookup:
                    organ = self.organ_lookup[organ].iri

                to[organ] = [NormAward(a) for a in awards]

    def _reporter(self, aids):
        # can't seem to get this to cooperate
        base = ('https://api.federalreporter.nih.gov'
                '/v1/projects/FetchBySmApplIds')
        resp = requests.post(base, json=aids, headers={'Accept': 'application/json',
                                                       'Content-Type': 'application/json'})
        breakpoint()
        return resp.json()

    def reporter(self, href):
        resp = requests.get(href)
        soup = self.BeautifulSoup(resp.content, 'lxml')
        #id = soup.find_all('span', {'id': 'spnPNUMB'})
        table = soup.find_all('table', {'summary': 'Details'})
        text = table[0].find_all('td')[1].text.strip()
        if 'Former' in text:
            award, rest = text.split(' ', 1)
            rest, former = text.rsplit(' ', 1)
            return [award, former]
        else:
            return [text, None]


class MetaMaker:
    """ FIXME this is a bad pattern :/ """
    schema_class = MetaOutSchema
    organ_to_award = OrganData().normalized
    award_to_organ = {}
    for k, vs in organ_to_award.items():
        for v in vs:
            award_to_organ[v] = k

    def __new__(cls, fthing):
        cls.schema = cls.schema_class()
        return super().__new__(cls)

    def __init__(self, fthing):
        self.f = fthing

    @staticmethod
    def _generic(gen):
        l = list(gen)
        if len(l) == 1:
            return l[0]
        else:
            return l

    @property
    def award_number(self):
        return self._generic(self.f.award)
      
    @property
    def principal_investigator(self):
        return self._generic(self.f.PI)

    @property
    def species(self):
        return self._generic(self.f.species)

    @property
    def organ(self):
        org = self._generic(self.f.organ)
        if org:
            return org
        else:
            ans = self.award_number
            for an in self.f.award:
                if an not in self.award_to_organ:
                    log.critical(f'Award number {an} not in known set!')
                    #breakpoint()
                    return

                return self.award_to_organ[an]

    @property
    def modality(self):
        return self._generic(self.f.modality)

    @property
    def contributor_count(self):
        # FIXME slow reads from disk every time ...
        t = self._generic(self.f.dataset_description)
        if isinstance(t, list):  # FIXME ick
            return
        elif t:
            # FIXME this rereads so it could change
            # after the original schema was validated ...
            return len(t.data['contributors'])

    @property
    def subject_count(self):
        t = self._generic(self.f.subjects)
        if isinstance(t, list):  # FIXME ick
            return
        if t:
            return len(t.data['subjects'])

    @property
    def sample_count(self):
        pass

    @property
    def human_uri(self):
        return self.f.path.cache.human_uri


class FakePathHelper:

    # it is not possible to attach extended attributes to a symlink
    # while we could come up with a convention to embed the information
    # we need in the link path it seems ... annoying?
    @property
    def real_path(self):
        path = self.path
        while '.fake' in path.suffixes:
            path = path.with_suffix('')

        return path

    @property
    def suffix(self):
        return self.real_path.suffix

    @property
    def suffixes(self):
        return self.real_path.suffixes

    @property
    def mimetype(self):
        mime, encoding = mimetypes.guess_type(self.real_path.as_uri())
        if mime:
            return mime

    @property
    def encoding(self):
        mime, encoding = mimetypes.guess_type(self.real_path.as_uri())
        if encoding:
            return encoding

    @property
    def _magic_mimetype(self):
        """ This can be slow because it has to open the files. """
        if self.real_path.exists():
            return magic.detect_from_filename(self.real_path).mime_type


class FThing(FakePathHelper):
    """ a homogenous representation """
    schema_class = DatasetSchema
    schema_out_class = DatasetOutSchema  # FIXME not sure if good idea ...

    def __new__(cls, path, cypher=hashlib.sha256, metamaker=MetaMaker):
        cls.schema = cls.schema_class()
        cls.schema_out = cls.schema_out_class()
        return super().__new__(cls)

    def __init__(self, path, cypher=hashlib.sha256, metamaker=MetaMaker):
        self._errors = []
        if isinstance(path, str):
            path = Path(path)

        self.path = path
        self.fake = '.fake' in self.path.suffixes
        self.status = CurationStatusStrict(self)
        self.lax = CurationStatusLax(self)

        self.cypher = cypher
        self.metamaker = metamaker(self)
        #self._pio_header = _pio_header  # FIXME ick

    @property
    def _meta(self):
        """ pathmeta NOT json meta (confusingly) """
        cache = self.path.cache
        if cache is not None:
            return self.path.cache.meta

    def rglob(self, pattern):
        # TODO
        pass 

    @property
    def bids_root(self):
        # FIXME this will find the first dataset description file at any depth!
        # this is incorrect behavior!
        """ Sometimes there is an intervening folder. """
        if self.is_dataset:
            dd_paths = list(self.path.rglob('dataset_description*.*'))  # FIXME possibly slow?
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
    def parent(self):
        if self.is_organization:
            return None

        if self.path.parent.cache:
            pp = FThing(self.path.parent)
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
    def is_organization(self):
        if hasattr(self, 'anchor'):
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
        if self._meta is not None:
            return self._meta.id

    @property
    def name(self):
        return self.path.name

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
        if self.path.is_file() and not self.fake:
            return self.path.checksum(cypher=self.cypher)

        elif self.path.is_dir():
            # TODO need to determine the hashing rule for folders
            pass

    @property
    def award(self):
        for award in self._award_raw:
            yield NormAward(NormAward(award))

    @property
    def _award_raw(self):
        for s in self.submission:
            dict_ = s.data
            if 'sparc_award_number' in dict_:
                yield dict_['sparc_award_number']
            #if 'SPARC Award number' in dict_:
                #yield dict_['SPARC Award number']

        for d in self.dataset_description:
            dict_ = d.data
            if 'funding' in dict_:
                yield dict_['funding']

    @property
    def PI(self):
        mp = 'contributor_role'
        os = ('PrincipalInvestigator',)
        p = 'contributors'
        for dd in self.dataset_description:
            dict_ = dd.data_with_errors
            if p in dict_:
                for contributor in dict_[p]:
                    if mp in contributor:
                        for role in contributor[mp]:
                            normrole = NormContributorRole(role)
                            if 'name' in contributor:
                                fn, ln = vldt.Derives.contributor_name(contributor['name'])
                                contributor['first_name'] = fn
                                contributor['last_name'] = ln
                                if normrole in os:
                                    #print(contributor)
                                    for s, p, o in self.triples_contributors(contributor):
                                        if p == a and o == owl.NamedIndividual:
                                            yield s

    @property
    def species(self):
        out = set()
        for subject_file in self.subjects:
            data = subject_file.data_with_errors
            if 'subjects' in data:
                subjects = data['subjects']
                for subject in subjects:
                    if 'species' in subject:
                        out.add(subject['species'])

        return tuple(out)

    @property
    def organ(self):
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

    @property
    def modality(self):
        # yield 'Unknown'  # TODO
        return
        yield

    @property
    def keywords(self):
        for dd in self.dataset_description:
            # FIXME this pattern leads to continually recomputing values
            # we need to be deriving all of this from a checkpoint on the fully
            # normalized and transformed data
            data = dd.data_with_errors
            if 'keywords' in data:  # already logged error ...
                yield from data['keywords']

    @property
    def protocol_uris(self):
        p = 'protocol_url_or_doi'
        for dd in self.dataset_description:
            if p in dd:
                for uri in dd[p]:
                    if uri.startswith('http'):
                        # TODO normalize
                        yield uri
                    else:
                        log.warning(f"protocol not uri {uri} '{self.id}'")

    @property
    def protocol_uris_resolved(self):
        if not hasattr(self, '_c_protocol_uris_resolved'):
            self._c_protocol_uris_resolved = list(self._protocol_uris_resolved)

        return self._c_protocol_uris_resolved

    @property
    def _protocol_uris_resolved(self):
        # FIXME quite slow ...
        for start_uri in self.protocol_uris:
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
        return
        # FIXME need a single place to get these from ...
        for uri in self.protocol_uris_resolved:
            if uri not in protocol_jsons:  # FIXME yay global variables
                j = self._get_protocol_json(uri)
                if j:
                    protocol_jsons[uri] = j
                else:
                    protocol_jsons[uri] = {}  # TODO try again later

                yield j

            else:
                yield protocol_jsons[uri]

    def _get_protocol_json(self, uri):
        #juri = uri + '.json'
        uri_path = uri.rsplit('/', 1)[-1]
        apiuri = 'https://protocols.io/api/v3/protocols/' + uri_path
        #'https://www.protocols.io/api/v3/groups/sparc/protocols'
        #'https://www.protocols.io/api/v3/filemanager/folders?top'
        #print(apiuri, header)
        resp = requests.get(apiuri, headers=self._pio_header)
        #log.info(str(resp.request.headers))
        j = resp.json()  # the api is reasonably consistent
        if resp.ok:
            return j
        else:
            log.error(f"protocol no access {uri} '{self.dataset.id}'")

    @property
    def _meta_file(self):
        """ DEPRECATED """
        try:
            return next(self.path.glob('N:*:*'))
        except StopIteration:
            return None

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
                    log.warning(f"path bad case '{path}'")
                yield path

            else:
                log.error(f"path has not been retrieved '{path}'")

            for path in gen:
                if not path.is_broken_symlink():
                    if path.name[0].isupper():
                        log.warning(f"path bad case '{path}'")

                    yield path

                else:
                    log.warning(f"path has not been retrieved '{path}'")

        except StopIteration:
            if self.parent is not None:
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
    def _submission_tables(self):
        for path in self.submission_paths:
            yield Tabular(path)

    @property
    def submission(self):
        for t in self._submission_tables:
            try:
                miss = SubmissionFile(t)
                if miss.data:
                    yield miss
            except SubmissionFile.NoDataError as e:
                self._errors.append(e)  # NOTE we treat empty file as no file

    @property
    def dataset_description_paths(self):
        yield from self._abstracted_paths('dataset_description')
        #yield from self.path.glob('dataset_description*.*')

    @property
    def _dataset_description_tables(self):
        for path in self.dataset_description_paths:
            yield Tabular(path)

    @property
    def _dd(self):
        for t in self._dataset_description_tables:
            yield DatasetDescription(t)

    @property
    def dataset_description(self):
        for t in self._dataset_description_tables:
            #yield from DatasetDescription(t)
            # TODO export adapters for this ... how to recombine and reuse ...
            try:
                dd = DatasetDescription(t)
                if dd.data:
                    yield dd
            except DatasetDescription.NoDataError as e:
                self._errors.append(e)  # NOTE we treat empty file as no file

    @property
    def subjects_paths(self):
        yield from self._abstracted_paths('subjects')
        #yield from self.path.glob('subjects*.*')

    @property
    def _subjects_tables(self):
        for path in self.subjects_paths:
            yield Tabular(path)

    @property
    def subjects(self):
        for table in self._subjects_tables:
            try:
                sf = SubjectsFile(table)
                if sf.data:
                    yield sf
            except DatasetDescription.NoDataError as e:
                self._errors.append(e)  # NOTE we treat empty file as no file

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
        yield from self._submission_tables
        yield from self._dataset_description_tables
        yield from self._subjects_tables

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

    def _add(self, data):
        # additions
        data['id'] = self.id

    def _lift(self, data, lifts):
        # TODO lifts, copies, etc can all go in a single structure
        # TODO proper mapping
        for section_name in lifts:
            try:
                section = next(getattr(self, section_name))  # FIXME multiple when require 1
                if section_name in data:
                    # just skip stuff that doesn't exist
                    data[section_name] = section.data_with_errors
            except StopIteration:
                #data[section_name] = {'errors':[{'message':'Nothing to see here?'}]}
                # these errors were redundant with the missing key that
                # the higher level schema will detect for us
                continue

    def _copy(self, data, copies):
        for source_path, target_path in copies:
            # don't need a base case for thing?
            # can't lift a dict outside of itself
            # in this context
            self.copy(data, source_path, target_path)

    def _move(self, data, moves):
        for source_path, target_path in moves:
            self.move(data, source_path, target_path)

    def _derive(self, data, derives):
        for source_path, function, target_paths, source_key_optional in derives:
            source_prefixes = source_path[:-1]
            source_key = source_path[-1]
            source = data
            failed = False
            for i, node_key in enumerate(source_prefixes):
                if node_key in source:
                    source = source[node_key]
                else:
                    msg = f'did not find {node_key} in {source.keys()} {self.path}'
                    if not i:
                        log.error(msg)
                        failed = True
                        break
                    raise exc.NoSourcePathError(msg)
                if isinstance(source, list) or isinstance(source, tuple):
                    new_source_path = source_prefixes[i + 1:] + [source_key]
                    new_target_paths = [tp[i + 1:] for tp in target_paths]
                    new_derives = [(new_source_path, function, new_target_paths, source_key_optional)]
                    for sub_source in source:
                        self._derive(sub_source, new_derives)

                    return  # no more to do here

            if failed:
                continue  # sometimes things are missing we continue to others

            if source_key not in source:
                msg = f'did not find {source_key} in {source.keys()} {self.path}'
                if source_key_optional:
                    return log.info(msg)
                else:
                    raise exc.NoSourcePathError(msg)

            source_value = source[source_key]
            new_values = function(source_value)
            if len(new_values) != len(target_paths):
                raise TypeError('wrong number of values returned for {function}\n'
                                'was {len(new_values)} expect {len(targets)}')
            temp = b'__temporary'
            data[temp] = {}  # bytes ensure no collisions
            for target_path, value in zip(target_paths, new_values):
                heh = str(target_path)
                data[temp][heh] = value
                source_path = temp, heh  # hah
                self.move(data, source_path, target_path)

            data.pop(temp)

    def transform(self, data, lifts, copies, moves, derives):
        """ lift sections using a python representation """
        self._add(data)
        self._lift(data, lifts)
        self._copy(data, copies)
        self._move(data, moves)
        self._derive(data, derives)

    def copy(self, data, source_path, target_path):
        self._copy_or_move(data, source_path, target_path)

    def move(self, data, source_path, target_path):
        self._copy_or_move(data, source_path, target_path, True)

    def _get_source(self, data, source_path):
        #print(source_path, target_path)
        source_prefixes = source_path[:-1]
        source_key = source_path[-1]
        yield source_key  # yield this because we don't know if move or copy
        source = data
        for node_key in source_prefixes:
            if node_key in source:
                source = source[node_key]
            else:
                # don't move if no source
                msg = f'did not find {node_key} in {source.keys()} {self.path}'
                raise exc.NoSourcePathError(msg)

        if source_key not in source:
            msg = f'did not find {source_key} in {source.keys()} {self.path}'
            raise exc.NoSourcePathError(msg)

        yield source

    def _get_source_value(self, data, source_path):
        source_key, source = self._get_source(data, source_path)
        return source[source_key]

    def _copy_or_move(self, data, source_path, target_path, move=False):
        """ if exists ... """
        # FIXME there is a more elegant way to do this
        try:
            source_key, source = self._get_source(data, source_path)
        except exc.NoSourcePathError as e:
            log.warning(e)
            return

        if move:
            _parent = source  # incase something goes wrong
            source = source.pop(source_key)
        else:
            source = source[source_key]

        if copy and source != data:
            # copy first then modify means we need to deepcopy these
            # otherwise we would delete original forms that were being
            # saved elsewhere in the schema for later
            source = copy.deepcopy(source)

        target_prefixes = target_path[:-1]
        target_key = target_path[-1]
        target = data
        try:
            for target_name in target_prefixes:
                if target_name not in target:  # TODO list indicies
                    target[target_name] = {}

                target = target[target_name]

            target[target_key] = source
        except TypeError as e:  # e.g. you try to go to a string
            if move:
                # this will change key ordering but
                # that is expected, and if you were relying
                # on dict key ordering HAH
                _parent[node_key] = source

            raise e

        return
        if hasattr(self, section_name):
            section = next(getattr(self, section_name))  # FIXME non-homogenous
            # we can safely call next here because missing or multi
            # sections will be kicked out
            # TODO n-ary cases may need to be handled separately
            sec_data = section.data_with_errors
            #out.update(sec_data)  # the magic of being self describing
            out[section_name] = sec_data  # FIXME

    @property
    def data_out(self):
        """ this part adds the meta bits we need after _with_errors
            and rearranges anything that needs to be moved about """

        data = self.data_with_errors  # FIXME without errors how?
        lift_out = 'submission', 'dataset_description', 'subjects'
        copies = ([['dataset_description', 'contributors'], ['contributors']],
                  [['subjects',], ['inputs', 'subjects']],)
        moves = ([['dataset_description',], ['inputs', 'dataset_description']],
                 [['subjects', 'software'], ['resources']],  # FIXME update vs replace
                 [['subjects', 'subjects'], ['subjects']],
                 [['submission',], ['inputs', 'submission']],)
        # contributor derives
        derives = ([['contributors', 'name'],
                    vldt.Derives.contributor_name,
                    [['contributors', 'first_name'],
                     ['contributors', 'last_name']],
                    True],)
        self.transform(data, lift_out, copies, moves, derives)
        self.add_meta(data)

        return data

    def add_meta(self, data):
        # lifted or computed
        # FIXME use copy/move for this
        want_fields = MetaOutSchema.extra_required
        meta_extra = {name:getattr(self.metamaker, name)
                      for name in want_fields}
        t = self.metamaker._generic(self.dataset_description)
        if t and not isinstance(t, list):
            d = t.data
            d.pop('contributors', None)  # FIXME FIXME FIXME
            _meta = {**d, **meta_extra}
            meta = {k:v for k, v in _meta.items() if v}
            # this is validated later in DataOutSchema ...
            #ok, valid, meta = self.metamaker.schema.validate(meta)
            #if not ok:
                #meta['errors'] = valid.json()

            data['meta'] = {k:v for k, v in meta.items() if v}
        else:
            if 'errors' not in data:
                data['errors'] = []
            data['errors'] += [{'message': 'Too many dataset_descriptions!'}]

    @property
    def data_with_errors(self):
        # FIXME remove this
        # this still ok, dwe is very simple for this case
        return self._with_errors(self.schema.validate(copy.deepcopy(self.data)), self.schema)

    @property
    def data_out_with_errors(self):
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

    def submission_completeness_index(self, inputs):
        total_possible_errors = self.schema.total_possible_errors
        data = inputs
        if not data:
            return 0
        else:
            actual_errors = 0
            for required in self.schema.schema['required']:
                actual_errors += 1
                if required in data:
                    r = list(getattr(self, required))
                    if len(r) == 1:
                        r = r[0]
                        actual_errors -= r.submission_completeness_index

            return (total_possible_errors - actual_errors) / total_possible_errors

    def dump_all(self):
        return {attr: express_or_return(getattr(self, attr))
                for attr in dir(self) if not attr.startswith('_')}

    def __eq__(self, other):
        # TODO order by status
        return self.name == other.name

    def __gt__(self, other):
        return self.name > other.name

    def __lt__(self, other):
        return self.name < other.name

    def __hash__(self):
        return hash((hash(self.__class__), self.id))  # FIXME checksum? (expensive!)

    def __repr__(self):
        return f'{self.__class__.__name__}(\'{self.path}\')'

    @property
    def bf_uri(self):
        return self.path.cache.api_uri

    def ddt(self, data):
        dsid = self.bf_uri
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
                    'human_uri': '',
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

    @property
    def members(self):
        if not hasattr(self.__class__, '_members'):
            log.debug('going to network for members')
            # there are other ways to get here but this one caches
            # e.g. self.organization.path.remote.bfobject
            # self.path.remote.oranization.bfobject
            # self.path.remote.bfl.organization.members
            self.__class__._members = self.path.remote.bfl.organization.members

        return self._members

    @property
    def member_index_f(self):
        if not hasattr(self.__class__, '_member_index_f'):
            mems = defaultdict(lambda:defaultdict(list))
            for member in self.members:
                fn = member.first_name.lower()
                ln = member.last_name.lower()
                current = mems[fn][ln].append(member)

            self.__class__._member_index_f = {fn:dict(lnd) for fn, lnd in mems.items()}

        return self._member_index_f

    @property
    def member_index_l(self):
        if not hasattr(self.__class__, '_member_index_l'):
            mems = defaultdict(dict)
            for fn, lnd in self.member_index_f.items():
                for ln, member_list in lnd.items():
                    mems[ln][fn] = member_list

            self.__class__._member_index_l = dict(mems)

        return self._member_index_l

    def get_member_by_name(self, first, last):
        def lookup(d, one, two):
            if one in d:
                ind = d[one]
                if two in ind:
                    member_list = ind[two]
                    if member_list:
                        member = member_list[0]
                        if len(member_list) > 1:
                            log.critical(f'WE NEED ORCIDS! {one} {two} -> {member_list}')
                            # organization maybe?
                            # or better, check by dataset?
                            
                        return member

        fnd = self.member_index_f
        lnd = self.member_index_l
        fn = first.lower()
        ln = last.lower()
        m = lookup(fnd, fn, ln)
        if not m:
            m = lookup(lnd, ln, fn)

        return m

    def triples_contributors(self, contributor):
        try:
            dsid = self.bf_uri
        except BaseException as e:  # FIXME ...
            log.error(e)
            return

        # get member if we can find them
        if 'name' in contributor:
            fn = contributor['first_name']
            ln = contributor['last_name']
            if ' ' in fn:
                fn, mn = fn.split(' ', 1)

            failover = f'{fn}-{ln}'
            member = self.get_member_by_name(fn, ln)

            if member is not None:
                userid = rdflib.URIRef('https://api.blackfynn.io/users/' + member.id)

        else:
            member = None
            failover = 'no-orcid-no-name'
            log.warning(f'No name!' + lj(contributor))

        if 'contributor_orcid_id' in contributor:
            s = rdflib.URIRef(contributor['contributor_orcid_id' ])
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
        converter = ContributorConverter(contributor)
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
        data = self.data_out_with_errors
        try:
            dsid = self.bf_uri
        except BaseException as e:  # FIXME ...
            raise e
            return

        if 'meta' in data:
            meta_converter = MetaConverter(data['meta'])
            yield from meta_converter.triples_gen(dsid)
        else:
            raise ValueError('wat')

        converter = DatasetConverter(data)
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
            for s_local, p, o in subjects.triples_local:
                yield subject_id(s_local), p, o

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
            (isAbout, rdflib.URIRef(self.path.cache.api_uri)),
            (TEMP.hasHumanUri, rdflib.URIRef(self.path.cache.human_uri)),
            (rdfs.label, rdflib.Literal(f'{self.name} curation export graph')),
            (rdfs.comment, self.header_graph_description),
            (owl.imports, sparc_methods),
        )
        for p, o in pos:
            yield ontid, p, o

    @property
    def ttl(self):
        g = makeGraph('', prefixes=OntCuries._dict)
        graph = g.g
        #graph = rdflib.Graph()
        [graph.add(t) for t in self.triples_header]
        [graph.add(t) for t in self.triples]
        #g = cull_prefixes(_graph, prefixes=OntCuries)

        return graph.serialize(format='nifttl')


class Summary(FThing):
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
        super().__init__(self.organization.path, self.cypher)
 
    def __iter__(self):
        """ Return the list of datasets for the organization
            of the current FThing regardless of whether that
            FThing is itself an organization node """

        # when going up or down the tree _ALWAYS_
        # use the filesystem as the source of truth
        # do not cache in here
        for path in self.organization.path.iterdir():
            # FIXME non homogenous, need to find a better way ...
            yield self.__class__.__base__(path)

    @property
    def completeness(self):
        """ completeness, name, and id for all datasets """
        for dataset in self:
            # FIXME metamaker was a bad, bad idea
            dowe = dataset.data_out_with_errors
            yield (dowe['error_index'],
                   dowe['submission_completeness_index'],
                   dataset.name,
                   dataset.id)

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
            self._data_cache = self.make_json(d.data_out_with_errors for d in self)

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
            dowe = dataset.data_out_with_errors
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
            dowe = dataset.data_out_with_errors

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


class LThing:
    def __init__(self, lst):
        self._lst = lst


class FTLax(FThing):
    def _abstracted_paths(self, name_prefix, glob_type='rglob'):
        yield from super()._abstracted_paths(name_prefix, glob_type)


def express_or_return(thing):
    return list(thing) if isinstance(thing, GeneratorType) else thing


def get_datasets(project_path, FTC=FThing):
    ds = [FTC(p) for p in project_path.iterdir() if p.is_dir()]
    dsd = {d.id:d for d in ds}
    return ds, dsd


def parse_meta(project_path):  # XXX deprecated
    ds, dsd = get_datasets(project_path)
    dump_all = [{attr: express_or_return(getattr(d, attr))
                 for attr in dir(d) if not attr.startswith('_')}
                for d in ds]
    dad = {d['id']:d for d in dump_all}

    def get_diversity(field_name):
        # FIXME recurse
        return sorted(set(value
                          for d in dump_all
                          for dict_ in d['dataset_description']
                          for _key, _value in dict_.items()
                          if _key == field_name or isinstance(_value, tuple)
                          for value in
                          ((_value,)
                           if _key == field_name else
                           (v for thing in _value
                            if isinstance(thing, dict)
                            for key, value in thing.items()
                            if key == field_name
                            for v in (value if isinstance(value, tuple) else (value,))
                           ))))

    deep = dsd['N:dataset:a7b035cf-e30e-48f6-b2ba-b5ee479d4de3']
    awards = sorted(set(n for d in dump_all for n in d['award']))
    n_awards = len(awards)

    # TODO filtering of all of these by value ...
    by_award = defaultdict(set)
    for d in ds:
        awards = list(d.award)
        for a in awards:
            by_award[a].add(d)

        if not awards:
            by_award[None].add(d)

    by_award = dict(by_award)
    award_report = {a:(len(f), [(d.dataset_name_proper, d.name, d.id) for d in f]) for a, f in by_award.items()}

    #exts = sorted(set(''.join(p.suffixes).split('.fake.')[0] for p in project_path.rglob('*') if p.is_file() and p.suffixes))
    exts = sorted(set(NormFileSuffix(FThing(p).suffix.strip('.') if '.gz' not in p.suffixes else ''.join(FThing(p).suffixes).strip('.'))
                      for p in project_path.rglob('*') if p.is_file()))  # TODO gz
    n_exts = len(exts)

    fts = [FThing(p) for p in project_path.rglob('*') if p.is_file()]
    mimes = sorted(t for t in set(f.mimetype for f in fts if f.mimetype is not None))
    all_type_info = sorted(set((f.suffix, f.mimetype, f._magic_mimetype)
                               for f in fts), key = lambda v: [e if e else '' for e in v])
    type_report = '\n'.join(['extension\tfilename mime\tmagic mime'] +
                            ['\t'.join([str(s) for s in _]) for _ in all_type_info])
    # and this is why manual data entry without immediate feedback is a bad idea
    cr = get_diversity('contributor_role')
    fun = get_diversity('funding')
    orcid = get_diversity('contributor_orcid_id')

    zero = [d for d in ds if not d.status.overall[-1]] 
    n_zero = len(zero)

    #report = ''.join(sorted(d.name + '\n' + d.report + '\nlax\n' + d.lax.report + '\n\n' for d in ds))
    report = '\n\n'.join([d.report for d in sorted(ds, key = lambda d: d.lax.overall[-1])])
    protocol_state = sorted([d.status.has_protocol_uri, d.status.has_protocol, d.status.has_protocol_annotations, d] for d in ds)
    embed()


def schema_check(ds, dsd):
    dds = [dd for d in ds for dd in d.dataset_description]

    # TODO nest the schemas conditionally
    dsvr = {d.id:d.validate() for d in ds}
    ddvr = {d.id:dd.validate() for d in ds for dd in d.dataset_description}
    missvr = {d.id:miss.validate() for d in ds for miss in d.submission}
    jectvr = {d.id:ject.validate() for d in ds for ject in d.subjects}

    dsve = {k:e for k, e in dsvr.items() if not isinstance(e, dict)}
    ddve = {k:e for k, e in ddvr.items() if not isinstance(e, dict)}
    missve = {k:e for k, e in missvr.items() if not isinstance(e, dict)}
    jectve = {k:e for k, e in jectvr.items() if not isinstance(e, dict)}

    subjects_header_diversity = sorted(set(f for d in ds for s in d.subjects for f in s.bc.header._fields))
    subjects_wat = [(s,c) for d in ds for s in d.subjects for c in s.bc.cols if c[0].startswith('TEMP_')] 
    unique_wat = sorted(set(a for a, b in subjects_wat), key = lambda a: a.path)
    encoding_errors = [e for d in ds for dd in chain(d.dataset_description, d.submission, d.subjects) for e in dd.errors]
    #embed()
    return [dsvr, ddvr, missvr, jectvr], [dsve, ddve, missve, jectve]


class CurationReport:
    def __init__(self, project_path):
        self.ds, self.dsd = get_datasets(project_path)
        self.dsl, self.dsdl = get_datasets(project_path, FTC=FTLax)

    def gen(self):
        ds, dsl = self.ds, self.dsl

        # total
        n_ds = len(ds)

        # validated
        validated = [d for d in ds if d.validate()]
        n_val = len(validated)
        validated_l = [d for d in dsl if d.validate()]
        n_val_l = len(validated_l)

        # award
        award = [d for d in ds if list(d.award)]
        n_award = len(award)
        award_l = [d for d in dsl if list(d.award)]
        n_award_l = len(award_l)

        # submission
        sub_l = [FThing(s) for s in ds[0].parent.path.rglob('[Ss]ubmission*.*')]
        n_sub_l = len(sub_l)
        subd_l = [s for f in sub_l for s in f.submission]
        n_subd_l = len(subd_l)

        # oof
        bads = [f for f in sub_l if not list(f.submission)]
        n_bads = len(bads)

        h = ('Total', 'Structure', 'Lax structure', 'Award', 'Lax award',
             'Lax submission files', 'Lax submission data')
        r = n_ds, n_val, n_val_l, n_award, n_award_l, n_sub_l, n_subd_l

        return h, r

def populate_annos(group_name='sparc-curation'):
    group = devconfig.secrets('hypothesis', 'group', group_name)
    get_annos, annos, stream_thread, exit_loop = annoSync(group_to_memfile(group),
                                                          helpers=(HypothesisHelper, Hybrid, protc),
                                                          group=group)

    [protc(a, annos) for a in annos]
    [Hybrid(a, annos) for a in annos]

def main():
    #populate_annos()
    #parse_meta()
    project_path = local_storage_prefix / 'SPARC Consortium'

    ds, dsd = get_datasets(project_path)
    nr, ne = schema_check(ds, dsd)

    dsl, dsdl = get_datasets(project_path, FTLax)
    lr, le = schema_check(dsl, dsdl)
    compare = lambda i: (ne[i], le[i])

    total_paths = list(map(lambda d_: len([p for d in d_ for p in d.meta_paths]), (ds, dsl)))
    total_metas = list(map(lambda d_: len([p for d in d_ for p in d.meta_sections]), (ds, dsl)))
    total_valid = list(map(lambda d_: len([p for d in d_ for p in d.meta_sections if isinstance(p.validate(), dict)]), (ds, dsl)))
    dst = [p for d in ds for p in d.meta_sections if isinstance(p, FThing) and not isinstance(p.validate(), dict)]  # n bad
    dstl = [p for d in dsl for p in d.meta_sections if isinstance(p, FThing) and not isinstance(p.validate(), dict)]  # n bad

    # review paths in lax vs real
    dp = [(d, p) for d in ds for p in d.meta_paths]
    dpl = [(d, p) for d in dsl for p in d.meta_paths]

    dr = [(d.dataset_name_proper, p.name, FThing(p).parent.is_dataset)
           for d in ds for p in d.meta_paths] 
    drl = [(d.dataset_name_proper, p.name, FThing(p).parent.is_dataset)
           for d in dsl for p in d.meta_paths] 

    if __name__ == '__main__':
        from IPython import embed
        embed()

if __name__ == '__main__':
    main()
