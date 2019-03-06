#!/usr/bin/env python3
import io
import csv
import copy
import json
import hashlib
import mimetypes
from types import GeneratorType
from itertools import chain
from collections import defaultdict, deque
import magic  # from sys-apps/file consider python-magic ?
import requests
from xlsx2csv import Xlsx2csv, SheetNotFoundException
from pyontutils.core import OntTerm, OntId
from pyontutils.utils import makeSimpleLogger, byCol
from pyontutils.config import devconfig
from pyontutils.namespaces import OntCuries
from protcur.analysis import parameter_expression
from protcur.core import annoSync
from protcur.analysis import protc, Hybrid, SparcMI
from pysercomb.pyr.units import ProtcParameter
from scibot.utils import resolution_chain
from hyputils.hypothesis import group_to_memfile, HypothesisHelper
from sparcur.config import local_storage_prefix
from sparcur.core import Path
from sparcur.protocols_io_api import get_protocols_io_auth
from sparcur.schemas import (JSONSchema, ValidationError,
                             DatasetSchema, SubmissionSchema,
                             DatasetDescriptionSchema, SubjectsSchema,
                             SummarySchema, DatasetOutSchema, MetaOutSchema)
from IPython import embed

project_path = local_storage_prefix / 'SPARC Consortium'
logger = makeSimpleLogger('dsload')
logger.setLevel('CRITICAL')
OntCuries({'orcid':'https://orcid.org/',
           'ORCID':'https://orcid.org/',})

# FIXME this is an awful way to do this ...
if False:  # TODO create a protocols.io class for dealing with fetching that can be initialized with auth stuff
    _pio_creds = get_protocols_io_auth()
    _pio_header = {'Authentication': 'Bearer ' + _pio_creds.access_token}
    protocol_jsons = {}

class CJEncode(json.JSONEncoder):
     def default(self, obj):
         if isinstance(obj, tuple):
             return list(obj)
         elif isinstance(obj, deque):
             return list(obj)
         elif isinstance(obj, ProtcParameter):
             return str(obj)
         elif isinstance(obj, OntId):
             return obj.curie + ',' + obj.label

         # Let the base class default method raise the TypeError
         return json.JSONEncoder.default(self, obj)

class EncodingError(Exception):
    """ Some encoding error has occured in a file """


def validate(data, schema):
    """ capture errors """
    try:
        ok = schema.validate(data)  # validate {} to get better error messages
        # return True, ok, data  # FIXME better format
        return data  # don't normalize the representation here

    except ValidationError as e:
        # return False, e, data  # FIXME better format
        return e

def transform_errors(validation_error):
    skip = 'schema', 'instance'
    return [{k:v if k not in skip else k + ' REMOVED'
             for k, v in e._contents().items()}
            for e in validation_error.errors]

def normalize_tabular_format():
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
            value = value.replace('-', '-OD')  # hack

        n = (value
             .replace('(', '')
             .replace(')', '')
             .rstrip('-01')
             .replace('SPARC', '')
             .strip('NIH-1')
             .strip('NIH-')
             .replace('-', '')
             .replace(' ', '')
             .strip('NIH '))
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
                    logger.error(message)
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
            logger.warning(f'Sheet weirdness in{self.path}')
            logger.warning(str(e))

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
            logger.error(f'\'{self.path}\' {e}')


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
        header = self.normalize_header(orig_header)
        #print(header)
        #print(f"'{tabular.path}'", header)
        self.fail = False
        if self.to_index:
            for head in self.to_index:
                if head not in header:
                    logger.error(f'\'{self.t.path}\' malformed header!')
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

    def validate(self):
        try:
            data = self.data
            ok = self.schema.validate(data)  # validate {} to get better error messages
            return data  # don't normalize the representation here

        except ValidationError as e:
            return e

    def validate_out(self):
        try:
            data = self.data_out
            ok = self.schema_out.validate(data)
            return data
        except ValidationError as e:
            return e

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
        return value

    def normalize(self, key, value):
        v = value.replace('\ufeff', '')  # FIXME utf-16 issue
        if v != value:
            message = f"encoding feff error in '{self.path}'"
            logger.error(message)
            self._errors.append(EncodingError(message))

        if v.lower().strip() not in ('n/a', 'na'):  # FIXME explicit null vs remove from structure
            return getattr(self, key, self.default)(v)

    def rename_key(self, key, *parent_keys):
        """ modify this in your class if you need to rename a key """
        # TODO parent key lists
        return key

    @property
    def data(self):
        index_col, *_ = self.to_index
        out = {}
        if not hasattr(self.bc, index_col):
            logger.error(f'\'{self.t.path}\' malformed header!')
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
                                   for normv in (self.normalize(key, value),)
                                   if normv)
                    value = tuple(set(_value))
                    if len(value) != len(_value):
                        # TODO counter to show the duplicate values
                        logger.warning(f"duplicate values in {normk} TODO '{self.t.path}'")

                    if normk in self.max_one:
                        if not value:
                            logger.warning(f"missing value for {normk} '{self.t.path}'")
                        elif len(value) > 1:
                            logger.warning(f"too many values for {normk} {value} '{self.t.path}'")
                            # FIXME not selecting the zeroth element here breaks the schema assumptions
                            value = 'AAAAAAAAAAA' + '\n|AAAAAAAAAAA|\n'.join(value)
                        else:
                            value = value[0]  # FIXME error handling etc.

                    if value:
                        out[normk] = value

        for key, keys in self.verticals.items():
            value = tuple(_ for _ in ({self.rename_key(k, key):normv
                                       for k, value in zip(nme, values)
                                       if k in keys and value
                                       for normv in (self.normalize(k, value),)
                                       if normv}
                                      for head, *values in self.bc.cols
                                      if head not in self.skip_cols)
                          if _)
            if value:
                out[key] = value

        return out

    @property
    def data_with_errors(self):
        """ data with errors added under the 'errors' key """
        # FIXME TODO regularize this with the FThing version
        data = self.validate()
        if isinstance(data, ValidationError):
            # FIXME this will dump the whole schema, go smaller
            error = data
            data = self.data  # FIXME cost of calling twice
            if 'errors' in data:
                class WatError(Exception):
                    """ WAT """

                raise WatError('wat')

            data['errors'] = transform_errors(error)
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


class SubmissionFile(Version1Header):
    to_index = 'submission_item',  # FIXME normalized in version 2
    skip_cols = 'submission_item', 'definition'  # FIXME normalized in version 2

    verticals = {'submission':('sparc_award_number', 'milestone_achieved', 'milestone_completion_date')}
    schema_class = SubmissionSchema

    @property
    def data(self):
        """ lift list with single element to object """
        d = super().data
        if d:
            if d['submission']:
                d['submission'] = d['submission'][0]
            else:
                d['submission'] = {}

        return d

    def __iter__(self):
        """ unused """
        # FIXME > 1
        for path in self.submission_paths:
            t = Tabular(path)
            tl = list(t)
            #print(path, t)
            try:
                yield {key:value for key, d, value, *fixme in tl[1:]}  # FIXME multiple milestones apparently?
            except ValueError as e:
                # TODO expected columns vs received columns for all these
                logger.warning(f'\'{path}\' malformed header.')
                # best guess interpretation
                if len(tl[0]) == 2:
                    yield {k:v for k, v in tl[1:]}


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
                 'examples': ('example_image_filename', 'example_image_locator', 'example_image_description'),
    }
    schema_class = DatasetDescriptionSchema

    def contributor_orcid_id(self, value):
        v = value.replace(' ', '')
        if not v:
            return
        if v.startswith('http:'):
            v = v.replace('http:', 'https:', 1)

        if not (v.startswith('ORCID:') or v.startswith('https:')):
            v = v.strip()
            if len(v) != 19:
                logger.error(f"orcid wrong length '{value}' '{self.t.path}'")
                return

            v = 'ORCID:' + v

        else:
            if v.startswith('https:'):
                _, numeric = v.rsplit('/', 1)
            elif v.startswith('ORCID:'):
                _, numeric = v.rsplit(':', 1)

            if len(numeric) != 19:
                logger.error(f"orcid wrong length '{value}' '{self.t.path}'")
                return

        try:
            #logger.debug(f"{v} '{self.t.path}'")
            return OntId(v)
        except OntId.BadCurieError as e:
            logger.error(f"orcid malformed '{value}' '{self.t.path}'")
            return value

    def contributor_role(self, value):
        # FIXME normalizing here momentarily to squash annoying errors
        return tuple(sorted(set(NormContributorRole(e.strip()) for e in value.split(','))))

    def is_contact_person(self, value):
        return value.lower() == 'yes'

    def rename_key(self, key, *parent_keys):
        # FIXME multiple parent keys...
        if parent_keys == ('contributors',):
            if key == 'contributors':
                return 'name'

        return key

    def __iter__(self):
        # TODO this needs to be exporting triples ...
        #print(self.bc.header)
        if not hasattr(self.bc, 'metadata_element'):
            logger.error(f'\'{self.t.path}\' malformed header!')
            return
        nme = Version1Header.normalize_header(self.bc.metadata_element)
        yield from ({k:nv for k, v in zip(nme, getattr(self.bc, col))
                     if v
                     for nv in (getattr(self, k, self.default)(v),)
                     if nv}
                    for col in self.bc.header[3:])


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
        return self.query(nv, 'NCBITaxon')

    def sex(self, value):
        nv = NormSex(value)
        return self.query(nv, 'PATO')

    def gender(self, value):
        # FIXME gender -> sex for animals, requires two pass normalization ...
        return self.sex(value)

    def age(self, value):
        _, v, rest = parameter_expression(value)
        if not v[0] == 'param:parse-failure':
            return str(ProtcParameter(v))
        else:
            # TODO warn
            return value

    def rrid_for_strain(self, value):
        return value

    def protocol_io_location(self, value):  # FIXME need to normalize this with dataset_description
        return value

    def process_dict(self, dict_):
        """ deal with multiple fields """
        out = {k:v for k, v in dict_.items() if k not in self.skip}
        for h_unit, h_value in zip(self.h_unit, self.h_value):
            compose = dict_[h_value] + dict_[h_unit]
            _, v, rest = parameter_expression(compose)
            out[h_value] = str(ProtcParameter(v))  # FIXME sparc repr

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
        yield from (self.process_dict({k:nv for k, v in zip(r._fields, r) if v
                                       and k not in self.skip_cols
                                       for nv in (self.normalize(k, v),) if nv})
                    for r in self.bc.rows)


class FakePathHelper:

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



class MetaMaker:
    """ FIXME this is a bad pattern :/ """
    schema_class = MetaOutSchema

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
        return self._generic(self.f.organ)

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

    def xattrs(self):
        # decode values here where appropriate
        return {k:v if k == 'bf.checksum' else v.decode()
                for k, v in self.path.xattrs().items()}

    @property
    def bids_root(self):
        # FIXME this will find the first dataset description file at any depth!
        # this is incorrect behavior!
        """ Sometimes there is an intervening folder. """
        if self.is_dataset:
            dd_paths = list(self.path.rglob('dataset_description*.*'))  # FIXME possibly slow?
            if not dd_paths:
                #logger.warning(f'No bids root for {self.name} {self.id}')  # logging in a property -> logspam
                return

            elif len(dd_paths) > 1:
                #logger.warning(f'More than one submission for {self.name} {self.id} {dd_paths}')
                pass

            return dd_paths[0].parent  # FIXME choose shorter version? if there are multiple?

        elif self.parent:  # organization has no parent
            return self.parent.bids_root

    @property
    def parent(self):
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
    def is_organization(self):
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
        attrs = self.xattrs()
        if 'bf.id' in attrs:
            return attrs['bf.id']

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
        attrs = self.xattrs()
        if 'bf.size' in attrs:
            return int(attrs['bf.size'])
        elif self.path.is_dir():
            size = 0
            for path in self.path.rglob('*'):
                if path.is_file():
                    try:
                        size += int(path.getxattr('bf.size'))
                    except OSError as e:
                        logger.warning(f'File xattrs. Assuming it is not tracked. {path}')

            return size

        else:
            print('WARNING: unknown thing at path', self.path)

    @property
    def bf_checksum(self):
        attrs = self.xattrs()
        if 'bf.checksum' in attrs:
            return attrs['bf.checksum']

        # TODO checksum rules for contained folders

    def checksum(self):
        if self.path.is_file() and not self.fake:
            return self.path.checksum(cypher=self.cypher)

        elif self.path.is_dir():
            # TODO need to determine the hashing rule for folders
            pass

    @property
    def award(self):
        for award in self._award_raw:
            yield NormAward(award)

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
        for dict_ in self.dataset_description:
            if p in dict_:
                for contributor in dict_[p]:
                    if mp in contributor:
                        for role in contributor[mp]:
                            normrole = NormContributorRole(role)
                            if normrole in os:
                                #print(contributor)
                                yield contributor['name']  # TODO orcid etc
                                break

    @property
    def species(self):
        out = set()
        for subject in self.subjects:
            if 'species' in subject:
                out.add(subject['species'])

        return tuple(out)

    @property
    def organ(self):
        # TODO
        return '',

    @property
    def modality(self):
        # TODO
        return '',

    @property
    def keywords(self):
        for dd in self.dataset_description:
            if 'keywords' in dd:  # already logged error ...
                yield from dd['keywords']

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
                        logger.warning(f"protocol not uri {uri} '{self.id}'")

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
        #logger.info(str(resp.request.headers))
        j = resp.json()  # the api is reasonably consistent
        if resp.ok:
            return j
        else:
            logger.error(f"protocol no access {uri} '{self.dataset.id}'")

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
            if '.fake' not in path.suffixes:
                if path.name[0].isupper():
                    logger.warning(f"filename bad case '{path}'")
                yield path

            else:
                logger.warning(f"filename not retrieved '{path}'")
            for path in gen:
                if '.fake' not in path.suffixes:
                    if path.name[0].isupper():
                        logger.warning(f"filename bad case '{path}'")

                    yield path

                else:
                    logger.warning(f"filename not retrieved '{path}'")

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

    validate = Version1Header.validate
    validate_out = Version1Header.validate_out

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


    def _with_errors(self, valid, thunk, schema):
        # FIXME this needs to be split into in and out
        # and really all this section should do is add the errors
        # on failure and it should to it in place in the pipeline

        # data in -> check -> with errors -> normalize/correct/map ->
        # -> augment and transform -> data out check -> out with errors

        # this flow splits at normalize/correct/map -> export back to user

        data = valid
        out = {}  # {'raw':{}, 'normalized':{},}
        # use the schema to auto fill things that we are responsible for
        # FIXME possibly separate this to our own step?
        try:
            if 'id' in schema.schema['required']:
                out['id'] = self.id
        except KeyError:
            pass

        if isinstance(data, ValidationError):
            # FIXME this will dump the whole schema, go smaller
            if 'errors' not in out:
                out['errors'] = []

            #out['errors'] += [{k:v if k != 'schema' else k
                               #for k, v in e._contents().items()}
                              #for e in data.errors]
            out['errors'] += transform_errors(data)

            data = thunk()  # FIXME cost of calling twice

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
                data[section_name] = {'errors':[{'message':'Nothing to see here?'}]}

    def _copy(self, data, copies):
        for source_path, target_path in copies:
            # don't need a base case for thing?
            # can't lift a dict outside of itself
            # in this context
            self.copy(data, source_path, target_path)

    def _move(self, data, moves):
        for source_path, target_path in moves:
            self.move(data, source_path, target_path)

    def transform(self, data, lifts, copies, moves):
        """ lift sections using a python representation """
        self._add(data)
        self._lift(data, lifts)
        self._copy(data, copies)
        self._move(data, moves)

    def copy(self, data, source_path, target_path):
        self._copy_or_move(data, source_path, target_path)

    def move(self, data, source_path, target_path):
        self._copy_or_move(data, source_path, target_path, True)

    def _copy_or_move(self, data, source_path, target_path, move=False):
        """ if exists ... """
        #print(source_path, target_path)
        source_prefixes = source_path[:-1]
        source_key = source_path[-1]
        source = data
        for node_key in source_prefixes:
            if node_key in source:
                source = source[node_key]
            else:
                # don't move if no source
                logger.warning(f'did not find {node_key} in {source.keys()} {self.path}')
                return

        # FIXME there is a more elegant way to do this
        if source_key not in source:
            logger.warning(f'did not find {source_key} in {source.keys()} {self.path}')
            return
        else:
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
        self.transform(data, lift_out, copies, moves)
        self.add_meta(data)

        if not data:
            data['errors'] = [{'message': 'Nothing to see here!?'}]

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
            meta = {**d, **meta_extra}
            valid = validate(meta, self.metamaker.schema)
            if valid != meta:
                meta['errors'] = transform_errors(valid)

            data['meta'] = {k:v for k, v in meta.items() if v}

        return

        # TODO raw vs normalized
        out = self.data_with_errors

        # TODO 1:1 remapping rules for in/out schema
        if self.is_dataset:  # FIXME :/ evil hardcoding
            # copy from path a to path b ...
            # '/dataset_description/contributors'
            # '/contributors'
            # while this may seem redundant there is going
            # to be a normalization step in between here
            # this will need to go in a loop or something
            # FIXME redundant ...
            try:
                cs = out['dataset_description']['contributors']
                out['contributors'] = cs
            except KeyError:  # FIXME ??? propagating errors!??!
                # FIXME
                out['contributors'] = {'errors':[{'message': 'Not our problem',
                                                  'cause': 'no dataset_description',
                                                  'type': 'DOWNSTREAM',}]}
            #except TypeError as e:
                #embed()

            out['inputs'] = {}
            for key in self.schema_out.schema['properties']['inputs']['properties']:
                if key in out:
                    #if key in self.transform:  # TODO
                    out['inputs'][key] = out.pop(key)

    @property
    def data_with_errors(self):
        # FIXME remove this
        # this still ok, dwe is very simple for this case
        return self._with_errors(self.validate(), lambda:self.data, self.schema)

    @property
    def data_out_with_errors(self):
        #return self._with_errors(self.validate_out(), lambda:self.data_out, self.schema_out)
        data = self.data_out
        val = validate(data, self.schema_out)
        if data != val:
            data['errors'] = transform_errors(val)

        return data

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

    def make_json(self, gen):
        # FIXME this and the datasets is kind of confusing ...
        # might be worth moving those into a reporting class
        # that always works at the top level regardless of which
        # file it is give?

        ds = list(gen)
        count = len(ds)
        meta = {'count': count}
        return {'id': self.id,
                'meta': meta,
                'datasets': ds}

    @property
    def data(self):
        # FIXME validating in vs out ...
        # return self.make_json(d.validate_out() for d in self)
        return self.make_json(d.data_out_with_errors for d in self)

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
    def disco(self):
        dsh = sorted(MetaOutSchema.schema['properties'])
        chs = sorted(('name',
                'contributor_orcid_id',
                'is_contact_person',
                'contributor_affiliation',
                'contributor_role'))

        datasets = [['id'] + dsh]
        contributors = [['id'] + chs]
        subjects = [['id', 'blob']]
        errors = [['id', 'blob']]
        resources = [['id', 'blob']]

        #cje = CJEncode()
        def normv(v):
            if isinstance(v, list) or isinstance(v, tuple):
                v = ','.join(str(_) for _ in v)
            elif any(isinstance(v, c) for c in
                     (int, float, str)):
                v = str(v)

            return v


        for dataset in self:
            id = dataset.id
            dowe = dataset.data_out_with_errors

            row = [id]  # FIXME this doubles up on the row
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
                row += [None for k in meta]

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
                sbs = dowe['subjects']
                if 'subjects' in sbs:
                    for subject in sbs['subjects']:
                        row = [id]
                        row.append(json.dumps(subject, cls=CJEncode))
                        subjects.append(row)

                # moved to resources if exists already
                #if 'software' in sbs:
                    #for software in sbs['software']:
                        #row = [id]
                        #row.append(json.dumps(software, cls=CJEncode))
                        #resources.append(row)

            if 'resources' in dowe:
                for res in dowe['resources']:
                    row = [id]
                    row.append(json.dumps(res, cls=CJEncode))

            if 'errors' in dowe:
                ers = dowe['errors']
                for er in ers:
                    row = [id]
                    row.append(json.dumps(er, cls=CJEncode))
                    errors.append(row)

        # TODO samples resources
        return (('datasets', datasets),
                ('contributors', contributors),
                ('subjects', subjects),
                ('resources', resources),
                ('errors', errors))


from hyputils.hypothesis import iterclass
class LThing:
    def __init__(self, lst):
        self._lst = lst


def JT(blob):
    def _populate(blob, top=False):
        if isinstance(blob, list) or isinstance(blob, tuple):
            # TODO alternatively if the schema is uniform, could use bc here ...
            def _all(self, l=blob):  # FIXME don't autocomplete?
                keys = set(k for b in l
                           if isinstance(b, dict)
                           for k in b)
                obj = {k:[] for k in keys}
                _list = []
                _other = []
                for b in l:
                    if isinstance(b, dict):
                        for k in keys:
                            if k in b:
                                obj[k].append(b[k])
                            else:
                                obj[k].append(None)

                    elif any(isinstance(b, t) for t in (list, tuple)):
                        _list.append(JT(b))

                    else:
                        _other.append(b)
                        for k in keys:
                            obj[k].append(None)  # super inefficient
                            
                if _list:
                    obj['_list'] = JT(_list)

                if obj:
                    j = JT(obj)
                else:
                    j = JT(blob)

                if _other:
                    #obj['_'] = _other  # infinite, though lazy
                    setattr(j, '_', _other)

                setattr(j, '_b', blob)
                #lb = len(blob)
                #setattr(j, '__len__', lambda: lb)  # FIXME len()
                return j

            def it(self, l=blob):
                for b in l:
                    if any(isinstance(b, t) for t in (dict, list, tuple)):
                        yield JT(b)
                    else:
                        yield b

            if top:
                # FIXME iter is non homogenous
                return [('__iter__', it), ('_all', property(_all))]
            #elif not [e for e in b if isinstance(self, dict)]:
                #return property(id)
            else:
                # FIXME this can render as {} if there are no keys
                return property(_all)
                #obj = {'_all': property(_all),
                       #'_l': property(it),}

                #j = JT(obj)
                #return j

                #nl = JT(obj)
                #nl._list = blob
                #return property(it)

        elif isinstance(blob, dict):
            if top:
                out = [('_keys', tuple(blob))]
                for k, v in blob.items():  # FIXME normalize keys ...
                    nv = _populate(v)
                    out.append((k, nv))
                    #setattr(cls, k, nv)
                return out
            else:
                return JT(blob)

        else:
            if top:
                raise TypeError('asdf')
            else:
                @property
                def prop(self, v=blob):
                    return v

                return prop

    def _repr(self, b=blob):  # because why not
        return 'JT(\n' + repr(b) + '\n)'

    #cd = {k:v for k, v in _populate(blob, True)}

    # populate the top level
    cd = {k:v for k, v in ((a, b) for t in _populate(blob, True)
                           for a, b in (t if isinstance(t, list) else (t,)))}
    cd['__repr__'] = _repr
    nc = type('JT' + str(type(blob)), (object,), cd)
    return nc()


class FTLax(FThing):
    def _abstracted_paths(self, name_prefix, glob_type='rglob'):
        yield from super()._abstracted_paths(name_prefix, glob_type)



def express_or_return(thing):
    return list(thing) if isinstance(thing, GeneratorType) else thing


def get_datasets(project_path, FTC=FThing):
    ds = [FTC(p) for p in project_path.iterdir() if p.is_dir()]
    dsd = {d.id:d for d in ds}
    return ds, dsd


def parse_meta():
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

def populate_annos():
    get_annos, annos, stream_thread, exit_loop = annoSync(group_to_memfile(devconfig.secrets('sparc-curation')),
                                                          helpers=(HypothesisHelper, Hybrid, protc, SparcMI),
                                                          group=devconfig.secrets('sparc-curation'))

    [protc(a, annos) for a in annos]
    [Hybrid(a, annos) for a in annos]

def main():
    #populate_annos()
    #parse_meta()
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

    embed()

if __name__ == '__main__':
    main()
