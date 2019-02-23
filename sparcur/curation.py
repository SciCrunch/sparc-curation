#!/usr/bin/env python3
import io
import csv
import hashlib
import mimetypes
from types import GeneratorType
import magic  # from sys-apps/file consider python-magic ?
from xlsx2csv import Xlsx2csv, SheetNotFoundException
from sparcur.blackfynn_api import local_storage_prefix, Path
from pyontutils.utils import makeSimpleLogger, byCol
from pyontutils.core import OntTerm, OntId
from pyontutils.namespaces import OntCuries
from protcur.analysis import parameter_expression
from IPython import embed

project_path = local_storage_prefix / 'SPARC Consortium'
logger = makeSimpleLogger('dsload')
OntCuries({'orcid':'https://orcid.org/',
           'ORCID':'https://orcid.org/',})

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
                    logger.error(f"encoding bad '{encoding}' '{self.path}'")
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
            logger.warn(f'Sheet weirdness in{self.path}')
            logger.warn(str(e))

    def normalize(self, rows):
        # FIXME need to detect changes
        for row in rows:
            yield [c.strip() for c in row]

    def __iter__(self):
        try:
            yield from self.normalize(getattr(self, self.file_extension)())
        except UnicodeDecodeError as e:
            logger.error(f'\'{self.path}\' {e}')


class Version1Header:
    to_index = tuple()
    skip_cols = 'metadata_element', 'description', 'example'
    verticals = dict()  # FIXME should really be immutable

    def __init__(self, tabular):
        self.skip_rows = tuple(key for keys in self.verticals.values() for key in keys)
        self.t = tabular
        orig_header, *rest = list(tabular)
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
                     .replace('\ufeff', '')
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


class DatasetDescription(Version1Header):
    to_index = 'metadata_element',
    skip_cols = 'metadata_element', 'description', 'example'
    max_one = (  # FIXME we probably want to write this as json schema or something ...
        'name',
        'description',
        'acknowledgements',
        'funding',
        #'originating_article_doi',  # and sometimes they comma separate them! >_< derp
        'protocol_url_or_doi',
        'additional_links',
        'link_description',
        'example_image_filename',
        'example_image_locator',
        'example_image_description',
        'completeness_of_data_set',
        'prior_batch_number',
        'title_for_complete_data_set')
    verticals = {'contributors': ('contributors',
                                  'contributor_orcid_id',
                                  'contributor_affiliation',
                                  'contributor_role',
                                  'is_contact_person',),}

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
        return tuple(sorted(set(e.strip() for e in value.split(','))))

    def is_contact_person(self, value):
        return value.lower() == 'yes'

    def default(self, value):
        # TODO need a 'run for all' which
        return value

    def normalize(self, key, value):
        return getattr(self, key, self.default)(value)

    @property
    def data(self):
        index_col = 'metadata_element'
        out = {}
        if not hasattr(self.bc, index_col):
            logger.error(f'\'{self.t.path}\' malformed header!')
            return out
        nme = Version1Header.normalize_header(self.bc.metadata_element)
        nmed = {v:normv for normv, v in zip(nme, self.bc.metadata_element)}

        for v, nt in self.bc._byCol__indexes[index_col].items():
            if v != index_col:
                normv = nmed[v]
                if normv not in self.skip_rows:
                    _value = tuple(normv for key, value in zip(nt._fields, nt)
                                   if key not in self.skip_cols and value
                                   for normv in (self.normalize(key, value),)
                                   if normv)
                    value = tuple(set(_value))
                    if len(value) != len(_value):
                        # TODO counter to show the duplicate values
                        logger.warn(f"duplicate values in {normv} TODO '{self.t.path}'")

                    if normv in self.max_one:
                        if not value:
                            logger.warn(f"missing value for {normv} '{self.t.path}'")
                        elif len(value) > 1:
                            logger.warn(f"too many values for {normv} {value} '{self.t.path}'")
                            # FIXME not selecting the zeroth element here breaks the schema assumptions
                            value = 'AAAAAAAAAAA' + '\n|AAAAAAAAAAA|\n'.join(value)
                        else:
                            value = value[0]  # FIXME error handling etc.

                    if value:
                        out[normv] = value

        for key, keys in self.verticals.items():
            out[key] = tuple(_ for _ in ({key:normv
                                          for key, value in zip(nme, values)
                                          if key in keys and value
                                          for normv in (self.normalize(key, value),)
                                          if normv}
                                         for head, *values in self.bc.cols
                                         if head not in self.skip_cols)
                             if _)

        return out

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
            return v
        else:
            # TODO warn
            return value

    def rrid_for_strain(self, value):
        return value

    def protocol_io_location(self, value):  # FIXME need to normalize this with dataset_description
        return value

    def default(self, value):
        v = value.replace('\ufeff', '')  # FIXME utf-16 issue
        if v != value:
            logger.error(f"encoding issue feff '{self.t.path}'")
        return v

    def normalize(self, key, value):
        return getattr(self, key, self.default)(value)

    def process_dict(self, dict_):
        """ deal with multiple fields """
        out = {k:v for k, v in dict_.items() if k not in self.skip}
        for h_unit, h_value in zip(self.h_unit, self.h_value):
            compose = dict_[h_value] + dict_[h_unit]
            _, v, rest = parameter_expression(compose)
            out[h_value] = v

        if 'gender' in out and 'species' in out:
            if out['species'] != OntTerm('NCBITaxon:9606'):
                out['sex'] = out.pop('gender')

        return out

    def __iter__(self):
        yield from (self.process_dict({k:self.normalize(k, v) for k, v in zip(r._fields, r) if v}) for r in self.bc.rows)


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
                 self.has_protocol)
        done = [p for p in parts if p]
        n_parts, n_done = len(parts), len(done)
        # TODO accumulate all the other metadata that we need from the files
        return n_parts, n_done

    @property
    def has_award(self):
        # FIXME len 1?
        return bool(list(self.dataset.award))

    @property
    def has_protocol(self):
        return bool(list(self.dataset.protocol_link))

    @property
    def protocol(self):
        return 0, 0

    def steps_done(self, stage_name):
        n_parts, n_done = getattr(self, stage_name)
        return f'{stage_name} {n_done}/{n_parts}'

    @property
    def report(self):
        return self.steps_done('overall')

    def __repr__(self):
        return self.dataset.dataset_name_proper + '\n' + self.report


class CurationStatusLax(CurationStatusStrict):
    # FIXME not quite working right ...
    @property
    def miss_paths(self):
        yield from self.dataset.path.rglob('submission*.*')

    @property
    def dd_paths(self):
        yield from self.dataset.path.rglob('dataset_description*.*')

    @property
    def jetc_paths(self):
        yield from self.dataset.path.rglob('subjects*.*')


class FThing(FakePathHelper):
    """ a homogenous representation """

    def __init__(self, path, cypher=hashlib.sha256):
        if isinstance(path, str):
            path = Path(path)

        self.path = path
        self.fake = '.fake' in self.path.suffixes
        self.status = CurationStatusStrict(self)
        self.lax = CurationStatusLax(self)

        self.cypher = cypher

    def xattrs(self):
        return self.path.xattrs()

    @property
    def bids_root(self):
        """ Sometimes there is an intervening folder. """
        if self.is_dataset:
            dd_paths = list(self.path.rglob('dataset_description*.*'))  # FIXME possibly slow?
            if not dd_paths:
                #logger.warn(f'No bids root for {self.name} {self.id}')  # logging in a property -> logspam
                return
            elif len(dd_paths) > 1:
                #logger.warn(f'More than one submission for {self.name} {self.id} {dd_paths}')
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
    def is_dataset(self):
        return self.id.startswith('N:dataset:')

    @property
    def dataset_id(self):
        if self.is_dataset:
            return self.id
        elif self.parent:
            return self.parent.dataset_id

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
        return f'{award} {PI} {self.species} {self.organ} {self.modality}'

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
                        logger.warn(f'File xattrs. Assuming it is not tracked. {path}')

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
            m = self.cypher()
            chunk_size = 4096
            with open(self.path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if chunk:
                        m.update(chunk)
                    else:
                        break

            return m.digest()

        elif self.path.is_dir():
            # TODO need to determine the hashing rule for folders
            pass

    @property
    def award(self):
        for award in self._award_raw:
            yield NormAward(award)

    @property
    def _award_raw(self):
        for dict_ in self.submission:
            if 'SPARC Award number' in dict_:
                yield dict_['SPARC Award number']

        for dict_ in self.dataset_description:
            if 'Funding' in dict_:
                yield dict_['Funding']

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
                                yield contributor['contributors']  # TODO orcid etc
                                break

    @property
    def species(self):
        return 'TODO'

    @property
    def organ(self):
        return 'TODO'

    @property
    def modality(self):
        return 'TODO'

    @property
    def protocol_link(self):
        p = 'protocol_url_or_doi'
        for dd in self.dataset_description:
            if p in dd:
                yield dd[p]

    @property
    def _meta_file(self):
        """ DEPRECATED """
        try:
            return next(self.path.glob('N:*:*'))
        except StopIteration:
            return None

    def _abstracted_paths(self, name_prefix):
        """ A bottom up search for the closest file in the parent directory.
            For datasets, if the bids root and path do not match, use the bids root.
            In the future this needs to be normalized because the extra code required
            for dealing with the intervening node is quite annoying to maintain.
        """
        if self.is_dataset and self.bids_root is not None and self.bids_root != self.path:
            path = self.bids_root
        else:
            path = self.path

        gen = path.glob(name_prefix + '*.*')

        try:
            path = next(gen)
            if '.fake' not in path.suffixes:
                yield path
            else:
                logger.warn(f'\'{path}\' has not been retrieved.')
            for path in gen:
                if '.fake' not in path.suffixes:
                    yield path
                else:
                    logger.warn(f'\'{path}\' has not been retrieved.')
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
    def submission(self):
        # FIXME > 1
        for path in self.submission_paths:
            t = Tabular(path)
            tl = list(t)
            #print(path, t)
            try:
                yield {key:value for key, d, value, *fixme in tl[1:]}  # FIXME multiple milestones apparently?
            except ValueError as e:
                # TODO expected columns vs received columns for all these
                logger.warn(f'\'{path}\' malformed header.')
                # best guess interpretation
                if len(tl[0]) == 2:
                    yield {k:v for k, v in tl[1:]}

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
            yield DatasetDescription(t).data

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
            yield from SubjectsFile(table)

    @property
    def report(self):
        return self.status.report

    def __repr__(self):
        return f'{self.__class__.__name__}(\'{self.path}\')'


def express_or_return(thing):
    return list(thing) if isinstance(thing, GeneratorType) else thing


def parse_meta():
    ds = [FThing(p) for p in project_path.iterdir() if p.is_dir()]
    dsd = {d.id:d for d in ds}
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

    #exts = sorted(set(''.join(p.suffixes).split('.fake.')[0] for p in project_path.rglob('*') if p.is_file() and p.suffixes))
    exts = sorted(set(NormFileSuffix(FThing(p).suffix.strip('.') if '.gz' not in p.suffixes else ''.join(FThing(p).suffixes).strip('.'))
                      for p in project_path.rglob('*') if p.is_file()))  # TODO gz
    n_exts = len(exts)

    fts = [FThing(p) for p in project_path.rglob('*') if p.is_file()]
    mimes = sorted(t for t in set(f.mimetype for f in fts if f.mimetype is not None))
    #all_type_info = sorted(set((f.suffix, f.mimetype, f._magic_mimetype)
                               #for f in fts), key = lambda v: [e if e else '' for e in v])
    # and this is why manual data entry without immediate feedback is a bad idea
    cr = get_diversity('contributor_role')
    fun = get_diversity('funding')
    orcid = get_diversity('contributor_orcid_id')

    report = sorted(d.report + ' lax ' + d.lax.report + ' ' + d.name for d in ds)

    embed()

def main():
    #normalize_tabular_format()
    # parse relevant csv files
    parse_meta()

if __name__ == '__main__':
    main()
