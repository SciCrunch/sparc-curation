#!/usr/bin/env python3
import io
import csv
import hashlib
from types import GeneratorType
from xlsx2csv import Xlsx2csv, SheetNotFoundException
from sparcur.blackfynn_api import local_storage_prefix, Path
from pyontutils.utils import makeSimpleLogger, byCol
from pyontutils.core import OntTerm
from protcur.analysis import parameter_expression
from IPython import embed

project_path = local_storage_prefix / 'SPARC Consortium'
logger = makeSimpleLogger('dsload')


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
                    logger.warn(f'\'{self.path}\' bad file encoding \'{encoding}\'')
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
    def __init__(self, tabular):
        self.t = tabular
        orig_header, *rest = list(tabular)
        #print(header)
        
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
                     .replace('/', '_')
                     .replace('?', '_')
                     .replace('#', 'number')
                     .replace('-', '_')
                     .lower()  # sigh
                )

            if not c:
                c = f'TEMP_{i}'

            if c in header:
                c = c + f'_TEMP_{i}'

            header.append(c)

        #print(header)
        self.bc = byCol(rest, header)

    def query(self, value, prefix):
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
    def __iter__(self):
        # TODO this needs to be exporting triples ...
        yield from ({k:v.strip('\ufeff') for k, v in zip(self.bc.metadata_element, getattr(self.bc, col)) if v}
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
   
    def age(self, value):
        _, v, rest = parameter_expression(value)
        if not v[0] == 'param:parse-failure':
            return v
        else:
            # TODO warn
            return value

    def rrid_for_strain(self, value):
        return value

    def protocol_io_location(self, value):
        return value

    def default(self, value):
        return value

    def process_dict(self, dict_):
        """ deal with multiple fields """
        out = {k:v for k, v in dict_.items() if k not in self.skip}
        for h_unit, h_value in zip(self.h_unit, self.h_value):
            compose = dict_[h_value] + dict_[h_unit]
            _, v, rest = parameter_expression(compose)
            out[h_value] = v

        return out

    def __iter__(self):
        yield from (self.process_dict({k:getattr(self, k, self.default)(v) for k, v in zip(r._fields, r) if v}) for r in self.bc.rows)


class FThing:
    """ a homogenous representation """

    def __init__(self, path, cypher=hashlib.sha256):
        if isinstance(path, str):
            path = Path(path)

        self.path = path
        self.fake = '.fake' in self.path.suffixes

        self.cypher = cypher

    def xattrs(self):
        return self.path.xattrs()

    @property
    def bids_root(self):
        """ Sometimes there is an intervening folder. """
        if self.is_dataset:
            ddpaths = list(self.path.rglob('dataset_description*.*'))  # FIXME possibly slow?
            if not ddpaths:
                #logger.warn(f'No bids root for {self.name} {self.id}')  # logging in a property -> logspam
                return
            elif len(ddpaths) > 1:
                #logger.warn(f'More than one submission for {self.name} {self.id} {ddpaths}')
                pass

            return ddpaths[0].parent  # FIXME choose shorter version? if there are multiple?

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
        mp = 'Contributor Role' 
        os = (   # oh boy
            'Principal Investigator',
            'PrincipalInvestigator',
            'Principle Investigator',
            'Principle Investigator, Contact Person',
            'Principle Investigator, Contact Person, Project Leader',
            'Principle Investigator, Contact person',
            'PrincipleInvestigator',
            'PrincipleInvestigator,ContactPerson',
        )
        p = 'Contributors'
        for dict_ in self.dataset_description:
            if mp in dict_ and dict_[mp] in os:
                yield dict_[p]  # TODO orcid etc

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
            t = list(Tabular(path))
            #print(path, t)
            yield {key:value for key, d, value, *fixme in t[1:]}  # FIXME multiple milestones apparently?

    @property
    def dataset_description_paths(self):
        yield from self._abstracted_paths('dataset_description')
        #yield from self.path.glob('dataset_description*.*')

    @property
    def dataset_description(self):
        for path in self.dataset_description_paths:
            yield from DatasetDescription(Tabular(path))
            # TODO export adapters for this ... how to recombine and reuse ...

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

    def get_diversity(field_name):
        return sorted(set(dict_[field_name]
                        for d in dump_all
                        for dict_ in d['dataset_description']
                        if field_name in dict_))

    deep = dsd['N:dataset:a7b035cf-e30e-48f6-b2ba-b5ee479d4de3']
    awards = sorted(set(n for d in dump_all for n in d['award']))
    n_awards = len(awards)

    # and this is why manual data entry without immediate feedback is a bad idea
    cr = get_diversity('Contributor Role')
    fun = get_diversity('Funding')
    orcid = get_diversity('Contributor ORCID ID')

    embed()

def main():
    #normalize_tabular_format()
    # parse relevant csv files
    parse_meta()

if __name__ == '__main__':
    main()
