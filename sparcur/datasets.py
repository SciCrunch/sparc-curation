import io
import csv
import copy
from xlsx2csv import Xlsx2csv, SheetNotFoundException
from pyontutils.utils import byCol
from pyontutils.namespaces import OntCuries, makeNamespaces, TEMP, isAbout
from pyontutils.closed_namespaces import rdf, rdfs, owl, skos, dc
from pysercomb.pyr.units import ProtcParameterParser
from sparcur import schemas as sc
from sparcur import validate as vldt
from sparcur import exceptions as exc
from sparcur import converters as conv
from sparcur import normalization as nml
from sparcur.core import log, logd, OntTerm, OntId, OrcidId, sparc

a = rdf.type


class Tabular:
    def __init__(self, path):
        self._errors = []
        self.path = path

    @property
    def file_extension(self):
        if self.path.suffixes:
            ext = self.path.suffixes[0]  # FIXME filenames with dots in them ...
            if ext != '.fake':
                return nml.NormFileSuffix(ext).strip('.')

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
                    self._errors.append(exc.EncodingError(message))
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

        error = exc.EncodingError(f"encoding feff error in '{self.path}'")
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
        ft = DatasetData(self.path)
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
    schema_class = sc.JSONSchema

    class NoDataError(Exception):
        """ FIXME HACK workaround for bad handling of empty sheets in byCol """

    def __new__(cls, path):
        cls.schema = cls.schema_class()
        return super().__new__(cls)

    def __init__(self, path):
        tabular = Tabular(path)
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
            self._errors.append(exc.EncodingError(message))

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
    def data(self):  # TODO candidate for memory.cache
        if hasattr(self, '_data_cache'):
            return self._data_cache

        index_col, *_ = self.to_index
        out = {}
        if not hasattr(self.bc, index_col):
            log.error(f'\'{self.t.path}\' malformed header!')
            self._data_cache = out
            return out

        ic = list(getattr(self.bc, index_col))
        nme = vldt.Header(ic).output
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
        # FIXME TODO regularize this with the DatasetData version
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


hasSchema = vldt.HasSchema()
@hasSchema.mark
class SubmissionFile(Version1Header):
    to_index = 'submission_item',  # FIXME normalized in version 2
    skip_cols = 'submission_item', 'definition'  # FIXME normalized in version 2

    verticals = {'submission':('sparc_award_number', 'milestone_achieved', 'milestone_completion_date')}
    schema_class = sc.SubmissionSchema

    @hasSchema(sc.SubmissionSchema)
    def data_test(self):
        return self.data

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


hasSchema = vldt.HasSchema()
@hasSchema.mark
class DatasetDescriptionFile(Version1Header):
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
    schema_class = sc.DatasetDescriptionSchema

    @hasSchema(sc.DatasetDescriptionSchema)
    def data_test(self):
        return super().data

    @property
    def data(self):
        return super().data

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
                logd.error(f"orcid wrong length '{value}' '{self.t.path}'")
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
                logd.error(f"orcid wrong length '{value}' '{self.t.path}'")
                return

        try:
            #log.debug(f"{v} '{self.t.path}'")
            orcid = OrcidId(v)
            if not orcid.checksumValid:
                # FIXME json schema can't do this ...
                logd.error(f"orcid failed checksum '{value}' '{self.t.path}'")
                return

            yield orcid

        except (OntId.BadCurieError, OrcidId.MalformedOrcidError) as e:
            logd.error(f"orcid malformed '{value}' '{self.t.path}'")
            yield value

    def contributor_role(self, value):
        # FIXME normalizing here momentarily to squash annoying errors
        yield tuple(sorted(set(nml.NormContributorRole(e.strip()) for e in value.split(','))))

    def is_contact_person(self, value):
        yield value.lower() == 'yes'

    def keywords(self, value):
        if ';' in value:
            # FIXME error for this
            values = [v.strip() for v in value.split(';') if v]
        elif ',' in value:
            # FIXME error for this
            values = [v.strip() for v in value.split(',') if v]
        else:
            values = value,

        #log.debug(f'{values}')
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


hasSchema = vldt.HasSchema()
@hasSchema.mark
class SubjectsFile(Version1Header):
    to_index = 'subject_id',  # the zeroth is what is used for unique rows by default  # FIXME doesn't work
    # subject id varies, so we have to do something a bit different here
    skip_cols = tuple()
    horizontals = {'software':('software', 'software_version', 'software_vendor', 'software_url', 'software_rrid')}
    schema_class = sc.SubjectsSchema

    @hasSchema(sc.SubjectsSchema)
    def data_test(self):
        return self.data

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

    def __init__(self, path):
        super().__init__(path)

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
        nv = nml.NormSpecies(value)
        yield self.query(nv, 'NCBITaxon')

    def sex(self, value):
        nv = nml.NormSex(value)
        yield self.query(nv, 'PATO')

    def gender(self, value):
        # FIXME gender -> sex for animals, requires two pass normalization ...
        yield from self.sex(value)

    def _param(self, value):
        pv = ProtcParameterParser(value)
        if not pv._tuple[0] == 'param:parse-failure':
            yield str(pv.for_text)  # this one needs to be a string since it is combined below
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
            #out[h_value] = str(ProtcParameterParser(compose).for_text)  # FIXME sparc repr
            out[h_value] = ProtcParameterParser(compose)

        if 'gender' in out and 'species' in out:
            if out['species'] != OntTerm('NCBITaxon:9606'):
                out['sex'] = out.pop('gender')

        return out

    def __iter__(self):
        """ this is still used """
        yield from (self.process_dict({k:nv for k, v in zip(r._fields, r) if v
                                       and k not in self.skip_cols
                                       for nv in self.normalize(k, v) if nv})
                    for r in self.bc.rows)

    def triples_gen(self, prefix_func):
        """ NOTE the subject is LOCAL """
        for i, subject in enumerate(self):
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
