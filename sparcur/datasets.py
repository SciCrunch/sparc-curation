import io
import ast
import csv
import copy
import json
import numbers
from types import GeneratorType
from itertools import chain
from collections import Counter
from xlsx2csv import Xlsx2csv, SheetNotFoundException
from terminaltables import AsciiTable
from pyontutils.utils import byCol, Async, deferred, python_identifier
from pyontutils.namespaces import OntCuries, makeNamespaces, TEMP, isAbout
from pyontutils.namespaces import rdf, rdfs, owl, skos, dc
from pysercomb.pyr import units as pyru
from augpathlib import FileSize
from sparcur import schemas as sc
from sparcur import exceptions as exc
from sparcur import normalization as nml
from sparcur.core import log, logd, OntTerm, OntId, OrcidId, DoiId, PioId
from sparcur.paths import Path

a = rdf.type


# FIXME review this :/ it is not a good implementation at all
def tos(hrm):
    if isinstance(hrm, GeneratorType):
        t = tuple(hrm)
        if not t:
            return
        elif len(t) == 1:
            return t[0]
        else:
            return t
    else:
        return hrm


# FIXME this whole file needs to be properly converted into pipelines

hasSchema = sc.HasSchema()
@hasSchema.mark
class Header:
    """ generic header normalization for python """
    # FIXME this is a really a pipeline stage
    def __init__(self, first_row_or_column):
        self.pipeline_start = first_row_or_column

    @property
    def normalized(self):
        orig_header = self.pipeline_start
        header = []
        for i, c in enumerate(orig_header):
            if c:
                c = python_identifier(c)
                c = nml.NormHeader(c)
            if not c:
                c = f'TEMP_{i}'

            if c in header:
                c = c + f'_TEMP_{i}'

            header.append(c)

        return header

    @hasSchema(sc.HeaderSchema)
    def data(self):
        return self.normalized

    @property
    def lut(self):
        return {n:h for n, h in zip(self.data, self.pipeline_start)}


class HasErrors:
    def __init__(self, *args, pipeline_stage=None, **kwargs):
        try:
            super().__init__(*args, **kwargs)
        except TypeError as e:  # this is so dumb
            super().__init__()

        self._pipeline_stage = pipeline_stage
        self._errors_set = set()

    def addError(self, error, pipeline_stage=None, logfunc=None):
        stage = (pipeline_stage if pipeline_stage is not None
                 else (self._pipeline_stage if self._pipeline_stage
                       else self.__class__.__name__))
        b = len(self._errors_set)
        self._errors_set.add((error, stage))
        a = len(self._errors_set)
        if logfunc is not None and a != b:  # only log on new errors
            logfunc(error)

    @property
    def _errors(self):
        for e, stage in self._errors_set:
            o = {'pipeline_stage': stage}  # FIXME

            if isinstance(e, str):
                o['message'] = e

            elif isinstance(e, BaseException):
                o['message'] = str(e)
                o['type'] = str(type(e))

            else:
                raise TypeError(repr(e))

            log.debug(o)
            yield o

    def embedErrors(self, data):
        el = list(self._errors)
        if el:
            if 'errors' in data:
                data['errors'].extend(el)
            elif el:
                data['errors'] = el


class DatasetMetadata(Path, HasErrors):

    @property
    def data(self):
        if self.cache is not None:
            return dict(id=self.cache.id,
                        meta=dict(folder_name=self.name,
                                  uri_human=self.cache.uri_human,
                                  uri_api=self.cache.uri_human,
                        ))
        else:
            return dict(id=self.id,
                        meta=dict(folder_name=self.name,
                                  uri_human=None,
                                  uri_api=None,
                        ))


DatasetMetadata._bind_flavours()


class DatasetStructure(Path, HasErrors):
    sections = None  # NOTE assigned in at end of file
    rglobs = 'manifest',
    default_glob = 'glob'
    max_childs = 40
    rate = 8  # set by Integrator from cli
    _refresh_on_missing = True

    @classmethod
    def _bind_sections(cls):
        for section, _sec_class in cls.sections.items():
            @property
            def sec(self, sec_class=_sec_class, name='_cache_' + _sec_class.__name__):
                if not hasattr(self, name):
                    mp = [p for p in self.meta_paths if isinstance(p, sec_class)]
                    if len(mp) == 1:
                        setattr(self, name, mp[0])
                    elif not mp:
                        setattr(self, name, None)
                    else:
                        setattr(self, name, mp)

                return getattr(self, name)

            setattr(cls, section, sec)

    @property
    def counts(self):
        if not hasattr(self, '_counts'):
            size = 0
            dirs = 0
            files = 0
            need_meta = []
            if not self.is_dir():
                gen = self,

            else:
                gen = self.rchildren

            for c in gen:
                if c.is_dir():
                    dirs += 1
                else:
                    files += 1  # testing for broken symlinks is hard
                    try:
                        maybe_size = c.cache.meta.size
                    except AttributeError as e:
                        log.error(f'no cache or no meta for {c}\n{e}')
                        continue

                    if maybe_size is None:
                        need_meta.append(c)
                    else:
                        size += maybe_size

            if need_meta and self._refresh_on_missing:
                nl = '\n'
                log.info(f'refreshing {len(need_meta)} files with missing metadata in {self}'
                         f'\n{nl.join(_.as_posix() for _ in need_meta)}')
                new_caches = Async(rate=self.rate)(deferred(c.cache.refresh)() for c in need_meta)
                for c in new_caches:  # FIXME first time around meta doesn't get updated ??
                    if c is None:
                        continue  # file was deleted (logged previously)

                    if c.meta is None:
                        log.critical(f'missing metdata! {c}')
                        continue

                    size += c.meta.size

            self._counts = dict(size=FileSize(size), dirs=dirs, files=files)

        return self._counts

    @property
    def total_size(self):
        """ total remote size, trigger retrieval of all remote metadata """
        return self.counts['size']

    @property
    def total_dirs(self):
        return self.counts['dirs']

    @property
    def total_files(self):
        return self.counts['files']

    @property
    def total_paths(self):
        return self.counts['dirs'] + self.counts['files']

    @property
    def bids_root(self):
        # FIXME this will find the first dataset description file at any depth!
        # this is incorrect behavior!
        """ Sometimes there is an intervening folder. """
        if self.cache.is_dataset:
            def check_fordd(paths, level=0, stop=3):
                if not paths:  # apparently the empty case recurses forever
                    return

                if len(paths) > self.max_childs:
                    log.warning(f'Not globing in a folder with > {self.max_childs} children! '
                                f'{self.as_posix()!r}')
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

            dd_paths = check_fordd((self,))

            if not dd_paths:
                #log.warning(f'No bids root for {self.name} {self.id}')  # logging in a property -> logspam
                return

            elif len(dd_paths) > 1:
                #log.warning(f'More than one submission for {self.name} {self.id} {dd_paths}')
                pass

            return dd_paths[0].parent  # FIXME choose shorter version? if there are multiple?

        elif self.parent:  # organization has no parent
            return self.parent.bids_root

    def _abstracted_paths(self, name_prefix, glob_type=None):
        """ A bottom up search for the closest file in the parent directory.
            For datasets, if the bids root and path do not match, use the bids root.
            In the future this needs to be normalized because the extra code required
            for dealing with the intervening node is quite annoying to maintain.
        """
        if glob_type is None:
            glob_type = self.default_glob

        path = self
        if (self.cache and
            self.cache.is_dataset and
            self.bids_root is not None and
            self.bids_root != self):
            path = self.bids_root

        first = name_prefix[0]
        cased_np = '[' + first.upper() + first + ']' + name_prefix[1:]  # FIXME warn and normalize
        glob = getattr(path, glob_type)
        gen = glob(cased_np + '*.*')

        try:
            path_class = self.sections[name_prefix]
            path = next(gen)
            for path in chain((path,), gen):
                path = path_class(path)
                if path.is_broken_symlink():
                    log.info(f'fetching unretrieved metadata path {path.as_posix()!r}'
                             '\nFIXME batch these using async in cli export ...')
                    path.cache.fetch(size_limit_mb=path.cache.meta.size.mb + 1)

                if path.suffix in path.stem:
                    msg = f'path has duplicate suffix {path.as_posix()!r}'
                    self.addError(msg)
                    logd.error(msg)

                if path.name[0].isupper():
                    msg = f'path has bad casing {path.as_posix()!r}'
                    self.addError(msg)
                    logd.error(msg)

                yield path

        except StopIteration:
            if (self.cache.parent.meta is not None and
                self.parent.cache != self.cache.anchor and
                self.parent != self):
                yield from getattr(self.parent, name_prefix + '_paths')

    @property
    def meta_paths(self):
        for section_name in self.sections:
            glob_type = 'rglob' if section_name in self.rglobs else None
            yield from self._abstracted_paths(section_name, glob_type=glob_type)

    @property
    def data(self):
        out = self.counts
        for section_name in self.sections:
            #section_paths_name = section_name + '_paths'
            #paths = list(getattr(self, section_paths_name))
            glob_type = 'rglob' if section_name in self.rglobs else 'glob'
            paths = list(self._abstracted_paths(section_name, glob_type=glob_type))
            section_key = section_name + '_file'
            if paths:
                if len(paths) == 1:
                    path, = paths
                    out[section_key] = path
                else:
                    out[section_key] = paths

        self.embedErrors(out)
        return out


DatasetStructure._bind_flavours()


class DatasetStructureLax(DatasetStructure):
    default_glob = 'rglob'


DatasetStructureLax._bind_flavours()


class ObjectPath(Path):
    obj = None
    @property
    def object(self):
        return self.obj(self)


ObjectPath._bind_flavours()


class Tabular(HasErrors):

    def __init__(self, path):
        super().__init__()
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
                    for row in csv.reader(f, delimiter=delimiter):
                        if row:
                            yield row
                        else:
                            message = f'empty row in {self.path.as_posix()!r}'
                            self.addError(message)
                            logd.error(message)

                if encoding != 'utf-8':
                    message = f'encoding bad {encoding!r} {self.path.as_posix()!r}'
                    self.addError(exc.EncodingError(message))
                    logd.error(message)
                return
            except UnicodeDecodeError:
                continue
            except csv.Error as e:
                logd.exception(e)
                message = f'WHAT HAVE YOU DONE {e!r} {self.path.as_posix()!r}'
                self.addError(message)
                logd.error(message)

    def xlsx(self):
        kwargs = {
            'delimiter' : '\t',
            'skip_empty_lines' : True,
            'outputencoding': 'utf-8',
            'hyperlinks': True,
        }
        sheetid = 1
        xlsx2csv = Xlsx2csv(self.path.as_posix(), **kwargs)
        ns = len(xlsx2csv.workbook.sheets)
        if ns > 1:
            message = f'too many sheets ({ns}) in {self.path.as_posix()!r}'
            self.addError(exc.EncodingError(message))
            logd.error(message)

        f = io.StringIO()
        try:
            xlsx2csv.convert(f, sheetid)
            f.seek(0)
            gen = csv.reader(f, delimiter='\t')
            yield from gen
        except SheetNotFoundException as e:
            log.warning(f'Sheet weirdness in{self.path}')
            log.warning(str(e))

    def _bad_filetype(self, type_):
        message = f'bad filetype {type_}\n{self.path.as_posix()!r}'
        raise exc.FileTypeError(message)

    def xls(self):
        return self._bad_filetype('xls')

    def normalize(self, rows):
        # FIXME need to detect changes
        # this removes any columns that are all dead

        #if any(not(c) for c in rows[0]):  # doesn't work because generators

        error = exc.EncodingError(f"encoding feff error in '{self.path}'")
        cleaned_rows = zip(*(t for t in zip(*rows) if not all(not(e) for e in t)))  # TODO check perf here
        for row in cleaned_rows:
            n_row = [c.strip().replace('\ufeff', '') for c in row
                     if (not self.addError(error, logfunc=logd.error)  # FIXME will probably append multiple ...
                         if '\ufeff' in c else True)]
            if not all(not(c) for c in n_row):  # skip totally empty rows
                yield n_row

    @property
    def normalized(self):
        try:
            yield from self.normalize(getattr(self, self.file_extension)())
        except UnicodeDecodeError as e:
            log.error(f'{self.path.as_posix()!r} {e}')

    @property
    def T(self):
        yield from zip(*self)

    def __iter__(self):
        try:
            yield from getattr(self, self.file_extension)()
        except UnicodeDecodeError as e:
            log.error(f'{self.path.as_posix()!r} {e}')

    #@property
    #def title(self):
        #path = Path(self.path)
        #return f'{path.name:<39} ' + path.cache.dataset.id + ' ' + path.cache.dataset.name

    @property
    def title(self):
        path = Path(self.path)
        out = path.name
        if path.cache is not None and path.cache.dataset is not None:
            out += ' ' + path.cache.dataset.name[:30] + ' ...'

        return out

    def __repr__(self):
        limit = 30
        rows = [[c[:limit] + ' ...' if isinstance(c, str)
                 and len(c) > limit else c
                 for c in r] for r in self]
        table = AsciiTable(rows, title=self.title)
        return table.table


class Version1Header(HasErrors):
    to_index = tuple()  # first element indexes row based data
    skip_cols = 'metadata_element', 'description', 'example'
    max_one = tuple()
    verticals = dict()  # FIXME should really be immutable
    #schema_class = sc.JSONSchema

    def __new__(cls, path):
        #cls.schema = cls.schema_class()
        return super().__new__(cls)

    def __init__(self, path):
        super().__init__()
        self.path = path
        if self._is_json:
            with open(self.path, 'rt') as f:
                try:
                    self._data_raw = json.load(f)
                except json.decoder.JSONDecodeError as e:
                    if not f.buffer.tell():
                        raise exc.NoDataError(self.path)
                    else:
                        raise exc.BadDataError(self.path) from e

            if isinstance(self._data_raw, dict):
                # FIXME this breaks downstream assumptions
                self._data_cache = {self.rename_key(k):tos(self.normalize(k, v))  # FIXME FIXME
                                    for k, v in self._data_raw.items()}

            return

        tabular = Tabular(self.path)
        self.skip_rows = tuple(key for keys in self.verticals.values() for key in keys)
        self.t = tabular
        l = list(tabular)
        if not l:
            # FIXME bad design, this try block is a workaround for bad handling of empty lists
            raise exc.NoDataError(self.path)

        self.orig_header, *rest = l
        header = Header(self.orig_header).data

        self.fail = False
        if self.to_index:
            for head in self.to_index:
                if head not in header:
                    log.error(f'\'{self.t.path}\' malformed header!')
                    self.fail = True

        if self.fail:
            try:
                self.bc = byCol(rest, header)
            except ValueError as e:
                raise exc.BadDataError(self.path) from e
        else:
            self.bc = byCol(rest, header, to_index=self.to_index)

    @property
    def _is_json(self):
        # impl detail, sigh
        # FIXME this should not be done here
        # json should conform directly to the schemas and should be loaded
        # directly via the object loader
        return self.path.suffix == '.json'

    def xopen(self):
        """ open file using xdg-open """
        self.path.xopen()

    @property
    def _errors(self):
        if hasattr(self, 't'):
            yield from self.t._errors

        yield from super()._errors

    @staticmethod
    def query(value, prefix):
        for query_type in ('term', 'search'):
            terms = list(OntTerm.query(prefix=prefix, **{query_type:value}))
            if terms:
                #print('matching', terms[0], value)
                #print('extra terms for', value, terms[1:])
                return terms[0]
            else:
                continue

        else:
            log.warning(f'No ontology id found for {value}')
            return value

    def default(self, value):
        yield value

    def normalize(self, key, value):
        if isinstance(value, str):
            v = value.replace('\ufeff', '')  # FIXME utf-16 issue
            if v != value:  # TODO can we decouple encoding from value normalization?
                message = f"encoding feff error in '{self.path}'"
                log.error(message)
                self.addError(exc.EncodingError(message))

            if v.lower().strip() in ('n/a', 'na', 'no'):  # FIXME explicit null vs remove from structure
                return

        else:
            v = value

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

        if self._is_json:
            self._data_cache = {k:v for k, v in self}
            return self.data

        index_col, *_ = self.to_index
        out = {}
        if not hasattr(self.bc, index_col):
            msg = f'{self.path.as_posix()!r} maformed header!'
            self.addError(msg)
            logd.error(msg)
            self.embedErrors(out)
            self._data_cache = out
            return out

        ic = list(getattr(self.bc, index_col))
        nme = Header(ic).data
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
                        msg = f'duplicate values in {normk} TODO {self.path.as_posix()!r}'
                        self.addError(msg)
                        logd.error(msg)

                    if normk in self.max_one:  # schema will handle this ..
                        if not value:
                            #log.warning(f"missing value for {normk} '{self.path}'")
                            pass
                        elif len(value) > 1:
                            msg = f'too many values for {normk} {value} {self.path.as_posix()!r}'
                            self.addError(msg)
                            logd.error(msg)
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

        self.embedErrors(out)
        self._data_cache = out
        return out


class SubmissionFile(Version1Header):
    to_index = 'submission_item',  # FIXME normalized in version 2
    skip_cols = 'submission_item', 'definition'  # FIXME normalized in version 2

    verticals = {'submission':('sparc_award_number', 'milestone_achieved', 'milestone_completion_date')}

    @property
    def data(self):
        """ lift list with single element to object """

        d = copy.deepcopy(super().data)
        if d and 'submission' in d:
            if d['submission']:
                d['submission'] = d['submission'][0]
            else:
                d['submission'] = {}

        return d


class SubmissionFilePath(ObjectPath):
    obj = SubmissionFile


SubmissionFilePath._bind_flavours()

ROW_TYPE = object()
COLUMN_TYPE = object()
N = object()


class Better(HasErrors):
    default_record_type = ROW_TYPE
    alt_groups = {}
    record_type_key = None
    missing_add = {}
    header_ignore = tuple()

    def __new__(cls, path):
        return super().__new__(cls)

    def __init__(self, path):
        super().__init__()
        self.path = path

    def xopen(self):
        self.path.xopen()

    def _data(self):
        if self.path.suffix == 'json':
            return RawJson(self.path).data

        else:
            return self._cull_rtk()

    def _cull_rtk(self):
        normalized = self._normalize_values()
        def crtk(thing):
            if isinstance(thing, dict):
                out = {}
                for k, v in thing.items():
                    if k != self.record_type_key:
                        nv = crtk(v)
                        if nv is not None:
                            out[k] = nv

                if out:
                    return out

            else:
                return thing

        crv = crtk(normalized)
        return crv

    def _normalize_values(self):
        nk = self._normalize_keys()
        def normv(thing, key=None, path=tuple()):
            print(path)
            if isinstance(thing, dict):
                out = {}
                for k, v in thing.items():
                    if k == self.record_type_key:
                        out[k] = v
                    elif k in self.norm_to_orig_header:
                        out[k] = normv(v, key, path)
                    elif k in self.norm_to_orig_alt:
                        out[k] = normv(v, k, path + (k,))
                    elif k in self.alt_groups:
                        out[k] = normv(v, k, path + (k,))
                    else:
                        raise ValueError(f'what is going on here?! {k} {v}')

                return out

            else:
                # TODO make use of path
                # FIXME I do NOT like this pattern :/
                if key is not None and hasattr(self, key):
                    return getattr(self, key)(thing)

                return thing

        nv = normv(nk)
        return nv

    def _normalize_keys(self):
        cleaned = self._clean()

        def normb(k):
            if k in self.orig_to_norm_header:
                nk = self.orig_to_norm_header[k]
            elif k in self.orig_to_norm_alt:
                nk = self.orig_to_norm_alt[k]
            else:
                nk = k

            return nk

        def normk(thing, is_rtk=False):
            if isinstance(thing, dict):
                out = {}
                for k, v in thing.items():
                    nk = normb(k)
                    out[nk] = normk(v, nk == self.record_type_key or is_rtk)

                return out
            elif is_rtk:
                return normb(thing)  # FIXME should this be done in normv or no?
            else:
                return thing

        return normk(cleaned)

    def _clean(self):
        culled, nrtk = self._cull()
        def clean(thing):
            if isinstance(thing, dict):
                out = {}
                for k, v in thing.items():
                    cv = clean(v)
                    if cv is not None:
                        out[k] = cv

                if out:
                    return out

            elif thing == '' or thing == {}:
                return None

            else:
                return thing

        cleaned = clean(culled)
        return cleaned

    def _cull(self):
        transformed, nrtk, remove = self._reshape()
        # FIXME deeper nesting might break this?
        culled = {k:{k:v for k, v in v.items()
                     if k not in remove}
                  for k, v in transformed.items()}
        return culled, nrtk

    def _reshape(self):
        t = Tabular(self.path)
        print(t)

        if self.default_record_type == ROW_TYPE:
            agen = iter(t)
            gen = iter(t.T)
        else:
            agen = iter(t.T)
            gen = iter(t)

        alt_header = next(agen)
        self.norm_to_orig_alt = Header(alt_header).lut
        self.orig_to_norm_alt = {v:k for k, v in self.norm_to_orig_alt.items()}
        header = next(gen)
        self.norm_to_orig_header = Header(header).lut
        self.orig_to_norm_header = {v:k for k, v in self.norm_to_orig_header.items()}
        nrtk = self.norm_to_orig_header[self.record_type_key]
        remove = [self.norm_to_orig_header[hn] for hn in self.header_ignore
                  if hn in self.norm_to_orig_alt]
        _objects = [{k:v for k, v in zip(header, r)} for r in gen]

        # add missing items, mostly needed for missing verions required for later steps

        for nah, (value, number) in self.missing_add.items():
            if nah not in self.norm_to_orig_alt:
                self.norm_to_orig_alt[nah] = nah
                cant_go_wrong = {}
                i = 0
                for h in header:
                    if (number == N or i < number) and h != nrtk and h not in remove:
                        v = value
                        i += 1
                    else:
                        v = ''

                    cant_go_wrong[h] = v

                {h:(value if number == N or i <= number else '')
                                 for i, h in enumerate(header) if h != nrtk}
                print(cant_go_wrong)
                cant_go_wrong[nrtk] = nah
                _objects.append(cant_go_wrong)

        breakpoint()
        # end add missing items

        keyed = {o[nrtk]:o for o in _objects}

        transformed = {}
        for key, alt_grouped_norm in self.alt_groups.items():
            grouped = [self.norm_to_orig_alt[ahn] for ahn in alt_grouped_norm]
            records = [keyed[h] for h in grouped]
            #print(alt_grouped_norm, self.norm_to_orig_alt, grouped, records)
            objected = {h:{record[nrtk]:record[h] for record in records} for h in header}
            transformed[key] = objected

        nunaccounted = (set(self.norm_to_orig_alt) -
                        (set(e for g in self.alt_groups.values()
                             for e in g) | {self.record_type_key}))
        unaccounted = [self.norm_to_orig_alt[nah] for nah in nunaccounted]

        for ah in unaccounted: 
            transformed[ah] = keyed[ah]

        return transformed, nrtk, remove

    @property
    def data(self):
        return self._data()


class DatasetDescriptionFile(Better):
    default_record_type = COLUMN_TYPE
    alt_groups = {'contributors': ('contributors',
                                   'contributor_orcid_id',
                                   'contributor_affiliation',
                                   'contributor_role',
                                   'is_contact_person',),
                  'links': ('additional_links', 'link_description'),
                  'examples': ('example_image_filename',
                               'example_image_locator',
                               'example_image_description'),}

    record_type_key = 'metadata_element'
    missing_add = {'metadata_version_do_not_change': ('1.0', 1)}
    header_ignore = 'example', 'description'

    def metadata_version_do_not_change(self, value):
        return 'lolololol'


class DatasetDescriptionFilePath(ObjectPath):
    obj = DatasetDescriptionFile


DatasetDescriptionFilePath._bind_flavours()


class _DatasetDescriptionFile(Version1Header):
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
        'title_for_complete_data_set',
        'version'
    )
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

    @property
    def data(self):
        d = copy.deepcopy(super().data)
        return d

    def contributors(self, value):
        if isinstance(value, list):
            for d in value:
                yield {self.rename_key(k):tos(nv)
                    for k, v in d.items()
                    for nv in self.normalize(k, v) if nv}
        else:
            return value

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
                msg = f'orcid wrong length {value!r} {self.path.as_posix()!r}'
                self.addError(OrcidId.OrcidLengthError(msg))
                logd.error(msg)
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
                msg = f'orcid wrong length {value!r} {self.path.as_posix()!r}'
                self.addError(OrcidId.OrcidLengthError(msg))
                logd.error(msg)
                return

        try:
            #log.debug(f"{v} '{self.path}'")
            orcid = OrcidId(v)
            if not orcid.checksumValid:
                # FIXME json schema can't do this ...
                msg = f'orcid failed checksum {value!r} {self.path.as_posix()!r}'
                self.addError(OrcidId.OrcidChecksumError(msg))
                logd.error(msg)
                return

            yield orcid

        except (OntId.BadCurieError, OrcidId.OrcidMalformedError) as e:
            msg = f'orcid malformed {value!r} {self.path.as_posix()!r}'
            self.addError(OrcidId.OrcidMalformedError(msg))
            logd.error(msg)
            yield value

    def contributor_role(self, value):
        # FIXME normalizing here momentarily to squash annoying errors
        if isinstance(value, list):
            yield tuple(sorted(set(nml.NormContributorRole(e.strip()) for e in value)))
        else:
            yield tuple(sorted(set(nml.NormContributorRole(e.strip()) for e in value.split(','))))

    def is_contact_person(self, value):
        # no truthy values only True itself
        yield value is True or isinstance(value, str) and value.lower() == 'yes'

    def _protocol_url_or_doi(self, value):
        doi = False
        if 'doi' in value:
            doi = True
        elif value.startswith('10.'):
            value = 'doi:' + value
            doi = True

        if doi:
            value = DoiId(value)
        else:
            value = PioId(value).normalize()

        return value

    def protocol_url_or_doi(self, value):
        for val in value.split(','):
            v = val.strip()
            if v:
                try:
                    yield  self._protocol_url_or_doi(v)
                except BaseException as e:
                    self.addError(e,
                                  pipeline_stage=f'{self.__class__.__name__}.protocol_url_or_doi',
                                  logfunc=logd.error)
                    self.addError(self.path.as_posix(),
                                  pipeline_stage=f'{self.__class__.__name__}.protocol_url_or_doi',
                                  logfunc=logd.critical)
                    # TODO raise exc.BadDataError from e

    def originating_article_doi(self, value):
        for val in value.split(','):
            v = val.strip()
            if v:
                doi = DoiId(v)
                if doi.valid:
                    # TODO make sure they resolve as well
                    # probably worth implementing this as part of OntId
                    yield doi

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

    #def metadata_version_do_not_change(self, value):
    #def version(self, value):

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
    horizontals = {'software':('software',
                               'software_version',
                               'software_vendor',
                               'software_url',
                               'software_rrid')}

    dict_key = 'subjects'

    @property
    def _data(self):
        return super().data

    @property
    def data(self):
        return self._data()

    def _data(self):
        # sigh
        #notunique = (len([r for r in self.bc.subject_id if r]) !=
                     #len([k for k in self.bc._byCol__indexes['subject_id']
                          #if k != 'subject_id' and k]))
        if self._is_json:
            # FIXME ... only if it is a json list ???
            sids = Counter(r['subject_id'] for r in self._data_raw if r)
            values = self._data_raw
        elif not hasattr(self.bc, 'subject_id'):
            sids = {}
            values = list(self)
            msg = f'subject_id column missing! for {self.path}'
            logd.critical(msg)
            self.addError(msg)
        else:
            sids = Counter(r for r in self.bc.subject_id if r)
            values = list(self)

        notunique = {s:c for s, c in sids.items() if c > 1}

        if notunique:
            msg = f'subject_ids are not unique for {self.path}\n{notunique}'
            logd.critical(msg)
            self.addError(msg)

        out = {self.dict_key: values}
        if not self._is_json:
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

        self.embedErrors(out)
        return out

    def __init__(self, path):
        super().__init__(path)
        if self._is_json:
            header = [d.keys() for d in self._data_raw]  # FIXME max may have wrong top level schema
        else:
            header = self.bc.header

        # units merging
        # TODO pull the units in the parens out
        self.h_unit = [k for k in header if '_unit' in k]
        h_value = [k.replace('_units', '').replace('_unit', '') for k in self.h_unit]
        no_unit = [k for k in header if '_unit' not in k]
        #self.h_value = [k for k in self.bc.header if '_units' not in k and any(k.startswith(hv) for hv in h_value)]
        self.h_value = [k for hv in h_value
                        for k in no_unit
                        if k.startswith(hv) or k.endswith(hv)]
        err = f'Problem! {self.h_unit} {self.h_value} {header} \'{self.path}\''
        #assert all(v in header for v in self.h_value), err
        assert len(self.h_unit) == len(self.h_value), err
        self.skip = self.h_unit + self.h_value

        self.skip_cols += tuple(set(_ for v in self.horizontals.values() for _ in v))

    def species(self, value):
        nv = nml.NormSpecies(value)
        yield self.query(nv, 'NCBITaxon')

    def strain(self, value):
        if value == 'DSH':
            value = 'domestic shorthair'
            return value

        wat = self.query(value, 'BIRNLEX')  # FIXME
        yield wat

    def sex(self, value):
        nv = nml.NormSex(value)
        yield self.query(nv, 'PATO')

    def gender(self, value):
        # FIXME gender -> sex for animals, requires two pass normalization ...
        yield from self.sex(value)

    def _param(self, value):
        if isinstance(value, numbers.Number):
            return pyru.ur.Quantity(value)

        try:
            pv = pyru.UnitsParser(value).asPython()
        except pyru.UnitsParser.ParseFailure as e:
            caller_name = e.__traceback__.tb_frame.f_back.f_code.co_name
            msg = f'Unexpected and unhandled value {value} for {caller_name}'
            log.error(msg)
            self.addError(msg, pipeline_stage=self.__class__.__name__ + '.curation-error')
            return value

        #if not pv[0] == 'param:parse-failure':
        if pv is not None:  # parser failure  # FIXME check on this ...
            yield pv  # this one needs to be a string since it is combined below
        else:
            # TODO warn
            yield value

    def _param_unit(self, value, unit):
        yield from self._param(value + unit)

    def age(self, value):
        yield from self._param(value)

    def age_years(self, value):
        # FIXME the proper way to do this is to detect
        # the units and lower them to the data, and leave the aspect
        yield from self._param_unit(value, 'years')

    def age_category(self, value):
        yield self.query(value, 'UBERON')

    def age_range_min(self, value):
        yield from self._param(value)

    def age_range_max(self, value):
        yield from self._param(value)

    def mass(self, value):
        yield from self._param(value)

    body_mass = mass

    def weight(self, value):
        yield from self._param(value)

    def weight_kg(self, value):  # TODO populate this?
        yield from self._param_unit(value, 'kg')

    def height_inches(self, value):
        yield from self._param_unit(value, 'in')

    def rrid_for_strain(self, value):
        yield value

    #def protocol_io_location(self, value):  # FIXME need to normalize this with dataset_description
        #yield value

    def process_dict(self, dict_):
        """ deal with multiple fields """
        out = {k:v for k, v in dict_.items() if k not in self.skip}
        for h_unit, h_value in zip(self.h_unit, self.h_value):
            if h_value not in dict_:  # we drop null cells so if one of these was null then we have to skip it here too
                continue

            dhv = dict_[h_value]
            if isinstance(dhv, str):
                try:
                    dhv = ast.literal_eval(dhv)
                except ValueError as e:
                    raise exc.UnhandledTypeError(f'{h_value} {dhv!r} was not parsed!') from e

            compose = dhv * pyru.ur.parse_units(dict_[h_unit])
            #_, v, rest = parameter_expression(compose)
            #out[h_value] = str(UnitsParser(compose).for_text)  # FIXME sparc repr
            #breakpoint()
            out[h_value] = compose #UnitsParser(compose).asPython()

        if 'gender' in out and 'species' in out:
            if out['species'] != OntTerm('NCBITaxon:9606'):
                out['sex'] = out.pop('gender')

        return out

    def __iter__(self):
        """ this is still used """
        if self._is_json:
            yield from (self.process_dict({k:nv for k, v in d.items()
                                           for nv in self.normalize(k, v) if nv})
                        for d in self._data_raw)

        else:
            yield from (self.process_dict({k:nv for k, v in zip(r._fields, r) if v
                                           and k not in self.skip_cols
                                           for nv in self.normalize(k, v) if nv})
                        for r in self.bc.rows)

    def triples_gen(self, prefix_func):
        """ NOTE the subject is LOCAL """


class SubjectsFilePath(ObjectPath):
    obj = SubjectsFile


SubjectsFilePath._bind_flavours()


class SamplesFile(SubjectsFile):
    """ TODO ... """
    to_index = 'sample_id', 'subject_id'
    dict_key = 'samples'

    @property
    def data(self):
        return self._data()

    def _data(self):
        #notunique = (len([r for r in self.bc.sample_id if r]) !=
                     #len([k for k in self.bc._byCol__indexes['sample_id']
                          #if k != 'sample_id' and k]))

        if self._is_json:
            sids = Counter(r['sample_id'] for r in self._data_raw if r)
        elif not hasattr(self.bc, 'sample_id'):
            sids = {}
            values = list(self)
            msg = f'sample_id column missing! for {self.path}'
            logd.critical(msg)
            self.addError(msg)
        else:
            sids = Counter(r for r in self.bc.sample_id if r)

        notunique = {s:c for s, c in sids.items() if c > 1}
        values = list(self)
        if notunique:
            if not values or 'sample_id' not in values[0]:
                # [] [{}], caused by a bunch of N/A or missing rows
                breakpoint()

            msg = f'sample_ids are not unique for {self.path}\n{notunique}'
            logd.critical(msg)
            self.addError(msg)
            # FIXME this needs to be pipelined so we can do the diff ??
            for i, v in enumerate(values):
                v['local_sample_id'] = v['sample_id']
                if 'subject_id' in v:
                    v['sample_id'] = v['subject_id'] + '-' + v['sample_id']  # FIXME '/' gets quoted ...
                else:
                    v['sample_id'] = f'ERROR-{i}-' + v['sample_id']

        out = {self.dict_key: values}
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

        self.embedErrors(out)
        return out

    def specimen_anatomical_location(self, value):
        seps = '|',
        for sep in seps:
            if sep in value:
                for v in value.split(sep):
                    if v:
                        yield self.query(v, 'UBERON')

                return

        else:
            return self.query(value, 'UBERON')


class SamplesFilePath(ObjectPath):
    obj = SamplesFile


SamplesFilePath._bind_flavours()


class ManifestFile:
    def __init__(self, path):
        self.path = path


class ManifestFilePath(ObjectPath):
    obj = ManifestFile


ManifestFilePath._bind_flavours()


DatasetStructure.sections = {'submission': SubmissionFilePath,
                             'dataset_description': DatasetDescriptionFilePath,
                             'subjects': SubjectsFilePath,
                             'samples': SamplesFilePath,
                             'manifest': ManifestFilePath,}

DatasetStructure._bind_sections()
