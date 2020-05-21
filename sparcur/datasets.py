import io
import csv
import copy
from types import GeneratorType
from itertools import chain
from collections import Counter
import requests
from xlsx2csv import Xlsx2csv, SheetNotFoundException, InvalidXlsxFileException
from terminaltables import AsciiTable
from pyontutils.utils import byCol, Async, deferred, python_identifier
from pyontutils.namespaces import OntCuries, makeNamespaces, TEMP, isAbout
from pyontutils.namespaces import rdf, rdfs, owl, skos, dc
from augpathlib import FileSize
from . import schemas as sc
from . import raw_json as rj
from . import exceptions as exc
from . import normalization as nml
from .core import log, logd, HasErrors
from .paths import Path
from .utils import is_list_or_tuple

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


hasSchema = sc.HasSchema()
@hasSchema.mark
class Header:
    """ generic header normalization for python """
    # FIXME this is a really a pipeline stage
    def __init__(self, first_row_or_column, normalize=True):
        self.pipeline_start = first_row_or_column
        self._normalize = normalize

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
    def _lut(self):
        return {n:h for n, h in zip(self.data, self.pipeline_start)}

    def lut(self, **renames):
        if self._normalize:
            return self._lut_normalize(renames)
        else:
            return self._lut_original(renames)

    def _lut_normalize(self, renames):
        """ do any renames beyond the specific normalization """
        return {renames[n] if n in renames else n:h
                for n, h in zip(self.data, self.pipeline_start)}

    def _lut_original(self, renames):
        original = self.pipeline_start
        normalized = self.normalized
        if len(set(original)) != len(original):
            dupes = [v for v, c in Counter(original).most_common() if c > 1]
            msg = f'Original header is not unique.\nDuplicate entries: {dupes}'
            raise exc.MalformedHeaderError(msg)

        return {renames[n] if n in renames else n:h
                for n, h in zip(original, original)}


class DatasetMetadata(Path, HasErrors):

    @property
    def data(self):
        if self.cache is not None:
            cmeta = self.cache.meta
            return dict(id=self.cache.id,
                        meta=dict(folder_name=self.name,
                                  uri_human=self.cache.uri_human,
                                  uri_api=self.cache.uri_api,
                                  timestamp_created=cmeta.created,
                                  timestamp_updated=cmeta.updated,
                        ))
        else:
            return dict(id=self.id,
                        meta=dict(folder_name=self.name,
                                  uri_human=None,
                                  uri_api=None,
                                  timestamp_created=None,
                                  timestamp_updated=None,
                        ))


DatasetMetadata._bind_flavours()


class DatasetStructure(Path):
    sections = None  # NOTE assigned in at end of file
    rglobs = 'manifest',
    default_glob = 'glob'
    max_childs = 40
    rate = 8  # set by Integrator from cli
    _refresh_on_missing = True

    @property
    def children(self):
        # for whatever reason the way pathlib constructs children
        # skips calling init, iirc because they use object.__new__
        # or something

        for child in super().children:
            child.__init__()
            yield child

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
                log.critical(f'OH NO THIS IS GOING TO CAUSE DUPLICATES (probably)\n{self}')
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
                    try:
                        path.cache.fetch(size_limit_mb=path.cache.meta.size.mb + 1)
                    except requests.exceptions.ConnectionError as e:
                        raise exc.NetworkFailedForPathError(path) from e

                if path.suffix in path.stem:
                    msg = f'path has duplicate suffix {path.as_posix()!r}'
                    self.addError(msg,
                                  blame='submission',
                                  path=path)
                    logd.error(msg)

                if path.name[0].isupper():
                    msg = f'path has bad casing {path.as_posix()!r}'
                    if self.addError(msg,
                                     blame='submission',
                                     path=path):
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


class ObjectPath(Path):
    obj = None
    @property
    def object(self):
        return self.obj(self)


ObjectPath._bind_flavours()


class Tabular(HasErrors):

    def __init__(self, path, rotate=False):
        super().__init__()
        self.path = path
        self._rotate = rotate

    @property
    def file_extension(self):
        if self.path.suffixes:
            ext = self.path.suffixes[0]  # FIXME filenames with dots in them ...
            if ext != '.fake':
                return nml.NormFileSuffix(ext).strip('.')

    def tsv(self):
        return self.csv(delimiter='\t')

    def csv(self, delimiter=','):
        # FIXME if the number of delimiters in a row other than the header
        # is greater than the number of delimiters in the header then any
        # columns beyond the number in the header will be truncated and
        # I'm not sure that this loading routine detects that
        for encoding in ('utf-8', 'latin-1'):
            try:
                with open(self.path, 'rt', encoding=encoding) as f:
                    rows_orig = list(csv.reader(f, delimiter=delimiter))

                rows = [row for row in rows_orig if row and any(row)]
                diff = len(rows_orig) - len(rows)
                if diff:
                    # LOL THE FILE WITH > 1 million empty rows, 8mb of commas
                    # totally the maximum errors champion
                    message = f'There are {diff} empty rows in {self.path.as_posix()!r}'
                    if self.addError(message,
                                     blame='submission',
                                     path=self.path):
                        logd.error(message)

                if encoding != 'utf-8':
                    message = f'encoding bad {encoding!r} {self.path.as_posix()!r}'
                    if self.addError(exc.EncodingError(message),
                                     blame='submission',
                                     path=self.path):
                        logd.error(message)

                yield from rows
                return
            except UnicodeDecodeError:
                continue
            except csv.Error as e:
                logd.exception(e)
                message = f'WHAT HAVE YOU DONE {e!r} {self.path.as_posix()!r}'
                if self.addError(message,
                                 blame='EVERYONE',
                                 path=self.path):
                    logd.error(message)

    def xlsx(self):
        kwargs = {
            'delimiter' : '\t',
            'skip_empty_lines' : True,
            'outputencoding': 'utf-8',
            'hyperlinks': True,
        }
        sheetid = 1
        try:
            xlsx2csv = Xlsx2csv(self.path.as_posix(), **kwargs)
        except InvalidXlsxFileException as e:
            raise exc.NoDataError(f'{self.path}') from e

        ns = len(xlsx2csv.workbook.sheets)
        if ns > 1:
            message = f'too many sheets ({ns}) in {self.path.as_posix()!r}'
            if self.addError(exc.EncodingError(message),
                             blame='submission',
                             path=self.path):
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
                     if ((not logd.error(error) if self.addError(error) else True)  # FIXME will probably append multiple ...
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
        return self.__class__(self.path, rotate=True)

    def __iter__(self):
        yield from self._iter_(normalize=True)

    def _iter_(self, normalize=False):
        try:
            if normalize:
                gen = self.normalize(getattr(self, self.file_extension)())
            else:
                gen = getattr(self, self.file_extension)()

            if self._rotate:
                yield from zip(*gen)
            else:
                yield from gen
        except UnicodeDecodeError as e:
            log.error(f'{self.path.as_posix()!r} {e}')

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


ROW_TYPE = object()
COLUMN_TYPE = object()
numberN = object()
GROUP_ALL = object()


def remove_rule(blob, rule):
    # FIXME this should be in _DictTransformer
    previous_target = blob
    target = blob
    previous_element = None
    for path_element in rule:
        if isinstance(path_element, str):
            if path_element in target:
                previous_target = target
                target = previous_target[path_element]
            else:
                return  # target not found

        elif isinstance(path_element, dict):
            if all([(k in target and target[k] == v)
                    for k, v in path_element.items()]):
                previous_target.pop(previous_element)
                return  # we're done here

        else:
            raise ValueError(f'what are you doing with {rule}')

        previous_element = path_element


class PrimaryKey:
    def __init__(self, name, column_names, combine_function,
                 nalt_header, agen, gen):
        self.name = name
        # because we add the primary key to alt header we remove it again here
        # because the rest of the generator values don't have a blank prepended
        nalt_header_less_pk = [nah for nah in nalt_header if nah != name]
        try:
            self.indexes = [nalt_header_less_pk.index(name) for name in column_names]
        except ValueError as e:
            msg = f'Missing a primary key component {e}'
            raise exc.MalformedHeaderError(msg) from e

        self.combine_function = combine_function
        self._agen = agen
        self._gen = gen

    def compute_primary(self, arecord):
        return self.combine_function(tuple(arecord[index] for index in self.indexes))

    def __iter__(self):
        yield (self.name, *[self.compute_primary(ar) for ar in self._agen])
        yield from self._gen

    @property
    def generator(self):
        return iter(self)


class MetadataFile(HasErrors):
    default_record_type = ROW_TYPE
    primary_key_rule = None  # name, the names of the alt_header columns to merge, function to merge tuple
    renames_alt = {}     # renames first since all following
    renames_header = {}  # refs look for the renamed form
    missing_add = {}  # missing also comes after renames
    record_type_key_alt = None  # record type key is affected by renames
    record_type_key_header = None  # record type key is affected by renames
    groups_alt = {}
    groups_header = {}
    ignore_alt = tuple()
    ignore_header = tuple()
    ignore_match = tuple()
    raw_json_class = rj.RawJson
    normalization_class = nml.NormValues
    normalize_alt = True
    normalize_header = True
    _expect_single = tuple()

    def __new__(cls, path, schema_version=None):
        if schema_version is not None:  # and schema_version < latest  # TODO
            logd.info(schema_version)  # TODO warn

        if cls.record_type_key_header is None or cls.record_type_key_alt is None:
            raise TypeError(f'record_type_key_? should not be None on {cls.__name__}')

        # TODO check for renames that didn't get picked up
        # especially from the 0, 0 cell

        return super().__new__(cls)

    def __init__(self, path, schema_version=None):
        super().__init__()
        self.path = path
        self.schema_version = schema_version

    def xopen(self):
        self.path.xopen()

    @property
    def data(self):
        data = self._data()

        condition = False
        if condition:
            breakpoint()

        return data

    def _data(self):
        if self.path.suffix == '.json':
            data = self.raw_json_class(self.path).data
            self._expand_string_lists = lambda : data  # FIXME hack
            self.norm_to_orig_header = {}
            def all_keys(d):
                if isinstance(d, dict):
                    for k, v in d.items():
                        if k not in self.groups_alt:
                            yield k

                        yield from all_keys(v)

                elif isinstance(d, list):
                    for e in d:
                        yield from all_keys(e)

            self.norm_to_orig_alt = {k:k for k in all_keys(data)}
            #return self.normalization_class(self).data  # FIXME incredible violation of encapsulation ...
            return self._condense()

        else:
            # TODO if this passes we can just render backward from _normalize
            return self._condense()

    def _condense(self):
        def condense(thing, key=None):
            if isinstance(thing, dict):
                hrm = []
                out = {}
                for k, v in thing.items():
                    nv = condense(v, k)
                    # bah bloody description appearing twice >_<
                    if k in self.norm_to_orig_header and k not in self.norm_to_orig_alt:
                        if is_list_or_tuple(nv):
                            # at this stage there are no lists of lists
                            hrm.extend(nv)
                        else:
                            hrm.append(nv)
                    else:
                        out[k] = nv

                if out and hrm:
                    raise ValueError(f'how!?\n{hrm}\n{out}')
                elif out:
                    return out
                elif hrm:
                    if (key in self._expect_single and
                        len(hrm) == 1):
                        return hrm[0]
                    else:
                        return tuple(hrm)

            #elif is_list_or_tuple(thing):
                #return thing
            else:
                return thing

        data_in = self._cull_rtk()
        data_out = condense(data_in)
        if data_out is None:
            # None breaks further pipelines
            # yes types would help here
            return {}
        else:
            return data_out

    def _cull_rtk(self):
        normalized = self._normalize_values()
        def crtk(thing, only_index_alt=False, only_index_header=False):
            if isinstance(thing, dict):
                out = {}
                for k, v in thing.items():
                    if k in self.groups_alt and self.groups_alt[k] == GROUP_ALL:
                        nv = crtk(v, only_index_alt=True)
                        if nv is not None:
                            out[k] = nv
                    elif k in self.groups_header and self.groups_header[k] == GROUP_ALL:
                        # this isn't used yet but putting it here for symmetry just in case
                        nv = crtk(v, only_index_header=True)
                        if nv is not None:
                            out[k] = nv
                    elif (only_index_alt and k == v == self.record_type_key_alt or
                          only_index_header and k == v == self.record_type_key_header):
                        continue
                    elif (k != self.record_type_key_header and
                          # FIXME requires that headers and alts be fully disjoint
                          k != self.record_type_key_alt or
                          only_index_alt and k == self.record_type_key_alt or
                          only_index_header and k == self.record_type_key_header):
                        nv = crtk(v, only_index_alt, only_index_header)
                        if nv is not None:
                            out[k] = nv

                if out:
                    return out

            else:
                return thing

        crv = crtk(normalized)
        return crv

    def _normalize_values(self):
        nc = self.normalization_class(self)  # calls _normalize_keys internally
        return nc.data

    def _clean(self):
        data_in = self._expand_string_lists()
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

            elif is_list_or_tuple(thing):
                return [v for v in [clean(v) for v in thing] if v is not None]

            else:
                return thing

        data_out = clean(data_in)
        return data_out

    def _expand_string_lists(self):
        data_in = self._filter()
        # TODO consider whether this step would be useful
        # I think it would because it would vastly simplify
        # the downstream implementation of things like
        # normalizing the contributor roles
        # TODO this might also go before filter?
        # might need to blacklist or whitelist fields?
        seps = '|', ';', ','
        data_out = data_in
        return data_out

    def _filter(self):
        # FIXME does in place modification
        culled = self._cull()
        for rule in self.ignore_match:
            remove_rule(culled, rule)

        return culled

    def _cull(self):
        transformed = self._transformed()
        # FIXME deeper nesting might break this?
        culled = {k:{k:v for k, v in v.items()
                     if k not in self.ignore_header}
                  for k, v in transformed.items()}
        return culled

    def _transformed(self):
        keyed = self._keyed()
        naccounted_baseline = set(e for g in self.groups_alt.values()
                                  if g != GROUP_ALL for e in g)

        # TODO how to flag exclude the header fields ?
        transformed = {}
        for key, alt_grouped_norm in self.groups_alt.items():
            merge = alt_grouped_norm == GROUP_ALL
            if merge:
                alt_grouped_norm = tuple(k for k in self.norm_to_orig_alt
                                         if k not in naccounted_baseline)
                naccounted_baseline.update(set(alt_grouped_norm))

            records = [keyed[nh] for nh in alt_grouped_norm
                       # FIXME versioning ... schema will take care of missing values later
                       if nh in keyed]

            objected = {nh:{record[self.record_type_key_header]:record[nh]
                            for record in records}
                        for nh in self.norm_to_orig_header}

            transformed[key] = {k:v for k, v in objected.items()
                                if v is not None and v != {}}

        nunaccounted = (set(self.norm_to_orig_alt) - naccounted_baseline)

        for nah in nunaccounted: 
            transformed[nah] = keyed[nah]

        return transformed

    def _keyed(self):
        objects = self._add_missing()
        keyed = {o[self.record_type_key_header]:o for o in objects}
        return keyed

    def _add_missing(self):
        objects = self._sanity_check()
        for nah, (value, number) in self.missing_add.items():
            if nah not in self.norm_to_orig_alt:
                self.norm_to_orig_alt[nah] = nah
                cant_go_wrong = {}
                i = 0
                for nh in self.norm_to_orig_header:
                    if ((number == numberN or i < number) and
                        nh != self.record_type_key_header and
                        nh not in self.ignore_header):
                        v = value
                        i += 1
                    else:
                        v = ''

                    cant_go_wrong[nh] = v

                {nh:(value if number == numberN or i <= number else '')
                 for i, nh in enumerate(self.norm_to_orig_header)
                 if nh != self.record_type_key_header}
                #print(cant_go_wrong)
                cant_go_wrong[self.record_type_key_header] = nah
                objects.append(cant_go_wrong)

        # end add missing items
        return objects

    def _sanity_check(self):
        """ first chance we have to check sanity since
            the generators have to have run """
        objects = self._objectify()
        for n, rtk, norm_dict in (('alt_header', self.record_type_key_alt, self.norm_to_orig_alt),
                                  ('header', self.record_type_key_header, self.norm_to_orig_header)):
            if rtk not in norm_dict:
                msg = (f'Could not find record primary key for {n} in\n"{self.path}"\n\n'
                       f'{rtk}\nnot in\n{list(norm_dict)}')

                raise exc.MalformedHeaderError(msg)

        return objects

    def _objectify(self):
        gen = self._norm_alt_headers()
        # WARNING assumes dict order
        objects = [{k:v for k, v in zip(self.norm_to_orig_header, r)} for r in gen]
        return objects

    def _norm_alt_headers(self):
        # headers MUST be normalized as early as possible due to the fact that
        # there are very likely to be duplicate keys
        gen = self._headers()

        def normb(k):
            if k in self.orig_to_norm_alt:
                nk = self.orig_to_norm_alt[k]
            else:
                nk = k

            return nk

        for row in gen:
            yield (normb(row[0]), *row[1:])

    def _headers(self):
        t = self._t()

        if self.default_record_type == ROW_TYPE:
            alt = t
            head = t.T
        else:
            alt = t.T
            head = t

        agen = iter(alt)
        gen = iter(head)

        try:
            if self.primary_key_rule is not None:
                pk_name, combine_names, combine_function = self.primary_key_rule
                self.alt_header = (pk_name, *next(agen))
            else:
                self.alt_header = next(agen)
        except StopIteration as e:
            raise exc.NoDataError(f'{self.path}') from e

        self._alt_header = Header(self.alt_header, normalize=self.normalize_alt)
        self.norm_to_orig_alt = self._alt_header.lut(**self.renames_alt)
        self.orig_to_norm_alt = {v:k for k, v in self.norm_to_orig_alt.items()}

        if self.primary_key_rule is not None:
            nalt_header = [self.orig_to_norm_alt[ah] for ah in self.alt_header]
            gen = PrimaryKey(pk_name, combine_names, combine_function,
                             nalt_header, agen, gen).generator

        # FIXME if the 'header' column is not at position zero
        # this will fail see version 1.1 subjects template for this
        # with a note that 1.1 had many issues
        self.header = next(gen)
        self._header = Header(self.header, normalize=self.normalize_header)
        self.norm_to_orig_header = self._header.lut(**self.renames_header)
        self.orig_to_norm_header = {v:k for k, v in self.norm_to_orig_header.items()}

        yield self.header
        yield from gen

    def _t(self):
        if self.path.suffix == '.json':
            return  # TODO json -> tabular conversion
        else:
            return Tabular(self.path)


class SubmissionFile(MetadataFile):
    __internal_id_1 = object()
    default_record_type = COLUMN_TYPE
    renames_alt = {'submission_item': __internal_id_1}
    renames_header = {'definition': 'definition_header'}
    record_type_key_alt = __internal_id_1
    record_type_key_header = 'submission_item'
    groups_alt = {'submission': ('sparc_award_number', 'milestone_achieved', 'milestone_completion_date')}
    ignore_alt = __internal_id_1,
    ignore_header = 'definition_header',
    raw_json_class = rj.RawJsonSubmission
    normalization_class = nml.NormSubmissionFile

    @property
    def data(self):
        """ lift list with single element to object """

        data = super().data  # LOL PYTHON can't super in a debugger -- pointless
        d = copy.deepcopy(data)

        if d and 'submission' in d:
            sub = d['submission']
            if sub:
                if is_list_or_tuple(sub):
                    d['submission'] = sub[0]
                elif isinstance(sub, dict):
                    return d
                else:
                    raise TypeError(f'What is a {type(sub)} {sub}?')
            else:
                d['submission'] = {}

        return d


class SubmissionFilePath(ObjectPath):
    obj = SubmissionFile


SubmissionFilePath._bind_flavours()


_props = sc.DatasetDescriptionSchema.schema['properties']
_props2 = sc.ContributorSchema.schema['properties']  # FIXME recurse ...
_nddfes = [k for k, v in chain(_props.items(), _props2.items())
           if isinstance(v, dict) and sc.not_array(v)]
_nddfes = sorted(set(_nddfes))


class DatasetDescriptionFile(MetadataFile):
    default_record_type = COLUMN_TYPE
    renames_alt = {'contributors': 'contributor_name',  # watch out for name collisions (heu heu heu)
                   'metadata_version_do_not_change': 'schema_version'}
    renames_header = {'description': 'description_header'}
    missing_add = {'schema_version': ('1.0', 1)}
    record_type_key_alt = 'metadata_element'
    record_type_key_header = record_type_key_alt
    groups_alt = {'contributors': ('contributor_name',
                                   'contributor_orcid_id',
                                   'contributor_affiliation',
                                   'contributor_role',
                                   'is_contact_person',),
                  'links': ('additional_links', 'link_description'),
                  'examples': ('example_image_filename',
                               'example_image_locator',
                               'example_image_description'),}

    ignore_header = 'metadata_element', 'example', 'description_header'
    raw_json_class = rj.RawJsonDatasetDescription
    normalization_class = nml.NormDatasetDescriptionFile
    _expect_single = _nddfes

    @property
    def data(self):
        data = super().data
        #breakpoint()
        return data


class DatasetDescriptionFilePath(ObjectPath):
    obj = DatasetDescriptionFile


DatasetDescriptionFilePath._bind_flavours()


_props = sc.SubjectsSchema.schema['properties']['subjects']['items']['properties']
_props2 = sc.SamplesFileSchema.schema['properties']['samples']['items']['properties']
_props3 = sc._software_schema['items']['properties']
_nsffes = [k for k, v in chain(_props.items(), _props2.items(), _props3.items())
           if isinstance(v, dict) and sc.not_array(v)]
_nsffes = sorted(set(_nsffes))


class SubjectsFile(MetadataFile):
    #default_record_type = COLUMN_TYPE
    __internal_id_1 = object()
    renames_header = {'subject_id': 'metadata_element',
                      ('Lab-based schema for identifying each subject, '
                       'should match folder names'): __internal_id_1,}
    record_type_key_alt = 'subject_id'
    record_type_key_header = 'metadata_element'
    groups_alt = {'subjects': GROUP_ALL,
                  'software':('software',
                              'software_version',
                              'software_vendor',
                              'software_url',
                              'software_rrid'),}
    ignore_header = __internal_id_1,
    ignore_alt = 'additional_fields_e_g__minds',
    ignore_match = [['subjects', 'sub-1', {'subject_id': 'sub-1',
                                           'pool_id': 'pool-1',
                                           'reference_atlas': 'Paxinos Rat V3',
                                           'handedness': 'right',}]]
    raw_json_class = rj.RawJsonSubjects
    normalization_class = nml.NormSubjectsFile
    normalize_header = False
    _expect_single = _nsffes

    @property
    def data(self):
        data = super().data
        #breakpoint()
        return data


class SubjectsFilePath(ObjectPath):
    obj = SubjectsFile


SubjectsFilePath._bind_flavours()


class SamplesFile(SubjectsFile):
    """ TODO ... """

    # TODO merge
    __internal_id_1 = object()
    __primary_key = 'primary_key'
    primary_key_rule = __primary_key, ('subject_id', 'sample_id'), '_'.join
    __rename_1 = primary_key_rule[2](('Lab-based schema for identifying each subject',
                                      ('Lab-based schema for identifying each sample, must '
                                       'be unique')))
    renames_header = {__primary_key: 'metadata_element',
                      __rename_1: __internal_id_1,}
    record_type_key_alt = __primary_key
    record_type_key_header = 'metadata_element'
    groups_alt = {'samples': GROUP_ALL,
                  'software':('software',
                              'software_version',
                              'software_vendor',
                              'software_url',
                              'software_rrid'),}
    ignore_header = __internal_id_1,
    ignore_alt = 'additional_fields_e_g__minds',
    ignore_match = [['samples', primary_key_rule[2](('sub-1', 'sub-1_sam-2')),
                     {'subject_id': 'sub-1',
                      'pool_id': 'pool-1',
                      'reference_atlas': 'Paxinos Rat V3',
                      'handedness': 'right',}]]
    normalization_class = nml.NormSamplesFile
    normalize_header = False
    _expect_single = _nsffes


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
DatasetStructure._pipeline_stage = DatasetStructure.__name__
DatasetStructure._bind_flavours(pos_helpers=(HasErrors,), win_helpers=(HasErrors,))
