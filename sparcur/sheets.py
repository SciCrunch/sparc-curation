import itertools
from functools import wraps
from collections import defaultdict
from urllib.parse import quote
from datetime import datetime, date
import idlib
from pyontutils.utils import isoformat
from pyontutils.sheets import Sheet as SheetBase, get_cells_from_grid
from sparcur import exceptions as exc
from sparcur.core import adops, OntId, OntTerm
from sparcur.utils import log, logd
from sparcur.config import auth

# google sheets

class Sheet(SheetBase):

    _do_cache = False  # retrieve to cache
    _re_cache = False  # refresh the cache
    _only_cache = False  # cache only aka no network

    def __init__(self, *args, **kwargs):
        self._cache_path = auth.get_path('cache-path') / 'google_sheets'
        if not self._only_cache:
            self._setup_saf(**kwargs)

        try:
            super().__init__(*args, **kwargs)
        finally:
            self._saf = None

    def _setup_saf(self, **kwargs):
        try:
            if 'readonly' not in kwargs or kwargs['readonly']:
                # readonly=True is default so we take this branch if not set
                self._saf = auth.get_path(
                    'google-api-service-account-file-readonly')
            else:
                self._saf = auth.get_path(
                    'google-api-service-account-file-rw')
        except KeyError as e:
            log.warn(e)
        except Exception as e:
            log.exception(e)

    def _setup(self):
        if not self._only_cache:
            super()._setup()

    def fetch(self, _refresh_cache=False, **kwargs):
        if self._do_cache or self._only_cache:
            cache = idlib.cache.cache(self._cache_path, create=True, return_path=True)
            @cache
            def cache_wrap_fetch(name, sheet_name, __f=super().fetch):
                return __f(**kwargs)

            cache_wrapper = cache_wrap_fetch
            (meta, meta_file, raw_values, raw_values_formula, grid), path = cache_wrapper(
                self.name, self.sheet_name,
                _refresh_cache=_refresh_cache or self._re_cache)
            values = [list(r) for r in  # XXX FIXME code duplication
                      zip(*itertools.zip_longest(*raw_values,
                                                 fillvalue=''))]
            values_formula = [list(r) for r in
                              zip(*itertools.zip_longest(*raw_values_formula,
                                                         fillvalue=''))]

            if grid:
                # XXX FIXME duplicated code from pyontutils sheets
                filter_cell = None
                cells = get_cells_from_grid(grid, self.sheet_name, filter_cell)
                cells_index = {(i, j):v for i, j, v in cells}
            else:
                cells_index = {}

            self._meta = meta
            self._meta_file = meta_file
            self.raw_values = raw_values
            self._values = values
            self.raw_values_formula = raw_values_formula
            self._values_formula = values_formula
            self.grid = grid
            self.cells_index = cells_index
            return meta, meta_file, raw_values, values, grid

        return super().fetch(**kwargs)

    def metadata(self, _refresh_cache=False):
        if self._do_cache or self._only_cache:
            cache = idlib.cache.cache(self._cache_path, create=True, return_path=True)
            @cache
            def cache_wrap_meta(name, sheet_name, __f=super().metadata):
                return __f()

            cache_wrapper = cache_wrap_meta
            meta, path = cache_wrapper(
                self.name, self.sheet_name,
                _refresh_cache=_refresh_cache or self._re_cache)
            self._meta = meta
            return meta

        return super().metadata()

    def metadata_file(self, _refresh_cache=False):
        if self._do_cache or self._only_cache:
            cache = idlib.cache.cache(self._cache_path, create=True, return_path=True)
            @cache
            def cache_wrap_meta_file(name, sheet_name, __f=super().metadata_file):
                return __f()

            cache_wrapper = cache_wrap_meta_file
            meta_file, path = cache_wrapper(
                self.name, self.sheet_name,
                _refresh_cache=_refresh_cache or self._re_cache)
            self._meta_file = meta_file
            return meta_file

        return super().metadata_file()


# master
class Master(Sheet):
    name = 'sparc-master'


class Overview(Master):
    sheet_name = 'July18_2019'

    def dataset_ids(self):
        header, *rows = self.values
        index = header.index('identifier')
        return set(i for i in [row[index]for row in rows] if i)


class Progress(Master):
    sheet_name = 'Curation Progess (OT Only)'


class Grants(Master):
    sheet_name = 'Grant to Blackfynn to Protocols.io CLEAN'


class ISAN(Master):
    sheet_name = 'ISAN Demo July 2019'


class Participants(Master):
    sheet_name = 'Participan Notes'


class Protocols(Master):
    sheet_name = 'Protocol URL--> Blackfynn URL'


# affiliations
class Affiliations(Sheet):
    name = 'sparc-affiliations'
    sheet_name = 'Affiliations'
    index_columns = 'affiliation_string',

    def __call__(self, affiliation_string):
        if not isinstance(affiliation_string, str):
            logd.critical(str(affiliation_string))
            return self(affiliation_string[0] + 'ERROR ERROR')

        m = self.mapping

        if not isinstance(affiliation_string, str):
            log.critical('sigh')
            return None

        if affiliation_string in m:
            return m[affiliation_string]
        else:
            # FIXME super inefficient
            las = len(affiliation_string)
            for l, s in sorted([(len(k), k) for k in m.keys()], reverse=True):
                if l <= las and s in affiliation_string:
                    return m[s]

    @property
    def mapping(self):
        def ror_id(row):
            r = row.ror_id().value
            return idlib.Ror(r) if r else None

        return {row.affiliation_string().value:ror_id(row) for row in self.rows()[1:]}


# class sparc reports

class Reports(Sheet):
    """ spreadsheet that contains all spc reports as tabs """
    name = 'spc-reports'

    @classmethod
    def asPreviewClass(cls):
        class ReportPreviewClass(ReportsPreview, cls):
            pass

        assert ReportPreviewClass.name != cls.name
        assert ReportPreviewClass.name.startswith(cls.name), (ReportPreviewClass.name, cls.name)
        return ReportPreviewClass

    @classmethod
    def makeReportSheet(cls, *index_cols, sheet_name=None):
        """ Decorator to build a Sheet class for a cli.Report method

            NOTE reports that do not have index columns will update
            the entire sheet every time, so probabaly don't edit those
        """
        _sheet_name = sheet_name  # avoid issues with class scope
        del sheet_name

        def _rowcellify(cell_value):
            if isinstance(cell_value, idlib.Stream):
                try:
                    return cell_value.asUri()
                except AttributeError as e:
                    breakpoint()
            elif (isinstance(cell_value, date) or
                  isinstance(cell_value, datetime)):
                # the default json encoder doesn't know what to do with these
                return isoformat(cell_value)
            elif isinstance(cell_value, str):
                if cell_value.lower() == 'x':
                    return True

            return cell_value

        def rowcellify(rows):
            return [[_rowcellify(cell_value) for cell_value in columns]
                    for columns in rows]

        def decorator(method):
            tags = method.__name__ == 'anno_tags'

            class ReportSheet(cls):
                __name__ = f'Report{method.__name__.capitalize()}'
                sheet_name = method.__name__ if _sheet_name is None else _sheet_name
                index_columns = index_cols

            method._report_class = ReportSheet

            @wraps(method)
            def inner(self, *args, **kwargs):
                if self.options.preview:
                    RC = method._report_class.asPreviewClass()
                else:
                    RC = method._report_class

                if 'ext' not in kwargs or kwargs['ext'] is None:
                    kwargs['ext'] = True
                elif isinstance(kwargs['ext'], bool) and kwargs['ext']:
                    pass  # already went through
                elif self.options.to_sheets:
                    msg = 'Should not have extension and export to sheets.'
                    raise TypeError(msg)
                else:
                    pass

                out = method(self, *args, **kwargs)

                if self.options.to_sheets:
                    if tags:
                        if args:
                            RC.sheet_name = args[0]
                        else:
                            RC.sheet_name = self.options.tag[0]

                    rows, title = out
                    # fetch=False since the remote sheet may not exist
                    # TODO improve the ergonimcs here
                    report = RC(fetch=False, readonly=False)
                    report.metadata()  # idlib.Stream vs ? behavior not decided
                    rows_stringified = rowcellify(rows)  # FIXME align to upstream schema
                    if report.sheetId() is None:
                        report.createRemoteSheet()
                        report.update(rows_stringified)
                    else:
                        report.fetch()
                        if report.index_columns and report.values:
                            # FIXME bad assumption that column
                            # ordering remains the same?  FIXME upsert
                            # should be ok in this situation but
                            # dropping the header causes issues
                            # XXX sort of fixedish no longer mismatching now at least
                            remap = [rows[0].index(c) if c in rows[0] else None
                                     for c in report.values[0]]
                            aligned = [[None if i is None else rs[i]
                                        for ui, i in enumerate(remap)]
                                       for rs in rows_stringified[1:]]
                            def gmr(a):
                                try:
                                    return report._row_from_index(row=a)[0].values
                                except AttributeError:
                                    return a

                            preserved = [[cr if ca is None else ca for cr, ca in zip(r, a)]
                                         for r, a in zip([gmr(a) for a in aligned], aligned)]
                            # FIXME seems to produce duplicate rows or something?
                            report.upsert(*preserved)
                        else:
                            report.update(rows)

                    report.commit()

                return out

            return inner

        return decorator


class ReportsPreview(Reports):
    """ reporting for the preview of the next iteration of the pipelines """
    name = 'spc-reports-preview'


class Mis(Reports):
    sheet_name = 'mis'

    # NOTE a ReportSheet for this sheet is generated as well
    # it is simple write-only functionality, so we create
    # another one here instead of hacking around
    # reports.Report.mis._report_class

    def triples(self):
        from pyontutils import namespaces as ns
        import rdflib
        for row in self.rows()[1:]:
            yield from self._rowhack(row, ns, rdflib)

    @staticmethod
    def _rowhack(self, ns, rdflib):
        # can't overwrite pyontutils.sheets.Row like we do in other cases sigh
        curie = self.curie().value
        if not curie:
            return

        oid = OntId(curie)
        s = oid.u

        _lon = (lambda v: None if not v else
                rdflib.Literal(v))
        lon = lambda c: _lon(c.value)

        _blon = lambda v: _lon(True if v == 'TRUE' else False)  # sigh sheets datatypes
        blon = lambda c: _blon(c.value)

        _oon = (lambda v: None if not v else
                OntId(v).u)
        oon = lambda c: _oon(c.value)

        self.type().value
        self.mistake().value
        pos = (
            (ns.ilxtr.curationInternal, blon(self.curation_internal())),
            (ns.definition, lon(self.definition())),
            (ns.editorNote, lon(self.notes())),
            (ns.rdf.type, oon(self.rdf_type())),
            (ns.replacedBy, oon(self.replacedby_())),
            (ns.ilxtr.futureType, oon(self.future_type())),
        )

        for p, o in pos:
            if o is not None:
                yield s, p, o


class AnnoTags(Reports):
    name = 'anno-tags'

    @classmethod
    def asPreviewClass(cls):
        """ anno tags don't need a preview class """
        return cls

    def _annotation_row(self, anno, edfix=False):
        key = self.index_columns[0]
        value = getattr(anno, key)
        value = value.strip()  # FIXME align with id normalization maybe?
        if edfix and value.endswith('ed'):
            ree = None
            for i in range(3):
                try:
                    return self._kv_row(key, value)
                except exc.NotMappedError as e:
                    if i == 0:
                        ree = e
                        value = value[:-2]
                    elif i == 1:
                        value = value + 'e'
                    else:
                        raise ree
        else:
            return self._kv_row(key, value)

    def _kv_row(self, key, value):
        header = self.row_object(0)
        #index = getattr(header, key)().column  # ;_; but it was so elegant
        #row = getattr(index, value)().row
        if key in header._trouble:
            index = header._trouble[key]().column
        else:
            index = getattr(header, key)().column

        if value in index._trouble:
            row = index._trouble[value]().row
        else:
            try:
                row = getattr(index, value)().row
            except AttributeError as e:
                try:
                    # FIXME yes indeed the obscured rules for
                    # when to downcase have come be to haunt us
                    # this was filtering out countless potential matches
                    row = getattr(index, value.lower())().row
                except AttributeError as e:
                    # TODO
                    raise exc.NotMappedError(f'no record for {value}') from e

        return row


class WorkingExecVerb(AnnoTags):
    sheet_name = 'working-protc:executor-verb'
    index_columns = 'value',

    def condense(self):
        marked_as_done = '-'
        mapping = defaultdict(list)

        def make_key(row):
            return tuple(c.value for c in [row.tag(), row.value(), row.text(), row.exact()])

        create = []
        for mt_cell in self.row_object(0).map_to().column.cells[1:]:
            if (mt_cell.value and mt_cell.value != marked_as_done and
                mt_cell.value != mt_cell.row.value().value):
                try:
                    row, iv = self._row_from_index(value=mt_cell.value)
                    key = make_key(row)
                except AttributeError as e:  # value not in index
                    key = ('protc:executor-verb', mt_cell.value, '', '')
                    if key not in create:
                        create.append(key)
                        log.exception(e)

                mapping[key].append(mt_cell.row.value().value)  # cells don't move so we're ok

        mapping = dict(mapping)
        value_to_map_to = {value:k for k, values in mapping.items()
                           for value in values}

        #breakpoint()
        return value_to_map_to, create  # old -> new, original -> correct

    def map(self, anno):
        row = self._annotation_row(anno, edfix=True)
        bad = row.bad().value
        help__ = row.help__().value
        map_to = row.map_to().value
        merge = row.merge().value

        if not (bad or help__):
            if map_to:
                if map_to == '-':
                    # suffix = getattr(anno, self.index_columns[0])  # XXX ignores normalization
                    # XXX TODO consider moving this kind of normalization to the anno class
                    # to avoid these situations
                    suffix = row.value().value
                else:
                    suffix = map_to
            elif merge:
                suffix = merge
            else:
                return  # FIXME error in this case?

            #if suffix == 'anaesthetize':  # finally, was a case conversion issue
                #breakpoint()
            return 'verb:' + suffix.replace(' ', '-')


class WorkingTechniques(AnnoTags):
    sheet_name = 'working-ilxtr:technique'
    index_columns = 'exact',  # FIXME pull index_columns the reports sheets decorator somehow?

    def map(self, anno):
        row = self._annotation_row(anno)
        oid = row.ontology_id().value
        label = row.ontology_label().value
        ilx_curie = row.interlex_id().value
        return OntTerm(oid, label=label), OntTerm(interlex_curie)


class WorkingAspects(AnnoTags):
    sheet_name = 'working-protc:aspect'
    index_columns = 'value',

    def map(self, anno):
        row = self._annotation_row(anno)
        bad = row.bad_().value
        notes_help = row.notes_help().value
        aspect = row.aspect().value
        parent_aspect = row.parent_aspect().value

        if not (bad or notes_help):
            if aspect:
                return 'asp:' + aspect.replace(' ', '-')
            elif parent_aspect:
                return 'asp:' + parent_aspect.replace(' ', '-')


class WorkingAspectsImplied(AnnoTags):
    sheet_name = 'working-protc:implied-aspect'
    index_columns = 'value',

    def map(self, anno):
        row = self._annotation_row(anno)
        map_to = row.map_to().value
        bad = row.bad().value
        if not bad and map_to:
            if map_to == '-':
                suffix = getattr(anno, self.index_columns[0])
            else:
                suffix = map_to

            return 'asp:' + suffix.replace(' ', '-')


class WorkingBlackBox(AnnoTags):
    sheet_name = 'working-protc:black-box'
    index_columns = 'value',

    def map(self, anno):
        row = self._annotation_row(anno)
        mapping_ok = row.mapping_ok().value == 'TRUE'  # FIXME
        not_input = row.not_input_().value
        bad_for_mapping = row.bad_for_mapping_().value
        manual_mapping = row.manual_mapping().value.split('|')[0]
        if mapping_ok and not not_input:
            pass

        if manual_mapping and ' ' in manual_mapping:
            log.error(f'Why does a manual mapping have a space in it {manual_mapping!r}')

        elif manual_mapping:
            return OntTerm(manual_mapping)

        elif mapping_ok:  # FIXME anno.astValue can drift from auto_mapping
            # this is so hilariously inefficient, we parse the same stuff
            # 3 times or something
            _v = anno.asPython().asPython()
            if hasattr(_v, 'black_box'):  # black-box-components only have a name
                # XXX this will fail when black_box is not a term because
                # mapping_ok is true but we removed the old mapping
                if hasattr(_v.black_box, 'curie'):
                    return OntTerm(_v.black_box.curie)
                else:
                    raise exc.NotMappedError(f'no curie mapping {_v}')
            else:
                raise exc.NotMappedError(f'no black_box mapping {_v} ')


class WorkingBlackBoxComponent(WorkingBlackBox):
    sheet_name = 'working-protc:black-box-component'


class WorkingInputs(WorkingBlackBox):
    sheet_name = 'working-protc:input'


class WorkingInputInstances(AnnoTags):
    sheet_name = 'working-protc:input-instance'
    index_columns = 'exact',

    def map(self, anno):
        row = self._annotation_row(anno)
        proposed_category = row.proposed_category().value
        interlex = row.interlex().value
        rrid = row.rrid().value
        # TODO multiple returns
        if rrid:
            return rrid


# field alignment
class FieldAlignment(Sheet):
    name = 'sparc-field-alignment'


class Subjects(FieldAlignment):
    sheet_name = 'Subjects'


class Keywords(FieldAlignment):
    sheet_name = 'All Keywords'


class KeywordSets(FieldAlignment):
    sheet_name = 'Keyword sets'


class _Organs(FieldAlignment):
    # just having fetch_grid = True is enough to bolox everything
    sheet_name = 'Organs'
    index_columns = 'id',
    fetch_grid = True  # THIS IS THE CULPRET
    #fetch_grid = False  # THIS FLIES
    def modality(self, dataset_id): None
    def organ_term(self, dataset_id): None
    def award_manual(self, dataset_id): None
    def techniques(self, dataset_id): return []
    def protocol_uris(self, dataset_id): return []


class Organs(FieldAlignment):
    sheet_name = 'Organs'
    index_columns = 'id',
    fetch_grid = False  # only need hyperlinks, now via FORMULA so no grid
    #fetch_grid = True
    _news = []

    def _lookup(self, dataset_id, fail=False, raw=True):
        try:
            row, iv = self._row_from_index('id', dataset_id)
            return row
        except AttributeError as e:
            # TODO update the sheet automatically
            if dataset_id not in self._news:
                log.critical(f'New dataset! {dataset_id}')
                self._news.append(dataset_id)

            if fail:
                raise e

    def _dataset_row_index(self, dataset_id):
        # FIXME dict or cache this for performance
        row = self._lookup(dataset_id, fail=True)
        return self.values.index(row.values)

    def _update_technique(self, cell):
        # NOTE some rows won't update if the dataset no longer exists
        value = cell.value
        if value:
            try:
                term = next(OntTerm.query(label=value))
                cell.value = term.asCellHyperlink()
            except StopIteration:
                log.info(f'no term for technique {value}')

    def _update_dataset_metadata(self, *, id, name, award, species, update_techniques=False):
        try:
            row_index = self._dataset_row_index(id)
            row = self.row_object(row_index)

            if update_techniques:
                for i in range(1,10):
                    h = f'technique{i}'
                    cell_t = getattr(row, h, lambda:None)()
                    if cell_t is not None:
                        self._update_technique(cell_t)

            cell_dsn = row.dataset_name()
            if cell_dsn.value != name:
                cell_dsn.value = name

            cell_award = row.award()
            if cell_award.value != award:
                cell_award.value = award

            cell_species = row.species()
            if cell_species.value != species:
                cell_species.value = species

        except AttributeError as e:
            row = ['', '', name, id, award, species]  # XXX this was causing index errors
            # because the columns were not being padded by appendRow
            self._appendRow(row)

    def update_from_ir(self, ir):
        oi = OntTerm.query._instrumented
        if oi is not OntTerm:
            OntTerm.query._instrumented = OntTerm

        def cformat(cell):
            if isinstance(cell, OntTerm):
                cell = cell.asCell()

            return cell

        try:
            dataset_blobs = ir['datasets']
            self._wat = self.values[8]
            for blob in dataset_blobs:
                meta = blob['meta']
                #species = adops.get(blob, ['subjects', int, 'species'], on_failure='')  # TODO not implemented
                if 'subjects' in blob:
                    species = '\n'.join(sorted(set(
                        [cformat(s['species']) for s in blob['subjects']
                         if 'species' in s])))
                else:
                    species = ''

                self._update_dataset_metadata(
                    id=blob['id'],
                    name=adops.get(blob, ['meta', 'folder_name'], on_failure=''),
                    award=adops.get(blob, ['meta', 'award_number'], on_failure=''),
                    species=species,
                )
        finally:
            # FIXME this is so dumb :/
            OntTerm.query._instrumented = oi
        log.debug(self.uncommitted())
        self.commit()

    def modality(self, dataset_id):
        """ tuple of modalities """
        row = self._lookup(dataset_id)
        if row:
            try:
                m1 = row.modality1()
                m2 = row.modality2()
                m3 = row.modality3()
                return tuple(_.value for _ in (m1, m2, m3) if _.value)
            except AttributeError as e:
                raise ValueError(f'issue in {self.name} {self.sheet_name}') from e

    def organ_term(self, dataset_id):
        row = self._lookup(dataset_id)
        if row:
            organ_term = row.organ_term()
            otv = organ_term.value
            ot = otv if otv else None
            if ot:
                try:
                    ts = tuple(OntId(t) for t in ot.split(' ') if t and t.lower() != 'na')
                    return ts
                except OntId.BadCurieError:
                    log.error(ot)

    def award(self, dataset_id):
        row = self._lookup(dataset_id)
        if row:
            award = row.award()
            av = award.value
            award_manual = row.award_manual()
            amv = award_manual.value
            return av if av else (amv if amv else None)
        
    def award_machine(self, dataset_id):
        row = self._lookup(dataset_id)
        if row:
            award = row.award()
            av = award.value
            return av if av else None

    def award_manual(self, dataset_id):
        row = self._lookup(dataset_id)
        if row:
            award_manual = row.award_manual()
            amv = award_manual.value
            return amv if amv else None

    def techniques(self, dataset_id):
        row = self._lookup(dataset_id)

        def mkval(cell):
            hl = cell.hyperlink
            if hl is not None:
                oid = OntId(hl)
                if oid.prefix == 'TEMP':
                    logd.warning(f'{cell.value} -> {oid!r}')
                    #return OntTerm(curie=f'lex:{quote(cell.value)}')
                #else:

                return oid.asTerm()

            else:
                logd.warning(f'unhandled technique {cell.value}')
                return cell.value

        if row:
            return [mkval(c) for d in dir(row)
                    if d.startswith('technique')
                    for c in (getattr(row, d)(),)
                    if c.value and c.value.lower() != 'na']
        else:
            return []

    def protocol_uris(self, dataset_id):
        row = self._lookup(dataset_id)

        def mkval(cell):
            hl = cell.hyperlink
            cv = cell.value
            if hl is None:
                hl = cv if cv else None

            if hl is not None:
                try:
                    return idlib.Pio(hl)
                except idlib.exc.IdlibError as e:
                    try:
                        return idlib.Doi(hl)
                    except idlib.exc.IdlibError as e:
                        # XXX WARNING the fall through here
                        # kicks the can down the road
                        pass

            logd.warning(f'unhandled value {cell.value!r}')
            return cv

        if row:
            skip = '"none"', 'NA', 'na', 'no protocols', 'take protocol from other spreadsheet, '
            return [mkval(c) for d in dir(row)
                    if d.startswith('protocol')
                    for c in (getattr(row, d)(),)
                    if c.value and c.value not in skip]
        else:
            return []


def mkval_tech(cell):
    hl = cell.hyperlink
    if hl is not None:
        oid = OntId(hl)
        if oid.prefix == 'TEMP':
            logd.warning(f'{cell.value} -> {oid!r}')
            #return OntTerm(curie=f'lex:{quote(cell.value)}')
        #else:

        return oid.asTerm()

    else:
        logd.warning(f'unhandled technique {cell.value}')
        return cell.value


def main():
    # simple testing
    o = Organs()
    print(o.values[0])


if __name__ == '__main__':
    main()
