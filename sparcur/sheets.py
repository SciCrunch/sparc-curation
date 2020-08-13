from collections import defaultdict
from functools import wraps
import idlib
from pyontutils.sheets import Sheet
from sparcur.core import adops, OntId, OntTerm
from sparcur.utils import log, logd
from urllib.parse import quote

# google sheets

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
        return {a:idlib.Ror(r) if r else None
                for a, r in zip(self.byCol.affiliation_string,
                                self.byCol.ror_id)}


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
    def makeReportSheet(cls, *index_cols):
        """ Decorator to build a Sheet class for a cli.Report method

            NOTE reports that do not have index columns will update
            the entire sheet every time, so probabaly don't edit those
        """
        def _rowcellify(cell_value):
            if isinstance(cell_value, idlib.Stream):
                try:
                    return cell_value.asUri()
                except AttributeError as e:
                    breakpoint()
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
                sheet_name = method.__name__
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
                else:
                    msg = 'Should not have extension and export to sheets.'
                    raise TypeError(msg)

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
                    rows_stringified = rowcellify(rows)
                    if report.sheetId() is None:
                        report.createRemoteSheet()
                        report.update(rows_stringified)
                    else:
                        report.fetch()
                        if report.index_columns:
                            report.upsert(*rows_stringified[1:])
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

    def _annotation_row(self, anno):
        key = self.index_columns[0]
        value = getattr(anno, key)
        value = value.strip()  # FIXME align with id normalization maybe?

        header = self.row_object(0)
        #index = getattr(header, key)().column  # ;_; but it was so elegant
        #row = getattr(index, value)().row
        if key in header._trouble:
            index = header._trouble[key]().column
        else:
            index = getattr(header, key)().column

        if value in index._trouble:
            row = index._trouble[value]
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
                    raise

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
        row = self._annotation_row(anno)
        bad = row.bad().value
        help__ = row.help__().value
        map_to = row.map_to().value
        merge = row.merge().value

        if not (bad or help__):
            if map_to:
                if map_to == '-':
                    suffix = getattr(anno, self.index_cols[0])
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
                suffix = getattr(anno, self.index_cols[0])
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
        manual_mapping = row.manual_mapping().value
        if mapping_ok and not not_input:
            pass

        if manual_mapping and ' ' in manual_mapping:
            log.error(f'Why does a manual mapping have a space in it {manual_mapping!r}')

        elif manual_mapping:
            return OntTerm(manual_mapping)

        elif mapping_ok:  # FIXME anno.astValue can drift from auto_mapping
            # this is so hilariously inefficient, we parse the same stuff
            # 3 times or something
            return OntTerm(anno.asPython().asPython().black_box.curie)


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
    fetch_grid = True
    #fetch_grid = False

    def _lookup(self, dataset_id, fail=False, raw=True):
        try:
            row = self.byCol.searchIndex('id', dataset_id, raw=raw)
            return row
        except KeyError as e:
            # TODO update the sheet automatically
            log.critical(f'New dataset! {dataset_id}')
            if fail:
                raise e

    def _dataset_row_index(self, dataset_id):
        # FIXME dict or cache this for performance
        row = self._lookup(dataset_id, fail=True)
        return self.values.index(row)

    def _update_dataset_metadata(self, id, name, award):
        try:
            row_index = self._dataset_row_index(id)
            row = self.row_object(row_index)

            cell_dsn = row.dataset_name()
            if cell_dsn.value != name:
                cell_dsn.value = name

            cell_award = row.award()
            if cell_award.value != award:
                cell_award.value = award

        except KeyError as e:
            row = ['', '', name, id, award]
            self._appendRow(row)

    def update_from_ir(self, ir):
        dataset_blobs = ir['datasets']
        self._wat = self.values[8]
        for blob in dataset_blobs:
            meta = blob['meta']
            self._update_dataset_metadata(
                id=blob['id'],
                name=adops.get(blob, ['meta', 'folder_name'], on_failure=''),
                award=adops.get(blob, ['meta', 'award_number'], on_failure=''),
            )

        #log.debug(self.uncommitted())
        self.commit()

    def modality(self, dataset_id):
        """ tuple of modalities """
        row = self._lookup(dataset_id)
        if row:
            try:
                m1 = self.byCol.header.index('modality1')
                m2 = self.byCol.header.index('modality2')
                return tuple(_ for _ in (row[m1], row[m2]) if _)
            except AttributeError as e:
                raise ValueError(f'issue in {self.name} {self.sheet_name}') from e

    def organ_term(self, dataset_id):
        row = self._lookup(dataset_id)
        organ_term = self.byCol.header.index('organ_term')
        if row:
            ot = row[organ_term] if row[organ_term] else None
            if ot:
                try:
                    ts = tuple(OntId(t) for t in ot.split(' ') if t and t.lower() != 'na')
                    return ts
                except OntId.BadCurieError:
                    log.error(ot)

    def award(self, dataset_id):
        row = self._lookup(dataset_id)
        award = self.byCol.header.index('award')
        award_manual = self.byCol.header.index('award_manual')
        if row:
            return row[award] if row[award] else (row[award_manual] if row[award_manual] else None)
        
    def award_machine(self, dataset_id):
        row = self._lookup(dataset_id)
        award = self.byCol.header.index('award')
        if row:
            return row[award] if row[award] else None

    def award_manual(self, dataset_id):
        row = self._lookup(dataset_id)
        award_manual = self.byCol.header.index('award_manual')
        if row:
            return row[award_manual] if row[award_manual] else None

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
            row_index = self._dataset_row_index(dataset_id)
            return [mkval(self.cell_object(row_index, column_index))
                    for column_index, (field, value)
                    in enumerate(zip(self.byCol.header, row))
                    if field.startswith('technique') and
                    value and
                    value.lower() != 'na']
        else:
            return []

    def protocol_uris(self, dataset_id):
        row = self._lookup(dataset_id)

        def mkval(cell):
            hl = cell.hyperlink
            if hl is not None:
                return AutoId(hl)

            else:
                logd.warning(f'unhandled value {cell.value}')
                return cell.value

        if row:
            skip = '"none"', 'NA', 'no protocols', 'take protocol from other spreadsheet, '
            row_index = self._dataset_row_index(dataset_id)
            return [mkval(self.cell_object(row_index, column_index))
                    for column_index, (field, value)
                    in enumerate(zip(self.byCol.header, row))
                    if field.startswith('Protocol') and value and value not in skip]
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


def mkval_pu(cell):
    hl = cell.hyperlink
    if hl is not None:
        return AutoId(hl)

    else:
        logd.warning(f'unhandled value {cell.value}')
        return cell.value
