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
                if 'ext' not in kwargs or kwargs['ext'] is None:
                    kwargs['ext'] = True
                else:
                    msg = 'Should not have extension and export to sheets.'
                    raise TypeError(msg)

                out = method(self, *args, **kwargs)

                if self.options.to_sheets:
                    if tags:
                        if args:
                            method._report_class.sheet_name = args[0]
                        else:
                            method._report_class.sheet_name = self.options.tag[0]

                    rows, title = out
                    # fetch=False since the remote sheet may not exist
                    # TODO improve the ergonimcs here
                    report = method._report_class(fetch=False, readonly=False)
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


class AnnoTags(Reports):
    name = 'anno-tags'


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
                ts = tuple(OntId(t) for t in ot.split(' ') if t and t.lower() != 'na')
                return ts

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
