from pyontutils.sheets import Sheet
from sparcur.core import OntId, OntTerm
from sparcur.utils import log, logd
from urllib.parse import quote

# google sheets

# master
class Master(Sheet):
    name = 'sparc-master'


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


# reports
class CurationReports(Sheet):
    name = 'sparc-curation-reports'


class Completeness(Sheet):
    sheet_name = 'Completeness'


# affiliations
class Affiliations(Sheet):
    name = 'sparc-affiliations'
    sheet_name = 'Affiliations'
    index_columns = 'affiliation_string',


# field alignment
class FieldAlignment(Sheet):
    name = 'sparc-field-alignment'


class Subjects(FieldAlignment):
    sheet_name = 'Subjects'


class Keywords(FieldAlignment):
    sheet_name = 'All Keywords'


class KeywordSets(FieldAlignment):
    sheet_name = 'Keyword sets'


class Organs(FieldAlignment):
    sheet_name = 'Organs'
    index_columns = 'id',
    fetch_grid = True

    def _lookup(self, dataset_id):
        try:
            return self.byCol.searchIndex('id', dataset_id)
        except KeyError as e:
            # TODO update the sheet automatically
            log.critical(f'New dataset! {dataset_id}')

    def _dataset_row_index(self, dataset_id):
        # FIXME dict or cache this for performance
        for i, id in enumerate(self.byCol.id):
            if id == dataset_id:
                return i + 1

    def modality(self, dataset_id):
        """ tuple of modalities """
        row = self._lookup(dataset_id)
        if row:
            return tuple(_ for _ in (row.modality, row.modality_2) if _)

    def organ_term(self, dataset_id):
        row = self._lookup(dataset_id)
        if row:
            ot = row.organ_term if row.organ_term else None
            if ot:
                ts = tuple(OntId(t) for t in ot.split(' ') if t)
                return ts

    def award(self, dataset_id):
        row = self._lookup(dataset_id)
        if row:
            return row.award if row.award else (row.award_manual if row.award_manual else None)
        
    def award_machine(self, dataset_id):
        row = self._lookup(dataset_id)
        if row:
            return row.award if row.award else None

    def award_manual(self, dataset_id):
        row = self._lookup(dataset_id)
        if row:
            return row.award_manual if row.award_manual else None

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

                return oid.asTerm

            else:
                logd.warning(f'unhandled technique {cell.value}')
                return cell.value

        if row:
            row_index = self._dataset_row_index(dataset_id)
            return [mkval(self.cell_object(row_index, column_index))
                    for column_index, (field, value)
                    in enumerate(zip(row._fields, row))
                    if field.startswith('technique') and value]
        else:
            return []

    def protocol_uris(self, dataset_id):
        row = self._lookup(dataset_id)

        def mkval(cell):
            hl = cell.hyperlink
            if hl is not None:
                return hl

            else:
                logd.warning(f'unhandled value {cell.value}')
                return cell.value

        if row:
            row_index = self._dataset_row_index(dataset_id)
            return [mkval(self.cell_object(row_index, column_index))
                    for column_index, (field, value)
                    in enumerate(zip(row._fields, row))
                    if field.startswith('Protocol') and value]
        else:
            return []

