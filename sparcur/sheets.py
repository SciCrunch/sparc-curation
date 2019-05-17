from pyontutils.core import OntId
from pyontutils.sheets import Sheet
from sparcur.utils import log

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

    def _lookup(self, dataset_id):
        try:
            return self.byCol.searchIndex('id', dataset_id)
        except KeyError as e:
            # TODO update the sheet automatically
            log.critical(f'New dataset! {dataset_id}')

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
