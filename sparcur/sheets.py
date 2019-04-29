from pyontutils.sheets import Sheet

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

    def modality(self, dataset_id):
        """ tuple of modalities """
        row = self.byCol.searchIndex('id', dataset_id)
        return tuple(_ for _ in (row.modality, modality_2) if _)

