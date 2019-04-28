""" string normalizers, strings that change their content to match a standard """

from sparcur.core import log


class NormSimple(str):
    def __new__(cls, value):
        return str.__new__(cls, cls.normalize(value))

    @classmethod
    def normalize(cls, value, preserve_case=False):
        v = value if preserve_case else value.lower()
        if v in cls.data:
            return cls.data[v]
        elif preserve_case:
            return value
        else:
            return v

class NormAward(NormSimple):
    data = {
        '1 OT2 OD23853': 'OT2OD023853',  # someone's university database stripped a leading zero
        #'grantOT2OD02387101S2': '',
    }
    @classmethod
    def normalize(cls, value):
        value = super().normalize(value, preserve_case=True)
        if 'OT2' in value and 'OD' not in value:
            # one is missing the OD >_<
            log.warning(value)
            value = value.replace('-', '-OD')  # hack

        n = (value
             .strip()
             .replace('-', '-')  # can you spot the difference?
             .replace('(', '')
             .replace(')', '')
             .replace('-01S1', '')
             .replace('-01', '')
             .replace('-02S2', '')
             .replace('-02', '')
             .replace('SPARC', '')
             .replace('NIH-1', '')
             .replace('NIH-', '')
             .replace('-', '')
             .replace('NIH ', '')
             .replace(' ', ''))
        if n[0] in ('1', '3', '5'):
            n = n[1:]

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


class NormContributorRole(str):
    values = ('ContactPerson',
              'DataCollector',
              'DataCurator',
              'DataManager',
              'Distributor',
              'Editor',
              'HostingInstitution',
              'PrincipalInvestigator',  # added for sparc map to ProjectLeader probably?
              'Creator',  # this is a separate field in datacite so we will need lift on export
              'Producer',
              'ProjectLeader',
              'ProjectManager',
              'ProjectMember',
              'RegistrationAgency',
              'RegistrationAuthority',
              'RelatedPerson',
              'Researcher',
              'ResearchGroup',
              'RightsHolder',
              'Sponsor',
              'Supervisor',
              'WorkPackageLeader',
              'Other',)

    def __new__(cls, value):
        return str.__new__(cls, cls.normalize(value))

    @staticmethod
    def levenshteinDistance(s1, s2):
        if len(s1) > len(s2):
            s1, s2 = s2, s1

        distances = range(len(s1) + 1)
        for i2, c2 in enumerate(s2):
            distances_ = [i2+1]
            for i1, c1 in enumerate(s1):
                if c1 == c2:
                    distances_.append(distances[i1])
                else:
                    distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])))
            distances = distances_
        return distances[-1]

    @classmethod
    def normalize(cls, value):
        # a hilariously slow way to do this
        # also not really normalization ... more, best guess for what people were shooting for
        if value:
            return sorted((cls.levenshteinDistance(value, v), v) for v in cls.values)[0][1]

