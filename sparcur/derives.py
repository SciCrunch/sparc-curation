from functools import wraps
from sparcur import normalization as nml
from typing import Tuple


def collect(*oops, unpacked=False):
    def decorator(generator_function):
        @wraps(generator_function)
        def inner(*args, **kwargs):
            out = tuple(generator_function(*args, **kwargs))
            if unpacked:
                return out
            else:
                return out,

        return inner

    if oops:
        generator_function, = oops
        return decorator(generator_function)
    else:
        return decorator


class Derives:
    """ all derives must return as a tuple, even if they are length 1 """

    @staticmethod
    def contributor_name(name) -> Tuple[str, str]: 
        if ',' in name:
            last, first = name.split(', ', 1)
        elif ' ' in name:
            first, last = name.split(' ', 1)
        else:
            first = 'ERROR IN NAME see lastName for content'
            last = name

        return first, last

    @staticmethod
    @collect
    def creators(contributors):
        for c in contributors:
            if 'contributor_role' in c and 'Creator' in c['contributor_role']:
                # FIXME diry diry mutation here that should happen in a documente way
                cont = {**c}  # instead of importing copy since this is a one deep
                cont.pop('contributor_role', None)
                cont.pop('is_contact_person', None)
                yield cont

    @staticmethod
    def award_number(raw_award_number, funding) -> Tuple[str]:
        return nml.NormAward(nml.NormAward(raw_award_number)),

    def _old_an():
        # old ... # but these do show that we need multi-source derives
        @property
        def award(self):
            for award in self._award_raw:
                yield nml.NormAward(nml.NormAward(award))

        @property
        def _award_raw(self):
            for s in self.submission:
                dict_ = s.data
                if 'sparc_award_number' in dict_:
                    yield dict_['sparc_award_number']
                #if 'SPARC Award number' in dict_:
                    #yield dict_['SPARC Award number']

            for d in self.dataset_description:
                dict_ = d.data
                if 'funding' in dict_:
                    yield dict_['funding']

    @staticmethod
    @collect
    def principal_investigator(contributors):
        mp = 'contributor_role'
        os = ('PrincipalInvestigator',)
        for contributor in contributors:
            if mp in contributor:
                for role in contributor[mp]:
                    normrole = nml.NormContributorRole(role)
                    if 'name' in contributor:
                        # when you look at this in confusion, realize that it is exactly
                        # as silly as you think it is
                        fn, ln = Derives.contributor_name(contributor['name'])
                        contributor['first_name'] = fn
                        contributor['last_name'] = ln
                        if normrole in os:
                            #print(contributor)
                            for s, p, o in self.triples_contributors(contributor):
                                if p == a and o == owl.NamedIndividual:
                                    pis.appned(s)
                                    yield s


    @staticmethod
    def dataset_species(subjects) -> Tuple[tuple]:
        out = set()
        for subject in subjects:
            if 'species' in subject:
                out.add(subject['species'])

        return tuple(out),
