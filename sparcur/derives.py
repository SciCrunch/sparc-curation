import pprint
from typing import Tuple
from functools import wraps
from sparcur import schemas as sc
from sparcur import normalization as nml
from sparcur.core import log, logd, JPointer


def collect(*oops, unpacked=True):
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
    def award_number(raw_award_number, funding) -> str:
        return nml.NormAward(nml.NormAward(raw_award_number))

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
    def pi(contributors):
        pointers = [JPointer(f'/contributors/{i}')
                    for i, c in enumerate(contributors)
                    if (('PrincipalInvestigator' in c['contributor_role'])
                        if 'contributor_role' in c else False)]

        if len(pointers) == 1:
            pointer, = pointers
            return pointer

        return pointers

    @staticmethod
    @collect
    def __principal_investigator(contributors):
        mp = 'contributor_role'
        os = ('PrincipalInvestigator',)
        for contributor in contributors:
            if mp in contributor:
                for role in contributor[mp]:
                    normrole = nml.NormContributorRole(role)
                    if normrole in os:
                        yield contributor

                    continue
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

        if len(out) == 1:
            return next(iter(out))

        return tuple(out)

    @staticmethod
    def submission_completeness_index(schema, subschemas, inputs):
        log.debug(pprint.pformat(inputs))
        def section_dsci(schema, section):
            if 'errors' not in section:
                return 1

            total_possible_errors = schema.total_possible_errors
            number_of_errors = len(section['errors'])
            return (total_possible_errors - number_of_errors) / total_possible_errors

        #schema = sc.DatasetOutSchema()
        total_possible_errors = schema.total_possible_errors
        data = inputs
        if not data:
            return 0

        else:
            actual_errors = 0
            for required_field in schema.schema['required']:
                jtype = schema.schema['properties'][required_field]
                actual_errors += 1
                if required_field in data:
                    required_value = data[required_field]
                    if jtype['type'] == 'object':
                        if isinstance(required_value, dict):
                            subschema = subshcemas[required_field]
                            actual_errors -= section_dsci(required_value)

            return (total_possible_errors - actual_errors) / total_possible_errors
