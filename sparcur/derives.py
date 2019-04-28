from sparcur import normalization as nml


class Derives:
    @staticmethod
    def contributor_name(name):
        if ',' in name:
            last, first = name.split(', ', 1)
        elif ' ' in name:
            first, last = name.split(' ', 1)
        else:
            first = 'ERROR IN NAME see lastName for content'
            last = name

        return first, last

    @staticmethod
    def award_number(raw_award_number):
        nml.NormAward(nml.NormAward(raw_award_number)),

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
                                    yield s

    @staticmethod
    def dataset_species(subjects):
        out = set()
        for subject in subjects:
            if 'species' in subject:
                out.add(subject['species'])

        return tuple(out)
