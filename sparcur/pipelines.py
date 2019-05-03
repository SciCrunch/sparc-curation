from sparcur import schemas as sc
from sparcur import validate as vldt
from sparcur.derives import Derives
from sparcur.core import DictTransformer, get_all_errors
from sparcur.core import JT, JEncode, log, logd, lj

DT = DictTransformer
De = Derives

hasSchema = vldt.HasSchema()
@hasSchema.mark
class Pipeline:
    # we can't quite do this yet, but these pipelines are best composed
    # by subclassing and then using super().previous_step to insert another
    # step along the way

    class SkipPipelineError(Exception):
        """ go directly to the end we are done here """
        def __init__(self, data):
            self.data = data

    def __init__(self, sources, lifters, helper_key,
                 runtime_context):
        """ sources stay, helpers are tracked for prov and then disappear """
        #log.debug(lj(sources))
        self.helper_key = helper_key
        self.lifters = lifters
        self.pipeline_start = sources
        self.runtime_context = runtime_context

    @property
    def data_lifted(self):
        """ doing this pipelines this way for now :/ """

        def section_binder(section):
            def lift_function(_, section=section):
                # throw 'em in a list and let the schema sort 'em out!
                dwe = [section.data_with_errors
                       for section in getattr(self.lifters, section)]
                if dwe:
                    dwe, *oops = dwe
                    if oops:
                        logd.error(f'{self.runtime_context.path} '
                                   f'has too may {section} files!')
                    return dwe

            return lift_function

        lifts = [[[section], func]
                 for section in ['submission', 'dataset_description', 'subjects']
                 for func in (section_binder(section),) if func]

        data = self.pipeline_start
        if 'errors' in data and len(data) == 1:
            raise self.SkipPipelineError(data)

        DictTransformer.lift(data, lifts)
        return data

    @property
    def data_derived_pre(self):
        # FIXME do we really need this ??
        # which copy steps actually depend on it??
        # in this case it is just easier to get some values
        # before moving everything around, provides
        # a way to operate on either side of an impenance mismatch
        # so if something changes, you just added another layer
        derives = [[[['submission', 'submission', 'sparc_award_number'],
                     ['dataset_description', 'funding']],
                    Derives.award_number,
                    [['meta', 'award_number']]],

                   [[['dataset_description', 'contributors']],
                    lambda cs: [DT.derive(c, [[[['name']],  # FIXME [['name]] as missing a nesting level
                                               De.contributor_name,  # and we got no warning ... :/
                                               [['first_name'], ['last_name']]]])
                                for c in cs],
                    []],
        ]
        data = self.data_lifted
        DictTransformer.derive(data, derives, source_key_optional=True)
        return data

    @property
    def data_copied(self):
        copies = ([['dataset_description', 'contributors'], ['contributors']],
                  [['subjects',], ['inputs', 'subjects']],)
        # meta copies
        copies += tuple([['dataset_description', source], ['meta', target]]
                        for source, target in
                        [[direct, direct] for direct in
                          ['principal_investigator',
                           'species',
                           'organ',
                           'modality',
                           'protocol_url_or_doi',

                           'completeness_of_dataset',
                           'funding',
                           'description',
                           'additional_links',
                           'keywords',
                           'acknowledgements',
                           'originating_article_doi',
                          ]] + [])

        data = self.data_derived_pre
        DictTransformer.copy(data, copies, source_key_optional=True)
        return data

    @property
    def data_moved(self):
        moves = ([['dataset_description',], ['inputs', 'dataset_description']],
                 [['subjects', 'software'], ['resources']],  # FIXME update vs replace
                 [['subjects'], ['subjects_file']],  # first step in ending the confusion
                 [['subjects_file', 'subjects'], ['subjects']],
                 [['submission',], ['inputs', 'submission']],)
        # contributor derives
        data = self.data_copied
        DictTransformer.move(data, moves, source_key_optional=True)
        return data

    @property
    def data_lifted_post(self):
        # essentially stateful injection of new data
        # In theory we could include ALL the prior data
        # in one big blob at the start and then never use lifts
        def pi(contributor):
            if 'contributor_orcid_id' in contributor:
                return contributor['contributor_orcid_id']

            # get member if we can find them
            elif 'name' in contributor and 'first_name' in contributor:
                fn = contributor['first_name']
                ln = contributor['last_name']
                if ' ' in fn:
                    fn, mn = fn.split(' ', 1)

                failover = f'{fn}-{ln}'
                member = self.lifters.member(fn, ln)

                if member is not None:
                    userid = rdflib.URIRef('https://api.blackfynn.io/users/' + member.id)

                return userid

            else:
                member = None
                failover = 'no-orcid-no-name'
                log.warning(f'No name!' + lj(contributor))
                return rdflib.URIRef(dsid + '/contributors/' + failover)

            
        lifts = [[['meta', 'principal_investigator'], pi],]
        data = self.data_moved
        DictTransformer.lift(data, lifts, source_key_optional=True)
        return data

    @property
    def data_derived_post(self):
        DT = DictTransformer
        De = Derives
        derives = ([[['contributors']],
                    Derives.creators,
                    [['creators']]],

                   [[['contributors']],
                    len,
                    ['meta', 'contributor_count']],

                   [[['subjects']],
                    Derives.dataset_species,
                    ['meta', 'species']],

                   [[['subjects']],
                    len,
                    ['meta', 'subject_count']],

                   )

        todo = [[[['samples']],
                 len,
                 [['meta', 'sample_count']]] ,
        ]

        data = self.data_lifted_post
        DictTransformer.derive(data, derives)
        return data

    @property
    def data_cleaned(self):
        """ clean up any cruft left over from transformations """


        pops = [['subjects_file'],
        ]
        data = self.data_derived_post
        removed = list(DictTransformer.pop(data, pops, source_key_optional=True))
        log.debug(f'cleaned the following values from {self}' + lj(removed))

        # FIXME post manual fixes ...

        # remote Creator since we lift it out
        if 'contributors' in data:
            for c in data['contributors']:
                if 'contributor_role' in c:
                    cr = c['contributor_role']
                    if 'Creator' in cr:
                        c['contributor_role'] = tuple(r for r in cr if cr != 'Creator')
                        

        return data

    @property
    def data_added(self):
        # FIXME this would seem to be where the Integration class comes in?
        # the schemas make it a bit of a dance
        adds = [[['id'], self.lifters.id],
                [['meta', 'uri_human'], self.lifters.uri_human]]
        data = self.data_cleaned
        DictTransformer.add(data, adds)
        return data

    @hasSchema(sc.DatasetOutSchema)
    def data_out(self):
        """ this part adds the meta bits we need after _with_errors
            and rearranges anything that needs to be moved about """

        try:
            data = self.data_added
        except self.SkipPipelineError as e:
            data = e.data
            data['id'] = self.lifters.id
            data['meta'] = {}
            data['meta']['uri_human'] = self.lifters.uri_human
            data['meta']['uri_api'] = self.lifters.uri_api
            data['error_index'] = 9999
            data['submission_completeness_index'] = 0
            log.debug('pipeline skipped to end due to errors')

        if 'award_number' not in data['meta']:
            am = self.lifters.award_manual
            if am:
                data['meta']['award_number'] = am
        if 'modality' not in data['meta']:
            m = self.lifters.modality
            if m:
                data['meta']['modality'] = m

        return data

    @property
    def data_post(self):
        # FIXME a bit of a hack
        data = self.data_out

        if 'error_index' not in data:
            ei = len(get_all_errors(data))
            if 'inputs' in data:
                #schema = self.__class__.data_out.schema
                schema = sc.DatasetSchema()
                subschemas = {'dataset_description':sc.DatasetDescriptionSchema(),
                            'submission':sc.SubmissionSchema(),
                            'subjects':sc.SubjectsSchema(),}
                sci = Derives.submission_completeness_index(schema, subschemas, data['inputs'])
            else:
                ei = 9999  # datasets without metadata can have between 0 and +inf.0 so 9999 seems charitable :)
                sci = 0

            data['error_index'] = ei
            data['submission_completeness_index'] = sci

        return data

    @property
    def data(self):
        return self.data_post


class PipelineExtras(Pipeline):

    @property
    def data_derived_post(self):
        derives = [[[['meta', 'award_number']],
                    lambda an: (self.lifters.organ(an),),
                    [['meta', 'organ']]]]
        data = super().data_derived_post
        DictTransformer.derive(data, derives)
        return data

    @property
    def data_added(self):
        adds = []
        todo = [[['meta', 'modality'], 'TODO should come from sheets maybe?']]
        data = super().data_added
        DictTransformer.add(data, adds)
        return data
