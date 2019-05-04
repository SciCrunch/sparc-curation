from sparcur import schemas as sc
from sparcur import validate as vldt
from sparcur.derives import Derives
from sparcur.core import DictTransformer, copy_all, get_all_errors
from sparcur.core import JT, JEncode, log, logd, lj, JPointer

DT = DictTransformer
De = Derives

class Pipeline:
    """ base pipeline """


class PrePipeline(Pipeline):
    """ base for pre-json pipelines """


hasSchema = vldt.HasSchema()
@hasSchema.mark
class JSONPipeline(Pipeline):
    # we can't quite do this yet, but these pipelines are best composed
    # by subclassing and then using super().previous_step to insert another
    # step along the way

    adds = []
    lifts = []
    copies = []
    moves = []
    derives = []
    cleans = []

    class SkipPipelineError(Exception):
        """ go directly to the end we are done here """
        def __init__(self, data):
            self.data = data

    def __init__(self, previous_pipeline, lifters, runtime_context):
        """ sources stay, helpers are tracked for prov and then disappear """
        #log.debug(lj(sources))
        self.previous_pipeline = previous_pipeline
        self.lifters = lifters
        self.runtime_context = runtime_context

    @property
    def pipeline_start(self):
        """ cooperate with HasSchema input schema validation """
        # pipeline start is whatever starting data you need

        return self.previous_pipeline.data

    @property
    def lifted(self):
        """ use contents of pipeline start + lifters to populate the data """
        # lifters are really 'run other pipelines'
        # or run_subpipelines
        data = self.pipeline_start
        DictTransformer.lift(data, [[path, function(self.lifters)]
                                    for path, function in self.lifts])
        return data

    @property
    def copied(self):
        data = self.lifted
        DictTransformer.copy(data, self.copies, source_key_optional=True)
        return data

    @property
    def moved(self):
        data = self.copied
        DictTransformer.move(data, self.moves, source_key_optional=True)
        return data

    @property
    def cleaned(self):
        data = self.moved
        DictTransformer.delete(data, self.cleans, source_key_optional=True)
        return data

    @property
    def augmented(self):
        data = self.cleaned
        DictTransformer.derive(data, self.derives, source_key_optional=True)
        return data

    @property
    def pipeline_end(self):
        """ pipeline skip lands here """
        try:
            return self.augmented
        except self.SkipPipelineError as e:
            # we don't catch here because each class
            # must determine how it handles pipeline errors
            # data = e.data
            raise e

    @property
    def added(self):
        """ bind referencing data such as ids or uris regardless
            or anything else that should happen regardless of whether
            pipelines failed or not """

        data = self.pipeline_end
        adds = [[path, function(self.lifters)] for path, function in self.adds]
        DictTransformer.add(data, adds)
        return data

    #@hasSchema(sc.JSONSchema)  # XXX put your schema here
    # other schemas can be used at other steps, but this is the convention
    @property
    def data(self):
        return self.added


def section_pipelines(*sections):
    # FIXME deprecate this ...
    def section_binder(section):
        def lift_wrapper(lifters):
            def lift_function(__unused_path):
                # throw 'em in a list and let the schema sort 'em out!
                dwe = [section.data_with_errors
                        for section in getattr(lifters, section)]
                if dwe:
                    dwe, *oops = dwe
                    if oops:
                        logd.error(f'{self.runtime_context.path} '
                                    f'has too may {section} files!')
                    return dwe

            return lift_function

        return lift_wrapper

    return [[[section], section_binder(section)]
            for section in sections]


hasSchema = vldt.HasSchema()
@hasSchema.mark
class SPARCBIDSPipeline(JSONPipeline):
    lifts = section_pipelines('submission', 'dataset_description', 'subjects')

    copies = ([['dataset_description', 'contributors'], ['contributors']],
                [['subjects',], ['inputs', 'subjects']],
                *copy_all(['dataset_description'], ['meta'],
                        'species',  # TODO validate all source paths against schema
                        'organ',
                        'modality',
                        'protocol_url_or_doi',

                        'completeness_of_dataset',
                        'funding',
                        'description',
                        'additional_links',
                        'keywords',
                        'acknowledgements',
                        'originating_article_doi',))

    moves = ([['dataset_description',], ['inputs', 'dataset_description']],
                [['subjects', 'software'], ['resources']],  # FIXME update vs replace
                [['subjects'], ['subjects_file']],  # first step in ending the confusion
                [['subjects_file', 'subjects'], ['subjects']],
                [['submission',], ['inputs', 'submission']],)

    derives = ([[['inputs', 'submission', 'submission', 'sparc_award_number'],
                 ['inputs', 'dataset_description', 'funding']],
                Derives.award_number,
                [['meta', 'award_number']]],

               [[['contributors']],
                lambda cs: [DT.derive(c, [[[['name']],  # FIXME [['name]] as missing a nesting level
                                           De.contributor_name,  # and we got no warning ... :/
                                           [['first_name'], ['last_name']]]])
                            for c in cs],
                []],

               [[['contributors']],
                lambda cs: [JPointer('/contributors/{i}') for i, c in enumerate(cs)
                            if 'PrincipalInvestigator' in
                            [r for t in list(DT.get(c, [['contributor_role']],
                                                    source_key_optional=True))
                             for r in t]]
                ['meta', 'principal_investigator']],

               [[['contributors']],
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

    cleans = [['subjects_file'],]

    adds = [[['id'], lambda lifters: lifters.id],
            [['meta', 'uri_human'], lambda lifters: lifters.uri_human],
            [['meta', 'uri_api'], lambda lifters: lifters.uri_api],]

    @property
    def lifted(self):
        """ doing this pipelines this way for now :/ """

        data = self.pipeline_start
        if 'errors' in data and len(data) == 1:
            raise self.SkipPipelineError(data)

        return super().lifted
        #lifts = [[path, function(self.lifters)] for path, function in self.lifts]
        #DictTransformer.lift(data, lifts)
        #return data

    @property
    def _copied(self):
        # meta copies
        _copies = tuple([['dataset_description', source_target], ['meta', source_target]]
                        for source_target in
                          ['species',  # TODO validate all source paths against schema
                           'organ',
                           'modality',
                           'protocol_url_or_doi',

                           'completeness_of_dataset',
                           'funding',
                           'description',
                           'additional_links',
                           'keywords',
                           'acknowledgements',
                           'originating_article_doi',]
                        + [])

        data = self.data_derived_pre
        DictTransformer.copy(data, copies, source_key_optional=True)
        return data

    @property
    def _data_moved(self):
        # contributor derives
        data = self.data_copied
        DictTransformer.move(data, moves, source_key_optional=True)
        return data

    @property
    def _data_lifted_post(self):  # I think we can just skip this and use json pointer?
        # essentially stateful injection of new data
        # In theory we could include ALL the prior data
        # in one big blob at the start and then never use lifts
        def pi_lift_wrapper(lifters):
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
                    member = lifters.member(fn, ln)

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
    def _derived_pre(self):
        # FIXME do we really need this ??
        # which copy steps actually depend on it??
        # in this case it is just easier to get some values
        # before moving everything around, provides
        # a way to operate on either side of an impenance mismatch
        # so if something changes, you just added another layer
        data = self.lifted
        DictTransformer.derive(data, derives, source_key_optional=True)
        return data

    @property
    def _data_derived_post(self):
        todo = [[[['samples']],
                 len,
                 [['meta', 'sample_count']]] ,
        ]

        data = self.data_lifted_post
        DictTransformer.derive(data, derives)
        return data

    @property
    def cleaned(self):
        """ clean up any cruft left over from transformations """

        data = self.moved
        removed = list(DictTransformer.pop(data, self.cleans, source_key_optional=True))
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
    def pipeline_end(self):
        """ this part adds the meta bits we need after _with_errors
            and rearranges anything that needs to be moved about """

        try:
            data = super().pipeline_end
        except self.SkipPipelineError as e:
            data = e.data
            #data['id'] = self.lifters.id
            data['meta'] = {}
            #data['meta']['uri_human'] = self.lifters.uri_human
            #data['meta']['uri_api'] = self.lifters.uri_api
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
    def _data_added(self):
        # FIXME this would seem to be where the Integration class comes in?
        # the schemas make it a bit of a dance
        data = self.data_cleaned
        DictTransformer.add(data, adds)
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

    @hasSchema(sc.DatasetOutSchema)
    def data(self):
        return super().data
        #return self.data_post


class PipelineExtras(SPARCBIDSPipeline):
    # subclassing allows us to pop additional steps
    # before or after their super()'s name

    @property
    def data_derived_post(self):
        todo = [[['meta', 'modality'], 'TODO should come from sheets maybe?']]
        derives = [[[['meta', 'award_number']],
                    lambda an: (self.lifters.organ(an),),
                    [['meta', 'organ']]]]
        data = super().data_derived_post
        DictTransformer.derive(data, derives)
        return data

    @property
    def _added(self):
        adds = []
        data = super().added
        DictTransformer.add(data, adds)
        return data
