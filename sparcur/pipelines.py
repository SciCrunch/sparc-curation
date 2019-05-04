from pathlib import Path
from sparcur import schemas as sc
from sparcur import validate as vldt
from sparcur import datasets as dat
from sparcur.derives import Derives
from sparcur.core import DictTransformer, copy_all, get_all_errors
from sparcur.core import JT, JEncode, log, logd, lj, JPointer

DT = DictTransformer
De = Derives


class Pipeline:
    """ base pipeline """


class PrePipeline(Pipeline):
    """ base for pre-json pipelines """


class FilePipeline(PrePipeline):
    # FIXME this is a temporary solution to reuse the conversion existing
    data_transformer_class = None
    def __init__(self, previous_pipeline, lifters, runtime_context):
        log.debug(lj(previous_pipeline.data))
        self.path = Path(previous_pipeline.data['path'])

    @property
    def transformed(self):
        dt = self.data_transformer_class(self.path)
        return dt.data

    # @hasSchema(sc.TransformerSchema)
    @property  # transformer out schema goes here
    def data(self):
        return self.transformed


hasSchema = vldt.HasSchema()
@hasSchema.mark
class SubmissionFilePipeline(FilePipeline):

    data_transformer_class = dat.SubmissionFile

    @hasSchema(sc.SubmissionSchema)
    def data(self):
        return super().data


hasSchema = vldt.HasSchema()
@hasSchema.mark
class DatasetDescriptionFilePipeline(FilePipeline):

    data_transformer_class = dat.DatasetDescriptionFile

    @hasSchema(sc.DatasetSchema)
    def data(self):
        return super().data


hasSchema = vldt.HasSchema()
@hasSchema.mark
class SubjectsFilePipeline(FilePipeline):

    data_transformer_class = dat.DatasetDescriptionFile

    @hasSchema(sc.SubjectsSchema)
    def data(self):
        return super().data


hasSchema = vldt.HasSchema()
@hasSchema.mark
class JSONPipeline(Pipeline):
    # we can't quite do this yet, but these pipelines are best composed
    # by subclassing and then using super().previous_step to insert another
    # step along the way

    previous_pipeline_class = None
    # If previous_pipeline_class is set then this pipeline is run on the previous_pipeline given
    # to this class at init. It might be more accurate to call this the previous pipeline processor.
    # basically this is the 'intervening' pipeline between the previous pipeline and this pipeline
    # need a better name for it ...

    adds = []
    lifts = []
    subpipelines = []
    copies = []
    moves = []
    derives = []
    cleans = []

    class SkipPipelineError(Exception):
        """ go directly to the end we are done here """
        def __init__(self, data):
            self.data = data


    @classmethod
    def check(cls):
        assert not [d for d in cls.derives if len(d) != 3]

    def __init__(self, previous_pipeline, lifters, runtime_context):
        """ sources stay, helpers are tracked for prov and then disappear """
        #log.debug(lj(sources))
        if self.previous_pipeline_class is not None:
            previous_pipeline = self.previous_pipeline_class(previous_pipeline,
                                                             lifters,
                                                             runtime_context)

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
        DictTransformer.lift(data, [[path, function(self.lifters, self.runtime_context)]
                                    for path, function in self.lifts])
        return data

    @property
    def subpipelined(self):
        data = self.lifted
        DictTransformer.subpipeline(data, self.runtime_context, self.subpipelines)
        return data

    @property
    def copied(self):
        data = self.subpipelined
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
        removed = list(DictTransformer.pop(data, self.cleans, source_key_optional=True))
        log.debug(f'cleaned the following values from {self}' + lj(removed))
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
        def lift_wrapper(lifters, runtime_context):
            def lift_function(__unused_path):
                # throw 'em in a list and let the schema sort 'em out!
                dwe = [section.data_with_errors
                       for section in getattr(lifters, section)]
                if dwe:
                    dwe, *oops = dwe
                    if oops:
                        logd.error(f'{runtime_context.path} '
                                    f'has too may {section} files!')
                    return dwe

            return lift_function

        return lift_wrapper

    return [[[section], section_binder(section)]
            for section in sections]


hasSchema = vldt.HasSchema()
@hasSchema.mark
class SPARCBIDSPipeline(JSONPipeline):
    #lifts = section_pipelines('submission', 'dataset_description', 'subjects')

    subpipelines = [
        [[[['submission'], ['path']]], SubmissionFilePipeline, ['submission']],
        [[[['dataset_description'], ['path']]], DatasetDescriptionFilePipeline, ['dataset_description']],
        [[[['subjects'], ['path']]], SubjectsFilePipeline, ['subjects']],
    ]

    copies = ([['dataset_description', 'contributors'], ['contributors']],
              [['subjects',], ['inputs', 'subjects']],
              *copy_all(['dataset_description'], ['meta'],
                        'species',  # TODO validate all source paths against schema
                        'organ',
                        'modality',
                        'protocol_url_or_doi',

                        'completeness_of_dataset',
                        'completeness_of_data_set',
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
                (lambda cs: [DT.derive(c, [[[['name']],  # FIXME [['name]] as missing a nesting level
                                           De.contributor_name,  # and we got no warning ... :/
                                           [['first_name'], ['last_name']]]])
                            for c in cs]),
                []],

               [[['contributors']],
                (lambda cs: [JPointer('/contributors/{i}') for i, c in enumerate(cs)
                            if ('PrincipalInvestigator' in c['contributor_role']
                                if 'contributor_role' in c else False)]),  # ah lambda and commas ...
                [['meta', 'principal_investigator']]],

               [[['contributors']],
                Derives.creators,
                [['creators']]],

               [[['contributors']],
                lambda v: (len(v),),
                [['meta', 'contributor_count']]],

               [[['subjects']],
                Derives.dataset_species,
                [['meta', 'species']]],

               [[['subjects']],
                lambda v: (len(v),),
                [['meta', 'subject_count']]],

               [[['samples']],
                 lambda v: (len(v),),
                [['meta', 'sample_count']]],

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
    def _data_derived_post(self):
        data = self.data_lifted_post
        DictTransformer.derive(data, derives)
        return data

    @property
    def cleaned(self):
        """ clean up any cruft left over from transformations """

        data = super().cleaned

        # FIXME post manual fixes ...
        # remote Creator since we lift it out
        if 'contributors' in data:
            for c in data['contributors']:
                if 'contributor_role' in c:
                    cr = c['contributor_role']
                    if 'Creator' in cr:
                        c['contributor_role'] = tuple(r for r in cr if r != 'Creator')

                    assert 'Creator' not in c['contributor_role']

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

        return data

    @property
    def data_post(self):
        # FIXME a bit of a hack
        data = self.data_out
    @hasSchema(sc.DatasetOutSchema)
    def data(self):
        return super().data
        #return self.data_post


class PipelineExtras(JSONPipeline):
    # subclassing allows us to pop additional steps
    # before or after their super()'s name

    previous_pipeline_class = SPARCBIDSPipeline

    lifts = [
    ]

    @property
    def lifted(self):
        data = super().lifted
        # FIXME conditional lifts ...
        if 'award_number' not in data['meta']:
            am = self.lifters.award_manual
            if am:
                data['meta']['award_number'] = am

        if 'modality' not in data['meta']:
            m = self.lifters.modality
            if m:
                data['meta']['modality'] = m

        if 'error_index' not in data:  # a failure will fill this in
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

        #data['old_errors'] = data.pop('errors')  # FIXME
        return data

    @hasSchema(sc.DatasetOutSchema)
    def data(self):
        return super().data

    @property
    def data_derived_post(self):
        todo = [[['meta', 'modality'], 'TODO should come from sheets maybe?']]
        derives = [[[['meta', 'award_number']],
                    lambda an: (self.lifters.organ(an),),
                    [['meta', 'organ']]]]
        data = super().data_derived_post
        DictTransformer.derive(data, derives)
        return data

SPARCBIDSPipeline.check()
PipelineExtras.check()
