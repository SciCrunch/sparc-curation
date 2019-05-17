from pathlib import Path
from sparcur import schemas as sc
from sparcur import datasets as dat
from sparcur import exceptions as exc
from sparcur.core import DictTransformer, copy_all, get_all_errors
from sparcur.core import JT, JEncode, log, logd, lj, OntId, OrcidId
from sparcur.state import State
from sparcur.derives import Derives

DT = DictTransformer
De = Derives


class Pipeline:
    """ base pipeline """


class DatasourcePipeline(Pipeline):
    """ pipeline that sources complex data from somewhere else """


class ContributorsPipeline(DatasourcePipeline):

    def __init__(self, previous_pipeline, lifters, runtime_context):
        self.member = State.member
        self.contributors = previous_pipeline.data
        self.runtime_context = runtime_context
        self.dataset_id = runtime_context.id
        self.dsid = runtime_context.uri_api  # FIXME need a BlackfynnId class
    
    @property
    def data(self):
        for contributor in self.contributors:
            self._process(contributor)

    def _process(self, contributor):
        # get member if we can find them
        if 'name' in contributor and 'first_name' in contributor:
            fn = contributor['first_name']
            ln = contributor['last_name']
            if ' ' in fn:
                fn, mn = fn.split(' ', 1)
                contributor['middle_name'] = mn
                contributor['first_name'] = fn

            failover = f'{fn}-{ln}'
            member = self.member(fn, ln)

            if member is not None:
                userid = OntId('https://api.blackfynn.io/users/' + member.id)
                contributor['blackfynn_user_id'] = userid

        else:
            member = None
            failover = 'no-orcid-no-name'
            log.warning(f'No name!' + lj(contributor))

        orcid = None
        if 'contributor_orcid_id' in contributor:
            orcid = contributor['contributor_orcid_id']
            if type(orcid) == str and 'orcid.org' in orcid:
                orcid = OrcidId(orcid)  # FIXME reloading from json

            if isinstance(orcid, OrcidId):
                s = orcid
            else:  # it's not an orcid or its a bad orcid
                orcid = None

        if orcid is None:
            if member is not None:
                s = userid
            else:
                log.debug(lj(contributor))
                s = OntId(self.dsid + '/contributors/' + failover)

        contributor['id'] = s


class PrePipeline(Pipeline):
    """ base for pre-json pipelines """


class PathPipeline(PrePipeline):
    # FIXME this is a temporary solution to reuse the conversion existing
    data_transformer_class = None
    def __init__(self, previous_pipeline, lifters, runtime_context):
        #log.debug(lj(previous_pipeline.data))
        if isinstance(previous_pipeline, Path):
            path = previous_pipeline
        else:
            path = previous_pipeline.data['path']  # we already caught the duplicate error

        if isinstance(path, list):
            path, *_ = path

        self.path = path

    @property
    def _transformer(self):
        try:
            return self.data_transformer_class(self.path)
        except (exc.FileTypeError, exc.NoDataError) as e:
            class NoData:  # FIXME
                data = {}
                t = f'No data for {self.path}'

            he = dat.HasErrors(pipeline_stage=self.__class__.__name__ + '._transformer')
            logd.exception(e)  # FIXME isn't this were we should accumulate errors?
            he.addError(e)
            he.embedErrors(NoData.data)
            return NoData

    @property
    def transformed(self):
        return self._transformer.data

    # @hasSchema(sc.TransformerSchema)
    @property  # transformer out schema goes here
    def data(self):
        return self.transformed


hasSchema = sc.HasSchema()
@hasSchema.mark
class DatasetStructurePipeline(PathPipeline):

    data_transformer_class = dat.DatasetStructure

    @hasSchema(sc.DatasetStructureSchema)
    def data(self):
        return super().data


hasSchema = sc.HasSchema()
@hasSchema.mark
class SubmissionFilePipeline(PathPipeline):

    data_transformer_class = dat.SubmissionFile

    @hasSchema(sc.SubmissionSchema)
    def data(self):
        return super().data


hasSchema = sc.HasSchema()
@hasSchema.mark
class DatasetDescriptionFilePipeline(PathPipeline):

    data_transformer_class = dat.DatasetDescriptionFile

    @hasSchema(sc.DatasetDescriptionSchema)
    def data(self):
        return super().data


hasSchema = sc.HasSchema()
@hasSchema.mark
class SubjectsFilePipeline(PathPipeline):

    data_transformer_class = dat.SubjectsFile

    @hasSchema(sc.SubjectsSchema)
    def data(self):
        return super().data


hasSchema = sc.HasSchema()
@hasSchema.mark
class SamplesFilePipeline(PathPipeline):

    data_transformer_class = dat.SamplesFile

    @hasSchema(sc.SamplesFileSchema)
    def data(self):
        return super().data


hasSchema = sc.HasSchema()
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

    subpipelines = []
    copies = []
    moves = []
    cleans = []
    derives = []
    updates = []
    adds = []

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

        if not isinstance(previous_pipeline, Pipeline):
            raise TypeError(f'{previous_pipeline} is not a Pipeline!')

        self.previous_pipeline = previous_pipeline
        self.lifters = lifters
        self.runtime_context = runtime_context

    def subpipeline_errors(self, errors):
        """ override this for pipeline specific error handling rules """
        for path, error, subpipeline_class in errors:
            log.error(f'{path}\n{error}\n{subpipeline_class}\n{self!r}')

    @property
    def pipeline_start(self):
        """ cooperate with HasSchema input schema validation """
        # pipeline start is whatever starting data you need

        return self.previous_pipeline.data

    @property
    def subpipelined(self):
        data = self.pipeline_start
        errors_subpipelines = list(DictTransformer.subpipeline(data, self.runtime_context, self.subpipelines))
        errors = tuple(es for es in errors_subpipelines if isinstance(es, tuple))
        self.subpipeline_errors(errors)
        self.subpipeline_instances = tuple(es for es in errors_subpipelines if isinstance(es, Pipeline))
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
        #log.debug(f'cleaned the following values from {self}' + lj(removed))
        log.debug(f'cleaned {len(removed)} values from {self}')
        return data

    @property
    def updated(self):
        data = self.cleaned
        DictTransformer.update(data, self.updates, source_key_optional=True)
        return data

    @property
    def augmented(self):
        data = self.updated
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


class LoadIR(JSONPipeline):
    """ Reload the internal python types from a json export """

    # TODO by key vs by path
    #updates = [['contributors', 'contributor_orcid_id'], derives.LoadIR.function]

    @classmethod
    def check(cls):
        schema = sc.DatasetOutSchema.schema
        for update in cls.updates:
            # make sure that the update path is in the schema
            # TODO path in schema function
            pass


hasSchema = sc.HasSchema()
@hasSchema.mark
class SPARCBIDSPipeline(JSONPipeline):

    previous_pipeline_class = DatasetStructurePipeline

    subpipelines = [
        [[[['submission_file'], ['path']]],
         SubmissionFilePipeline,
         ['submission_file']],

        [[[['dataset_description_file'], ['path']]],
         DatasetDescriptionFilePipeline,
         ['dataset_description_file']],

        [[[['subjects_file'], ['path']]],
         SubjectsFilePipeline,
         ['subjects_file']],

        [[[['samples_file'], ['path']]],
         SamplesFilePipeline,
         ['samples_file']],
    ]

    copies = ([['dataset_description_file', 'contributors'], ['contributors']],
              [['subjects_file',], ['inputs', 'subjects_file']],
              [['submission_file',], ['inputs', 'submission_file']],
              [['samples_file',], ['inputs', 'samples_file']],
              [['manifest_file',], ['inputs', 'manifest_file']],
              [['dirs',], ['meta', 'dirs']],  # FIXME not quite right ...
              [['files',], ['meta', 'files']],
              [['size',], ['meta', 'size']],
              [['dataset_description_file', 'name'], ['meta', 'title']],
              *copy_all(['dataset_description_file'], ['meta'],
                        'species',  # TODO validate all source paths against schema
                        'organ',
                        'modality',
                        'protocol_url_or_doi',

                        'completeness_of_data_set',
                        'funding',
                        'description',
                        'additional_links',
                        'keywords',
                        'acknowledgements',
                        'originating_article_doi',
                        'title_for_complete_dataset',
              ))

    moves = ([['dataset_description_file',], ['inputs', 'dataset_description_file']],
             [['subjects_file', 'software'], ['resources']],  # FIXME update vs replace
             #[['subjects'], ['subjects_file']],  # first step in ending the confusion
             [['subjects_file', 'subjects'], ['subjects']],
             [['samples_file', 'samples'], ['samples']],
    )

    cleans = [['submission_file'], ['subjects_file'], ['samples_file'], ['manifest_file']]

    derives = ([[['inputs', 'submission_file', 'submission', 'sparc_award_number'],
                 ['inputs', 'dataset_description_file', 'funding']],
                DT.BOX(De.award_number),
                [['meta', 'award_number']]],

               [[['contributors']],
                (lambda cs: [DT.derive(c, [[[['name']],  # FIXME [['name]] as missing a nesting level
                                           De.contributor_name,  # and we got no warning ... :/
                                           [['first_name'], ['last_name']]]])
                            for c in cs]),
                []],

               [[['contributors']],
                DT.BOX(De.pi),  # ah lambda and commas ...
                [['meta', 'principal_investigator']]],

               [[['contributors']],
                DT.BOX(De.creators),
                [['creators']]],

               [[['contributors']],
                DT.BOX(len),
                [['meta', 'contributor_count']]],

               [[['subjects']],
                DT.BOX(De.dataset_species),
                [['meta', 'species']]],

               [[['subjects']],
                DT.BOX(len),
                [['meta', 'subject_count']]],

               [[['samples']],
                DT.BOX(len),
                [['meta', 'sample_count']]],
    )

    updates = []

    adds = [[['id'], lambda lifters: lifters.id],
            [['meta', 'folder_name'], lambda lifters: lifters.name],
            [['meta', 'uri_human'], lambda lifters: lifters.uri_human],
            [['meta', 'uri_api'], lambda lifters: lifters.uri_api],]

    def subpipeline_errors(self, errors):
        paths = []
        saf = ['samples_file']
        suf = ['subjects_file']
        for_super = []
        for path, error, subpipeline_class in errors:
            paths.append(path)
            if path not in (saf, suf):
                for_super.append((path, error, subpipeline_class))

        if saf not in paths and suf not in paths:
            log.error(f'samples_file nor subjects_file')

        super().subpipeline_errors(for_super)

    @property
    def pipeline_start(self):
        data = super().pipeline_start
        if 'errors' in data:
            get_paths = set(tuple(gp) for gas, _, _ in self.subpipelines for gp, _ in gas)
            sections = set((s,) for s in data)
            # FIXME probably should just use adops.get to check for the path
            both = get_paths & sections
            if not both:
                log.debug(f'{get_paths}\n{sections}\n{both}')
                raise self.SkipPipelineError(data)

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
            data['meta'] = {}
            data['status'] = {}
            si = 5555
            ci = 4444
            data['status']['submission_index'] = si
            data['status']['curation_index'] = ci
            data['status']['error_index'] = si + ci
            #data['protocol_index'] = 9999  # I think this one has to go in reverse?
            log.debug('pipeline skipped to end due to errors')

        return data

    @hasSchema(sc.DatasetOutSchema)
    def data(self):
        return super().data


class PipelineExtras(JSONPipeline):
    # subclassing allows us to pop additional steps
    # before or after their super()'s name

    previous_pipeline_class = SPARCBIDSPipeline

    subpipelines = [
        [[[['contributors'], None]],
         ContributorsPipeline,
         None],
    ]

    @property
    def added(self):
        data = super().added
        # FIXME conditional lifts ...
        if 'award_number' not in data['meta']:
            am = self.lifters.award_manual
            if am:
                data['meta']['award_number'] = am

        if 'modality' not in data['meta']:
            m = self.lifters.modality
            if m:
                data['meta']['modality'] = m

        if 'organ' not in data['meta']:
            if 'award_number' in data['meta']:
                an = data['meta']['award_number']
                o = self.lifters.organ(an)
                if o:
                    data['meta']['organ'] = o

        if 'organ' not in data['meta'] or data['meta']['organ'] == 'othertargets':
            o = self.lifters.organ_term
            if o:
                data['meta']['organ'] = o

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


class PipelineEnd(JSONPipeline):
    """ This is the final pipeline, it computes post processing stats
        such as errors it it does not validate itself """

    previous_pipeline_class = PipelineExtras

    _submission = (
        'DatasetStructure',
        'DatasetStructurePipeline.data',
        'Tabular',  # FIXME which Tabular
        'SubmissionFile',
        'SubmissionFilePipeline.data',
        'DatasetDescriptionFile',
        'DatasetDescriptionFilePipeline._transformer',
        'DatasetDescriptionFilePipeline.data',
        'SubjectsFilePipeline.data',
        'SamplesFilePipeline._transformer',
        'SamplesFilePipeline.data',
    )
    _curation = (
        'DatasetStructure.curation-error',
        'SubjectsFile.curation-error',
        'SPARCBIDSPipeline.data',
        'PipelineExtras.data',
    )

    @classmethod
    def _indexes(cls, data):
        """ compute submission and curation error indexes """
        errors = get_all_errors(data)
        submission_errors = []
        curation_errors = []
        for error in reversed(errors):
            if error in submission_errors or error in curation_errors:
                log.debug('error detected multiple times not counting '
                          'subsequent occurances' + lj(error))
                continue

            stage = error['pipeline_stage']
            message = error['message']
            if stage in cls._submission:
                submission_errors.append(error)
            elif stage in cls._curation:
                curation_errors.append(error)
            else:
                raise ValueError(f'Unhandled stage {stage} {message}')

        si = len(submission_errors)
        ci = len(curation_errors)
        data['status'] = {}
        data['status']['submission_index'] = si
        data['status']['curation_index'] = ci
        data['status']['error_index'] = si + ci
        if submission_errors:
            data['status']['submission_errors'] = submission_errors

        if curation_errors:
            data['status']['curation_errors'] = curation_errors

        return si + ci

    @property
    def added(self):
        data = super().added

        if 'status' not in data:
            self._indexes(data)

        return data

    @hasSchema(sc.PostSchema, fail=True)
    def data(self):
        return super().data


SPARCBIDSPipeline.check()
PipelineExtras.check()
PipelineEnd.check()
