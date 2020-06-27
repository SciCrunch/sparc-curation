import copy
import json
from pathlib import Path
from collections import deque
import idlib
import rdflib
from joblib import Parallel, delayed
from pyontutils.core import OntRes, OntGraph
from pyontutils.utils import utcnowtz, isoformat, subclasses
from pyontutils.namespaces import TEMP, isAbout  # FIXME split export pipelines into their own file?
from pyontutils.closed_namespaces import rdf, rdfs, owl
from sparcur import apinat
from sparcur import schemas as sc
from sparcur import datasets as dat
from sparcur import converters as conv
from sparcur import exceptions as exc
from sparcur.core import DictTransformer, copy_all, get_all_errors, compact_errors
from sparcur.core import JT, JEncode, log, logd, lj, OntId, OntTerm, OntCuries
from sparcur.core import JApplyRecursive, json_identifier_expansion, dereference_all_identifiers
from sparcur.state import State
from sparcur.derives import Derives

DT = DictTransformer
De = Derives

a = rdf.type


class MapPathsCombinator:

    # FIXME could be implemented as a subpipeline of a subpipeline ?
    # not quite sure how to do that though

    def __init__(self, PipelineClass, debug=True, n_jobs=12):
        # FIXME need to figure out how to pass config variables in
        self.PipelineClass = PipelineClass
        self.debug = debug
        self.n_jobs = n_jobs

    def __call__(self, previous_pipeline, lifters, runtime_context,
                 # FIXME from runtime context or something?
                 debug=None, n_jobs=None):

        if debug is not None:
            self.debug = debug

        if n_jobs is not None:
            self.n_jobs = n_jobs

        class DataWrapper:
            def __init__(self, data):
                self.data = data

        data = previous_pipeline.data
        sv = data['schema_version'] if 'schema_version' in data else None
        previous_pipelines = [DataWrapper({'path': p, 'schema_version': sv,})
                              for p in data['paths']]
        self.pipes = [self.PipelineClass(previous_pipeline,
                                         lifters,
                                         runtime_context)
                      for previous_pipeline in previous_pipelines]

        return self # hack to mimic __init__

    @property
    def data(self):
        if self.debug or self.n_jobs == 1:
            return [p.data for p in self.pipes]
        else:
            return Parallel(n_jobs=self.n_jobs)(delayed(lambda :p.data)()
                                                for p in self.pipes)


class Pipeline:
    """ base pipeline """

    object_class = None

    @property
    def object(self):
        return self.object_class(self.data)


class DatasourcePipeline(Pipeline):
    """ pipeline that sources complex data from somewhere else """


class SubmissionPipeline(DatasourcePipeline):

    def __init__(self, previous_pipeline, lifters, runtime_context):
        self.submission = previous_pipeline.data

    @property
    def data(self):
        # go look in the master sheet data
        def fix(mcd):
            log.debug(mcd)
            return mcd

        if 'milestone_completion_date' in self.submission:
            mcd = self.submission['milestone_completion_date']
            self.submission['milestone_completion_date'] = fix(mcd)

        return self.submission


class ContributorsPipeline(DatasourcePipeline):

    def __init__(self, previous_pipeline, lifters, runtime_context):
        if hasattr(State, 'member'):
            self.member = State.member
        else:
            log.error('State missing member, using State seems '
                      'like a good idea until you go to multiprocessing')
            self.member = lambda first, last: None

        self.contributors = previous_pipeline.data
        self.runtime_context = runtime_context
        self.dataset_id = runtime_context.id
        self.dsid = runtime_context.uri_api  # FIXME need a BlackfynnId class
        self.lifters = lifters
    
    @property
    def data(self):
        for contributor in self.contributors:
            self._process(contributor)

    def _process(self, contributor):
        # get member if we can find them
        he = dat.HasErrors(pipeline_stage=self.__class__.__name__ + '.data')
        if 'contributor_name' in contributor and 'first_name' in contributor:
            name = contributor['contributor_name']
            if ';' in name:
                msg = f'Bad symbol in name {name!r}'
                he.addError(msg)
                logd.error(msg)

            fn = contributor['first_name']
            ln = contributor['last_name']
            if ' ' in fn:
                fn, mn = fn.split(' ', 1)
                mn, _mn = mn.rstrip('.'), mn
                if mn != _mn:
                    msg = f'Middle initials don\'t need periods :) {name!r}'
                    #if he.addError(msg):
                    logd.debug(msg)

                contributor['middle_name'] = mn
                contributor['first_name'] = fn

            if ' ' in ln:
                msg = f'Malformed last_name {ln!r}'
                he.addError(msg)
                logd.error(msg)
                ln = ln.replace(' ', '-')

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
                orcid = idlib.Orcid(orcid)  # FIXME reloading from json

            if isinstance(orcid, idlib.Orcid):
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
        he.embedErrors(contributor)

        # lifting + adding
        if 'contributor_affiliation' in contributor:
            ca = contributor['contributor_affiliation']
            maybe_ror = self.lifters.affiliations(ca)
            if maybe_ror is not None:
                contributor['affiliation'] = maybe_ror


class PrePipeline(Pipeline):
    """ base for pre-json pipelines """


class PathPipeline(PrePipeline):
    # FIXME this is a temporary solution to reuse the conversion existing
    data_transformer_class = None
    def __init__(self, previous_pipeline, lifters, runtime_context):
        #log.debug(lj(previous_pipeline.data))
        if isinstance(previous_pipeline, Path):
            path = previous_pipeline
            schema_version = None
        else:
            path = previous_pipeline.data['path']  # we already caught the duplicate error
            try:
                schema_version = previous_pipeline.data['schema_version']
            except KeyError:
                schema_version = None

        if isinstance(path, list):
            path, *_ = path

        self.path = path
        self.schema_version = schema_version

    @property
    def _transformer(self):
        try:
            return self.data_transformer_class(self.path, schema_version=self.schema_version)
        except (exc.FileTypeError, exc.NoDataError, exc.BadDataError) as e:
            class NoData:  # FIXME
                data = {}
                t = f'No data for {self.path}'

            he = dat.HasErrors(pipeline_stage=self.__class__.__name__ + '._transformer')
            if he.addError(e, path=self.path):
                logd.exception(e)  # FIXME isn't this were we should accumulate errors?

            he.embedErrors(NoData.data)
            return NoData

    @property
    def transformed(self):
        try:
            return self._transformer.data
        except (exc.FileTypeError, exc.NoDataError, exc.BadDataError) as e:
            # these errors mostly happen after __init__ now
            # since they are properly pipelined
            data = {}
            he = dat.HasErrors(pipeline_stage=self.__class__.__name__ + '.transformer')
            if he.addError(e, path=self.path):
                logd.exception(e)  # FIXME isn't this were we should accumulate errors?

            he.embedErrors(data)
            return data

    # @hasSchema(sc.TransformerSchema)
    @property  # transformer out schema goes here
    def data(self):
        data = self.transformed

        condition = False
        # condition = data is not None and [k for k in data if not isinstance(k, str)]
        if condition:
            qf = self._transformer
            breakpoint()

        return data


hasSchema = sc.HasSchema()
@hasSchema.mark
class DatasetMetadataPipeline(PathPipeline):

    data_transformer_class = dat.DatasetMetadata

    #@hasSchema(sc.DatasetMetadataSchema)
    @property
    def data(self):
        return super().data


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
        out = super().data
        if 'schema_version' not in out:  # schema version MUST be present or downstream pipelines break
            out['schema_version'] = None
        return out


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
class ManifestFilePipeline(PathPipeline):

    data_transformer_class = dat.ManifestFile

    @property
    def transformed(self):
        dsr_path = self.path.relative_to(self.path.cache.dataset)
        data = super().transformed
        self.path.populateJsonMetadata(data)
        return data

    @hasSchema(sc.ManifestFileSchema)
    def data(self):
        return super().data


class Merge(Pipeline):

    def __init__(self, pipelines):
        self.pipelines = pipelines

    @property
    def data(self):
        data = {}
        for pipeline in self.pipelines:
            try:
                data.update(pipeline.data)
            except TypeError as e:
                raise TypeError(f'Issue in {pipeline}.data') from e

        return data


hasSchema = sc.HasSchema()
@hasSchema.mark
class JSONPipeline(Pipeline):
    # we can't quite do this yet, but these pipelines are best composed
    # by subclassing and then using super().previous_step to insert another
    # step along the way

    previous_pipeline_classes = tuple()
    # If previous_pipeline_classes is set then this pipeline is run on the previous_pipeline given
    # to this class at init. It might be more accurate to call this the previous pipeline processor.
    # basically this is the 'intervening' pipeline between the previous pipeline and this pipeline
    # need a better name for it ... if an instance of the previous pipeline class is passed to the
    # constructor then it is used as is
    # if there is more than one class any overlapping keys will match the output
    # of the last class in the pipeline FIXME duplicate keys should probably error loudly ...

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

    def __init__(self, previous_pipeline, lifters=None, runtime_context=None):
        """ sources stay, helpers are tracked for prov and then disappear """
        #log.debug(lj(sources))

        if self.previous_pipeline_classes:
            #not isinstance(previous_pipeline, self.previous_pipeline_classes)):
            previous_pipeline = Merge([p(previous_pipeline, lifters, runtime_context)
                                       for p in self.previous_pipeline_classes])

        if not isinstance(previous_pipeline, Pipeline):
            breakpoint()
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
        errors_subpipelines = list(DictTransformer.subpipeline(data,
                                                               self.runtime_context,
                                                               self.subpipelines,
                                                               lifters=self.lifters))
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
        #log.debug(log.handlers)
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


class LoadJSON(Pipeline):
    def __init__(self, path, *args, **kwargs):
        self.path = path

    @property
    def data(self):
        with open(self.path, 'rt') as f:
            blob = json.load(f)

        # TODO convert various things back to internal representation
        return blob


class ExportPipeline(Pipeline):
    """ Pipelines that start with some json blob and return something else. """
    # TODO we need a flexible way to map export pipelines to json pipelines
    # for maximum reusability and in order to set default export pipelines

    def __init__(self, data_pipeline_endpoint):
        self._pipeline_start = data_pipeline_endpoint

    @property
    def pipeline_start(self):
        return self._pipeline_start.data


class RdfPipeline(ExportPipeline):

    object_class = lambda g: g

    # previous pipeline and converter should match
    previous_pipeline_classes = tuple()
    converter_class = None

    @property
    def triples(self):
        yield from self.converter_class(self.pipeline_start, self).triples_gen

    @property
    def triples_header(self):
        yield TEMP.subject, TEMP.predicate, TEMP.object
        raise NotImplementedError

    def populate(self, graph):
        def warn(triple):
            for element in triple:
                if (not (isinstance(element, rdflib.URIRef) or
                         isinstance(element, rdflib.BNode) or
                         isinstance(element, rdflib.Literal)) or
                    (hasattr(element, '_value') and isinstance(element._value, dict))):
                    log.critical(element)

            return triple

        OntCuries.populate(graph)  # ah smalltalk thinking
        [graph.add(t) for t in self.triples_header if warn(t)]
        [graph.add(t) for t in self.triples if warn(t)]

    @property
    def graph(self):
        """ you can populate other graphs, but this one runs once """
        if not hasattr(self, '_graph'):
            graph = OntGraph()
            self.populate(graph)
            self._graph = graph

        return self._graph

    @property
    def data(self):
        return self.graph


class TurtleExport:
    """ bad way to implement helper class for export
        the choice of serialization can be deferred
        until much later in time, so creating a high
        level invariant is not required """

    @property
    def data(self):
        raise NotImplementedError("don't use this, it is here as an example only")
        return self.graph.serialize(format='nifttl')


hasSchema = sc.HasSchema()
@hasSchema.mark
class ApiNATOMY(JSONPipeline):

    previous_pipeline_classes = LoadJSON,

    #@hasSchema(sc.ApiNATOMYSchema, fail=True)  # resourceMap fails
    @hasSchema(sc.ApiNATOMYSchema)
    def data(self):
        return self.pipeline_start


# TODO register export pipelines with the json endpoint pipeline?
# is one way to solve this problem
class ApiNATOMY_rdf(RdfPipeline):

    converter_class = lambda self, a, b: apinat.Graph(a)

    @property
    def id(self):
        mid = self.pipeline_start['id'].replace(' ', '-')
        return mid
        # FIXME this seems wrong ...
        # the pipelines are separate from the objects they manipulate or output
        return self._pipeline_start.object.id

    @property
    def triples_header(self):
        yield from tuple()


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


class PipelineStart(JSONPipeline):

    previous_pipeline_classes = DatasetMetadataPipeline, DatasetStructurePipeline
    # metadata should come first since structure can fail, error handling sigh

    subpipelines = [
        # FIXME in theory subpipelines can run sequentially
        # except for the fact that _all_ pipeline input selections
        # happen first at the same time enforcing a tiny bit of functional thinking
        [[[['dataset_description_file'], ['path']]],
         DatasetDescriptionFilePipeline,
         ['dataset_description_file']],
    ]

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


hasSchema = sc.HasSchema()
@hasSchema.mark
class SPARCBIDSPipeline(JSONPipeline):

    previous_pipeline_classes = PipelineStart,
    #previous_pipeline_classes = DatasetMetadataPipeline, DatasetStructurePipeline

    subpipelines = [
        #[[[['dataset_description_file'], ['path']]],
         #DatasetDescriptionFilePipeline,
         #['dataset_description_file']],

        [[[['submission_file'], ['path']],
          [['dataset_description_file', 'schema_version'], ['schema_version']]],
         SubmissionFilePipeline,
         ['submission_file']],

        [[[['subjects_file'], ['path']],
          [['dataset_description_file', 'schema_version'], ['schema_version']]],
         SubjectsFilePipeline,
         ['subjects_file']],

        [[[['samples_file'], ['path']],
          [['dataset_description_file', 'schema_version'], ['schema_version']]],
         SamplesFilePipeline,
         ['samples_file']],

        [[[['manifest_file'], ['paths']],
          [['dataset_description_file', 'schema_version'], ['schema_version']]],
         MapPathsCombinator(ManifestFilePipeline),
         ['manifest_file']],
    ]

    copies = ([['dataset_description_file', 'contributors'], ['contributors']],
              [['subjects_file',], ['inputs', 'subjects_file']],
              [['submission_file', 'submission'], ['submission']],
              [['submission_file',], ['inputs', 'submission_file']],
              [['samples_file',], ['inputs', 'samples_file']],
              [['manifest_file',], ['inputs', 'manifest_file']],
              [['dirs',], ['meta', 'dirs']],  # FIXME not quite right ...
              [['files',], ['meta', 'files']],
              [['size',], ['meta', 'size']],
              [['dataset_description_file', 'name'], ['meta', 'title']],
              *copy_all(['dataset_description_file'], ['meta'],
                        'schema_version',
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
                        'title_for_complete_data_set',

                        'number_of_subjects',
                        'number_of_samples',
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
                (lambda cs: [DT.derive(c, [[[['contributor_name']],  # FIXME [['name]] as missing a nesting level
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

    # replace lifters with proper upstream pipelines (now done with DatasetMetadata)
    adds = [[['prov', 'timestamp_export_start'], lambda lifters: lifters.timestamp_export_start],]

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
            logd.error(f'samples_file nor subjects_file')

        super().subpipeline_errors(for_super)

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

        #breakpoint()
        return data

    @property
    def pipeline_end(self):
        """ this part adds the meta bits we need after _with_errors
            and rearranges anything that needs to be moved about """

        try:
            data = super().pipeline_end
        except self.SkipPipelineError as e:
            data = e.data
            if 'meta' not in data:
                data['meta'] = {}

            if 'status' not in data:
                data['status'] = {}

            si = 5555
            ci = 4444
            data['status']['submission_index'] = si
            data['status']['curation_index'] = ci
            data['status']['error_index'] = si + ci
            data['status']['submission_errors'] = []
            data['status']['curation_errors'] = []
            #data['protocol_index'] = 9999  # I think this one has to go in reverse?
            log.debug('pipeline skipped to end due to errors')

        return data

    @hasSchema(sc.DatasetOutSchema)
    def data(self):
        data = super().data

        # dereference should happen here not at the end of PiplineStart
        # though harder to work backwards to the offending file right now

        # FIXME pure side effecting going on here, also definitely wrong place
        he = dat.HasErrors(pipeline_stage=self.__class__.__name__ + '.data')
        JApplyRecursive(dereference_all_identifiers, data, self, addError=he.addError)
        he.embedErrors(data)

        return data


class PipelineExtras(JSONPipeline):
    # subclassing allows us to pop additional steps
    # before or after their super()'s name

    previous_pipeline_classes = SPARCBIDSPipeline,

    subpipelines = [
        [[[['contributors'], None]],
         ContributorsPipeline,
         None],
        [[[['submission'], None]],
         SubmissionPipeline,
         ['submission']],
    ]

    adds = [[['meta', 'techniques'], lambda lifters: lifters.techniques]]

    filter_failures = (  # XXX TODO implement this for other pipelines as well ?
        lambda p: ('inputs' not in p and 'contributor_role' in p),
        lambda p: ('inputs' not in p and 'contributor_orcid' in p),
    )

    @property
    def cleaned(self):
        """ extra cleaning (that could become standard like other stages)
            to remove paths that do not pass the schema and that are thus
            have a potential to break things during export """

        data = super().cleaned

        if 'errors' in data:
            pop_paths = [tuple(p) for p in [e['path'] for e in data['errors']
                                            if 'path' in e]
                         if any(ff(p) for ff in self.filter_failures)]

            if pop_paths:
                # need to remove from the tail of a list so that indexes don't
                # shift until after we have already passed them
                # some paths may have more than one error so use set to avoid double remove issues
                safe_ordering = sorted(set(pop_paths), reverse=True)

                try:
                    garbage = [(p, tuple(DT.pop(data, [p]))) for p in safe_ordering]
                except exc.NoSourcePathError as e:
                    raise exc.SparCurError(f'{self.runtime_context.path}') from e

                msg = '\n\t'.join(str(t) for t in garbage)
                logd.warning(f'Garbage truck says:\n{msg}')

            # get all error paths
            # filter the paths to remove
            # pop the values at those paths

            #candidates_for_removal = [e for e in data['errors'] if 'path' in e]
            #not_input = [e for e in candidates_for_removal if e['path'][0] != 'inputs']
            #cand_paths = [e['path'] for e in not_input]
            #if 'contributors' in data:
                # FIXME will have to sort these in reverse
                #hrm = list(DT.pop(data, [['contributors', 0, 'contributor_role', 1]]))
                #breakpoint()

        return data

    @property
    def added(self):
        data = super().added
        if data['meta'] == {'techniques': []}:
            breakpoint()

        # FIXME conditional lifts ...
        if 'award_number' not in data['meta']:
            am = self.lifters.award_manual
            if am:
                data['meta']['award_number'] = am

        if 'modality' not in data['meta']:
            m = self.lifters.modality
            if m:
                data['meta']['modality'] = m

        '''  # XXX delete
        if False and 'organ' not in data['meta']:
            # skip here, now attached directly to award
            if 'award_number' in data['meta']:
                an = data['meta']['award_number']
                o = self.lifters.organ(an)
                if o:
                    if o != 'othertargets':
                        o = OntId(o)
                        if o.prefix == 'FMA':
                            ot = OntTerm(o)
                            o = next(OntTerm.query(label=ot.label, prefix='UBERON'))

                    data['meta']['organ'] = o

        '''
        if 'organ' not in data['meta'] or data['meta']['organ'] == 'othertargets':
            o = self.lifters.organ_term
            if o:
                if isinstance(o, str):
                    o = o,

                out = tuple()
                for _o in o:
                    _o = OntId(_o)
                    if _o.prefix == 'FMA':
                        ot = OntTerm(_o)
                        _o = next(OntTerm.query(label=ot.label, prefix='UBERON'))

                    out += (_o,)

                data['meta']['organ'] = out

        if 'protocol_url_or_doi' not in data['meta']:
            if self.lifters.protocol_uris:
                data['meta']['protocol_url_or_doi'] = tuple(self.lifters.protocol_uris)

        else:
            if not isinstance(data['meta']['protocol_url_or_doi'], tuple):
                _test_path = deque(['meta', 'protocol_url_or_doi'])
                if not [e for e in data['errors']
                        if 'path' in e and e['path'] == _test_path]:
                    raise ext.ShouldNotHappenError('urg')

            else:
                data['meta']['protocol_url_or_doi'] += tuple(self.lifters.protocol_uris)
                data['meta']['protocol_url_or_doi'] = tuple(sorted(set(data['meta']['protocol_url_or_doi'])))  # ick


        # FIXME this is a really bad way to do this :/ maybe stick the folder in data['prov'] ?
        # and indeed, when we added PipelineStart this shifted and broke everything
        # FIXME use the rmeta
        local = (self
                 .previous_pipeline.pipelines[0]
                 .previous_pipeline.pipelines[0]
                 .previous_pipeline.pipelines[0]
                 .path)
        remote = local.remote
        if 'doi' not in data['meta']:
            doi = remote.doi
            if doi is not None:
                try:
                    metadata = doi.metadata()
                    if metadata is not None:
                        data['meta']['doi'] = doi
                except idlib.exceptions.ResolutionError:
                    pass

        if 'status' not in data:
            data['status'] = {}

        if 'status_on_platform' not in data['status']:
            try:
                data['status']['status_on_platform'] = remote.bfobject.status
            except exc.NoRemoteFileWithThatIdError as e:
                log.exception(e)
                if remote.cache is not None and remote.cache.exists():
                    remote.cache.crumple()

        return data

    @hasSchema(sc.DatasetOutSchema)
    def data(self):
        return super().data

    @property
    def data_derived_post(self):  # XXX deprecated
        raise NotImplementedError
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

    previous_pipeline_classes = PipelineExtras,

    # FIXME HasErrors needs to require a level specification
    # and not define these here because they have to be maintained
    # and of course my workaround was simply to error on unhandled
    # which is an entirely reasonable approach, if astoundingly
    # inefficient
    _submission = [
        'DatasetStructure',
        'DatasetStructurePipeline.data',

        'Tabular',  # FIXME which Tabular


        'SubmissionFile',
        'RawJsonSubmission.data',

        'SubmissionFilePipeline._transformer',
        'SubmissionFilePipeline.transformer',
        'SubmissionFilePipeline.data',


        'DatasetDescriptionFile',
        'RawJsonDatasetDescription.data',

        'NormDatasetDescriptionFile',
        #'NormDatasetDescriptionFile.contributor_orcid_id',
        #'NormDatasetDescriptionFile.contributor_role',
        #'NormDatasetDescriptionFile.funding',
        #'NormDatasetDescriptionFile.originating_article_doi',
        'NormDatasetDescriptionFile.protocol_url_or_doi',

        'DatasetDescriptionFilePipeline._transformer',
        'DatasetDescriptionFilePipeline.transformer',
        'DatasetDescriptionFilePipeline.data',

        'ContributorsPipeline.data',

        'SubjectsFile',
        'RawJsonSubjects.data',

        'NormSubjectsFile',
        'NormSubjectsFile.age',
        'NormSubjectsFile.protocol_url_or_doi',

        'SubjectsFilePipeline._transformer',
        'SubjectsFilePipeline.transformer',
        'SubjectsFilePipeline.data',


        'SamplesFile',

        'SamplesFilePipeline._transformer',
        'SamplesFilePipeline.transformer',
        'SamplesFilePipeline.data',

        'ManifestFilePipeline.transformer',
        'ManifestFilePipeline.data',
    ]
    _curation = [
        'DatasetStructure.curation-error',
        'SubjectsFile.curation-error',
        'SPARCBIDSPipeline.data',
        'PipelineExtras.data',
        'ProtocolData',  # FIXME this is here as a placeholder
    ]

    _blame_stage = object()
    _blame_curation = object()
    _blame_submission = object()
    _blame_everyone = object()
    _blame_export = object()
    _blame = {
        'submission': _blame_submission,
        'pipeline': _blame_curation,
        'debug': _blame_curation,
        'stage': _blame_stage,
        'EVERYONE': _blame_everyone,
        'export': _blame_export,
    }

    @classmethod
    def _expand_stages(cls):
        ocs = {c.__name__:c for c in subclasses(object)}
        for type_ in (cls._submission, cls._curation):
            for name in type_:
                cname, *rest = name.split('.', 1)
                c = ocs[cname]
                suffix = ''
                if rest:
                    suffix = '.' + ''.join(rest)

                for sc in subclasses(c):
                    type_.append(sc.__name__ + suffix)


    @classmethod
    def _indexes(cls, data):
        """ compute submission and curation error indexes """
        path_errors = get_all_errors(data)
        (compacted, path_error_report, by_invariant_path,
         errors) = compact_errors(path_errors)
        unclassified_stages = []
        submission_errors = []
        curation_errors = []
        unclassified_errors = []
        for error in reversed(errors):
            if error in submission_errors or error in curation_errors:
                #log.debug('error detected multiple times not counting '
                          #'subsequent occurances' + lj(error))
                continue

            if 'blame' not in error:
                breakpoint()

            blame = error['blame']
            stage = error['pipeline_stage']
            message = error['message']

            blamed = False
            if blame is not None:
                if blame in cls._blame:
                    blame_target = cls._blame[blame]
                    if blame_target == cls._blame_stage:
                        pass
                    elif blame_target == cls._blame_everyone:
                        submission_errors.append(error)
                        curation_errors.append(error)
                        blamed = True
                    elif blame_target == cls._blame_submission:
                        submission_errors.append(error)
                        blamed = True
                    elif blame_target == cls._blame_curation:
                        curation_errors.append(error)
                        blamed = True
                    else:
                        raise ValueError(f'Unhandled blame target {blame_target}\n{message}')

                else:
                    raise ValueError(f'Unhandled blame type {blame}\n{message}')

            if stage in cls._submission:
                if not blamed:
                    submission_errors.append(error)
            elif stage in cls._curation:
                if not blamed:
                    curation_errors.append(error)
            else:
                if blame not in ('pipeline', 'submission', 'debug'):
                    unclassified_errors.append(error)
                    unclassified_stages.append(blame)
                    #raise ValueError(f'Unhandled stage {stage}\n{message}')

        si = len(submission_errors)
        ci = len(curation_errors)
        ui = len(unclassified_errors)
        if 'status' not in data:
            data['status'] = {}

        data['status']['submission_index'] = si
        data['status']['curation_index'] = ci
        data['status']['unclassified_index'] = ui
        data['status']['error_index'] = si + ci + ui
        data['status']['submission_errors'] = submission_errors
        data['status']['curation_errors'] = curation_errors
        data['status']['unclassified_errors'] = unclassified_errors
        data['status']['unclassified_stages'] = unclassified_stages
        data['status']['path_error_report'] = path_error_report

        return si + ci

    @property
    def added(self):
        data = super().added

        if 'status' not in data or 'error_index' not in data['status']:
            self._indexes(data)

        return data

    @hasSchema(sc.PostSchema, fail=True)
    def data(self):
        return super().data


class IrToExportJsonPipeline(JSONPipeline):
    """ transform json ir -> json export """

    def __init__(self, blob_ir):
        self.blob_ir = blob_ir

    @property
    def augmented(self):
        data_in = self.blob_ir
        # XXX NOTE JApplyRecursive is actually functional
        data = JApplyRecursive(json_identifier_expansion,
                               data_in,
                               skip_keys=('errors',),
                               preserve_keys=('inputs', 'id'))
        return data

    @hasSchema(sc.DatasetOutExportSchema)
    def data(self):
        return super().data


class ToJsonLdPipeline(JSONPipeline):
    moves = (
        [['contributors'], ['contributors', '@graph']],
        [['subjects'], ['subjects', '@graph']],
        [['samples'], ['samples', '@graph']],
        [['resources'], ['resources', '@graph']],
    )

    updates = (
        [['id'], lambda v: 'dataset:' + v.rsplit(':', 1)[-1] + '/#dataset-graph'],
    )

    __includes = []

    JApplyRecursive(sc.update_include_paths,
                    sc.DatasetOutExportSchema.schema,
                    preserve_keys=('inputs',),
                    moves=moves,  # have to account for the moves from the defining schema
                    collect=__includes)

    adds = (
        [['@context'], lambda l: sc.base_context],
        [['meta', '@context'], lambda l: sc.MetaOutExportSchema.context()],
        [['contributors', '@context'], lambda l: sc.ContributorExportSchema.context()],
        [['subjects', '@context'], lambda l: sc.SubjectsExportSchema.context()],
        [['samples', '@context'], lambda l: sc.SamplesFileExportSchema.context()],
        #[['resources', '@context'], lambda lifters: sc.ResourcesExportSchema.context()]
        *__includes  # owl:NamedIndividual in here ... FIXME would be nice to not have to hack it in this way
    )

    derives_after_adds = (
        [[['meta', 'uri_api']],
         DT.BOX(lambda uri_api: uri_api + '/'),
         [['@context', '@base']]],

        [[['meta', 'uri_api']],
         DT.BOX(lambda uri_api: {'@id': uri_api + '/', '@prefix': True}),
         [['@context', '_bfc']]],


        [[['meta', 'uri_api']],
         DT.BOX(lambda uri_api: uri_api + '/#subjects-graph'),
         [['subjects', '@id']]],

        [[['meta', 'uri_api']],
         DT.BOX(lambda uri_api: {'@id': '@id', '@context': {'@base': uri_api + '/subjects/'}}),
         [['subjects', '@context', 'subject_id']]],


        [[['meta', 'uri_api']],
         DT.BOX(lambda uri_api: uri_api + '/#samples-graph'),
         [['samples', '@id']]],

        [[['meta', 'uri_api']],
         DT.BOX(lambda uri_api: {'@id': '@id', '@context': {'@base': uri_api + '/samples/'}}),
         [['samples', '@context', 'primary_key']]],


        [[['meta', 'uri_api']],
         DT.BOX(lambda uri_api: uri_api + '/#contributors-graph'),
         [['contributors', '@id']]],
    )

    def __init__(self, blob):
        self.blob = blob
        self.runtime_context = self
        self.lifters = self

    @property
    def pipeline_start(self):
        return copy.deepcopy(self.blob)

    @property
    def added(self):
        # a hack to avoid need to rewirte the other stuff
        return self.augmented_after_added

    @property
    def augmented_after_added(self):
        data = super().added
        DictTransformer.derive(data, self.derives_after_adds, source_key_optional=True)
        return data

    @property
    def data(self):
        return super().data


class ListAllDatasetsPipeline(Pipeline):
    def __init__(self, base_path):
        self.base_path = base_path

    @property
    def data(self):
        return list(self.base_path.children)


hasSchema = sc.HasSchema()
@hasSchema.mark
class SummaryPipeline(JSONPipeline):
    #previous_pipeline_classes = ListAllDatasetsPipeline

    @property
    def pipeline_end(self):
        if not hasattr(self, '_data_cache'):
            # FIXME validating in vs out ...
            # return self.make_json(d.validate_out() for d in self)
            self._data_cache = self.make_json(d.data for d in self.iter_datasets)

        return self._data_cache

    @hasSchema(sc.SummarySchema, fail=True)
    def data(self):
        data = self.pipeline_end
        return data


SPARCBIDSPipeline.check()
PipelineExtras.check()
PipelineEnd.check()
PipelineEnd._expand_stages()
