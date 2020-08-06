import copy
import json
from pathlib import Path
from itertools import chain
from collections import deque, defaultdict
import idlib
import rdflib
import augpathlib as aug
from joblib import Parallel, delayed
from hyputils import hypothesis as hyp
from pyontutils.core import OntRes, OntGraph
from pyontutils.utils import utcnowtz, isoformat, subclasses
from pyontutils.namespaces import TEMP, isAbout  # FIXME split export pipelines into their own file?
from pyontutils.closed_namespaces import rdf, rdfs, owl
from protcur import document as ptcdoc
from protcur.analysis import protc
import sparcur
from sparcur import apinat
from sparcur import schemas as sc
from sparcur import datasets as dat
from sparcur import converters as conv
from sparcur import exceptions as exc
from sparcur import normalization as norm
from sparcur.core import DictTransformer, copy_all, get_all_errors, compact_errors
from sparcur.core import JT, JEncode, log, logd, lj, OntId, OntTerm, OntCuries, get_nested_by_key
from sparcur.core import JApplyRecursive, json_identifier_expansion, dereference_all_identifiers
from sparcur.state import State
from sparcur.config import auth
from sparcur.derives import Derives
from sparcur.extract import xml as exml

DT = DictTransformer
De = Derives

a = rdf.type


# used to pass the runtime path of the dataset since
# we don't want to pre-extract all the fs information
_THIS_PATH_KEY = object()
THIS_PATH = [_THIS_PATH_KEY]


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
        sv = data['template_schema_version'] if 'template_schema_version' in data else None
        previous_pipelines = [DataWrapper({'path': p, 'template_schema_version': sv,})
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


class BlackfynnDatasetDataPipeline(DatasourcePipeline):

    def __init__(self, previous_pipeline, lifters, runtime_context):
        self.dataset_id = previous_pipeline.data['id']
        if not hasattr(self.__class__, 'BlackfynnDatasetData'):
            from sparcur.backends import BlackfynnDatasetData
            self.__class__.BlackfynnDatasetData = BlackfynnDatasetData

    @property
    def data(self):
        bdd = self.BlackfynnDatasetData(self.dataset_id)
        try:
            return bdd.fromCache()
        except FileNotFoundError as e:
            raise exc.NetworkSandboxError from e


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
            template_schema_version = None
        else:
            path = previous_pipeline.data['path']  # we already caught the duplicate error
            try:
                template_schema_version = previous_pipeline.data['template_schema_version']
            except KeyError:
                template_schema_version = None

        if isinstance(path, list):
            path, *_ = path

        self.path = path
        self.template_schema_version = template_schema_version

    @property
    def _transformer(self):
        try:
            return self.data_transformer_class(self.path, template_schema_version=self.template_schema_version)
        except (exc.FileTypeError, exc.NoDataError, exc.BadDataError) as e:
            # sigh code duplication
            class NoData:  # FIXME
                data = {}
                t = f'No data for {self.path}'

            he = dat.HasErrors(pipeline_stage=self.__class__.__name__ + '._transformer')
            if he.addError(e, path=self.path):
                logd.exception(e)  # FIXME isn't this were we should accumulate errors?

            he.embedErrors(NoData.data)
            return NoData

        except Exception as e:
            # sigh code duplication
            class NoData:  # FIXME
                data = {}
                t = f'No data for {self.path}'

            he = dat.HasErrors(pipeline_stage=self.__class__.__name__ + '._transformer')
            if he.addError(e, path=self.path):
                logd.exception(e)  # FIXME isn't this were we should accumulate errors?
                log.critical(f'Unhandled nearly fatal error in {self.path} {e} {type(e)}')

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
        if 'template_schema_version' not in out:  # schema version MUST be present or downstream pipelines break
            out['template_schema_version'] = None
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
        #dsr_path = self.path.relative_to(self.path.cache.dataset)
        contents = super().transformed
        data = {'contents': contents}
        self.path.populateJsonMetadata(data)
        return data

    @hasSchema(sc.ManifestFileSchema)
    def data(self):
        return super().data


class XmlFilePipeline(Pipeline):  # XXX FIXME temporary (HAH)

    def __init__(self, previous_pipeline, lifters, runtime_context):
        self.id = previous_pipeline.data['id']
        self.path = runtime_context.path # FIXME nasty implicit nonsense

    @property  # schema is handled internally
    def data(self):
        return self.do_xml_metadata(self.path, self.id)

    @staticmethod
    def _path_to_json_meta(path):
        e = exml.ExtractXml(path)
        metadata = path._jsonMetadata(do_expensive_operations=False)
        # XXX sideeffects to retrieve path metadata
        # FIXME expensive operations is hardcoded & blocked
        if e.mimetype:
            metadata['contents'] = e.asDict()
            # FIXME TODO probably want to check that the mimetypes are compatible when overwriting?
            metadata['mimetype'] = e.mimetype  # overwrites the type

        return metadata

    @classmethod
    def do_xml_metadata(cls, local, id):
        # FIXME this is extremely inefficient to run on all datasets
        # we have a find command to subset the datasets so we don't
        # have to do an exhaustive search ... someone has to do it
        # but it should only be done once maybe at the start?
        local_xmls = list(local.rglob('*.xml'))
        missing = [p.as_posix() for p in local_xmls if not p.exists()]
        if missing:
            oops = "\n".join(missing)
            raise BaseException(f'unfetched children\n{oops}')

        blob = {'type': 'all-xml-files',  # FIXME not quite correct use of type here
                'dataset_id': id,
                'xml': tuple()}
        blob['xml'] = [cls._path_to_json_meta(path) for path in local_xmls]
        return blob


class Merge(Pipeline):

    def __init__(self, pipelines, lifters=None, runtime_context=None):
        self.pipelines = pipelines
        self.lifters = lifters
        self.runtime_context = runtime_context

    @property
    def data(self):
        data = {}
        for pipeline in self.pipelines:
            try:
                data.update(pipeline.data)
            except TypeError as e:
                msg = (f'Issue in {pipeline}.data '
                       f'{self.runtime_context.id}\n'
                       f'{self.runtime_context.path}')
                if hasattr(pipeline, 'path'):
                    # FIXME this doesn't work right now
                    # but it should be possible to pinpoint
                    # the exact file causing errors, though
                    # the dataset is good enough to target
                    # the debugging
                    msg += f'\n{pipeline.path}'

                raise TypeError(msg) from e

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
    cleans = []  # FIXME should really be able to do cleans after derives too
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
            # FIXME this is a flagrant violation of inversion of control
            # HOWEVER it does allow a pipeline to ensure that it knows what it is getting
            # but that is much better done as a type check than forcing execution from
            # the input that the string of previous pipelines expects
            # p4(p3(p2(p1(some-static-input)))) has its own problems but is easier to
            # understand than what we do here which is
            # p1(ssi): do1(ssi)
            # p2(ssi): do2(p1(ssi))
            # p3(ssi): do3(p2(ssi))
            # p4(ssi): do4(p3(ssi))
            # p4(ssi) -> do4(do3(do2(do1(ssi))))
            # this makes access to the do functions inaccessible and uncomposable which is bad
            # better to specify the do functions directy and then wrap them in a function
            # that takes the arguments that we want to start from than forcing the ordering

            # of course because python doesn't have a reasonable way to reuse code
            # the tradeoff here is that if you want to actually reuse code you have to
            # obscure the do4

            previous_pipeline = Merge([p(previous_pipeline, lifters, runtime_context)
                                        for p in self.previous_pipeline_classes],
                                      lifters,
                                      runtime_context)

        if False:
            # unfortunately this breaks assumptions about the pipeline structure
            # as predicated on my comment in PipelineExtras.added so we can't use
            # it right now
            if len(self.previous_pipeline_classes) > 1:
                previous_pipeline = Merge([p(previous_pipeline, lifters, runtime_context)
                                           for p in self.previous_pipeline_classes],
                                          lifters,
                                          runtime_context)
            else:
                PPC, = self.previous_pipeline_classes
                previous_pipeline = PPC(previous_pipeline, lifters, runtime_context)

        if not isinstance(previous_pipeline, Pipeline):
            raise TypeError(f'{previous_pipeline} is not a Pipeline!')

        self.previous_pipeline = previous_pipeline
        self.lifters = lifters
        self.runtime_context = runtime_context
        if hasattr(runtime_context, 'path'):
            self.path = runtime_context.path

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

        if hasattr(self, 'path'):
            data[_THIS_PATH_KEY] = self.path

        # FIXME THIS_PATH is cool but but violates our desire to keep validating the
        # existence of things separate from rearranging them
        try:
            DictTransformer.derive(data, self.derives, source_key_optional=True)
        finally:
            if _THIS_PATH_KEY in data:
                data.pop(_THIS_PATH_KEY)

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
        try:
            data = super().pipeline_start
        except self.SkipPipelineError:
            raise
        except Exception as e:
            log.critical(f'Unhandled exception {e}')
            log.exception(e)
            # FIXME probably need more than this
            # FIXME hard to maintain this
            data = {'id': self.runtime_context.id,
                    'meta': {'uri_api': self.runtime_context.uri_api,
                             'uri_human': self.runtime_context.uri_human,}}
            raise self.SkipPipelineError(data) from e

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
          [['dataset_description_file', 'template_schema_version'], ['template_schema_version']]],
         SubmissionFilePipeline,
         ['submission_file']],

        [[[['subjects_file'], ['path']],
          [['dataset_description_file', 'template_schema_version'], ['template_schema_version']]],
         SubjectsFilePipeline,
         ['subjects_file']],

        [[[['samples_file'], ['path']],
          [['dataset_description_file', 'template_schema_version'], ['template_schema_version']]],
         SamplesFilePipeline,
         ['samples_file']],

        [[[['manifest_file'], ['paths']],
          [['dataset_description_file', 'template_schema_version'], ['template_schema_version']]],
         MapPathsCombinator(ManifestFilePipeline),
         ['manifest_file']],
    ]

    copies = ([['dataset_description_file', 'contributors'], ['contributors']],
              [['subjects_file',], ['inputs', 'subjects_file']],
              [['submission_file', 'submission'], ['submission']],
              [['submission_file',], ['inputs', 'submission_file']],
              [['samples_file',], ['inputs', 'samples_file']],
              [['manifest_file',], ['inputs', 'manifest_file']],
              [['dataset_description_file', 'name'], ['meta', 'title']],
              *copy_all(['dataset_description_file'], ['meta'],
                        'template_schema_version',
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

    moves = (
        [['dirs',], ['meta', 'dirs']],  # FIXME not quite right ...
        [['files',], ['meta', 'files']],
        [['size',], ['meta', 'size']],

        [['dataset_description_file',], ['inputs', 'dataset_description_file']],
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

    # replace lifters with proper upstream pipelines (now done with DatasetMetadata)
    adds = [[['prov', 'timestamp_export_start'], lambda lifters: lifters.timestamp_export_start],
            [['prov', 'sparcur_version'], lambda _: sparcur.__version__],
            ]

    __filerp = aug.RepoPath(__file__)
    if __filerp.working_dir is not None:
        # TODO consider embedding a commit during release?
        __commit = __filerp.repo.active_branch.commit.hexsha
        adds.append([['prov', 'sparcur_commit'], lambda _, commit=__commit: commit])

    @property
    def pipeline_start(self):
        # XXX can't just assign property from PipelineStart
        # because super() will break file under LOL PYTHON
        # otherwise the methods should be identical >_<
        # I have tried every variant, but the way super()
        # works is completely broken code reuse
        try:
            data = super().pipeline_start
        except self.SkipPipelineError:
            raise
        except Exception as e:
            log.critical(f'Unhandled exception {e}')
            log.exception(e)
            # FIXME probably need more than this
            # FIXME hard to maintain this
            data = {'id': self.runtime_context.id,
                    'meta': {'uri_api': self.runtime_context.uri_api,
                             'uri_human': self.runtime_context.uri_human,}}
            raise self.SkipPipelineError(data) from e

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

    subpipelines = (
        [[[['id'], ['id']]],
         BlackfynnDatasetDataPipeline,
         ['inputs', 'remote_dataset_metadata']],

        [[[['id'], ['id']]],  # TODO need to know in which datasets to do which filetypes
         XmlFilePipeline,  # FIXME need a way to mrege lists
         ['inputs', 'xml']],  # FIXME need to remove this when we're done

        [[[['contributors'], None]],
         ContributorsPipeline,
         None],

        [[[['submission'], None]],
         SubmissionPipeline,
         ['submission']],
    )

    copies = (

        [['inputs', 'remote_dataset_metadata', 'status-log', 'entries', 0],
         ['status', 'status_on_platform']],

        [['inputs', 'remote_dataset_metadata', 'readme', 'readme'],
         ['rmeta', 'readme']],
    )

    derives = (
        [[['inputs', 'remote_dataset_metadata', 'doi']],
         DT.BOX(De.doi),
         [['meta', 'doi']]],

        [[THIS_PATH, ['inputs', 'manifest_file'], ['inputs', 'xml']],  # TODO
        De.path_metadata,  # XXX WARNING this goes back and hits the file system
         [['path_metadata'], ['scaffolds']]
         ],
    )

    __mr_path = ['metadata_file', int, 'contents', 'manifest_records', int]
    updates = [
        # this is the stage at which normalization should actually happen I think
        # or at least normalization that is not required to prevent pipelines from
        # breaking themselves
        # TODO a way to indicate any key matching ? or does that seem like a bad idea?
        # FIXME this is a nasty network step
        # maybe move these to a PipelineNormalization class? that would simplify
        # the madness that is the current normalization implementation ...
        [['meta', 'protocol_url_or_doi'], norm.protocol_url_or_doi],
        [['samples', int, 'protocol_url_or_doi'], norm.protocol_url_or_doi],
        [['subjects', int, 'protocol_url_or_doi'], norm.protocol_url_or_doi],
        [__mr_path + ['protocol_url_or_doi'], norm.protocol_url_or_doi],
        #[__mr_path + ['software_rrid'], norm.rrid],  # FIXME how do we handle errors here?

        # TODO ... this isn't really norm at this point, more mapping.species
        #[['samples', int, 'species'], norm.species],
        #[['subjects', int, 'species'], norm.species],
    ]

    adds = [[['meta', 'techniques'], lambda lifters: lifters.techniques]]

    filter_failures = (  # XXX TODO implement this for other pipelines as well ?
        lambda p: ('inputs' not in p and 'contributor_role' in p),
        lambda p: ('inputs' not in p and 'contributor_orcid' in p),
    )

    @property
    def updated(self):
        return super().updated

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
                        try:
                            if ot.curie == 'FMA:7202':
                                label = 'gall bladder'
                            else:
                                label = ot.label

                            _o = next(OntTerm.query(label=label, prefix='UBERON'))
                        except BaseException as e:
                            log.critical(ot)
                            continue

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
                    breakpoint()
                    raise exc.ShouldNotHappenError('urg')

            else:
                data['meta']['protocol_url_or_doi'] += tuple(self.lifters.protocol_uris)
                data['meta']['protocol_url_or_doi'] = tuple(sorted(set(data['meta']['protocol_url_or_doi'])))  # ick

        # FIXME HACK this is a clean
        if 'xml' in data['inputs']:
            # FIXME should be trivial except that clean comes before derives
            data['inputs'].pop('xml')

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
        [['@context'], lambda l: copy.deepcopy(sc.base_context)],  # FIXME should be a link
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


class ExtractProtocolIds(Pipeline):

    def __init__(self, blob):
        self._blob = blob

    @property
    def data(self):
        data = self._blob
        collect = []
        JApplyRecursive(get_nested_by_key,
                        data,
                        'protocol_url_or_doi',
                        collect=collect)
        unique = sorted(set(collect))  # already dereferenced and normalized at this point
        return [i for i in ids if isinstance(i, idlib.Pio)]


class ProtcurPipeline(Pipeline):

    # there is no previous pipeline here
    # FIXME cleanup and align naming with path pipeline probably?

    def __init__(self, *hypothesis_group_names, no_network=False):
        self._hypothesis_group_names = hypothesis_group_names
        self._no_network = no_network

    def load(self):  # if the dereferenced resource is remote, get a local copy
        annos = []
        for group_name in self._hypothesis_group_names:
            group_id = auth.user_config.secrets('hypothesis', 'group', group_name)
            cache_file = Path(hyp.group_to_memfile(group_id + 'sparcur'))
            get_annos = hyp.AnnoReader(cache_file, group_id)
            annos.extend(get_annos())
            # FIXME hyputils has never been engineered to work with
            # multiple groups at the same time using the same auth
            # so everything is backwards from what it should be since
            # the perspective of the original imlementation was focused
            # on groups (and because no implementation should be focused
            # on auth, which should be handled orthgonally)

        return annos#[:5000]  # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

    _logged_nn = set()
    @staticmethod
    def _annos_to_json(data, annos, sheets_lookup, Term):
        def partition(a):
            return ('ast-no-parents'
                    if a.is_protcur_lang() else
                    ('not-ast' if not a.isAstNode else None))

        def normalize_tech(a):
            have_value = False
            cands = (
                'tech:',
                'NLXINV:',
                'BIRNLEX:',
            )
            for tag in chain(a.tags, (t for c in a.replies for t in c.tags)):
                if tag == 'ilxtr:technique':
                    continue
                elif any(tag.startswith(t) for t in cands):
                    return OntTerm(tag)  # FIXME jsonld string issue ...
                elif 'technique' in tag and ' ' not in tag:
                    return tag

            #return a.value

        def normalize_node(a):

            try:
                racket = a.asPython()
            except a.pyru.RacketParser.ParseFailure as e:
                log.exception(e)  # bigger deal than the parse errors below
                return

            if racket is None:
                log.debug(f'bad protc:parameter* value {a.value}')
                return

            try:
                ast = racket.asPython()
            except racket.ParseFailure as e:
                log.debug(repr(e))
                return
            except Exception as e:
                log.exception(e)
                return
                #return normalize_other(a)  # we already have this under TEMPRAW

            # FIXME FIXME EVIL NETWORK
            at = ast.prov.astType
            key = 'working-' + at
            if key in sheets_lookup:
                try:
                    aligned = sheets_lookup[key].map(a)
                    if aligned is not None:
                        # TODO label
                        alt = Term(aligned, None, ast._value)  # None to avoid mismatch in OntTerm
                        nast = ast.__class__(alt, *ast.body, prov=ast.prov)
                        return nast
                except AttributeError as e:
                    # TODO
                    pass
            else:
                if at not in ProtcurPipeline._logged_nn:
                    ProtcurPipeline._logged_nn.add(at)
                    log.critical(f'no working sheet for {at}')

            return ast

        def normalize_working(a):
            # TODO alignment
            # FIXME need to sandbox the network access here
            # do it by attaching everything to the hypothesis annotations
            #lookup(a.value)
            return

        def normalize_other(a):
            for tag in a.tags:
                if tag.startswith('UBERON:'):
                    return OntTerm(tag)  # FIXME jsonld string issue

            if a.value:
                return a.value
            elif a.exact:
                return a.exact
            else:
                return a.text


        def lift_urls(a):
            if isinstance(a, OntTerm):
                return a

        def clean(v):
            if isinstance(v, str) and not isinstance(v, OntId):  # sigh, yep, idlib has the better design
                nbsp = '\xa0'  # the vails of the non breaking space for formatting :/
                return v.strip().replace(nbsp, ' ')
            else:
                return v

        hrm = defaultdict(list)
        _ = [hrm[partition(a)].append(a) for a in annos]
        anp = hrm['ast-no-parents']  # TODO
        na = hrm['not-ast']
        nested = hrm[None]  # TODO

        #data['TEMP:hasNumberOfProtcurAnnotations'] = len(anp)
        data['protcur_anno_count'] = len(anp)

        tpn = (
                ('ilxtr:technique', 'TEMPRAW:protocolEmploysTechnique'),

                ('protc:aspect',         'TEMPRAW:protocolInvolvesAspect'),        # FIXME source from protcs
                ('protc:implied-aspect', 'TEMPRAW:protocolInvolvesAspect'),        # FIXME source from protcs
                ('protc:input',          'TEMPRAW:protocolInvolvesInput'),         # FIXME source from protcs
                ('protc:implied-input',  'TEMPRAW:protocolInvolvesInput'),         # FIXME source from protcs
                ('protc:input-instance', 'TEMPRAW:protocolInvolvesInputInstance'), # FIXME source from protcs
                ('protc:output',         'TEMPRAW:protocolInvolvesOutput'),        # FIXME source from protcs
                ('protc:parameter*',     'TEMPRAW:protocolInvolvesParameter'),     # FIXME source from protcs
                ('protc:invariant',      'TEMPRAW:protocolInvolvesInvariant'),     # FIXME source from protcs
                ('protc:executor-verb',  'TEMPRAW:protocolInvolvesAction'),        # FIXME source from protcs

                ('protc:black-box',            'TEMPRAW:lol'),
                ('protc:black-box-component',  'TEMPRAW:lol'),
                ('protc:objective*',           'TEMPRAW:lol'),

                # TODO aspects of subjects vs reagents, primary particiant in the
                # core protocols vs subprotocols

                ('sparc:AnatomicalLocation', 'TEMPRAW:involvesAnatomicalRegion'),
                ('sparc:Measurement',        'TEMPRAW:protocolMakesMeasurement'),
                ('sparc:Reagent',            'TEMPRAW:protocolUsesReagent'),
                ('sparc:Tool',               'TEMPRAW:protocolUsesTool'),
                ('sparc:Sample',             'TEMPRAW:protocolInvolvesSampleType'),
                ('sparc:OrganismSubject',    'TEMPRAW:protocolInvolvesSubjectType'),
                ('sparc:Procedure',          'TEMPRAW:whatIsThisDoingHere'),
        )

        norms = {
            'ilxtr:technique': normalize_tech,

            'protc:input': normalize_node,
            'protc:implied-input': normalize_node,

            'protc:aspect': normalize_node,
            'protc:implied-aspect': normalize_node,

            'protc:parameter*': normalize_node,
            'protc:invariant': normalize_node,

            'sparc:Tool': normalize_working,
            'sparc:Reagent': normalize_working,

            'sparc:AnatomicalLocation': lift_urls,
        }

        tpn += tuple(
            (k,
             #[pred for tag, pred, *norm in tpn if tag == k][0].replace('TEMPRAW:', 'TEMP:'),
             ('protocolEmploysTechnique'
              if k == 'ilxtr:technique' else
              k.replace('protc:', 'protcur:').replace('sparc:', 'sparcur:')),
             n) for k, n in norms.items())

        class Derp(str):
            def __lt__(self, other):
                try:
                    return super().__le__(self, other)
                except TypeError:
                    return not other >= self

        def key(v):
            if isinstance(v, str):
                return Derp(v)
            else:
                return v

        for tag, predicate, *norm in tpn:
            #log.critical((tag, predicate, *norm))
            if not norm:
                norm = normalize_other
            else:
                norm, = norm

            normed = [norm(a) for a in annos if tag in a.tags]
            # FIXME not at all clear that these should be sorted
            # it added tons of complexity to the pyru implementation
            vals = sorted(set([clean(n) for n in normed if n]),
                          key=key)
            if vals:
                #if [p for p in ('ilxtr:', 'protc:', 'sparc:') if tag.startswith(p)]:
                if predicate.startswith('TEMPRAW:'):
                    pred = tag
                else:
                    pred = predicate

                pred = pred.replace(':', '_')  # FIXME sigh
                if predicate.startswith('TEMPRAW:'):
                    data[pred] = vals
                else:
                    #log.critical((tag, pred, norm, vals))
                    #if [v for v in vals if isinstance(v, str)]:
                        #breakpoint()

                    data[pred] = ['protcur:' + v.prov.id
                                  if hasattr(v, 'prov') else
                                  v  # assume it is an id probably wrongly
                                  for v in vals]  # FIXME
                    yield from vals  # they go in the graph
                #data[tag] = vals
            #else:
                #log.critical([t for a in annos for t in a.tags])

    def _idints(self, annos=None):
        if annos is None:
            annos = self.load()

        _annos = annos
        annos = [ptcdoc.Annotation(a) for a in annos]
        #pool = ptcdoc.Pool(annos)
        #anno_counts = ptcdoc.AnnoCounts(pool)
        #idn = ptcdoc.IdNormalization(anno_counts.all())
        idn = ptcdoc.IdNormalization(annos)
        protc.reset(reset_annos_dict=True)  # sigh, yes we have to do this here
        #breakpoint()
        protcs = [protc(f, annos) for f in annos]
        protc.pyru.Term._OntTerm = OntTerm  # FIXME this is and EXTREMELY obscure
        # place to doing what is effectively an import :/ talk about bad engineering
        # decisions and requirements interacting to produce completely pathalogical
        # behavior :/ slow startup from cli are the primary issue :/
        idints = idn._uri_api_ints()

        if None in idints:
            nones = idints.pop(None)
            nidn = ptcdoc.IdNormalization(nones)
            idints.update(nidn.normalized())

        pidints = {k:[protc.byId(a.id) for a in v] for k, v in idints.items()}
        return annos, idints, pidints

    def _sheets_lookup(self):
        if self._no_network:  # FIXME HACK
            return {}

        from sparcur import sheets  # FIXME EVIL EVIL EVIL
        sheets_lookup = {S.sheet_name:S()
                         for S in subclasses(sheets.AnnoTags)
                         if 'working' in S.sheet_name}
        return sheets_lookup

    def _hrm(self):
        def urih(data, pio):
            if (hasattr(pio, '_progenitors') and
                'id-converted-from' in pio._progenitors):
                orig_pio = pio._progenitors['id-converted-from']
                if orig_pio.asStr() != pio.asStr():
                    try:
                        if hasattr(orig_pio, 'uri_human') and orig_pio.uri_human:
                            data['uri_human'] = orig_pio.uri_human
                        elif orig_pio.slug_tail:
                            data['uri_human'] = orig_pio
                    except idlib.exc.RemoteError as e:  # FIXME network sandbox violation
                        logd.exception(e)

        sheets_lookup = self._sheets_lookup()
        annos, idints, pidints = self._idints()
        out = []
        for pio, pannos in pidints.items():
            data = {}
            if type(pio) != str:
                _id = pio.asStr()
            else:
                _id = pio

            data['id'] = pio
            data['@type'] = ['sparc:Protocol', 'owl:NamedIndividual']
            try:
                if hasattr(pio, 'uri_human') and pio.uri_human:
                    # calling hasattr on a property evokes sideffects !? # file under LOL PYTHON
                    data['uri_human'] = pio.uri_human
                else:
                    urih(data, pio)
                    if 'uri_human' not in data:
                        msg = ('No uri_human for '
                               f'{pio.asStr() if hasattr(pio, "asStr") else pio}')
                        log.error(msg)
                        #annos = None
                        #idints = None
                        #pidints = None
                        #pannos = None
                        #breakpoint()
            except idlib.exc.RemoteError as e:  # FIXME network sandbox violation
                urih(data, pio)
                if 'uri_human' not in data:
                    log.error(f'No uri_human for {pio.asStr()}')
                    # TODO embed error status
                    #annos = None
                    #idints = None
                    #pidints = None
                    #pannos = None
                    #breakpoint()

            try:
                if hasattr(pio, 'doi') and pio.doi:
                    data['doi'] = pio.doi
            except idlib.exc.RemoteError as e:
                # TODO embed error status
                pass

            from pysercomb.pyr.units import Term
            extras = list(self._annos_to_json(data, pannos, sheets_lookup, Term))
            #log.debug(data)
            out.append(data)
            out.extend(extras)

        return out

    @property
    def data(self):
        return self._hrm()


class ProtocolPipeline(Pipeline):

    # previous pipeline is pipeline end, but
    # it doesn't run that way

    def __init__(self, blob):
        pass


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

    def __init__(self):
        pass

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
