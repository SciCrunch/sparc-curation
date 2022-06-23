import json
import logging
import tempfile
from socket import gethostname
from functools import wraps
import idlib
from pyontutils.utils import Async, deferred, makeSimpleLogger # byCol as _byCol
from sparcur import exceptions as exc
from sparcur import datasets as dat
from sparcur.core import JT
from sparcur.core import adops, OntTerm, JEncode
from sparcur.paths import Path, CacheL, PennsieveCache as RemoteCache
from sparcur.state import State
from sparcur.utils import log, fromJson, register_type
from sparcur import schemas as sc
from sparcur.export.triples import TriplesExportDataset, TriplesExportSummary
from sparcur.datasources import OrganData, OntologyData
from sparcur.protocols import ProtocolData
from sparcur import schemas as sc
from sparcur import pipelines as pipes
from sparcur import sheets


class PathData:
    def __new__(cls, path):
        return super().__new__(cls)

    def __init__(self, path):
        self._errors = []
        if isinstance(path, str):
            path = Path(path)

        if not hasattr(self, 'path'):
            self.path = path

    @property
    def id(self):
        return self.path.cache.id

    @property
    def uri_human(self):
        cache = self.path.cache
        if cache.is_organization():
            return cache.identifier.uri_human()

        return cache.uri_human(cache.organization.identifier,
                               cache.dataset.identifier)

    @property
    def uri_api(self):
        return self.path.cache.uri_api

    @property
    def _meta(self):
        """ pathmeta NOT json meta (confusingly) """
        cache = self.path.cache
        if cache is not None:
            return self.path.cache.meta

    @property
    def anchor(self):
        return self.__class__(self.path.cache.anchor.local)


def _wrap_path_gen(prop):
    @property
    @wraps(prop.__get__)
    def impostor(self):
        for v in prop.__get__(self.path):
            yield self.__class__(v)

    return impostor


class Integrator(PathData, OntologyData):
    """ pull everything together anchored to the DatasetStructure """
    # note that mro means that we have to run methods backwards
    # so any class that uses something produced by a function
    # of an earlier class needs to be further down the mro so
    # that the producer masks its NotImplementedErrors

    # class level helpers, instance level helpers go as mixins

    no_google = None
    rate = 8

    parent = _wrap_path_gen(Path.parent)
    parents = _wrap_path_gen(Path.parents)
    children = _wrap_path_gen(Path.children)
    rchildren = _wrap_path_gen(Path.rchildren)

    triples_class = TriplesExportDataset

    @classmethod
    def setup(cls, *, local_only=False):
        # FIXME this is a mess
        """ make sure we have all datasources
            calling this again will refresh helpers
        """
        if hasattr(Integrator, '__setup') and Integrator.__setup:
            return  # already setup

        Integrator.__setup = True

        for _cls in cls.mro():
            if _cls != cls:
                if hasattr(_cls, 'setup'):
                    _cls.setup()

        dat.DatasetStructure.rate = cls.rate

        class FakeOrganSheet:
            modality = lambda v: None
            organ_term = lambda v: None
            award_manual = lambda v: None
            #byCol = _byCol([['award', 'award_manual', 'organ_term'], []])
            techniques = lambda v: []
            protocol_uris = lambda v: []

        class FakeAffilSheet:
            def __call__(self, *args, **kwargs):
                return
        class FakeOverviewSheet:
            def __call__(self, *args, **kwargs):
                return

        # unanchored helpers
        if cls.no_google or local_only:
            log.critical('no google no organ data')
            cls.organs_sheet = FakeOrganSheet
            cls.affiliations = FakeAffilSheet()
            #cls.overview_sheet = FakeOverviewSheet()
        else:
            # ipv6 resolution issues :/ also issues with pickling
            #cls.organs_sheet = sheets.Organs(fetch_grid=True)  # this kills parallelism
            cls.organs_sheet = sheets.Organs()  # if fetch_grid = False @ class level ok
            cls.affiliations = sheets.Affiliations()
            #cls.overview_sheet = sheets.Overview()

            # zap all the services (apparently doesn't help)
            # yep, its just the organ sheet, these go in and out just fine
            #if hasattr(sheets.Sheet, '_Sheet__spreadsheet_service'):
                #delattr(sheets.Sheet, '_Sheet__spreadsheet_service')
            #if hasattr(sheets.Sheet, '_Sheet__spreadsheet_service_ro'):
                #delattr(sheets.Sheet, '_Sheet__spreadsheet_service_ro')

            #for s in (cls.organs_sheet, cls.affiliations, cls.overview_sheet):
                #if hasattr(s, '_spreadsheet_service'):
                    #delattr(s, '_spreadsheet_service')

            # YOU THOUGHT IT WAS GOOGLE IT WAS ME ORGANS ALL ALONG!
            #cls.organs_sheet = FakeOrganSheet  # organs is BAD

            #cls.affiliations = FakeAffilSheet()  # affiliations is OK
            #cls.overview_sheet = FakeOverviewSheet()  # overview is OK

            #breakpoint()
            # remove byCol which is unpickleable (super duper sigh)
            #for s in (cls.organs_sheet, cls.affiliations, cls.overview_sheet):
                #if hasattr(s, 'byCol'):
                    #delattr(s, 'byCol')

        if cls.no_google:
            cls.organ = lambda award: None

        if local_only:
            cls.organ = lambda award: None
            cls.member = lambda first, last: None
        else:
            cls.organ = OrganData()
            if hasattr(State, 'member'):
                cls.member = State.member
            else:
                log.error('State missing member, using State seems '
                          'like a good idea until you go to multiprocessing')
                cls.member = lambda first, last: None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datasetdata = dat.DatasetStructure(self.path)

    @property
    def triples(self):
        # convenience property that ttl doesn't use directly
        # but is here in case we want to access the triples
        yield from self.triples_class(self.data()).triples

    @property
    def ttl(self):  # FIXME BAD PATTERN IS BAD
        return self.triples_class(self.data()).ttl

    @property
    def name(self):
        return self.path.name

    def pipeline(self, timestamp):
        if hasattr(self, '_pipeline'):
            return self._pipeline

        datasetdata = self.datasetdata
        dsc = datasetdata.cache.dataset
        if dsc is None:
            raise exc.NotInDatasetError

        #dataset = dat.DatasetStructure(dsc)  # FIXME should be able to go back

        class ProtocolHelper(ProtocolData):  # FIXME so ... bad ...
            @property
            def protocol_uris(self, outer_self=self):  # FIXME this needs to be pipelined
                try:
                    yield from adops.get(outer_self.data(), ['meta', 'protocol_url_or_doi'])
                except exc.NoSourcePathError:
                    pass

        ph = ProtocolHelper()

        # FIXME except for the adds, everything here should be a pipeline
        class Lifters:  # do this to prevent accidental data leaks
            # context
            id = dsc.id  # in case we are somewhere else
            remote = self.path._cache_class._remote_class._remote_type
            timestamp_export_start = timestamp
            #folder_name = dsc.name
            #uri_api = dsc.uri_api
            #uri_human = dsc.uri_human

            # protocols
            protocol = ph.protocol
            #protcur = self.protcur

            # aux
            organ = self.organ
            #member = self.member  # not used via lifters, instead used via state
            affiliations = self.affiliations

            # hypothesis that the calling of methods was the issue
            # also fails
            _organs_sheet = self.organs_sheet
            #modality = self.organs_sheet.modality(id)
            #award_manual = self.organs_sheet.award_manual(id)
            #techniques = self.organs_sheet.techniques(id)
            #protocol_uris = self.organs_sheet.protocol_uris(id)
            @property
            def modality(self):
                return self._organs_sheet.modality(self.id)
            @property
            def award_manual(self):
                return self._organs_sheet.award_manual(self.id)
            @property
            def techniques(self):
                return self._organs_sheet.techniques(self.id)
            @property
            def protocol_uris(self):
                return self._organs_sheet.protocol_uris(self.id)

            #sheets
            #organ_term = self.organs_sheet.organ_term(id)
            @property
            def organ_term(self):
                return self._organs_sheet.organ_term(self.id)

        class RuntimeContext:
            """ utility for logging etc. """
            id = dsc.id  # TODO can we eliminate the need for lifters this way?
            uri_api = dsc.uri_api  # FIXME black blackfynn ids their own thing
            uri_human = dsc.uri_human  # so uri_api and uri_human can be computed from them ...
            path = self.path

        lifters = Lifters()
        # helper key was bad because it means that pipelines have to
        # know the structure of their helper pipelines and maintain them
        # across changes, better to use the lifters as other pipelines that
        # can start from a single piece of data and return a formatted result
        # and track their own provenance, the concept of a helper that holds
        # all the prior existing data is not a bad one, it just just that it
        # is better to create objects that can take the information already
        # in the data for the current pipeline and return an expanded verion
        self._pipeline = pipes.PipelineEnd(self.path, lifters, RuntimeContext())
        return self._pipeline

    def data(self, timestamp=None):
        if hasattr(self, '_data'):
            return self._data

        self._data = self.pipeline(timestamp).data
        return self._data

    def data_for_export(self, timestamp):
        data = self.data(timestamp)
        # NOTE this timestamps the cached data AS INTENDED
        # XXX DO NOT OVERWRITE THE PROV FROM THE PIPELINE ITSELF!
        #data['prov'] = {
            #'export_system_identifier': Path.sysid,
            #'export_hostname': gethostname(),
            #'export_project_path': self.path.cache.anchor.local,
        #}
        #data['prov']['timestamp_export_start'] = timestamp
        # FIXME XXX there has GOT to be a better way to get this out
        data['prov']['export_project_path'] = self.path.cache.anchor.local
        return data

    @property
    def keywords(self):
        try:
            yield from adops.get(self.data(), ['meta', 'keywords'])
        except exc.NoSourcePathError:
            pass

    @property
    def triples_exporter(self):
        if not hasattr(self, '_triples_exporter'):
            self._triples_exporter = TriplesExportDataset(self.data())

        return self._triples_exporter

    def __getnewargs_ex__(self):
        return (self.path,), {}

    def __getstate__(self):
        state = self.__dict__
        state['no_google'] = self.no_google
        return state

    def __setstate__(self, state):
        self.__class__.no_google = state['no_google']
        self.__class__.setup()
        self.__dict__.update(state)


class IntegratorSafe(Integrator):
    """ bypass setup since we should already have the data """

    def setup(cls, *, local_only=False):
        """ make really sure that we aren't running setup """

    def add_helpers(self, helpers):
        for k, v in helpers.items():
            setattr(self, k, v)

    def __setstate__(self, state):
        self.__class__.no_google = state['no_google']
        #self.__class__.setup()  # NOPE
        self.__dict__.update(state)


class JsonObject:
    @classmethod
    def from_json(cls, json):
        self = object.__new__(cls)
        self.data = json
        return self


class DatasetObject(JsonObject):
    """ Object representation of a dataset. Currently static. """

    @classmethod
    def from_pipeline(cls, pipeline):
        """ inversion of control, use this in pipeline.object """
        self = object.__new__(cls)
        self.data = pipeline.data
        return self

    @property
    def errors(self):
        for error_type in 'submission', 'curation':
            for error_blob in self.data()['status'][error_type + '_errors']:
                yield ErrorObject.from_json(error_blob)


class ErrorObject(JsonObject):
    """ Static representation of an error. """
    # FIXME error type ...
    _keys = ('pipeline_stage',
             'message',
             'schema_path',
             'validator',
             'validator_value')

    @classmethod
    def from_json(cls, json):
        self = super().from_json(json)
        for key in self._keys:
            if key in self.data():
                value = self.data()[key]
            else:
                value = None

            setattr(self, key, value)

        return self

    def as_table(self):
        d = self.data()
        def ex(k):
            v = getattr(self, k)
            if v is None:
                return ''
            else:
                return str(v)

        return [['Key', 'Value']] + [[key, ex(key)] for key in self._keys]


class ExporterSummarizer:
    def __init__(self, data_json):
        self.data_json = data_json

    def __iter__(self):
        for dataset_blob in self.data_json['datasets']:
            yield dataset_blob

    @property
    def completeness(self):
        """ completeness, name, and id for all datasets """
        for dataset_blob in self:
            yield self._completeness(dataset_blob)

    @staticmethod
    def _completeness(data):
        accessor = JT(data)  # can go direct if elements are always present
        #organ = accessor.query('meta', 'organ')
        try:
            organ = adops.get(data, ['meta', 'organ'])
        except:
            organ = None

        if isinstance(organ, list) or isinstance(organ, tuple):
            if len(organ) == 1:
                organ, = organ
                try:
                    organ = OntTerm(organ)
                except Exception as e:
                    log.exception(e)
                    organ = ['GARBAGE', organ]
            else:
                try:
                    organ = [OntTerm(o) for o in organ]
                except Exception as e:
                    log.exception(e)
                    organ = ['GARBAGE', *organ, 'GARBAGE']

        elif organ == 'othertargets':
            pass
        elif organ:
            try:
                organ = OntTerm(organ)
            except Exception as e:
                log.exception(e)
                organ = ['GARBAGE', organ]

        return (accessor.status.submission_index,
                accessor.status.curation_index,
                accessor.status.error_index,
                #accessor.submission_completeness_index,
                #dataset.name,  # from filename (do we not have that in meta!?)
                accessor.query('meta', 'folder_name'),
                accessor.id, #if 'id' in dowe else None,
                accessor.query('meta', 'award_number'),
                organ,
            )

    @property
    def triples_exporter(self):
        if not hasattr(self, '_triples_exporter'):
            self._triples_exporter = TriplesExportSummary(self.data_json)

        return self._triples_exporter

    @property
    def ttl(self):
        return self.triples_exporter.ttl


hasSchema = sc.HasSchema()
@hasSchema.mark
class Summary(Integrator, ExporterSummarizer):
    """ A class that summarizes members of its __base__ class """
    schema_class = sc.SummarySchema
    schema_out_class = sc.SummarySchema
    triples_class = TriplesExportSummary
    _debug = False
    _n_jobs = 12

    def __new__(cls, path, dataset_paths=tuple(), exclude=tuple()):
        #cls.schema = cls.schema_class()
        cls.schema_out = cls.schema_out_class()
        return super().__new__(cls, path)

    def __init__(self, path, dataset_paths=tuple(), exclude=tuple()):
        super().__init__(path)
        # not sure if this is kosher ... but it works

        # avoid infinite recursion of calling self.anchor.path
        super().__init__(self.path.cache.anchor.local) # FIXME ugh
        self._use_these_datasets = dataset_paths
        self._exclude = exclude
 
    @property
    def iter_datasets_safe(self):
        if self._use_these_datasets:
            gen = enumerate(self._use_these_datasets)
        else:
            gen = enumerate(self.anchor.path.iterdir())

        for i, path in gen:
            # FIXME non homogenous, need to find a better way ...
            if path.cache is None and path.skip_cache:
                continue

            if path.cache.id in self._exclude:
                log.info(f'skipping export of {path} because '
                         f'{path.cache.id} is in noexport')
                continue

            yield IntegratorSafe(path)

    @property
    def iter_datasets(self):
        """ Return the list of datasets for the organization
            of the current Integrator regardless of whether that
            Integrator is itself an organization node """

        if self._use_these_datasets:
            gen = enumerate(self._use_these_datasets)
        else:
            gen = enumerate(self.anchor.path.children)

        # when going up or down the tree _ALWAYS_
        # use the filesystem as the source of truth
        # do not cache in here
        for i, path in gen:
            # FIXME non homogenous, need to find a better way ...
            if path.cache is None and path.skip_cache:
                continue

            if path.cache.id in self._exclude:
                log.info(f'skipping export of {path} because {path.cache.id} is in noexport')
                continue

            yield self.__class__.__base__(path)

    def __iter__(self):
        for ds in self.iter_datasets:
            yield ds.data()  # FIXME SIGH can't pass timestamp KILL IT WITH FIRE

    @property
    def completeness(self):
        """ completeness, name, and id for all datasets """
        for dataset_blob in self:
            yield self._completeness(dataset_blob)

    def make_json(self, gen):
        # FIXME this and the datasets is kind of confusing ...
        # might be worth moving those into a reporting class
        # that always works at the top level regardless of which
        # file it is give?

        # TODO parallelize and stream this!
        out = {'id': self.id,
               'meta': {'folder_name': self.name,
                        'uri_api': self.uri_api,
                        'uri_human': self.uri_human,},
               'prov': {'export_system_identifier': Path.sysid,
                        'export_hostname': gethostname(),
                        'export_project_path': self.path.cache.anchor.local,},}
        ds = list(gen)
        count = len(ds)
        out['datasets'] = ds
        out['meta']['count'] = count
        return out

    @property
    def pipeline_end(self):
        return self._pipeline_end()

    def _pipeline_end(self, timestamp=None):
        if not hasattr(self, '_data_cache'):
            # FIXME validating in vs out ...
            # return self.make_json(d.validate_out() for d in self)

            helpers = {
                'organ': self.organ,
                'member': self.member,
            }

            # running Integrator.setup again here breaks the enforcment of single threads

            helpers.update({
                'organs_sheet': self.organs_sheet,
                'affiliations': self.affiliations,
                #'overview_sheet': self.overview_sheet,
            })

            #self.organs_sheet.fetch_grid = False  # does not solve the issue by itself

            # running setup again here does not
            #Integrator.no_google = True
            #Integrator.setup()
            # HOWEVER: overwriting organs_sheet does
            #helpers['organs_sheet'] = self.organs_sheet

            ca = self.path._cache_class._remote_class._cache_anchor

            if not self._debug and self._n_jobs > 1:  # yes debug masks jobs for now
                from joblib import Parallel, delayed
                # this flies with no_google, so something is up with
                # the reserialization of the Sheets classes I think
                # FIXME I suspect that the major bottleneck here is deserialization
                # of the types back into this thread, once we have the json -> IR
                # loader we should be able to simply write out the json for each
                # dataset and then reload them all at once and possibly even do
                # the per-dataset ttl conversion as well, parsing the ttl is probably
                # a bad call though
                hrm = Parallel(n_jobs=self._n_jobs)(
                    delayed(datame)(d, ca, timestamp, helpers, log.level)
                    for d in self.iter_datasets_safe)
                #hrm = Async()(deferred(datame)(d) for d in self.iter_datasets)
                self._data_cache = self.make_json(hrm)
            else:
                self._data_cache = self.make_json(datame(d, ca, timestamp, helpers)
                                                  for d in self.iter_datasets_safe)

        return self._data_cache

    @hasSchema.f(sc.SummarySchema, fail=True)
    def data(self, timestamp=None):
        data = self._pipeline_end(timestamp)
        return data  # FIXME we want objects that wrap the output rather than generate it ...

    @hasSchema.f(sc.SummarySchema, fail=True)
    def data_for_export(self, timestamp):
        data = self._pipeline_end(timestamp)
        # NOTE this timestamps the cached data AS INTENDED
        data['prov']['timestamp_export_start'] = timestamp
        return data


_p = Path(tempfile.gettempdir()) / 'asdf'
_p.mkdir(exist_ok=True)  # FIXME XXXXXXXXXXXXXXXXXXXXXXXXXX
def datame(d, ca, timestamp, helpers=None, log_level=logging.INFO, dp=_p,
           evil=[False], dumb=False):
    """ sigh, pickles """
    log_names = ('sparcur',
                 'idlib',
                 'protcur',
                 'orthauth',
                 'ontquery',
                 'augpathlib',
                 'pyontutils')
    for log_name in log_names:
        log = logging.getLogger(log_name)
        if not log.handlers:
            log = makeSimpleLogger(log_name)
            log.setLevel(log_level)
            log.info(f'{log_name} had no handler')
        else:
            if log.level != log_level:
                log.setLevel(log_level)

    rc = d.path._cache_class._remote_class
    if not hasattr(rc, '_cache_anchor'):
        rc._setup()
        rc.anchorTo(ca)

    if not hasattr(RemoteCache, '_anchor'):
        # the fact that we only needed this much later in time
        # tells me that we had actually done an excellent job
        # of firewalling the validation pipeline from anything
        # related to the cache beyond the xatter data

        # can't use ca.__class__ because it is the posix variant of # _cache_class
        RemoteCache._anchor = ca

    prp = d.path.project_relative_path
    if helpers is not None:
        d.add_helpers(helpers)

    did = d.id if '/' not in d.id else d.id.replace('/', '-')  # XXXXXXXXXXXXX FIXME local remote case
    out_path = (dp / did).with_suffix('.json')
    if out_path.exists() and dumb:
        if not evil[0]:  # FIXME this is SO DUMB to do in here, but ...
            from pysercomb.pyr import units as pyru
            iso8601s = (pyru.Iso8601DurationTime,
                        pyru.Iso8601DurationDatetime,
                        pyru.Iso8601DurationDate,)
            [register_type(c, c.tag) for c in
             (pyru._Quant, pyru.Range, pyru.Approximately, *iso8601s)]
            pyru.Term._OntTerm = OntTerm  # the tangled web grows ever deeper :x
            evil[0] = True

        log.warning(f'loading from path {out_path}')
        # FIXME this is _idiotically_ slow with joblib
        # multiple orders of magnitude faster just using listcomp
        with open(out_path, 'rt') as f:
            return fromJson(json.load(f))

    blob_dataset = d.data_for_export(timestamp)
    with open(out_path.with_suffix('.raw.json'), 'wt') as f:  # FIXME XXXXXXXXXXXXXXXXXXXXXXXXXXXX
        json.dump(blob_dataset, f, sort_keys=True, indent=2, cls=JEncode)

    try:
        pipe = pipes.IrToExportJsonPipeline(blob_dataset)  # FIXME network sandbox violation
        blob_export = pipe.data
        with open(out_path, 'wt') as f:  # FIXME XXXXXXXXXXXXXXXXXXXXXXXXXXXX
            json.dump(blob_export, f, sort_keys=True, indent=2, cls=JEncode)
    except Exception as e:
        log.exception(e)
        log.critical(f'error during fancy json export, see previous log entry')

    return blob_dataset
