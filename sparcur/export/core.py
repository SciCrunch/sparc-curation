# cat export/core.py | grep -v '^#' | grep -o '\(self\.[a-zA-Z0-9_\.]\+\)[\ (),{}:]' | rev | cut -c 2- | rev | sort -u

import csv
import json
from socket import gethostname
from itertools import chain
from collections import Counter
import idlib
import requests
from pyontutils.core import OntGraph
from pyontutils.utils import Async, deferred
from sparcur import export as ex
from sparcur import schemas as sc
from sparcur import curation as cur  # FIXME implicit state must be set in cli
from sparcur import pipelines as pipes
from sparcur.core import JEncode, JFixKeys, adops, register_type, fromJson
from sparcur.paths import Path
from sparcur.utils import symlink_latest, loge, logd


def export_schemas(export_schemas_path):
    schemas = (sc.DatasetDescriptionSchema,
               sc.SubjectsSchema,
               sc.SamplesFileSchema,
               sc.SubmissionSchema,)

    if not export_schemas_path.exists():
        export_schemas_path.mkdir()

    for s in schemas:
        s.export(export_schemas_path)


class ExportBase:

    export_type = None
    filename_ir = None

    def __init__(self,
                 export_path,
                 export_source_path,
                 folder_timestamp,
                 timestamp,
                 latest=False,
                 partial=False,
                 open_when_done=False,
                 org_id=None):
        if org_id is None:
            self.export_source_path = export_source_path
            id = export_source_path.cache.anchor.id
        else:
            # do not set export_source_path, to prevent accidental export
            id = org_id

        self.export_base = Path(export_path, id, self.export_type)
        self.latest = latest
        self.partial = partial
        self.folder_timestamp = folder_timestamp
        self.timestamp = timestamp
        self.open_when_done = open_when_done

    @staticmethod
    def make_dump_path(dump_path):
        if not dump_path.exists():
            dump_path.mkdir(parents=True)

    @staticmethod
    def write_json(filepath, blob):
        # FIXME we still create a new export folder every time even if the json didn't change ...
        with open(filepath.with_suffix('.json'), 'wt') as f:
            json.dump(blob, f, sort_keys=True, indent=2, cls=JEncode)

    @property
    def LATEST_PARTIAL(self):
        return self.export_base / 'LATEST_PARTIAL'

    @property
    def LATEST(self):
        """ implicitly LATEST_SUCCESS """
        return self.export_base / 'LATEST'

    @property
    def LATEST_RUN(self):
        return self.export_base / 'LATEST_RUN'

    @property
    def base_path(self):
        return self.LATEST_PARTIAL if self.partial else self.LATEST

    @property
    def latest_export_path(self):
        return self.base_path / self.filename_ir

    @property
    def latest_export(self):
        with open(self.latest_export_path, 'rt') as f:
            return json.load(f)

    @property
    def latest_ir(self):
        # TODO not entirely sure about the best way to do this
        # possibly use a json -> ir pipeline a la raw_json?
        return fromJson(self.latest_export)

    @property
    def dump_path(self):
        return self.export_base / self.folder_timestamp

    @property
    def filepath_ir(self):
        return self.dump_path / self.filename_ir

    def export(self, *args, **kwargs):
        dump_path = self.dump_path
        # make the dump directory
        self.make_dump_path(dump_path)
        symlink_latest(dump_path, self.LATEST_RUN)

        filepath_ir = self.filepath_ir
        # build or load the export of the internal representation
        blob_ir, *rest_ir = self.make_ir(**kwargs)
        export_ir = self.export_ir(blob_ir)
        self.write_json(filepath_ir, export_ir)
        symlink_latest(dump_path, self.LATEST_PARTIAL)

        # build or load derived exports
        self.export_other_formats(dump_path, filepath_ir, blob_ir, *rest_ir)
        symlink_latest(dump_path, self.LATEST)

        return (blob_ir, *rest_ir)  # FIXME :/

    def make_ir(self, *args, **kwargs):
        raise NotImplementedError('implement in subclass')

    def export_ir(self, *args, **kwargs):
        """
        if your ir is identical to your export
        just implement this as
        def export_ir(self, blob_ir): return blob_ir
        """
        raise NotImplementedError('implement in subclass')

    def export_other_formats(self, dump_path, filepath_ir, blob_ir, *rest):
        """ explicitly make this a noop if there aren't other formats you care about """
        raise NotImplementedError('implement in subclass')


class ExportXml(ExportBase):
    """ convert and export metadata embedded in xml files """

    export_type = 'filetype'
    filename_ir = 'xml-export.json'

    def export(self, dataset_paths=tuple(), **kwargs):
        return super().export(dataset_paths=dataset_paths, **kwargs)

    def export_other_formats(self, *args, **kwargs):
        pass

    def make_ir(self, dataset_paths=tuple(), jobs=None, debug=False):
        from sparcur.extract import xml as exml
        def do_xml_metadata(local, id):  # FIXME HACK needs its own pipeline
            local_xmls = list(local.rglob('*.xml'))
            missing = [p.as_posix() for p in local_xmls if not p.exists()]
            if missing:
                oops = "\n".join(missing)
                raise BaseException(f'unfetched children\n{oops}')

            blob = {'type': 'all-xml-files',
                    'dataset_id': id,
                    'xml': tuple()}
            blob['xml'] = [{'path': x.relative_to(local).as_posix(),
                            'type': e.mimetype,  # FIXME should this in the extracted ??
                            'extracted': e.asDict() if e.mimetype else None}
                           for x in local_xmls
                           for e in (exml.ExtractXml(x),)]

            return blob

        if jobs == 1 or debug:
            dataset_dict = {}
            for dataset in dataset_paths:
                blob = do_xml_metadata(dataset.local, dataset.id)
                dataset_dict[dataset.id] = blob
        else:
            # 3.7 0m25.395s, pypy3 fails iwth unpickling error
            from joblib import Parallel, delayed
            from joblib.externals.loky import get_reusable_executor
            hrm = Parallel(n_jobs=9)(delayed(do_xml_metadata)
                                     (dataset.local, dataset.id)
                                     for dataset in dataset_paths)
            get_reusable_executor().shutdown()  # close the loky executor to clear memory
            dataset_dict = {d.id:b for d, b in zip(dataset_paths, hrm)}

        blob_ir = dataset_dict
        return blob_ir,


class Export(ExportBase):

    export_type = 'integrated'
    filename_ir = 'curation-export.json'
    id_metadata = 'identifier-metadata.json'

    _pyru_loaded = False

    @property
    def latest_ir(self):
        if not self.__class__._pyru_loaded:
            self.__class__._pyru_loaded = True
            from pysercomb.pyr import units as pyru
            [register_type(c, c.tag) for c in (pyru._Quant, pyru.Range)]

        return super().latest_ir

    @property
    def latest_ttl_path(self):
        return self.latest_export_path.with_suffix('.ttl')

    @property
    def latest_protocols_path(self):
        return self.base_path / 'protocols.json'

    @property
    def latest_protocols(self):
        with open(self.latest_protocols_path, 'rt') as f:
            return json.load(f)

    @property
    def latest_id_met_path(self):
        return self.base_path / self.id_metadata

    @property
    def latest_id_met(self):
        with open(self.latest_id_met_path, 'rt') as f:
            return json.load(f)

    @property
    def latest_datasets_path(self):
        return self.base_path / 'datasets'

    def latest_export_ttl_populate(self, graph):
        # intentionally fail if the ttl export failed
        lce = self.latest_ttl_path.as_posix()
        return graph.parse(lce, format='ttl')

    def export_single_dataset(self):
        intr = cur.Integrator(self.export_source_path)  # FIXME implicit state set by cli
        dump_path = self.export_base / 'datasets' / intr.id / self.folder_timestamp
        latest_path = self.export_base / 'datasets' / intr.id / 'LATEST'
        latest_partial_path = self.export_base / 'datasets' / intr.id / 'LATEST_PARTIAL'
        if not dump_path.exists():
            dump_path.mkdir(parents=True)

        functions = []
        suffixes = []
        modes = []
        blob_data = intr.data_for_export(self.timestamp)  # build and cache the data
        epipe = pipes.IrToExportJsonPipeline(blob_data)
        blob_export = epipe.data

        # always dump the json
        j = lambda f: json.dump(blob_export, f, sort_keys=True, indent=2, cls=JEncode)
        functions.append(j)
        suffixes.append('.json')
        modes.append('wt')

        # always dump the ttl (for single datasets this is probably ok)
        t = lambda f: f.write(ex.TriplesExportDataset(blob_data).ttl)
        functions.append(t)
        suffixes.append('.ttl')
        modes.append('wb')

        filename = 'curation-export'
        filepath = dump_path / filename

        for function, suffix, mode in zip(functions, suffixes, modes):
            out = filepath.with_suffix(suffix)
            with open(out, mode) as f:
                function(f)

            if suffix == '.json':
                symlink_latest(dump_path, latest_partial_path)

            elif suffix == '.ttl':
                loge.info(f'dataset graph exported to {out}')

            if self.open_when_done:
                out.xopen()

        symlink_latest(dump_path, latest_path)
        return blob_data, intr

    def export_rdf(self, dump_path, latest_path, dataset_blobs):
        dataset_dump_path = dump_path / 'datasets'
        dataset_dump_path.mkdir()
        suffix = '.ttl'
        mode = 'wb'

        wat = [b['id'] for b in dataset_blobs]
        counts = Counter([d for d in wat])
        bads = set(id for id, c in counts.most_common() if c > 1)
        key = lambda d: d['id']
        dupes = sorted([b for b in dataset_blobs if b['id'] in bads], key=key)
        if bads:
            loge.critical(bads)
            # TODO
            #breakpoint()
            #raise BaseException('NOPE')

        teds = []
        for dataset_blob in dataset_blobs:
            filename = dataset_blob['id']
            if filename in bads:
                loge.critical(filename)
                continue
            filepath = dataset_dump_path / filename
            filepsuf = filepath.with_suffix(suffix)
            lfilepath = self.latest_datasets_path / filename
            lfilepath = latest_path / filename
            lfilepsuf = lfilepath.with_suffix(suffix)

            ted = ex.TriplesExportDataset(dataset_blob)
            teds.append(ted)

            if self.latest and lfilepsuf.exists():
                filepsuf.copy_from(lfilepsuf)
                graph = OntGraph(path=lfilepsuf).parse()
                ted._graph = graph
            else:
                ted.graph.write(filepsuf)  # yay OntGraph defaults

            loge.info(f'dataset graph exported to {filepsuf}')

        return teds

    def export_protocols(self, dump_path, dataset_blobs, summary):

        if (self.latest and
            self.latest_protocols_path.exists()):
            blob_protocols = self.latest_protocols
        else:
            protocols = list(summary.protocols(dataset_blobs))
            # FIXME regularize summary structure for discoverability
            blob_protocols = {
                'meta': {'count': len(protocols)},
                'prov': {'timestamp_export_start': self.timestamp,
                         'export_system_identifier': Path.sysid,
                         'export_hostname': gethostname(),
                         'export_project_path': self.export_source_path.cache.anchor,},
                'protocols': protocols,  # FIXME regularize elements ?
            }

        with open(dump_path / 'protocols.json', 'wt') as f:
            json.dump(blob_protocols, f, sort_keys=True, indent=2, cls=JEncode)

        return blob_protocols

    def export_identifier_metadata(self, dump_path, latest_path, dataset_blobs):

        latest_id_met_path = latest_path / self.id_metadata
        if (self.latest and latest_id_met_path.exists()):
            with open(latest_id_met_path, 'rt') as f:
                blob_id_met = json.load(f)

        else:
            def fetch(id):  # FIXME error proof version ...
                try:
                    metadata = id.metadata()
                    metadata['id'] = id
                    return metadata
                except (requests.exceptions.HTTPError, idlib.exceptions.ResolutionError) as e:
                    logd.error(e)
                except (requests.exceptions.ConnectionError, requests.exceptions.SSLError) as e:
                    log.error(e)

            # retrieve doi metadata and materialize it in the dataset
            _dois = set([id
                         if isinstance(id, idlib.Stream) else
                         (fromJson(id) if isinstance(id, dict) else idlib.Auto(id))
                         for blob in dataset_blobs for id in
                         chain(adops.get(blob, ['meta', 'protocol_url_or_doi'], on_failure=[]),
                               adops.get(blob, ['meta', 'originating_article_doi'], on_failure=[]),
                               # TODO data["links"]?
                               [blob['meta']['doi']] if 'doi' in blob['meta'] else [])
                         if id is not None])

            dois = [d for d in _dois if isinstance(d, idlib.Doi)]
            metadatas = Async(rate=10)(deferred(fetch)(d) for d in dois)
            bads = [{'id': d, 'reason': 'no metadata'}  # TODO more granular reporting e.g. 404
                    for d, m in zip(dois, metadatas)
                    if m is None]
            metadatas = [m for m in metadatas if m is not None]
            blob_id_met = {'id': 'identifier-metadata',  # TODO is this ok ?
                           'identifier_metadata': metadatas,
                           'errors': bads,
                           'meta': {'count': len(metadatas)},
                           'prov': {'timestamp_export_start': self.timestamp,
                                    'export_system_identifier': Path.sysid,
                                    'export_hostname': gethostname(),
                                    'export_project_path': self.export_source_path.cache.anchor,},
            }

        with open(dump_path / self.id_metadata, 'wt') as f:
            json.dump(blob_id_met, f, sort_keys=True, indent=2, cls=JEncode)

        return blob_id_met

    @staticmethod
    def export_identifier_rdf(dump_path, identifier_metadata):
        # FIXME not currently dumping ...
        teim = ex.TriplesExportIdentifierMetadata(identifier_metadata)
        return teim

    @staticmethod
    def export_xml(filepath, dataset_blobs):
        # xml export TODO paralleize
        for xml_name, xml in ex.xml(dataset_blobs):
            with open(filepath.with_suffix(f'.{xml_name}.xml'), 'wb') as f:
                f.write(xml)

    @staticmethod
    def export_disco(filepath, dataset_blobs, teds):
        # datasets, contributors, subjects, samples, resources
        for table_name, tabular in ex.disco(dataset_blobs, [t.graph for t in teds]):
            with open(filepath.with_suffix(f'.{table_name}.tsv'), 'wt') as f:
                writer = csv.writer(f, delimiter='\t', lineterminator='\n')
                writer.writerows(tabular)

    def export(self, dataset_paths=tuple(), exclude=tuple()):
        """ export output of curation workflows to file """
        if self.export_source_path != self.export_source_path.cache.anchor:
            if not self.export_source_path.cache.is_dataset():  # FIXME just go find the dataset in that case?
                print(f'{export_source_path.cache} is not at dataset level!')
                sys.exit(123)

            return self.export_single_dataset()  # FIXME unused except for `spc export .` from inside a dataset folder

        else:
            return super().export(dataset_paths=dataset_paths, exclude=exclude)

    def make_ir(self, dataset_paths=tuple(), exclude=tuple()):
        """ build the internal representation """
        # FIXME inversion of control would be nice here :/
        # FIXME this should really be coming from a fully
        # factored pipeline end without all the insane hidden state

        # previous latest must be stored to a variable before
        # symlink_latest of LATEST_PARTIAL otherwise export_other_formats
        # that need access to partial export of other things beyond
        # just the internal representation will fail i.e. there are
        # multiple possible partial stages but we only explicitly track
        # the one that corresponds to export_ir
        previous_latest = self.base_path.resolve()
        previous_latest_datasets = self.latest_datasets_path.resolve()
        # data
        # FIXME Summary has implicit state set by cli
        summary = cur.Summary(self.export_source_path, dataset_paths=dataset_paths, exclude=exclude)
        if self.latest:
            blob_data = self.latest_ir
        else:
            blob_data = summary.data_for_export(self.timestamp)

        return blob_data, summary, previous_latest, previous_latest_datasets

    def export_ir(self, blob_ir):
        # if the ir contains python objects then we probably want an explicit transform step

        # FIXME hack
        datasets = blob_ir['datasets']
        export_ir = {k:v for k, v in blob_ir.items() if k != 'datasets'}
        export_ir['datasets'] = []
        for blob_dataset in datasets:
            pipe = pipes.IrToExportJsonPipeline(blob_dataset)
            data = pipe.data
            export_ir['datasets'].append(pipe.data)

        return export_ir

    def export_other_formats(self, dump_path, filepath_ir, blob_ir, *rest):
        summary, previous_latest, previous_latest_datasets = rest
        dataset_blobs = blob_ir['datasets']

        # identifier metadata
        blob_id_met = self.export_identifier_metadata(dump_path, previous_latest, dataset_blobs)
        teim = self.export_identifier_rdf(dump_path, blob_id_met)

        # protocol
        #blob_protocol = self.export_protocols(dump_path, dataset_blobs, summary)

        # rdf
        teds = self.export_rdf(dump_path, previous_latest_datasets, dataset_blobs)
        tes = ex.TriplesExportSummary(blob_ir, teds=teds + [teim])

        with open(filepath_ir.with_suffix('.ttl'), 'wb') as f:
            f.write(tes.ttl)

        # xml
        self.export_xml(filepath_ir, dataset_blobs)

        # disco
        self.export_disco(filepath_ir, dataset_blobs, teds)
