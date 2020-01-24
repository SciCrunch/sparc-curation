# cat export/core.py | grep -v '^#' | grep -o '\(self\.[a-zA-Z0-9_\.]\+\)[\ (),{}:]' | rev | cut -c 2- | rev | sort -u

import csv
import json
from socket import gethostname
from sparcur import export as ex
from sparcur import schemas as sc
from sparcur import curation as cur  # FIXME implicit state must be set in cli
from sparcur.core import JEncode
from sparcur.paths import Path
from sparcur.utils import symlink_latest, loge


def export_schemas(export_path):
    schemas = (sc.DatasetDescriptionSchema,
               sc.SubjectsSchema,
               sc.SamplesFileSchema,
               sc.SubmissionSchema,)

    sb = export_path / 'schemas'
    for s in schemas:
        s.export(sb)


class Export:

    def __init__(self,
                 export_path,
                 export_source_path,
                 folder_timestamp,
                 timestamp,
                 latest=False,
                 partial=False,
                 open_when_done=False,):
        self.export_source_path = export_source_path
        self.export_base = Path(export_path, export_source_path.cache.anchor.id)
        self.latest = latest
        self.partial = partial
        self.folder_timestamp = folder_timestamp
        self.timestamp = timestamp
        self.open_when_done = open_when_done

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
    def latest_export(self):
        base_path = self.LATEST_PARTIAL if self.partial else self.LATEST
        with open(base_path / 'curation-export.json', 'rt') as f:
            return json.load(f)

    @property
    def latest_ttl_path(self):
        base_path = self.LATEST_PARTIAL if self.partial else self.LATEST
        return base_path / 'curation-export.ttl'

    @property
    def latest_protocols_path(self):
        base_path = self.LATEST_PARTIAL if self.partial else self.LATEST
        return base_path / 'protocols.json'

    @property
    def latest_protocols(self):
        with open(self.latest_protocols_path, 'rt') as f:
            return json.load(f)

    @property
    def latest_datasets_path(self):
        base_path = self.LATEST_PARTIAL if self.partial else self.LATEST
        return base_path / 'datasets'

    def latest_export_ttl_populate(self, graph):
        base_path = self.LATEST_PARTIAL if self.partial else self.LATEST
        # intentionally fail if the ttl export failed
        lce = (base_path / 'curation-export.ttl').as_posix()
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
        data = intr.data_for_export(self.timestamp)  # build and cache the data

        # always dump the json
        j = lambda f: json.dump(data, f, sort_keys=True, indent=2, cls=JEncode)
        functions.append(j)
        suffixes.append('.json')
        modes.append('wt')

        # always dump the ttl (for single datasets this is probably ok)
        t = lambda f: f.write(ex.TriplesExportDataset(data).ttl)
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
        return intr

    def export_rdf(self, dump_path, dataset_blobs):
        dataset_dump_path = dump_path / 'datasets'
        dataset_dump_path.mkdir()
        suffix = '.ttl'
        mode = 'wb'

        teds = []
        for dataset_blob in dataset_blobs:
            filename = dataset_blob['id']
            filepath = dataset_dump_path / filename
            filepsuf = filepath.with_suffix(suffix)
            lfilepath = self.latest_datasets_path / filename
            lfilepsuf = lfilepath.with_suffix(suffix)

            ted = ex.TriplesExportDataset(dataset_blob)
            teds.append(ted)

            if self.latest and lfilepsuf.exists():
                breakpoint()
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

    def export(self):
        """ export output of curation workflows to file """
        if self.export_source_path != self.export_source_path.cache.anchor:
            if not self.export_source_path.cache.is_dataset():  # FIXME just go find the dataset in that case?
                print(f'{export_source_path.cache} is not at dataset level!')
                sys.exit(123)

            return self.export_single_dataset()

        else:
            return self.export_datasets()

    def export_datasets(self):
        # start time not end time ...
        # obviously not transactional ...
        filename = 'curation-export'
        dump_path = self.export_base / self.folder_timestamp
        if not dump_path.exists():
            dump_path.mkdir(parents=True)

        symlink_latest(dump_path, self.LATEST_RUN)

        filepath = dump_path / filename

        # data
        summary = cur.Summary(self.export_source_path)  # FIXME implicit state set by cli
        blob_data = (self.latest_export if
                     self.latest else
                     summary.data_for_export(self.timestamp))

        # FIXME we still create a new export folder every time even if the json didn't change ...
        with open(filepath.with_suffix('.json'), 'wt') as f:
            json.dump(blob_data, f, sort_keys=True, indent=2, cls=JEncode)

        symlink_latest(dump_path, self.LATEST_PARTIAL)

        dataset_blobs = blob_data['datasets']

        # protocol
        blob_protocol = self.export_protocols(dump_path, dataset_blobs, summary)

        # rdf
        teds = self.export_rdf(dump_path, dataset_blobs)
        tes = ex.TriplesExportSummary(blob_data, teds=teds)

        with open(filepath.with_suffix('.ttl'), 'wb') as f:
            f.write(tes.ttl)

        # xml
        self.export_xml(filepath, dataset_blobs)

        # disco
        self.export_disco(filepath, dataset_blobs, teds)

        symlink_latest(dump_path, self.LATEST)
        return summary
