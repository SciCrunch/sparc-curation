#!/usr/bin/env python3.7
""" SPARC curation cli for fetching, validating datasets, and reporting.
Usage:
    spc clone <project-id>
    spc pull [options] [<directory>...]
    spc refresh [options] [<path>...]
    spc fetch [options] [<path>...]
    spc find [options] --name=<PAT>...
    spc status [options]
    spc meta [--uri] [--browser] [--human] [--diff] [<path>...]
    spc export [ttl json datasets] [options]
    spc report size [options] [<path>...]
    spc report tofetch [options] [<directory>...]
    spc report terms [anatomy cells subcelluar] [options]
    spc report [completeness filetypes pathids keywords subjects errors test] [options]
    spc shell [integration] [options]
    spc server [options]
    spc tables [<directory>...]
    spc annos [export shell]
    spc feedback <feedback-file> <feedback>...
    spc missing [options]
    spc xattrs [options]
    spc demos [options]
    spc goto <remote-id>

Commands:
    clone       clone a remote project (creates a new folder in the current directory)

    pull        retrieve remote file structure

                options: --empty

    refresh     retrieve remote file sizes and fild ids (can also fetch using the new data)

                options: --fetch
                       : --level
                       : --only-no-file-id

    fetch       fetch remote data based on local metadata (NOTE does NOT refresh first)

                options: --level

    find        list unfetched files with option to fetch

                options: --name=<PAT>...  glob options should be quoted to avoid expansion
                       : --existing       include existing files in search
                       : --refresh        refresh matching files
                       : --fetch          fetch matching files
                       : --level

    status      list existing files where local meta does not match cached

    meta        display the metadata the current folder or specified paths

                options: --diff     diff the local and cached metadata
                       : --browser  navigate to the human uri for this file
                       : --human

    export      export extracted data to json (and everything else)

                datasets        ttl for individual datasets in addition to full export
                json            json for a single dataset
                ttl             turtle for a single dataset

                options: --latest   run derived pipelines from latest json
                       : --open     open the output file using xopen

    report      print a report on all datasets

                size            dataset sizes and file counts
                completeness    submission and curation completeness
                filetypes       filetypes used across datasets
                pathids         mapping from local path to cached id
                keywords        keywords used per dataset
                terms           all ontology terms used in the export

                                anatomy
                                cells
                                subcelluar

                subjects        all headings from subjects files
                errors          list of all errors per dataset

                options: --latest   run reports on latest json export
                       : --tab-table
                       : --sort-count-desc
                       : --debug

    shell       drop into an ipython shell

                integration     integration subshell with different defaults

    server      reporting server

                options: --latest   run server from latest json export

    missing     find and fix missing metadata
    xattrs      populate metastore / backup xattrs
    demos       long running example queries
    goto        given an id cd to the containing directory
                invoke as `pushd $(spc goto <id>)`

Options:
    -f --fetch              fetch matching files
    -R --refresh            refresh matching files
    -r --rate=HZ            sometimes we can go too fast when fetching [default: 5]
    -l --limit=SIZE_MB      the maximum size to download in megabytes [default: 2]
                            use negative numbers to indicate no limit
    -L --level=LEVEL        how deep to go in a refresh
                            used by any command that acceps <path>...
    -p --pretend            if the defult is to act, dont, opposite of fetch

    -h --human              print human readable values
    -b --browser            open the uri in default browser
    -u --uri                print the human uri for the path in question
    -a --uri-api            print the api uri for the path in question
    -n --name=<PAT>         filename pattern to match (like find -name)
    -e --empty              only pull empty directories
    -x --exists             when searching include files that have already been pulled
    -m --only-meta          only pull known dataset metadata files
    -z --only-no-file-id    only pull files missing file_id
    -o --overwrite          fetch even if the file exists
    --project-path=<PTH>    set the project path manually

    -t --tab-table          print simple table using tabs for copying
    -A --latest             run reporting on the latest export

    -S --sort-size-desc     sort by file size, largest first
    -C --sort-count-desc    sort by count, largest first

    -O --open               open the output file
    -U --upload             update remote target (e.g. a google sheet) if one exists
    -N --no-google          hack for ipv6 issues
    -D --diff               diff local vs cache

    --port=PORT             server port [default: 7250]

    -d --debug              drop into a shell after running a step
    -v --verbose            print extra information
    --log-location=PATH     folder into which logs are saved [default: ${SPARC_EXPORTS}/log/]
"""

import sys
import csv
import json
import errno
import types
import pprint
import logging
from itertools import chain
from collections import Counter
import requests
import htmlfn as hfn
from pyontutils import clifun as clif
from pyontutils.utils import NOWDANGER, NOWISO, UTCNOWISO
from pysercomb.pyr import units as pyru
from terminaltables import AsciiTable
from sparcur import config
from sparcur import schemas as sc
from sparcur import datasets as dat
from sparcur import exceptions as exc
from sparcur.core import JT, log, logd, JPointer
from sparcur.core import OntId, OntTerm, get_all_errors, DictTransformer as DT, adops
from sparcur.utils import FileSize, python_identifier, want_prefixes
from sparcur.paths import Path, BlackfynnCache, PathMeta
from sparcur.state import State
from sparcur.derives import Derives as De
from sparcur.backends import BlackfynnRemoteFactory
from sparcur.curation import PathData, Summary, Integrator, ExporterSummarizer
from sparcur.curation import JEncode, TriplesExportDataset, TriplesExportSummary
from sparcur.protocols import ProtocolData
from sparcur.blackfynn_api import BFLocal
from IPython import embed


class Options(clif.Options):

    @property
    def limit(self):
        l = int(self.args['--limit'])
        if l >= 0:
            return l

    @property
    def level(self):
        return int(self.args['--level']) if self.args['--level'] else None

    @property
    def rate(self):
        return int(self.args['--rate']) if self.args['--rate'] else None


class Dispatcher(clif.Dispatcher):
    spcignore = ('.git',
                 '.~lock',)

    def _print_table(self, rows, title=None, align=None, ext=None):
        """ ext is only used when self.options.server -> True """
        def simple_tsv(rows):
            return '\n'.join('\t'.join((str(c) for c in r)) for r in rows) + '\n'

        if self.options.tab_table:
            if title:
                print(title)

            print(simple_tsv(rows))

        elif self.options.server:
            if ext is not None:
                if ext == '.tsv':
                    nowish = UTCNOWISO('seconds')
                    fn = json.dumps(f'{title} {nowish}')
                    return simple_tsv(rows), 200, {'Content-Type': 'text/tsv; charset=utf-8',
                                                   'Content-Disposition': f'attachment; filename={fn}'}
                if isinstance(ext, types.FunctionType):
                    return ext(hfn.render_table(rows[1:], *rows[0]), title=title)
                else:
                    return 'Not found', 404

            return hfn.render_table(rows[1:], *rows[0]), title

        else:
            table = AsciiTable(rows, title=title)
            if align:
                assert len(align) == len(rows[0])
                table.justify_columns = {i:('left' if v == 'l'
                                            else ('center' if v == 'c'
                                                  else ('right' if v == 'r' else 'left')))
                                         for i, v in enumerate(align)}
            print(table.table)

    def _print_paths(self, paths, title=None):
        if self.options.sort_size_desc:
            key = lambda ps: -ps[-1]
        else:
            key = lambda ps: ps

        rows = [['Path', 'size', '?'],
                *((p, s.hr if isinstance(s, FileSize) else s, 'x' if p.exists() else '')
                  for p, s in
                  sorted(([p, ('/' if p.is_dir() else
                               (p.cache.meta.size if p.cache.meta.size else '??')
                               if p.cache.meta else '_')]
                          for p in paths), key=key))]
        self._print_table(rows, title)


class Main(Dispatcher):
    child_port_attrs = ('anchor',
                        'project_path',
                        'project_id',
                        'bfl',
                        'summary',
                        'cwd',
                        'cwdintr')
    # things all children should have
    # kind of like a non optional provides you WILL have these in your namespace
    def __init__(self, options):
        super().__init__(options)
        if not self.options.verbose:
            log.setLevel('INFO')
            logd.setLevel('INFO')

        Integrator.rate = self.options.rate
        Integrator.no_google = self.options.no_google
        self.cwd = Path.cwd()
        self.cwdintr = Integrator(self.cwd)

        if (self.options.clone or
            self.options.meta or
            self.options.goto or
            self.options.tofetch or  # size does need a remote but could do it lazily
            self.options.filetypes or
            self.options.status or  # eventually this should be able to query whether there is new data since the last check
            self.options.pretend):
            # short circuit since we don't know where we are yet
            return

        self._setup()  # if this isn't run up here the internal state of the program get's wonky

    def _setup(self):
        # set our local class TODO probably ok to do this by default
        # but needs testing to make sure there aren't things that only
        # work correctly because they encounter _local_class = None ...
        BlackfynnCache._local_class = Path

        if self.options.project_path:
            path_string = self.options.project_path
        else:
            path_string = '.'

        # we have to start from the cache class so that
        # we can configure
        local = Path(path_string).resolve()
        try:
            path = local.cache  # FIXME project vs subfolder
            if path is None:
                raise exc.NoCachedMetadataError  # FIXME somehow we decided not to raise this!??!
            self.anchor = path.anchor
        except exc.NoCachedMetadataError as e:
            root = local.find_cache_root()
            if root is not None:
                self.anchor = root.anchor
                raise NotImplementedError('TODO recover meta?')
            else:
                print(f'{local} is not in a project!')
                sys.exit(111)

        self.project_path = self.anchor.local

        BlackfynnCache.setup(Path, BlackfynnRemoteFactory)
        PathData.project_path = self.project_path

        # the way this works now the project should always exist
        self.summary = Summary(self.project_path)

        self.anchor.remote  # trigger creation of _remote_class
        BlackfynnRemote = BlackfynnCache._remote_class
        self.bfl = BlackfynnRemote._api
        State.bind_blackfynn(self.bfl)
        ProtocolData.setup()  # FIXME this suggests that we need a more generic setup file than this cli
        Integrator.setup(self.bfl)

    @property
    def project_name(self):
        return self.anchor.name
        #return self.bfl.organization.name

    @property
    def project_id(self):
        #self.bfl.organization.id
        return self.anchor.id

    @property
    def datasets(self):
        yield from self.anchor.children  # ok to yield from cache now that it is the bridge

    @property
    def datasets_remote(self):
        for d in self.anchor.remote.children:
            # FIXME lo the crossover (good for testing assumptions ...)
            #yield d.local
            yield d

    @property
    def datasets_local(self):
        for d in self.datasets:
            if d.local.exists():
                yield d.local

    ###
    ## vars
    ###

    @property
    def directories(self):
        return [Path(string_dir).absolute() for string_dir in self.options.directory]

    @property
    def paths(self):
        return [Path(string_path).absolute() for string_path in self.options.path]

    @property
    def _paths(self):
        """ all relevant paths determined by the flags that have been set """
        # but if you use the generator version of _paths
        # then if you add a folder to the previous path
        # then it will yeild that folder! which is SUPER COOL
        # but breaks lots of asusmptions elsehwere
        paths = self.paths
        if not paths:
            paths = Path.cwd(),

        if self.options.only_meta:
            paths = (mp.absolute() for p in paths for mp in dat.DatasetStructureLax(p).meta_paths)
            yield from paths
            return

        yield from self._build_paths(paths)

    def _build_paths(self, paths):
        def inner(paths, level=0, stop=self.options.level):
            """ depth first traversal of children """
            for path in paths:
                if self.options.only_no_file_id:
                    if (path.is_broken_symlink() and
                        (path.cache.meta.file_id is None)):
                        yield path
                        continue

                elif self.options.empty:
                    if path.is_dir():
                        try:
                            next(path.children)
                            # if a path has children we still want to
                            # for empties in them to the level specified
                        except StopIteration:
                            yield path
                    else:
                        continue
                else:
                    yield path

                if stop is None:
                    if self.options.only_no_file_id:
                        for rc in path.rchildren:
                            if (rc.is_broken_symlink() and
                                rc.cache.meta.file_id is None):
                                yield rc
                    else:
                        yield from path.rchildren

                elif level <= stop:
                    yield from inner(path.children, level + 1)

        yield from inner(paths)

    @property
    def _dirs(self):
        for p in self._paths:
            if p.is_dir():
                yield p

    @property
    def _not_dirs(self):
        for p in self._paths:
            if not p.is_dir():
                yield p

    def clone(self):
        project_id = self.options.project_id
        if project_id is None:
            print('no remote project id listed')
            sys.exit(4)
        # given that we are cloning it makes sense to _not_ catch a connection error here
        try:
            project_name = BFLocal(project_id).project_name  # FIXME reuse this somehow??
        except exc.MissingSecretError:
            print(f'missing api secret entry for {project_id}')
            sys.exit(11)
        BlackfynnCache.setup(Path, BlackfynnRemoteFactory)
        meta = PathMeta(id=project_id)

        # make sure that we aren't in a project already
        anchor_local = Path(project_name).absolute()
        root = anchor_local.find_cache_root()
        if root is not None:
            message = f'fatal: already in project located at {root.resolve()!r}'
            print(message)
            sys.exit(3)

        anchor = BlackfynnCache(project_name, meta=meta).resolve()
        if anchor.exists():
            if list(anchor.local.children):
                message = f'fatal: destination path {anchor} already exists and is not an empty directory.'
                sys.exit(2)

        self.anchor = anchor
        self.project_path = self.anchor.local
        with anchor:
            self.pull()

    def pull(self):
        # TODO folder meta -> org
        only = tuple()
        recursive = self.options.level is None  # FIXME we offer levels zero and infinite!
        dirs = self.directories
        cwd = Path.cwd()
        if self.project_path.parent.name == 'big':
            skip = self.skip
            only = self.skip_big
        else:
            skip = self.skip_big + self.skip

        if not dirs:
            dirs = cwd,

        existing_locals = set(rc for d in dirs for rc in d.rchildren)

        for d in dirs:
            if self.options.empty:
                if list(d.children):
                    continue

            if d.is_dir():
                if not d.remote.is_dataset():
                    log.warning('You are pulling recursively from below dataset level.')

                r = d.remote
                # FIXME for some reason this does not seem to be working as expected
                # because new datasets are being added when there is an existing dataset
                r.refresh(update_cache=True)  # if the parent folder has moved make sure to move it first
                d = r.local  # in case a folder moved
                caches = d.remote.bootstrap(recursive=recursive, only=only, skip=skip)

        new_locals = set(c.local for c in caches)
        maybe_removed = set(existing_locals) - set(new_locals)
        maybe_new = set(new_locals) - set(existing_locals)
        if maybe_removed:
            # FIXME pull sometimes has fake file extensions
            from pyontutils.utils import Async, deferred
            Async(rate=self.options.rate)(deferred(l.cache.refresh)() for l in maybe_removed)

    ###
    skip = (
            'N:dataset:83e0ebd2-dae2-4ca0-ad6e-81eb39cfc053',  # hackathon
            'N:dataset:a896935a-4718-4906-8a7b-b6d76fb260b6',  # test computational resource
        )

    skip_big = (
            'N:dataset:ec2e13ae-c42a-4606-b25b-ad4af90c01bb',  # big max
            'N:dataset:2d0a2996-be8a-441d-816c-adfe3577fc7d',  # big rna
            #'N:dataset:a7b035cf-e30e-48f6-b2ba-b5ee479d4de3',  # powley done
        )
    ###

    def refresh(self):
        paths = self.paths
        cwd = Path.cwd()
        if not paths:
            paths = cwd,

        to_root = sorted(set(parent
                             for path in paths
                             for parent in path.parents
                             if parent.cache is not None),
                         key=lambda p: len(p.parts))

        if self.options.pretend:
            ap = list(chain(to_root, self._paths))
            self._print_paths(ap)
            print(f'total = {len(ap):<10}rate = {self.options.rate}')
            return

        self._print_paths(chain(to_root, self._paths))

        from pyontutils.utils import Async, deferred
        hz = self.options.rate
        fetch = self.options.fetch
        limit = self.options.limit

        drs = [d.remote for d in chain(to_root, self._dirs)]

        if not self.options.debug:
            refreshed = Async(rate=hz)(deferred(r.refresh)() for r in drs)
        else:
            refreshed = [r.refresh() for r in drs]

        moved = []
        parent_moved = []
        for new, r in zip(refreshed, drs):
            if new is None:
                log.critical('utoh')

            oldl = r.local
            try:
                r.update_cache()
            except FileNotFoundError as e:
                parent_moved.append(oldl)
                continue
            except OSError as e:
                if e.errno == errno.ENOTEMPTY:
                    log.error(f'{e}')
                    continue
                else:
                    raise e

            newl = r.local
            if oldl != newl:
                moved.append([oldl, newl])

        if moved:
            self._print_table(moved, title='Folders moved')
            for old, new in moved:
                if old == cwd:
                    log.info(f'Changing directory to {new}')
                    new.chdir()

        if parent_moved:
            self._print_paths(parent_moved, title='Parent moved')

        if not self.options.debug:
            refreshed = Async(rate=hz)(deferred(path.cache.refresh)(update_data=fetch,
                                                                    size_limit_mb=limit)
                                       for path in self._not_dirs)

        else:
            for path in self._not_dirs:
                path.cache.refresh(update_data=fetch, size_limit_mb=limit)

    def fetch(self):
        paths = [p for p in self._paths if not p.is_dir()]
        self._print_paths(paths)
        if self.options.pretend:
            return

        from pyontutils.utils import Async, deferred
        hz = self.options.rate
        Async(rate=hz)(deferred(path.cache.fetch)(size_limit_mb=self.options.limit)
                       for path in paths)

    @property
    def export_base(self):
        return self.project_path.parent / 'export' / self.project_id

    @property
    def LATEST(self):
        return self.project_path.parent / 'export' / self.project_id / 'LATEST'

    @property
    def latest_export(self):
        with open(self.LATEST / 'curation-export.json', 'rt') as f:
            return json.load(f)

    def latest_export_ttl_populate(self, graph):
        # intentionally fail if the ttl export failed
        return graph.parse((self.LATEST / 'curation-export.ttl').as_posix(), format='ttl')

    def export(self):
        """ export output of curation workflows to file """
        #org_id = Integrator(self.project_path).organization.id

        cwd = Path.cwd()
        timestamp = NOWDANGER(implicit_tz='PST PDT')
        format_specified = self.options.ttl or self.options.json  # This is OR not XOR you dumdum
        if cwd != cwd.cache.anchor and format_specified:
            if not cwd.cache.is_dataset:
                print(f'{cwd.cache} is not at dataset level!')
                sys.exit(123)

            intr = Integrator(cwd)
            dump_path = self.export_base / 'datasets' / intr.id / timestamp
            latest_path = self.export_base / 'datasets' / intr.id / 'LATEST'
            if not dump_path.exists():
                dump_path.mkdir(parents=True)

            functions = []
            suffixes = []
            modes = []
            if self.options.json:  # json first since we can cache dowe
                j = lambda f: json.dump(intr.data, f,
                                        sort_keys=True, indent=2, cls=JEncode)
                functions.append(j)
                suffixes.append('.json')
                modes.append('wt')

            if self.options.ttl:
                t = lambda f: f.write(intr.ttl)
                functions.append(t)
                suffixes.append('.ttl')
                modes.append('wb')

            filename = 'curation-export'
            filepath = dump_path / filename

            for function, suffix, mode in zip(functions, suffixes, modes):
                out = filepath.with_suffix(suffix)
                with open(out, mode) as f:
                    function(f)

                log.info(f'dataset graph exported to {out}')

                if self.options.open:
                    out.xopen()

            if latest_path.exists():
                if not latest_path.is_symlink():
                    raise TypeError(f'Why is LATEST not a symlink? {latest_path!r}')

                latest_path.unlink()

            latest_path.symlink_to(dump_path)

            return

        # start time not end time ...
        # obviously not transactional ...
        filename = 'curation-export'
        dump_path = self.export_base / timestamp
        latest_path = self.LATEST
        if not dump_path.exists():
            dump_path.mkdir(parents=True)

        filepath = dump_path / filename

        data = self.latest_export if self.options.latest else self.summary.data

        # FIXME we still create a new export folder every time even if the json didn't change ...
        with open(filepath.with_suffix('.json'), 'wt') as f:
            json.dump(data, f, sort_keys=True, indent=2, cls=JEncode)

        es = ExporterSummarizer(data)

        with open(filepath.with_suffix('.ttl'), 'wb') as f:
            f.write(es.ttl)

        for xml_name, xml in es.xml:
            with open(filepath.with_suffix(f'.{xml_name}.xml'), 'wb') as f:
                f.write(xml)

        # datasets, contributors, subjects, samples, resources
        for table_name, tabular in es.disco:
            with open(filepath.with_suffix(f'.{table_name}.tsv'), 'wt') as f:
                writer = csv.writer(f, delimiter='\t', lineterminator='\n')
                writer.writerows(tabular)

        if self.options.datasets:
            dataset_dump_path = dump_path / 'datasets'
            dataset_dump_path.mkdir()
            suffix = '.ttl'
            mode = 'wb'
            for dataset_blob in es:
                filepath = dataset_dump_path / dataset_blob['id']
                out = filepath.with_suffix(suffix)
                with open(out, 'wb') as f:
                    f.write(TriplesExportDataset(dataset_blob).ttl)

                log.info(f'dataset graph exported to {out}')

        if latest_path.exists():
            if not latest_path.is_symlink():
                raise TypeError(f'Why is LATEST not a symlink? {latest_path!r}')

            latest_path.unlink()

        latest_path.symlink_to(dump_path)

        if self.options.debug:
            embed()

    def annos(self):
        from protcur.analysis import protc, Hybrid
        from sparcur.protocols import ProtcurSource
        ProtcurSource.populate_annos()
        if self.options.export:
            with open('/tmp/sparc-protcur.rkt', 'wt') as f:
                f.write(protc.parsed())

        all_blackfynn_uris = set(u for d in self.summary for u in d.protocol_uris_resolved)
        all_hypotehsis_uris = set(a.uri for a in protc)
        if self.options.shell or self.options.debug:
            p, *rest = self._paths
            f = Integrator(p)
            all_annos = [list(protc.byIri(uri)) for uri in f.protocol_uris_resolved]
            embed()

    def demos(self):
        # get the first dataset
        dataset = next(iter(summary))

        # another way to get the first dataset
        dataset_alt = next(org.children)

        # view all dataset descriptions call repr(tabular_view_demo)
        tabular_view_demo = [next(d.dataset_description).t
                                for d in ds[:1]
                                if 'dataset_description' in d.data]

        # get package testing
        bigskip = ['N:dataset:2d0a2996-be8a-441d-816c-adfe3577fc7d',
                    'N:dataset:ec2e13ae-c42a-4606-b25b-ad4af90c01bb']
        bfds = self.bfl.bf.datasets()
        packages = [list(d.packages) for d in bfds[:3]
                    if d.id not in bigskip]
        n_packages = [len(ps) for ps in packages]

        # bootstrap a new local mirror
        # FIXME at the moment we can only have of these at a time
        # sigh more factories incoming
        #anchor = BlackfynnCache('/tmp/demo-local-storage')
        #anchor.bootstrap()

        if False:
            ### this is the equivalent of export, quite slow to run
            # export everything
            dowe = summary.data

            # show all the errors from export everything
            error_id_messages = [(d['id'], e['message']) for d in dowe['datasets'] for e in d['errors']]
            error_messages = [e['message'] for d in dowe['datasets'] for e in d['errors']]

        #rchilds = list(datasets[0].rchildren)
        #package, file = [a for a in rchilds if a.id == 'N:package:8303b979-290d-4e31-abe5-26a4d30734b4']

        return self.shell()

    def tables(self):
        """ print summary view of raw metadata tables, possibly per dataset """
        # TODO per dataset
        from sparcur import pipelines as pipes
        summary = self.summary
        tables = []
        datasets = self.summary.iter_datasets if self.cwd.cache.is_organization() else (Integrator(self.cwd.cache.dataset.local),)
        for intr in datasets:
            pipe = intr.pipeline
            while not isinstance(pipe, pipes.SPARCBIDSPipeline):
                pipe = pipe.previous_pipeline

            try:
                pipe.subpipelined
            except pipe.SkipPipelineError:
                continue

            for sp in pipe.subpipeline_instances:
                tabular = sp._transformer.t
                tables.append(tabular)

        [print(repr(t)) for t in tables]
        return
        tabular_view_demo = [next(d.dataset_description).t
                                for d in summary
                                if 'dataset_description' in d.data]
        print(repr(tabular_view_demo))

    def find(self):
        paths = []
        if self.options.name:
            patterns = self.options.name
            path = Path('.').resolve()
            for pattern in patterns:
                # TODO filesize mismatches on non-fake
                # no longer needed due to switching to symlinks
                #if '.fake' not in pattern and not self.options.overwrite:
                    #pattern = pattern + '.fake*'

                for file in path.rglob(pattern):
                    paths.append(file)

        if paths:
            paths = [p for p in paths if not p.is_dir()]
            search_exists = self.options.exists
            if self.options.limit:
                old_paths = paths
                paths = [p for p in paths
                         if p.cache.meta.size is None or  # if we have no known size don't limit it
                         search_exists or
                         not p.exists() and p.cache.meta.size.mb < self.options.limit
                         or p.exists() and p.size != p.cache.meta.size and
                         (not log.info(f'Truncated transfer detected for {p}\n'
                                       f'{p.size} != {p.cache.meta.size}'))
                         and p.cache.meta.size.mb < self.options.limit]

                n_skipped = len(set(p for p in old_paths if p.is_broken_symlink()) - set(paths))

            if self.options.pretend:
                self._print_paths(paths)
                print(f'skipped = {n_skipped:<10}rate = {self.options.rate}')
                return

            if self.options.verbose:
                for p in paths:
                    print(p.cache.meta.as_pretty(pathobject=p))

            if self.options.fetch or self.options.refresh:
                from pyontutils.utils import Async, deferred
                hz = self.options.rate  # was 30
                limit = self.options.limit
                fetch = self.options.fetch
                if self.options.refresh:
                    Async(rate=hz)(deferred(path.remote.refresh)
                                   (update_cache=True, update_data=fetch, size_limit_mb=limit)
                                   for path in paths)
                elif fetch:
                    Async(rate=hz)(deferred(path.cache.fetch)(size_limit_mb=limit)
                                   for path in paths)

            else:
                self._print_paths(paths)
                print(f'skipped = {n_skipped:<10}rate = {self.options.rate}')

    def feedback(self):
        file = self.options.feedback_file
        feedback = ' '.join(self.options.feedback)
        path = Path(file).resolve()
        eff = Integrator(path)
        # TODO pagenote and/or database
        print(eff, feedback)

    def missing(self):
        self.bfl.find_missing_meta()

    def xattrs(self):
        self.bfl.populate_metastore()

    def meta(self):
        if self.options.browser:
            import webbrowser

        BlackfynnCache._local_class = Path  # since we skipped _setup
        paths = self.paths
        if not paths:
            paths = Path('.').resolve(),

        old_level = log.level
        log.setLevel('ERROR')
        def inner(path):
            if self.options.uri or self.options.browser:
                uri = path.cache.uri_human
                print('+' + '-' * (len(uri) + 2) + '+')
                print(f'| {uri} |')
                if self.options.browser:
                    webbrowser.open(uri)

            try:
                cmeta = path.cache.meta
                if cmeta is not None:
                    if self.options.diff:
                        lmeta = path.meta
                        print(lmeta.as_pretty_diff(cmeta, pathobject=path, human=self.options.human))
                    else:
                        print(cmeta.as_pretty(pathobject=path, human=self.options.human))

            except exc.NoCachedMetadataError:
                print(f'No metadata for {path}. Run `spc refresh {path}`')

        for path in paths:
            inner(path)

        log.setLevel(old_level)

    def goto(self):
        # TODO this needs an inverted index
        for rc in self.cwd.rchildren:
            if rc.cache.id == self.options.remote_id:
                if rc.is_broken_symlink() or rc.is_file():
                    rc = rc.parent

                print(rc.relative_to(self.cwd).as_posix())
                return

    def status(self):
        project_path = self.cwd.find_cache_root()
        if project_path is None:
            print(f'{self.cwd} is not in a project!')
            sys.exit(111)

        existing_files = [f for f in project_path.rchildren if f.is_file()]
        different = []
        for f in existing_files:
            cmeta = f.cache.meta
            lmeta = f.meta if cmeta.checksum else f.meta_no_checksum
            if lmeta.content_different(cmeta):
                print(lmeta.as_pretty_diff(cmeta, pathobject=f, human=self.options.human))

    def server(self):
        from sparcur.server import make_app, url_for
        self.report = Report(self)
        self.dataset_index = {d.meta.id:Integrator(d) for d in self.datasets}

        Report.url_for = staticmethod(url_for)
        self.__class__.url_for = staticmethod(url_for)

        app, *_ = make_app(self)
        self.app = app
        app.debug = False
        app.run(host='localhost', port=self.options.port, threaded=True)

    ### sub dispatchers

    def report(self):
        report = Report(self)
        report()

    def shell(self):
        """ drop into an shell with classes loaded """
        shell = Shell(self)
        shell()


class Report(Dispatcher):

    paths = Main.paths
    _paths = Main._paths

    export_base = Main.export_base
    LATEST = Main.LATEST
    latest_export = Main.latest_export
    latest_export_ttl_populate = Main.latest_export_ttl_populate

    @property
    def _sort_key(self):
        if self.options.sort_count_desc:
            return lambda kv: -kv[-1]
        else:
            return lambda kv: kv

    def tofetch(self, dirs=None):
        if dirs is None:
            dirs = self.options.directory
            if not dirs:
                dirs.append('.')

        data = []

        def dead(p):
            raise ValueError(p)

        for d in dirs:
            if not Path(d).is_dir():
                continue  # helper files at the top level, and the symlinks that destory python
            path = Path(d).resolve()
            paths = path.rchildren #list(path.rglob('*'))
            path_meta = {p:p.cache.meta if p.cache else dead(p) for p in paths
                         if p.suffix not in ('.swp',)}
            outstanding = 0
            total = 0
            tf = 0
            ff = 0
            td = 0
            uncertain = False  # TODO
            for p, m in path_meta.items():
                #if p.is_file() and not any(p.stem.startswith(pf) for pf in self.spcignore):
                if p.is_file() or p.is_broken_symlink():
                    s = m.size
                    if s is None:
                        uncertain = True
                        continue

                    tf += 1
                    if s:
                        total += s

                    #if '.fake' in p.suffixes:
                    if p.is_broken_symlink():
                        ff += 1
                        if s:
                            outstanding += s

                elif p.is_dir():
                    td += 1

            data.append([path.name,
                         FileSize(total - outstanding),
                         FileSize(outstanding),
                         FileSize(total),
                         uncertain,
                         (tf - ff),
                         ff,
                         tf,
                         td])

        formatted = [[n, l.hr, o.hr, t.hr if not u else '??', lf, ff, tf, td]
                     for n, l, o, t, u, lf, ff, tf, td in
                     sorted(data, key=lambda r: (r[4], -r[3]))]
        rows = [['Folder', 'Local', 'To Retrieve', 'Total', 'L', 'R', 'T', 'TD'],
                *formatted]

        return self._print_table(rows, title='File size counts', ext=ext)

    def filetypes(self, ext=None):
        key = self._sort_key
        paths = self.paths if self.paths else (Path('.').resolve(),)
        paths = [c for p in paths for c in p.rchildren if not c.is_dir()]

        def count(thing):
            return sorted([(k if k else '', v) for k, v in
                            Counter([getattr(f, thing)
                                     for f in paths]).items()], key=key)

        each = {t:count(t) for t in ('suffix', 'mimetype', '_magic_mimetype')}

        for title, rows in each.items():
            yield self._print_table(((title, 'count'), *rows), title=title.replace('_', ' ').strip())

        all_counts = sorted([(*[m if m else '' for m in k], v) for k, v in
                                Counter([(f.suffix, f.mimetype, f._magic_mimetype)
                                        for f in paths]).items()], key=key)

        header = ['suffix', 'mimetype', 'magic mimetype', 'count']
        return self._print_table((header, *all_counts),
                                 title='All types aligned (has duplicates)',
                                 ext=ext)

    def subjects(self, ext=None):
        data = self.latest_export if self.options.latest else self.summary.data
        datasets = data['datasets']
        key = self._sort_key
        # FIXME we need the blob wrapper in addition to the blob generator
        # FIXME these are the normalized ones ...
        subjects_headers = tuple(k for dataset_blob in datasets
                                 if 'subjects' in dataset_blob  # FIXME inputs?
                                 for subject_blob in dataset_blob['subjects']
                                 for k in subject_blob)
        counts = tuple(kv for kv in sorted(Counter(subjects_headers).items(),
                                            key=key))

        rows = ((f'Column Name unique = {len(counts)}', '#'), *counts)
        return self._print_table(rows, title='Subjects Report', ext=ext)

    def completeness(self, ext=None):
        if self.options.latest:
            datasets = self.latest_export['datasets']
            raw = [self.summary._completeness(data) for data in datasets]

        else:
            raw = self.summary.completeness

        def rformat(i, si, ci, ei, name, id, award, organ):
            if self.options.server and isinstance(ext, types.FunctionType):
                rsurl = 'https://projectreporter.nih.gov/reporter_searchresults.cfm'
                dataset_dash_url = self.url_for('route_datasets_id', id=id)
                si = hfn.atag('/TODO', si)
                ci = hfn.atag('/TODO', ci)
                ei = hfn.atag('/TODO', ei)
                name = hfn.atag(dataset_dash_url, name)
                id = hfn.atag(dataset_dash_url, id[:10] + '...')
                award = hfn.atag(('https://scicrunch.org/scicrunch/data/source/'
                                  f'nif-0000-10319-1/search?q={award}'), award) if award else 'MISSING'
                organ = organ if organ else ''
                if isinstance(organ, list) or isinstance(organ, tuple):
                    organ = ' '.join([o.atag() for o in organ])
                if isinstance(organ, OntTerm):
                    organ = organ.atag()
            else:
                award = award if award else ''
                organ = (repr(organ) if isinstance(organ, OntTerm) else organ) if organ else ''
                if isinstance(organ, list):
                    organ = ' '.join([repr(o) for o in organ])
                    

            return (i + 1, si, ci, ei, name, id, award, organ)

        rows = [('', 'SI', 'CI', 'EI', 'name', 'id', 'award', 'organ')]
        rows += [rformat(i, *cols) for i, cols in
                 enumerate(sorted(raw, key=lambda t: (t[0], t[1], t[5] if t[5] else 'z' * 999, t[3])))]

        return self._print_table(rows, title='Completeness Report', ext=ext)

    def keywords(self, ext=None):
        data = self.latest_export if self.options.latest else self.summary.data
        datasets = data['datasets']
        _rows = [sorted(set(dataset_blob.get('meta', {}).get('keywords', [])), key=lambda v: -len(v))
                    for dataset_blob in datasets]
        rows = [list(r) for r in sorted(set(tuple(r) for r in _rows if r),
                                        key = lambda r: (len(r), tuple(len(c) for c in r if c), r))]
        header = [[f'{i + 1}' for i, _ in enumerate(rows[-1])]]
        rows = header + rows
        return self._print_table(rows, title='Keywords Report')

    def size(self, dirs=None, ext=None):
        if dirs is None:
            dirs = self.paths
        intrs = [Integrator(p) for p in dirs]
        if not intrs:
            intrs = self.cwdintr,

        rows = [['path', 'id', 'dirs', 'files', 'size', 'hr'],
                *sorted([[d.name, d.id, c['dirs'], c['files'], c['size'], c['size'].hr]
                         for d in intrs
                         for c in (d.datasetdata.counts,)], key=lambda r: -r[-2])]

        return self._print_table(rows, title='Size Report', align=['l', 'l', 'r', 'r', 'r', 'r'], ext=ext)

    def test(self, ext=None):
        rows = [['hello', 'world'], [1, 2]]
        return self._print_table(rows, title='Report Test', ext=ext)

    def errors(self, ext=None):
        if self.options.latest:
            datasets = self.latest_export['datasets']
        else:
            self.summary.data['datasets']

        pprint.pprint(sorted([(d['meta']['folder_name'], [e['message']
                                                   for e in get_all_errors(d)])
                              for d in datasets], key=lambda ab: -len(ab[-1])))

    def pathids(self, ext=None):
        base = self.project_path.parent
        rows = [['path', 'id']] + sorted([c.relative_to(base), c.cache.id]#, c.cache.uri_api, c.cache.uri_human]
                                         # slower to include the uris
                                         for c in chain((self.cwd,), self.cwd.rchildren)
        )
        return self._print_table(rows, title='Path identifiers', ext=ext)

    def terms(self, ext=None):
        # anatomy
        # cells
        # subcelluar
        import rdflib
        # FIXME cache these results and only recompute if latest changes?
        if self.options.latest:
            graph = rdflib.Graph()
            self.latest_export_ttl_populate(graph)
        else:
            graph = self.summary.triples_exporter.graph

        objects = set()
        skipped_prefixes = set()
        for t in graph:
            for e in t:
                if isinstance(e, rdflib.URIRef):
                    oid = OntId(e)
                    if oid.prefix in want_prefixes:
                        objects.add(oid)
                    else:
                        skipped_prefixes.add(oid.prefix)

        if self.options.server and isinstance(ext, types.FunctionType):
            def reformat(ot):
                return [ot.label if hasattr(ot, 'label') and ot.label else '', ot.atag(curie=True)]

        else:
            def reformat(ot):
                return [ot.label if hasattr(ot, 'label') and ot.label else '', ot.curie]

        log.info(' '.join(sorted(skipped_prefixes)))
        objs = [OntTerm(o) if o.prefix not in ('TEMP', 'sparc') else o for o in objects]
        term_sets = {title:[o for o in objs if o.prefix == prefix]
                     for prefix, title in
                     (('NCBITaxon', 'Species'),
                      ('UBERON', 'Anatomy and age category'),  # FIXME
                      ('FMA', 'Anatomy'),
                      ('PATO', 'Qualities'),
                      ('sparc', 'MIS terms'),
                      ('TEMP', 'Suggested terms'),
                     )}

        term_sets['Other'] = set(objs) - set(ot for v in term_sets.values() for ot in v)

        for title, terms in term_sets.items():
            header = [['Label', 'CURIE']]
            rows = header + [reformat(ot) for ot in
                            sorted(terms,
                                   key=lambda ot: (ot.prefix, ot.label.lower()
                                                   if hasattr(ot, 'label') and ot.label else ''))]

            yield self._print_table(rows, title=title, ext=ext)


class Shell(Dispatcher):
    # property ports
    paths = Main.paths
    _paths = Main._paths
    _build_paths = Main._build_paths
    datasets = Main.datasets
    datasets_local = Main.datasets_local
    export_base = Main.export_base
    LATEST = Main.LATEST
    latest_export = Main.latest_export
    latest_export_ttl_populate = Main.latest_export_ttl_populate

    def default(self):
        datasets = list(self.datasets)
        datasets_local = list(self.datasets_local)
        dsd = {d.meta.id:d for d in datasets}
        ds = datasets
        summary = self.summary
        org = Integrator(self.project_path)

        p, *rest = self._paths
        if p.cache.is_dataset():
            intr = Integrator(p)
            j = JT(intr.data)
            #triples = list(f.triples)

        try:
            latest_datasets = self.latest_export['datasets']
        except:
            pass

        embed()

    def integration(self):
        from protcur.analysis import protc, Hybrid
        from sparcur import sheets
        #from sparcur.sheets import Organs, Progress, Grants, ISAN, Participants, Protocols as ProtocolsSheet
        from sparcur.protocols import ProtocolData, ProtcurData
        p, *rest = self._paths
        intr = Integrator(p)
        j = JT(intr.data)
        pj = list(intr.protocol_jsons)
        pc = list(intr.triples_exporter.protcur)
        #apj = [pj for c in intr.anchor.children for pj in c.protocol_jsons]
        embed()


def main():
    from docopt import docopt, parse_defaults
    args = docopt(__doc__, version='spc 0.0.0')
    defaults = {o.name:o.value if o.argcount else None for o in parse_defaults(__doc__)}

    # set logging to file before anything else is done
    # FIXME remove this hardcoded nonsense
    from pyontutils.utils import isoformat_safe, utcnowtz
    sparc_export = Path('~/files/blackfynn_local/export').expanduser()  # no s for maximum confusion
    ll = Path(args['--log-location'].replace('${SPARC_EXPORTS}', sparc_export.as_posix()))
    if not ll.exists():
        ll.mkdir()

    lf = ll / isoformat_safe(utcnowtz())
    lfh = logging.FileHandler(lf.as_posix())
    lfh.setFormatter(log.handlers[0].formatter)
    log.addHandler(lfh)
    logd.addHandler(lfh)

    options = Options(args, defaults)
    main = Main(options)
    if main.options.debug:
        print(main.options)

    main()


if __name__ == '__main__':
    main()
