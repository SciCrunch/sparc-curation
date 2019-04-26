#!/usr/bin/env python3.7
""" SPARC curation cli for fetching, validating datasets, and reporting.
Usage:
    spc clone <project-id>
    spc pull [options] [<directory>...]
    spc refresh [options] [<path>...]
    spc fetch [options] [<path>...]
    spc annos [export shell]
    spc stats [<directory>...]
    spc report [completeness filetypes keywords subjects] [options]
    spc tables [<directory>...]
    spc missing
    spc xattrs
    spc export [ttl]
    spc demos
    spc shell [<path>...]
    spc feedback <feedback-file> <feedback>...
    spc find [options] <file>...
    spc find [options] --name=<PAT>...
    spc meta [--uri] [<path>...]

Commands:
    clone     clone a remote project (creates a new folder in the current directory)
    pull      pull down the remote list of files
    refresh   refresh to get file sizes and data
    fetch     fetch based on the metadata that we have
    stats     print stats for specified or current directory
    report    print a report on all datasets
    missing   find and fix missing metadata
    xattrs    populate metastore / backup xattrs
    export    export extracted data
    demos     long running example queries
    shell     drop into an ipython shell
    find      list and fetch unfetched files
    meta      display the metadata the current folder or list of folders

Options:
    -f --fetch              fetch the files
    -l --limit=SIZE_MB      the maximum size to download in megabytes [default: 2]
                            use negative numbers to indicate no limit
    -L --level=LEVEL        how deep to go in a refresh
    -n --name=<PAT>         filename pattern to match (like find -name)
    -v --verbose            print extra information
    -u --uri                print the human uri for the path in question
    -p --project-path=<PTH> set the project path manually
    -o --overwrite          fetch even if the file exists
    -e --empty              only pull empty directories
    -m --only-meta          only pull known dataset metadata files
    -g --get-data          download the actual data in files

    -i --include-folders    include folders when refreshing remote metadata

    -d --debug              drop into a shell after running a step
"""

import sys
import csv
import json
import pprint
from datetime import datetime
from itertools import chain
from collections import Counter
import requests
from terminaltables import AsciiTable
from sparcur import config
from sparcur import schemas as sc
from sparcur import exceptions as exc
from sparcur.core import JT, log
from sparcur.paths import Path, BlackfynnCache, PathMeta
from sparcur.backends import BlackfynnRemoteFactory
from sparcur.curation import FThing, FTLax, CurationReport, Summary
from sparcur.curation import get_datasets, JEncode, get_all_errors
from sparcur.blackfynn_api import BFLocal
from IPython import embed


class Options:
    def __init__(self, args):
        self.args = args

    @property
    def debug(self):
        return self.args['--debug']

    @property
    def overwrite(self):
        return self.args['--overwrite']

    @property
    def empty(self):
        return self.args['--empty']

    @property
    def include_folders(self):
        return self.args['--include-folders']

    @property
    def limit(self):
        l = int(self.args['--limit'])
        if l >= 0:
            return l

    @property
    def level(self):
        return int(self.args['--level']) if self.args['--level'] else None

    @property
    def only_meta(self):
        return self.args['--only-meta']

    @property
    def get_data(self):
        return self.args['--get-data']

    @property
    def uri(self):
        return self.args['--uri']

    @property
    def ttl(self):
        return self.args['ttl']

class Dispatch:
    spcignore = ('.git',
                 '.~lock',)
    def __init__(self, args):
        self.args = args
        self.options = Options(args)

        if self.args['clone'] or self.args['meta']:
            # short circuit since we don't know where we are yet
            return

        self._setup()  # if this isn't run up here the internal state of the program get's wonky

    def _setup(self):
        args = self.args
        if args['--project-path']:
            path_string = args['--project-path']
        else:
            path_string = '.'

        # we have to start from the cache class so that
        # we can configure
        path = BlackfynnCache(path_string).resolve()  # avoid infinite recursion from '.'
        try:
            self.anchor = path.anchor
        except exc.NotInProjectError as e:
            print(e.message)
            sys.exit(1)

        BlackfynnCache.setup(Path, BlackfynnRemoteFactory)
        self.project_path = self.anchor.local
        FThing.anchor = FThing(self.project_path)

        # the way this works now the project should always exist
        self.summary = Summary(self.project_path)

        # get the datasets to tigger instantiation of the remote
        list(self.datasets_remote)
        list(self.datasets)
        self.BlackfynnRemote = BlackfynnCache._remote_class
        self.bfl = self.BlackfynnRemote.bfl

    def __call__(self):
        # FIXME this might fail to run annos -> shell correctly
        for k, v in self.args.items():
            if v and not any(k.startswith(c) for c in ('-', '<')):
                getattr(self, k)()
                return
        else:
            self.default()

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
        return [Path(string_dir).absolute() for string_dir in self.args['<directory>']]

    @property
    def paths(self):
        return [Path(string_path).absolute() for string_path in self.args['<path>']]

    @property
    def _paths(self):
        """ all relevant paths determined by the flags that have been set """
        # but if you use the generator version of _paths
        # then if you add a folder to the previous path
        # then it will yeild that folder! which is SUPER COOL
        # but breaks lots of asusmptions elsehwere
        paths = self.paths
        if not paths:
            paths = Path('.').resolve(),
        
        if self.options.only_meta:
            paths = (mp.absolute() for p in paths for mp in FTLax(p).meta_paths)
            yield from paths
            return

        def inner(paths, level=0, stop=self.options.level):
            """ depth first traversal of children """
            for path in paths:
                if self.options.empty:
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
                    yield from path.rchildren
                elif level <= stop:
                    yield from inner(path.children, level + 1)

        yield from inner(paths)

    def clone(self):
        project_id = self.args['<project-id>']
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
        anchor_local = Path(project_name)
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

        with anchor:
            self.pull()

    def pull(self):
        # TODO folder meta -> org
        skip = (
            'N:dataset:83e0ebd2-dae2-4ca0-ad6e-81eb39cfc053',  # hackathon
            'N:dataset:ec2e13ae-c42a-4606-b25b-ad4af90c01bb',  # big max
            'N:dataset:2d0a2996-be8a-441d-816c-adfe3577fc7d',  # big rna
            #'N:dataset:a7b035cf-e30e-48f6-b2ba-b5ee479d4de3',  # powley done
        )
        only = tuple()
        recursive = self.options.level is None  # FIXME we offer levels zero and infinite!
        dirs = self.directories
        if dirs:
            for d in dirs:
                if d.is_dir():
                    if not d.remote.is_dataset():
                        log.warning('You are pulling recursively from below dataset level.')

                    d.remote.bootstrap(recursive=recursive)

        else:
            Path.cwd().remote.bootstrap(recursive=recursive, only=only, skip=skip)

            #self.anchor.remote.bootstrap(recursive=recursive, only=only, skip=skip)

    def refresh(self):
        from pyontutils.utils import Async, deferred
        print(AsciiTable([['Path']] + sorted([p] for p in self._paths)).table)
        Async()(deferred(path.remote.refresh)(update_cache=True,
                                              update_data=self.options.get_data,
                                              size_limit_mb=self.options.limit)
                for path in list(self._paths))
        return
        for path in self._paths:
            path.remote.refresh(update_cache=True,
                                update_data=self.options.get_data,
                                size_limit_mb=self.options.limit)


        return

    def fetch(self):
        from pyontutils.utils import Async, deferred
        print(AsciiTable([['Path']] + sorted([p] for p in self._paths)).table)
        Async(10)(deferred(path.cache.fetch)(size_limit_mb=self.options.limit)
                  for path in list(self._paths))
        
    def annos(self):
        args = self.args
        from protcur.analysis import protc, Hybrid
        from sparcur.curation import populate_annos
        populate_annos()
        if args['export']:
            with open('/tmp/sparc-protcur.rkt', 'wt') as f:
                f.write(protc.parsed())

        if args['shell'] or self.options.debug:
            embed()

    def stats(self):
        args = self.args
        dirs = args['<directory>']
        if not dirs:
            dirs.append('.')

        G = 1024 ** 3
        data = []

        for d in dirs:
            if not Path(d).is_dir():
                continue  # helper files at the top level, and the symlinks that destory python
            path = Path(d).resolve()
            paths = path.children #list(path.rglob('*'))
            path_meta = {p:p.cache.meta for p in paths}
            outstanding = 0
            total = 0
            tf = 0
            ff = 0
            td = 0
            uncertain = False  # TODO
            for p, m in path_meta.items():
                #if p.is_file() and not any(p.stem.startswith(pf) for pf in self.spcignore):
                if p.is_file() or p.is_symlink() and not p.exists():
                    s = m.size
                    if s is None:
                        uncertain = True
                        continue
                    #try:
                        #s = m.size
                    #except KeyError as e:
                        #print(repr(p))
                        #raise e
                    tf += 1
                    if s:
                        total += s

                    #if '.fake' in p.suffixes:
                    if p.is_symlink():
                        ff += 1
                        if s:
                            outstanding += s

                elif p.is_dir():
                    td += 1

            data.append((path.name, outstanding / G, total / G, ff, tf, td))

        maxn = max(len(n) for n, *_ in data)
        align = 4
        fmt = '\n'.join(f'{n:<{maxn+4}} {(gt - go) * 1024:7.2f}M {go:>8.2f} {gt:>8.2f}G{"":>4}{tf - ff:>{align}} {ff:>{align}} {tf:>{align}} {td:>{align}}'
                        for n, go, gt, ff, tf, tc in data)

        h = 'Folder', 'Local', 'Remote', 'Total', 'L', 'R', 'T', 'TD'
        print(f'{{:<{maxn+4}}} {{:>8}} {{:>8}} {{:>9}}{"":>4}{{:>{align}}} {{:>{align}}} {{:>{align}}} {{:>{align}}}'.format(*h))
        print(fmt)

    def demos(self):
        return self.shell()

    def shell(self):
        """ drop into an shell with classes loaded """
        datasets = list(self.datasets)
        dsd = {d.meta.id:d for d in datasets}
        ds = datasets
        summary = self.summary
        org = FThing(self.project_path)
        datasets_local = list(self.datasets_local)
        if self.args['demos']:
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

        elif False:
            ### this is the equivalent of export, quite slow to run
            # export everything
            dowe = summary.data_out_with_errors

            # show all the errors from export everything
            error_id_messages = [(d['id'], e['message']) for d in dowe['datasets'] for e in d['errors']]
            error_messages = [e['message'] for d in dowe['datasets'] for e in d['errors']]

        #rchilds = list(datasets[0].rchildren)
        #package, file = [a for a in rchilds if a.id == 'N:package:8303b979-290d-4e31-abe5-26a4d30734b4']
        p, *rest = self._paths
        f = FThing(p)
        triples = list(f.triples)
        embed()

    def tables(self):
        """ print summary view of raw metadata tables, possibly per dataset """
        # TODO per dataset
        summary = self.summary
        tabular_view_demo = [next(d.dataset_description).t
                                for d in summary
                                if 'dataset_description' in d.data]
        print(repr(tabular_view_demo))

    def export(self):
        """ export output of curation workflows to file """
        #from sparcur.curation import get_datasets
        #ds, dsd = get_datasets(self.project_path)
        #org_id = FThing(self.project_path).organization.id
        cwd = Path.cwd()
        timestamp = datetime.now().isoformat().replace('.', ',')
        if self.options.ttl and cwd != cwd.cache.anchor:
            if not cwd.cache.is_dataset:
                print(f'{cwd.cache} is not at dataset level!')
                sys.exit(123)

            ft = FThing(cwd)
            dump_path = cwd.cache.anchor.local.parent / 'export/datasets' / ft.id / timestamp
            latest_path = dump_path.parent / 'LATEST'
            if not dump_path.exists():
                dump_path.mkdir(parents=True)
                if latest_path.exists():
                    if not latest_path.is_symlink():
                        raise TypeError(f'Why is LATEST not a symlink? {latest_path!r}')

                    latest_path.unlink()

                latest_path.symlink_to(dump_path)

            filename = 'curation-export'
            filepath = dump_path / filename

            with open(filepath.with_suffix('.ttl'), 'wb') as f:
                f.write(ft.ttl)

            print(f'dataset graph exported to {filepath.with_suffix(".ttl")}')
            return
        
        summary = self.summary
        # start time not end time ...
        # obviously not transactional ...
        filename = 'curation-export'
        dump_path = summary.path.parent / 'export' / summary.id / timestamp
        latest_path = dump_path.parent / 'LATEST'
        if not dump_path.exists():
            dump_path.mkdir(parents=True)
            if latest_path.exists():
                if not latest_path.is_symlink():
                    raise TypeError(f'Why is LATEST not a symlink? {latest_path!r}')

                latest_path.unlink()

            latest_path.symlink_to(dump_path)

        filepath = dump_path / filename

        for xml_name, xml in summary.xml:
            with open(filepath.with_suffix(f'.{xml_name}.xml'), 'wb') as f:
                f.write(xml)

        with open(filepath.with_suffix('.json'), 'wt') as f:
            json.dump(summary.data_out_with_errors, f, sort_keys=True, indent=2, cls=JEncode)

        with open(filepath.with_suffix('.ttl'), 'wb') as f:
            f.write(summary.ttl)

        # datasets, contributors, subjects, samples, resources
        for table_name, tabular in summary.disco:
            with open(filepath.with_suffix(f'.{table_name}.tsv'), 'wt') as f:
                writer = csv.writer(f, delimiter='\t', lineterminator='\n')
                writer.writerows(tabular)

        if self.options.debug:
            embed()

    def find(self):
        args = self.args
        paths = []
        if args['<file>']:
            files = args['<file>']
            for file in files:
                path = Path(file).resolve()
                if path.is_dir():
                    paths.extend(path.rglob('*.fake.*'))
                else:
                    paths.append(path)

        elif args['--name']:
            patterns = args['--name']
            path = Path('.').resolve()
            for pattern in patterns:
                # TODO filesize mismatches on non-fake
                # no longer needed due to switching to symlinks
                #if '.fake' not in pattern and not self.options.overwrite:
                    #pattern = pattern + '.fake*'

                for file in path.rglob(pattern):
                    paths.append(file)

        if paths:
            if self.options.limit:
                paths = [p for p in paths
                         if p.cache.meta.size is None or  # if we have no known size don't limit it
                         not p.exists() and p.cache.meta.size.mb < self.options.limit
                         or p.exists() and p.meta.size != p.cache.meta.size and
                         (not log.info(f'Truncated transfer detected for {p}\n'
                                       f'{p.meta.size} != {p.cache.meta.size}'))
                         and p.cache.meta.size.mb < self.options.limit]

            if args['--verbose']:
                for p in paths:
                    print(p.cache.meta.as_pretty(pathobject=p))

            if args['--fetch']:
                from pyontutils.utils import Async, deferred
                #Async()(deferred(self.bfl.fetch_path)(path, self.options.overwrite) for path in paths)
                Async(rate=30)(deferred(path.cache.fetch)() for path in paths)
            else:
                [print(p) for p in paths]

    def report(self):
        if self.args['filetypes']:
            #root = FThing(self.project_path)
            fts = [FThing(p) for p in self.project_path.rglob('*') if p.is_file()]
            #all_counts = sorted([(*k, v) for k, v in Counter([(f.suffix, f.mimetype, f._magic_mimetype)
                                                              #for f in fts]).items()], key=lambda r:r[-1])

            def count(thing):
                return sorted([(k, v) for k, v in Counter([getattr(f, thing)
                                                            for f in fts]).items()], key=lambda r:-r[-1])
            #each = {t:count(t) for t in ('suffix', 'mimetype', )}
            each = {t:count(t) for t in ('_magic_mimetype', )}

            for head, tups in each.items():
                print(head)
                print('\n'.join(['\t'.join(str(e).strip('.') for e in t) for t in tups]))

        elif self.args['subjects']:
            subjects_headers = tuple(h for ft in self.summary
                                     for sf in ft.subjects
                                     for h in sf.bc.header)
            counts = tuple(kv for kv in sorted(Counter(subjects_headers).items(),
                                               key=lambda kv:-kv[-1]))
            print(AsciiTable(((f'Column Name unique = {len(counts)}', '#'), *counts)).table)

        elif self.args['completeness']:
            rows = [('', 'EI', 'DSCI', 'name', 'id')]
            rows += [(i + 1, ei, f'{index:.{2}f}' if index else 0, *rest) for i, (ei, index, *rest) in
                     enumerate(sorted(self.summary.completeness, key=lambda t:(t[0], -t[1], *t[2:])))]
            print(AsciiTable(rows).table)

        elif self.args['keywords']:
            rows = [sorted(dataset.keywords, key=lambda v: -len(v))
                    for dataset in self.summary]
            print(AsciiTable(rows).table)

        if self.options.debug:
            embed()

    def feedback(self):
        args = self.args
        file = args['<feedback-file>']
        feedback = ' '.join(args['<feedback>'])
        path = Path(file).resolve()
        eff = FThing(path)
        # TODO pagenote and/or database
        print(eff, feedback)

    def missing(self):
        self.bfl.find_missing_meta()

    def xattrs(self):
        self.bfl.populate_metastore()

    def meta(self):
        BlackfynnCache._local_class = Path  # since we skipped _setup
        paths = self.paths
        if not paths:
            paths = Path('.').resolve(),

        old_level = log.level
        log.setLevel('ERROR')
        def inner(path):
            if self.options.uri:
                uri = path.cache.human_uri
                print('+' + '-' * (len(uri) + 2) + '+')
                print(f'| {uri} |')
            try:
                meta = path.cache.meta
                if meta is not None:
                    print(path.cache.meta.as_pretty(pathobject=path))
            except exc.NoCachedMetadataError:
                print(f'No metadata for {path}. Run `spc refresh {path}`')

        for path in paths:
            inner(path)

        log.setLevel(old_level)


def main():
    from docopt import docopt, parse_defaults
    args = docopt(__doc__, version='spc 0.0.0')
    defaults = {o.name:o.value if o.argcount else None for o in parse_defaults(__doc__)}
    dispatch = Dispatch(args)
    dispatch()


if __name__ == '__main__':
    main()
