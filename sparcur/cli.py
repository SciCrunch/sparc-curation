#!/usr/bin/env python3
""" SPARC curation cli for fetching, validating datasets, and reporting.
Usage:
    spc pull
    spc annos [export shell]
    spc stats [<directory>...]
    spc report [filetypes subjects completeness] [options]
    spc tables [<directory>...]
    spc missing
    spc xattrs
    spc export
    spc demos
    spc shell [--project]
    spc feedback <feedback-file> <feedback>...
    spc [options] <file>...
    spc [options] --name=<PAT>...

Commands:
              list and fetch unfetched files
    pull      pull down the remote list of files
    stats     print stats for specified or current directory
    report    print a report on all datasets
    missing   find and fix missing metadata
    xattrs    populate metastore / backup xattrs
    export    export extracted data
    demos     long running example queries
    shell     drop into an ipython shell

Options:
    -f --fetch              fetch the files
    -l --limit=LIMIT        the maximum size to download in megabytes
    -n --name=<PAT>         filename pattern to match (like find -name)
    -v --verbose            print extra information
    -j --project            find the project_path by looking for N:organization:
    -p --project-path=<PTH> set the project path manually
    -o --overwrite          fetch even if the file exists
    -d --debug              drop into a shell after running a step

"""

import csv
import json
import pprint
from datetime import datetime
from collections import Counter
import requests
from terminaltables import AsciiTable
from sparcur import schemas as sc
from sparcur import curation
from sparcur.blackfynn_api import BFLocal
from sparcur.backends import BlackfynnRemoteFactory
from sparcur.paths import Path, BlackfynnCache, PathMeta
from sparcur.curation import FThing, FTLax, CurationReport, Summary
from sparcur.curation import get_datasets, JEncode
from sparcur.core import JT
from IPython import embed


class Dispatch:
    spcignore = ('.git',
                 '.~lock',)
    def __init__(self, args):
        self.args = args

        if args['--project'] or args['--project-path']:
            if args['--project-path']:
                path = args['--project-path']
            else:
                path = '.'
            # FIXME only works for fs version, bfl will not change
            current_ft = FThing(Path(path).resolve())
            while not current_ft.is_organization:
                current_ft = current_ft.parent

            pp = current_ft.path
            curation.project_path = pp # FIXME BAD BAD BAD

        try:
            self.bfl = BFLocal()
        except requests.exceptions.ConnectionError:
            # FIXME should we be failing early here?
            self.bfl = 'Could not connect to blackfynn'

        # we don't instantiate these directly so you can have one
        # but if it isn't anchored to the file system (bootstrap)
        # then it won't be very useful
        self.BlackfynnRemote = BlackfynnRemoteFactory(Path, BlackfynnCache, self.bfl)

        if curation.project_path.exists():
            self.summary = Summary(curation.project_path)
        else:
            self.summary = None

    def __call__(self):
        # FIXME this might fail to run annos -> shell correctly
        for k, v in self.args.items():
            if v and not any(k.startswith(c) for c in ('-', '<')):
                getattr(self, k)()
                return
        else:
            self.default()

    @property
    def project_path(self):
        return curation.project_path

    @property
    def debug(self):
        return self.args['--debug']

    @property
    def overwrite(self):
        return self.args['--overwrite']

    def pull(self):
        # TODO folder meta -> org
        self.bfl.cons()

    def annos(self):
        args = self.args
        from protcur.analysis import protc, Hybrid
        from sparcur.curation import populate_annos
        populate_annos()
        if args['export']:
            with open('/tmp/sparc-protcur.rkt', 'wt') as f:
                f.write(protc.parsed())

        if args['shell'] or self.debug:
            embed()

    def stats(self):
        args = self.args
        dirs = args['<directory>']
        if not dirs:
            dirs.append('.')

        G = 1024 ** 3
        data = []

        for d in dirs:
            path = Path(d).resolve()
            if not path.is_dir():
                continue
            paths = list(path.rglob('*'))
            path_meta = {p:p.cache.meta for p in paths}
            outstanding = 0
            total = 0
            tf = 0
            ff = 0
            for p, m in path_meta.items():
                if p.is_file() and not any(p.stem.startswith(pf) for pf in self.spcignore):
                    try:
                        s = m.size
                    except KeyError as e:
                        print(repr(p))
                        raise e
                    tf += 1
                    if s:
                        total += s

                    if '.fake' in p.suffixes:
                        ff += 1
                        if s:
                            outstanding += s

            data.append((path.name, outstanding / G, total / G, ff, tf))

        maxn = max(len(n) for n, *_ in data)
        align = 4
        fmt = '\n'.join(f'{n:<{maxn+4}} {(gt - go) * 1024:7.2f}M {go:>8.2f} {gt:>8.2f}G{"":>4}{tf - ff:>{align}} {ff:>{align}} {tf:>{align}}'
                        for n, go, gt, ff, tf in data)

        h = 'Folder', 'Local', 'Remote', 'Total', 'L', 'R', 'T'
        print(f'{{:<{maxn+4}}} {{:>8}} {{:>8}} {{:>9}}{"":>4}{{:>{align}}} {{:>{align}}} {{:>{align}}}'.format(*h))
        print(fmt)

    def demos(self):
        return self.shell()

    def shell(self):
        """ drop into an shell with classes loaded """
        ds, dsd = get_datasets(curation.project_path)  # FIXME deprecate
        summary = self.summary
        org = FThing(curation.project_path)
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

        elif False:
            ### this is the equivalent of export, quite slow to run
            # export everything
            dowe = summary.data_out_with_errors

            # show all the errors from export everything
            error_id_messages = [(d['id'], e['message']) for d in dowe['datasets'] for e in d['errors']]
            error_messages = [e['message'] for d in dowe['datasets'] for e in d['errors']]

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
        #ds, dsd = get_datasets(curation.project_path)
        #org_id = FThing(curation.project_path).organization.id
        summary = self.summary
        timestamp = datetime.now().isoformat()
        # start time not end time ...
        # obviously not transactional ...
        filename = 'curation-export'
        dump_path = summary.path.parent / 'export' / summary.id / timestamp
        if not dump_path.exists():
            dump_path.mkdir(parents=True)

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

        if self.debug:
            embed()

    def default(self):
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
                if '.fake' not in pattern and not self.overwrite:
                    pattern = pattern + '.fake*'

                for file in path.rglob(pattern):
                    paths.append(file)

        if paths:
            if args['--limit']:
                limit = int(args['--limit']) * 1024 ** 2
                paths = [p for p in paths if p.cache.meta.size < limit]

            if args['--fetch']:
                from pyontutils.utils import Async, deferred
                Async()(deferred(self.bfl.fetch_path)(path, self.overwrite) for path in paths)
            else:
                if args['--verbose']:
                    for p in paths:
                        print(p, p.xattrs())
                else:
                    [print(p) for p in paths]

    def report(self):
        if self.args['filetypes']:
            #root = FThing(curation.project_path)
            fts = [FThing(p) for p in curation.project_path.rglob('*') if p.is_file()]
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
            rows = [('', 'DSCI', 'name', 'id')]
            rows += [(i, *rest) for i, rest in
                     enumerate(sorted(self.summary.completeness, reverse=True))]
            print(AsciiTable(rows).table)

        if self.debug:
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


def main():
    from docopt import docopt, parse_defaults
    args = docopt(__doc__, version='bfc 0.0.0')
    defaults = {o.name:o.value if o.argcount else None for o in parse_defaults(__doc__)}
    dispatch = Dispatch(args)
    dispatch()


if __name__ == '__main__':
    main()
