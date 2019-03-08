#!/usr/bin/env python3
""" Blackfynn cli for working with the file system.
Usage:
    bfc pull
    bfc annos [export shell]
    bfc stats [<directory>...]
    bfc report [filetypes]
    bfc missing
    bfc xattrs
    bfc export
    bfc shell [--project]
    bfc feedback <feedback-file> <feedback>...
    bfc [options] <file>...
    bfc [options] --name=<PAT>...

Commands:
              list and fetch unfetched files
    pull      pull down the remote list of files
    stats     print stats for specified or current directory
    report    print a report on all datasets
    missing   find and fix missing metadata
    xattrs    populate metastore / backup xattrs
    export    export extracted data
    shell     drop into an ipython shell

Options:
    -f --fetch              fetch the files
    -l --limit=LIMIT        the maximum size to download in megabytes
    -n --name=<PAT>         filename pattern to match (like find -name)
    -v --verbose            print extra information
    -p --project            find the project_path by looking for N:organization:
    -o --overwrite          fetch even if the file exists

"""

from sparcur.blackfynn_api import BFLocal, Path
from sparcur.curation import FThing, FTLax, CurationReport, Summary, JT

def main():
    from docopt import docopt, parse_defaults
    args = docopt(__doc__, version='bfc 0.0.0')
    defaults = {o.name:o.value if o.argcount else None for o in parse_defaults(__doc__)}
    from IPython import embed
    from sparcur import curation

    overwrite = args['--overwrite']

    if args['--project']:
        # FIXME only works for fs version, bfl will not change
        current_ft = FThing(Path('.').resolve())
        while not current_ft.is_organization:
            current_ft = current_ft.parent

        pp = current_ft.path
        curation.project_path = pp # FIXME BAD BAD BAD

    if args['annos']:
        from protcur.analysis import protc, Hybrid
        from sparcur.curation import populate_annos
        populate_annos()
        if args['export']:
            with open('/tmp/sparc-protcur.rkt', 'wt') as f:
                f.write(protc.parsed())

        if args['shell']:
            embed()

    elif args['pull']:
        # TODO folder meta -> org
        bfl = BFLocal()
        bfl.cons()

    elif args['stats']:
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
            path_meta = {p:p.xattrs() for p in paths}
            outstanding = 0
            total = 0
            tf = 0
            ff = 0
            for p, m in path_meta.items():
                if p.is_file() and not any(p.stem.startswith(pf) for pf in ('.~lock',)):
                    try:
                        s = int(m['bf.size'])
                    except KeyError as e:
                        print(p)
                        raise e
                    tf += 1
                    total += s
                    if '.fake' in p.suffixes:
                        ff += 1
                        outstanding += s

            data.append((path.name, outstanding / G, total / G, ff, tf))

        maxn = max(len(n) for n, *_ in data)
        align = 4
        fmt = '\n'.join(f'{n:<{maxn+4}} {(gt - go) * 1024:7.2f}M {go:>8.2f} {gt:>8.2f}G{"":>4}{tf - ff:>{align}} {ff:>{align}} {tf:>{align}}'
                        for n, go, gt, ff, tf in data)

        h = 'Folder', 'Local', 'Remote', 'Total', 'L', 'R', 'T'
        print(f'{{:<{maxn+4}}} {{:>8}} {{:>8}} {{:>9}}{"":>4}{{:>{align}}} {{:>{align}}} {{:>{align}}}'.format(*h))
        print(fmt)

    elif args['report']:
        from collections import Counter
        if args['filetypes']:
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
            embed()
        #rep = curation_report()
        #print(rep)

    elif args['feedback']:
        file = args['<feedback-file>']
        feedback = ' '.join(args['<feedback>'])
        path = Path(file).resolve()
        eff = FThing(path)
        # TODO pagenote and/or database
        print(eff, feedback)

    elif args['missing']:
        bfl = BFLocal()
        bfl.find_missing_meta()

    elif args['xattrs']:
        bfl = BFLocal()
        bfl.populate_metastore()

    elif args['export']:
        import csv
        import json
        from datetime import datetime
        from sparcur.curation import CJEncode
        #from sparcur.curation import get_datasets
        #ds, dsd = get_datasets(curation.project_path)
        #org_id = FThing(curation.project_path).organization.id
        summary = Summary(curation.project_path)
        timestamp = datetime.now().isoformat()
        # start time not end time ...
        # obviously not transactional ...
        filename = f'curation-export-{summary.id}-{timestamp}'
        dump_path = summary.path.parent / 'export'
        if not dump_path.exists():
            dump_path.mkdir()

        filepath = dump_path / filename

        with open(filepath.with_suffix('.json'), 'wt') as f:
            json.dump(summary.data_out_with_errors, f, sort_keys=True, indent=2, cls=CJEncode)

        # datasets, contributors, subjects, samples, resources
        for table_name, tabular in summary.disco:
            with open(filepath.with_suffix(f'.{table_name}.tsv'), 'wt') as f:
                    writer = csv.writer(f, delimiter='\t', lineterminator='\n')
                    writer.writerows(tabular)

    elif args['shell']:
        from sparcur.curation import get_datasets
        bfl = BFLocal()
        ds, dsd = get_datasets(curation.project_path)
        summary = Summary(curation.project_path)
        # dowe = summary.data_out_with_errors
        # error_messages = [(d['id'], e['message']) for d in dwe['datasets'] for e in d['errors']]
        # error_messages = [e['message'] for d in dwe['datasets'] for e in d['errors']]
        embed()

    else:
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
                if '.fake' not in pattern and not overwrite:
                    pattern = pattern + '.fake*'

                for file in path.rglob(pattern):
                    paths.append(file)

        if paths:
            if args['--limit']:
                limit = int(args['--limit']) * 1024 ** 2
                paths = [p for p in paths if int(p.getxattr('bf.size')) < limit]

            if args['--fetch']:
                bfl = BFLocal()
                from pyontutils.utils import Async, deferred
                Async()(deferred(bfl.fetch_path)(path, overwrite) for path in paths)
            else:
                if args['--verbose']:
                    for p in paths:
                        print(p, p.xattrs())
                else:
                    [print(p) for p in paths]


if __name__ == '__main__':
    main()
