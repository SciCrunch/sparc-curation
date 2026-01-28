import json
from pathlib import PurePath
from collections import defaultdict
from sparcur.config import auth
from sparcur.utils import PennsieveId as RemoteId, log
from terminaltables3 import AsciiTable


def diff_part0(paths):
    s = set(path.parts[0] for path in paths)
    return len(s) > 1


def shared_prefix(paths):
    sfxs = set(p.suffix for p in paths)
    for s in sfxs:
        for s2 in sfxs:
            if s != s2:
                if s in s2 or s2 in s:
                    return True


def f_check_path_meta(j, f000, do_report_tar=False):
    k_drp = 'dataset_relative_path'
    drps = []
    dd = defaultdict(list)
    dd2 = defaultdict(list)
    dd3 = defaultdict(lambda:defaultdict(list))
    by_drp = {}
    for p in j['data']:
        if 'mimetype' in p and p['mimetype'] != 'inode/directory' or 'mimetype' not in p:
            drp = PurePath(p[k_drp])
            by_drp[drp] = p
            drps.append(drp)
            stem = drp.stem
            i = 0
            if drp.suffix != '':
                while '.' in stem and stem != '.':
                    stem = PurePath(stem).stem
                    i += 1
                    if i > 100:
                        breakpoint()
                        pass

            dd[drp.parent, stem].append(drp)
            parts = drp.parts
            if len(parts) > 1 and (parts[0] in ('source', 'primary',)):
                dd2[drp.parts[1:-1], stem].append(drp)
                dd3[stem][*drp.suffixes].append(drp)
            #else:
                #print('sigh', drp)

    # in some cases this is ok
    multi_stems = {k: v for k, v in dd.items() if len(v) > 1 and shared_prefix(v)}
    hrm = {k: [by_drp[p] for p in v] for k, v in multi_stems.items()}
    mismatched_source_primary = 'TODO'

    multi_srcpri = {k: v for k, v in dd2.items() if len(v) > 1 and diff_part0(v)}

    wat = set(k for v in dd3.values() for k in v.keys())
    no_tiff = {}
    no_tar = {}
    no_gci = {}
    no_ktf = {}
    for k, v in dd3.items():
        if ((('.tar',) in v or ('.tar', '.gz',) in v) and
            (('.tiff',) not in v and ('.tif',) not in v)):
            no_tiff[k] = v

        elif ((('.tar',) not in v and ('.tar', '.gz',) not in v) and
              (('.tiff',) in v or ('.tif',) in v)):
            no_tar[k] = v

        if (('.tar',) in v or ('.tar', '.gz',) in v or ('.tiff',) in v or ('.tif',) in v):

            if ('.gci',) not in v:
                if k in no_tiff or k in no_tar:
                    # log.info('skipping no gci due to no tiff')
                    pass
                else:
                    no_gci[k] = v

            if ('.ktf',) not in v:
                if k in no_tiff or k in no_tar:
                    # log.info('skipping no ktf due to no tiff')
                    pass
                else:
                    no_ktf[k] = v

    did = j['data'][0]['dataset_id']
    row = [did, len(no_tar), len(no_tiff), len(no_gci), len(no_ktf), len(multi_stems)]
    did = did + ' f' + f000
    report = multi_stems, no_tiff, no_tar, no_gci, no_ktf

    sep = '\n---------------------------------------------'
    if no_tar and do_report_tar:
        r_no_tar = '\ntar '.join(['\ntar '.join(sorted([p.as_posix() for v in d.values() for p in v])) for d in no_tar.values()]).replace(' source', '  source')
        log.info(f'\n{did} no tar\ntar {r_no_tar}{sep}')

    if no_tiff:
        r_no_tiff = '\n\ntif '.join(['\ntif '.join(sorted([p.as_posix() for v in d.values() for p in v])) for d in no_tiff.values()]).replace(' source', '  source')
        log.info(f'\n{did} no tiff\ntif {r_no_tiff}{sep}')

    if no_gci:
        r_no_gci = '\n\ngci '.join(['\ngci '.join(sorted([p.as_posix() for v in d.values() for p in v])) for d in no_gci.values()]).replace(' source', '  source')
        log.info(f'\n{did} no gci\ngci {r_no_gci}{sep}')

    if no_ktf:
        r_no_ktf = '\n\nktf '.join(['\nktf '.join(sorted([p.as_posix() for v in d.values() for p in v])) for d in no_ktf.values()]).replace(' source', '  source')
        log.info(f'\n{did} no ktf\nktf {r_no_ktf}{sep}')

    if multi_stems:
        r_ms = '\n\n ms '.join(['\n ms '.join(sorted([p.as_posix() for p in d])) for d in multi_stems.values()])
        log.info(f'\n{did} multi suffix\n ms {r_ms}{sep}')
        pass

    return row, report


def main():
    path_export = auth.get_path('export-path')
    path_ex_datasets = path_export / 'datasets'

    dids = [RemoteId(i).uuid for i in auth.get_list('datasets-ft')]
    rows = [['id', 'f000', 'no tar', 'no tiff', 'no gci', 'no ktf', 'multi stems']]
    for i, did in enumerate(dids):
        f000 = f'{i + 1:0>3}'  # XXX only by accident of sorting in the config
        path_ds_latest = path_ex_datasets / did / 'LATEST'
        path_path_meta = path_ds_latest / 'path-metadata.json'
        path = path_path_meta
        if not path.exists():
            log.error(path)
            continue

        with open(path, 'rt') as f:
            j = json.load(f)

        row, rep = f_check_path_meta(j, f000)

        row = row[:1] + [f000] + row[1:]
        rows.append(row)

    table = AsciiTable(rows).table
    log.info(f'\n{table}')


if __name__ == '__main__':
    main()
