import re
import json
from pprint import pformat
from pathlib import PurePath
from collections import defaultdict
from terminaltables3 import AsciiTable

try:
    from sparcur.utils import log
except ModuleNotFoundError:
    class log:
        info = print
        debug = print


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


def f_check_path_meta(j, f000, do_report_tar=False, do_multi_stems=False, do_ff=False):
    k_drp = 'dataset_relative_path'
    drps = []
    dd = defaultdict(list)
    dd2 = defaultdict(list)
    dd3 = defaultdict(lambda:defaultdict(list))
    old_ihc = 0
    n_sgc = 0
    by_drp = {}
    mangled = defaultdict(set)
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

            parts = drp.parts
            if 'old_ihc' in parts:
                old_ihc += 1
                continue

            dd[drp.parent, stem].append(drp)
            if len(parts) > 1 and (parts[0] in ('source', 'primary',)):
                dd2[drp.parts[1:-1], stem].append(drp)
                raw_stem = stem
                if '_sgc_' in stem:
                    stem, *rest = re.split('_sgc_', stem)
                    n_sgc += 1

                if stem.endswith('_s'):
                    # TODO th_s.tiff and th_th_s.tiff cases
                    if stem.endswith('_th_th_s'):
                        stem = stem[:-5]
                    elif stem.endswith('_th_s'):
                        stem = stem[:-5]

                if stem.startswith('B848'):  # FIXME HACK f011 hacked fix
                    hrm = stem.split('_')
                    #if 'T4L' in raw_stem:
                        #breakpoint()
                    if 'Level' in hrm:
                        li = hrm.index('Level')
                        let = hrm[2]
                        lil = let in 'ABCDEFGHIJKLMNOP'
                        if li - 1 != 2 or not lil:
                            intermed = let if lil else 'A'
                            asdf = hrm[:2] + [intermed] + hrm[li:]
                            xstem = '_'.join(asdf)
                            if stem != xstem:
                                mangled[xstem].add(stem)
                                #mangled[xstem] = stem
                                #dd3[xstem][*drp.suffixes].append(drp)
                                stem = xstem
                                #if 'T4L' in raw_stem:
                                #if 'RL' in raw_stem:
                                    #breakpoint()
                                    #pass

                dd3[stem][*drp.suffixes].append(drp)

    # in some cases this is ok
    # multi_stems is actually ok most of the time even for tiff/tif becuase tif is 4 channel and tiff is 3 channel processed for segmentation
    multi_stems = {k: v for k, v in dd.items() if len(v) > 1 and shared_prefix(v)}
    hrm = {k: [by_drp[p] for p in v] for k, v in multi_stems.items()}
    mismatched_source_primary = 'TODO'

    multi_srcpri = {k: v for k, v in dd2.items() if len(v) > 1 and diff_part0(v)}

    wat = set(k for v in dd3.values() for k in v.keys())
    no_tiff = {}
    no_ff = {}
    no_f = {}
    no_tar = {}
    no_gci = {}
    no_ktf = {}
    mang = {}
    for k, v in dd3.items():
        if k in mangled and len(v) > 1:
            mang[k] = set(_.name#.stem
                          for vs in v.values() for _ in vs)
            #ev = dd3[mangled[k]]
            #v = {**v, **ev}

        if ((('.tar',) in v or ('.tar', '.gz',) in v) and
            (('.tiff',) not in v and ('.tif',) not in v)):
            no_tiff[k] = v

        elif ((('.tar',) not in v and ('.tar', '.gz',) not in v) and
              (('.tiff',) in v or ('.tif',) in v)):
            no_tar[k] = v

        if (('.tar',) in v or ('.tar', '.gz',) in v or ('.tiff',) in v or ('.tif',) in v):
            if k not in no_tiff:
                if ('.tif',) not in v:
                    no_f[k] = v
                if ('.tiff',) not in v:
                    no_ff[k] = v

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

    if mang:
        # watch out for desync calls to flush for stdout vs stderr
        mang10 = {k: v for k, v in mang.items() if len(v) == 10}
        mmang = {k: v for k, v in mang.items() if len(v) != 10}
        log.debug(f'10-mang\n{pformat(mang10)}')
        log.debug(f'not-10-mang\n{pformat(mmang)}')

    did = j['data'][0]['dataset_id']
    row = [did, len(no_tar), len(no_tiff), len(no_f), len(no_ff), len(no_gci), len(no_ktf), len(multi_stems), n_sgc, (n_sgc - (4 * len(no_ff))), old_ihc, len(mang)]
    did = did + ' f' + f000
    report = multi_stems, no_tiff, no_tar, no_gci, no_ktf

    sep = '\n---------------------------------------------'
    if no_tar and do_report_tar:
        r_no_tar = '\ntar '.join(['\ntar '.join(sorted([p.as_posix() for v in d.values() for p in v])) for d in no_tar.values()]).replace(' source', '  source')
        log.info(f'\n{did} no tar\ntar {r_no_tar}{sep}')

    if no_tiff:
        r_no_tiff = '\n\ntif '.join(['\ntif '.join(sorted([p.as_posix() for v in d.values() for p in v])) for d in no_tiff.values()]).replace(' source', '  source')
        log.info(f'\n{did} no tiff\ntif {r_no_tiff}{sep}')

    if no_f and do_ff:
        r_no_f = '\n\n  f '.join(['\n  f '.join(sorted([p.as_posix() for v in d.values() for p in v])) for d in no_f.values()]).replace(' source', '  source')
        log.info(f'\n{did} no f\n  f {r_no_f}{sep}')

    if no_ff:# and do_ff:
        r_no_ff = '\n\n ff '.join(['\n ff '.join(sorted([p.as_posix() for v in d.values() for p in v])) for d in no_ff.values()]).replace(' source', '  source')
        log.info(f'\n{did} no tiff\n ff {r_no_ff}{sep}')

    if no_gci:
        r_no_gci = '\n\ngci '.join(['\ngci '.join(sorted([p.as_posix() for v in d.values() for p in v])) for d in no_gci.values()]).replace(' source', '  source')
        log.info(f'\n{did} no gci\ngci {r_no_gci}{sep}')

    if no_ktf:
        r_no_ktf = '\n\nktf '.join(['\nktf '.join(sorted([p.as_posix() for v in d.values() for p in v])) for d in no_ktf.values()]).replace(' source', '  source')
        log.info(f'\n{did} no ktf\nktf {r_no_ktf}{sep}')

    if multi_stems and do_multi_stems:
        r_ms = '\n\n ms '.join(['\n ms '.join(sorted([p.as_posix() for p in d])) for d in multi_stems.values()])
        log.info(f'\n{did} multi suffix\n ms {r_ms}{sep}')
        pass

    return row, report


def main(local=False, dids_from_config=False, do_report_tar=True):
    if local or dids_from_config:
        from sparcur.config import auth

    if dids_from_config:
        from sparcur.utils import PennsieveId as RemoteId
        dids = [RemoteId(i).uuid for i in auth.get_list('datasets-ft')]
    else:
        dids = [
            # put uuids here
        ]

    if local:
        path_export = auth.get_path('export-path')
        path_ex_datasets = path_export / 'datasets'
    else:
        import requests

    rows = [['id', 'f000', 'no tar', 'no tiff', 'no f', 'no ff', 'no gci', 'no ktf', 'multi stems', 'n sgc', '(- nsgc (* 4 noff))', 'old ihc', 'mang', 'n3', 'n4']]
    for i, did in enumerate(dids):
        f000 = f'{i + 1:0>3}'  # XXX only by accident of sorting in the config
        if local:
            path_ds_latest = path_ex_datasets / did / 'LATEST'
            path_path_meta = path_ds_latest / 'path-metadata.json'
            path_cexp = path_ds_latest / 'curation-export.json'
            path = path_path_meta
            if not path.exists():
                log.error(path)
                continue

            with open(path, 'rt') as f:
                j = json.load(f)
            with open(path_cexp, 'rt') as f:
                jc = json.load(f)

        else:
            resp = requests.get(f'https://cassava.ucsd.edu/sparc/datasets/{did}/LATEST/path-metadata.json')
            j = resp.json()
            resp = requests.get(f'https://cassava.ucsd.edu/sparc/datasets/{did}/LATEST/curation-export.json')
            jc = resp.json()

        row, rep = f_check_path_meta(j, f000, do_report_tar=do_report_tar)
        n3, n4 = '?', '?'
        if 'sites' in jc:
            n3, n4 = 0, 0
            for site in jc['sites']:
                if 'stain_type' in site:
                    st = site['stain_type']
                    if st == '3 stain':
                        n3 += 1
                    elif st == '4 stain':
                        n4 += 1
                    else:
                        log.error(f'wat {st}')
                else:
                    log.error(f'wat {list(site.keys())}')

        row = row[:1] + [f000] + row[1:] + [n3, n4]
        rows.append(row)

    table = AsciiTable(rows).table
    log.info(f'\n{table}')


if __name__ == '__main__':
    main()
