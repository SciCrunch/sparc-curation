# example data for subject 1
subject_spec_1 = dict(
    id = 'sub-f001',
    # number of segments per laterality
    # this is expected to vary per subject
    lat_seg = {
        'l': 4,
        'r': 3,
    },
    # mapping between sites and segments for each laterality
    # this is expected to vary per subject
    lat_seg_site = {
        'l': {
            1: ['nd'],
            2: ['cg', 'eg'],
            3: [],
            4: [],
        },
        'r': {
            1: ['nd'],
            2: ['cg', 'eg'],
            3: [],
        },
    },
)

site_conventions = {
    'nd': 'nodose ganglion',
    'eg': 'esophogeal ganglaion',
    'cg': 'coeliac ganglion',
}

# mapping of section number to protocol type
# FIXME if can this vary per subject then we need to embed it in the subject_spec
# if it can vary per site then we likely need theme an variations with per subject
# and then allow override at site
section_type = {
    1: 'he',
    2: 'ihc-chat',
    3: 'ihc-th',
    4: 'ihc-nav1',
    5: 'em',
    6: 'ihc-chat',
    7: 'ihc-th',
    8: 'ihc-nav1',
}

# the expected data files per section type
type_example_files = {
    'he': ['20x.tiff', '100x.tiff'],
    'em': ['big-em-image.tiff',],
    'ihc': ['20x.tiff', '100x.tiff'],
}


# conventions for padding identifiers can be discussed, right now they are unpadded
def make_tree(path, pdo, subject_spec, ultrasound_as_site=True, histology_as_folder=True,
              mct_primary_name='stacked',  # change as needed/inspired
              n_virt_sec=7,
              n_ihc_fasc=10,  # less than 100 per section
              ):
    primary = pdo == 'primary'
    derived = pdo == 'derived'
    source = pdo == 'source'
    other = not (primary or derived or source)

    subject = subject_spec['id']
    lat_seg = subject_spec['lat_seg']
    lat_seg_site = subject_spec['lat_seg_site']
    # section_type = subject_spec['section_type']  # XXX see note above
    # site_section_type = subject_spec['site_section_type']  # XXX note above
    n_sec = len(section_type)

    dirs = []
    files = []
    us_type = 'site' if ultrasound_as_site else 'sam'
    # laterality
    for lat, nseg in lat_seg.items():
        # ultrasound
        p = f'{us_type}-{lat}/ultrasound'  # TODO we may need site-vl and site-vr for this
        dirs.append(p)
        if primary:
            f = p + '/image.image'
            files.append(f)
        elif derived:
            pass  # content unknown at this time
        elif source:
            pass
        else:
            pass
        # segments
        for seg in range(1, nseg + 1):
            # microct
            p = f'sam-{lat}/sam-{lat}-seg-{seg}/microct'
            dirs.append(p)
            if primary:
                for ext in ('jpx', 'tiff'):
                    f = p + f'/{mct_primary_name}.{ext}'
                    files.append(f)
            elif derived:
                # there will be one output file per 2d section from the microct
                # the fascicles would be identified per "virtual section"
                for n in range(1, n_virt_sec + 1):
                    files.append(p + f'/vsec-rois-{n}.tabular')
            elif source:
                # right now only seeing 2d coronoal slices come out of the CT software
                # in principle other sections could come out
                # assume that the raw tilt series exists and will be provided
                files.append(p + f'/tilt-series.image')
                for n in range(1, n_virt_sec + 1):
                    f = p + f'/raw-{n}.image'
                    files.append(f)
            else:
                pass
            # sites
            for site in lat_seg_site[lat][seg]:
                if histology_as_folder:
                    # if we go with histology as folder we will want to use
                    # the manifest to map to sections and probably put the
                    # section id in the file name as a cross reference
                    p = f'sam-{lat}/sam-{lat}-seg-{seg}/histology'
                    dirs.append(p)
                    # TODO generate the manifest mapping sec seg sam
                    files.append(p + '/manifest.tabular')
                # section_type = site_section_type[site]
                # sections
                for sec, _type in section_type.items():
                    if not histology_as_folder:
                        p = f'sam-{lat}/sam-{lat}-seg-{seg}/sam-{lat}-{site}-sec-{sec}'
                        dirs.append(p)

                    type = _type.split('-')[0]
                    if primary:
                        if histology_as_folder:
                            # primary section files
                            for _f in type_example_files[type]:
                                f = f'{lat}-{site}-sec-{sec}-{_f}'
                                files.append(p + f'/{f}')
                        else:
                            for f in type_example_files[type]:
                                files.append(p + f'/{f}')
                    elif derived:
                        files.append(p + f'/fascicles.csv')
                        # TODO manifest at higher level to further reduce file overhead ??
                        files.append(p + f'/manifest.tabular')  # put here to reduce number of files
                        _bp = p
                        for n in range(1, n_ihc_fasc + 1):
                            # TODO what about nested fascicles? do you see them?
                            # TODO naming for this is incomplete
                            p = _bp + f'/roi-{site}-sec-{sec}-fasc-{n:0>3}'
                            dirs.append(p)
                            files.append(p + f'/crop.image')
                            files.append(p + f'/human.image')
                            files.append(p + f'/machine.image')
                            files.append(p + f'/fibers.tabular')
                        p = _bp  # histology foloder issue
                    elif source:
                        pass
                    else:
                        pass

    tld = 'primary' if primary else ('derivative' if derived else ('source' if source else 'wat'))
    path_dirs = [path / tld / subject / d for d in dirs]
    path_files = [path / tld / subject / f for f in files]
    return path_dirs, path_files


def main(subject_id='sub-f001', make=True, shell=False,
         ultrasound_as_site=True, histology_as_folder=True):
    from pathlib import Path
    spec_index = {'sub-f001': subject_spec_1}
    subject_spec = spec_index[subject_id]
    #path = Path('.').expanduser()
    path = Path('~/git/sparc-curation/resources/examples/reva-11/').expanduser()
    dirs, files = [], []
    for tld in ('primary', 'derived', 'source'):
        ds, fs = make_tree(path, tld, subject_spec, ultrasound_as_site, histology_as_folder)
        dirs.extend(ds)
        files.extend(fs)

    if make:
        [d.mkdir(parents=True, exist_ok=True)for d in dirs]
        [f.touch() for f in files]
    elif shell:
        script = f'#!/usr/bin/env bash\npushd {path.as_posix()!r} || exit $?\n'
        script += '\n'.join([f'mkdir -p {d.relative_to(path).as_posix()!r}' for d in dirs])
        script += '\npopd'
        print(script)
    else:
        df = [[p.as_posix()] for p in [*dirs, *files]]
        from pprint import pprint
        pprint(df)
        return df


if __name__ == '__main__':
    out = main()
