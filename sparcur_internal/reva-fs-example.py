# number of segments per laterality
# this is expected to vary per subject
lat_seg = {
    'l': 4,
    'r': 3,
}
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
}
# mapping of slide number to protocol type
slide_type = {
    1: 'he',
    2: 'ihc-chat',
    3: 'ihc-th',
    4: 'ihc-nav1',
    5: 'em',
    6: 'ihc-chat',
    7: 'ihc-th',
    8: 'ihc-nav1',
}

# the expected data files per type
type_example_files = {
    'he': ['20x.tiff', '100x.tiff'],
    'em': ['big-em-image.tiff',],
    'ihc': ['20x.tiff', '100x.tiff'],
}

# conventions for padding identifiers can be discussed, right now they are unpadded
def make_tree(path, pdo, subject, lat_seg, lat_seg_site):
    primary = pdo == 'primary'
    derived = pdo == 'derived'
    other = not primary and not derived
    dirs = []
    files = []
    for lat, nseg in lat_seg.items():
        p = f'sam-v{lat}/ultrasound'  # TODO we may need site-vl and site-vr for this
        dirs.append(p)
        if primary:
            f = p + '/image.image'
            files.append(f)
        elif derived:
            pass  # content unknown at this time
        else:
            pass
        for seg in range(1, nseg + 1):
            p = f'sam-v{lat}/sam-v{lat}-seg-{seg}/microct'
            dirs.append(p)
            if primary:
                f = p + '/image.image'
                files.append(f)
            elif derived:
                pass  # content unknown at this time
            else:
                pass
            for site in lat_seg_site[lat][seg]:
                for slide in range(1, 9):
                    p = f'sam-v{lat}/sam-v{lat}-seg-{seg}/sam-v{lat}-{site}-s-{slide}'
                    dirs.append(p)
                    type = slide_type[slide].split('-')[0]
                    if primary:
                        for f in type_example_files[type]:
                            files.append(p + f'/{f}')
                    elif derived:
                        files.append(p + f'/manifest.xlsx')  # put here to reduce number of files
                        # TODO manifest at higher level to further reduce file overhead ??
                        _bp = p
                        for n in range(1,10):  # 10 is almost certainly too small
                            # TODO what about nested fascicles? do you see them?
                            p = _bp + f'/roi-fasc-{n:0>4}'
                            dirs.append(p)
                            files.append(p + f'/crop.image')
                            files.append(p + f'/human.image')
                            files.append(p + f'/machine.image')
                            files.append(p + f'/fibers.tabular')
                    else:
                        pass

    tld = 'primary' if primary else ('derivative' if derived else 'wat')
    path_dirs = [path / tld / subject / d for d in dirs]
    path_files = [path / tld / subject / f for f in files]
    return path_dirs, path_files


def main(subject='sub-f001'):
    from pathlib import Path

    #path = Path('.').expanduser()
    path = Path('~/git/sparc-curation/resources/examples/reva-4/').expanduser()
    dirs, files = [], []
    for tld in ('primary', 'derived'):
        ds, fs = make_tree(path, tld, subject, lat_seg, lat_seg_site)
        dirs.extend(ds)
        files.extend(fs)

    make = False
    if make:
        [d.mkdir(parents=True, exist_ok=True)for d in dirs]
        [f.touch() for f in files]

    return [[p.as_posix()] for p in [*dirs, *files]]

if __name__ == '__main__':
    out = main()
    #return out
