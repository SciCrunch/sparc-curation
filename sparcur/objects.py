"""
the file system organization for this index
is designed so that the symlinks from the
main index to the latest and archive versions
of any given dataset/object combination fit
within the 59 byte limit that allows symlinks
to be stored directly in i_block on ext4
# https://www.kernel.org/doc/html/latest/filesystems/ext4/dynamic.html#symbolic-links

the current implementation use 57 bytes so
in theory we could add one more expansion
factor somewhere in the dataset path, we
can't easily go beyond the 16 ** 3 ** 2
for the current set up, but when we get
beyond reasonable limits (1e11 objects)
we will have succeded beyond our wildest
dreams and can switch to some other system
(note that we can't really switch to use
b64 for the uuids because 64 ** 3 at 200k
is too many nested dirs and we would have
to explore 16 ** 4)

this initial implementation does not include
many potential mechanisms for reducing the
amount of recomputation in the combine step
it does implement them in the extract step

e.g. we could have a file in each dataset
folder in combine that holds the latest
updated cache transitive or something
"""
import os
import sys
import json
import base64
import pprint
import pathlib
import subprocess
from uuid import UUID
from math import log as log2
import augpathlib as aug
from dateutil import parser as dateparser
from pyontutils.asyncd import Async, deferred
from sparcur import datasets as dat
from sparcur import exceptions as exc
from sparcur import pipelines as pipes
from sparcur.core import JEncode, HasErrors
from sparcur.paths import Path
from sparcur.utils import fromJson, register_type, log as _log, logd as _logd
from sparcur.utils import transitive_paths, GetTimeNow, PennsieveId as RemoteId, levenshteinDistance
from sparcur.config import auth
from sparcur.extract import xml as exml

log = _log.getChild('sob')
logd = _logd.getChild('sob')

__pathmeta_version__ = 0
__extract_version__ = 0
__combine_version__ = 0
__index_version__ = 0

sf_dir_name = 'objs'  # this really is more of an objects index ...
sf_export_base = Path(auth.get_path('export-path')) / sf_dir_name  # TODO ensure configable for testing switching versions

pathmeta_dir_name = 'pathmeta'
extract_dir_name = 'extract'
combine_dir_name = 'combine'

# this approach requires
combine_latest_dir_name = 'L'  # short to keep symlinks small enough to fit in inodes
combine_archive_dir_name = 'A'  # short to keep symlinks small enough to fit in inodes

index_dir_name = 'index'

latest_link_name = 'L'  # short to keep symlinks small enough to fit in inodes
archive_dir_name = 'A'  # short to keep symlinks small enough to fit in inodes

_expex_types = (  # aka expex_type
    None,
    'inode/directory',
    'manifest',
    'xml',

    'dataset_description',
    'submission',
    'subjects',
    'samples',
    'sites',
    'performances',
    'code_description',
    'submission',
)
_metadata_file_types = (
    'manifest', 'dataset_description', 'subjects', 'samples', 'sites',
    'performances', 'code_description', 'submission',
)

def test():
    # make sure we have the basics right
    # sourcing of the uuids will ultimately be different
    top_paths = [
        [
            extract_path(),  # FIXME these are really x_version_path ...
            combine_path(),
            index_path(),

            index_combine_latest_path(),
            index_combine_archive_path(),
            
         ]
    ]
    [print(p) for p in top_paths[0]]

    tdss = [
        'a95b4304-c457-4fa3-9bc9-cc557a220d3e',  # many xmls ~2k files so good balance, but lol ZERO of the files actually traced are provided ???

        '3bb4788f-edab-4f04-8e96-bfc87d69e4e5',  # much better, only 100ish files with xmls, XXX woo! this one has xml_index not empty!
        'ded103ed-e02d-41fd-8c3e-3ef54989da81',  # ooo, has a non-empty xml_index :D
        # also has a case where we get exracted without extracted but with errors because it has the mbf xml with no namespace

        '645267cb-0684-4317-b22f-dd431c6de323',  # small dataset with lots of manifests
        # '21cd1d1b-9434-4957-b1d2-07af75ad3546',  # big xml test sadly too many files (> 80k) to be practical for testing
        # '46bcac1d-3e45-499b-82a8-0ee1852bcc4d',  # manifest column naming issues
        '57466879-2cdd-4af2-8bd6-7d867423c709',  # has vesselucida files
        '75133e23-a572-42ee-876c-7dc127d280db',  # some segmentation

        'e4bfb720-a367-42ab-92dd-31fd7eefb82e',  # XXX big memory usage ... its going to be lxml again isn't it
        'f58c75a2-7d86-439a-8883-e9a4ee33d7fa',  # jpegs might correspond to subject id ??? ... ya subjects and samples but oof oof

        'bec4d335-9377-4863-9017-ecd01170f354',
        'd484110a-e6e3-4574-aab2-418703c978e2',  # many xmls ~200 files, but only the segmentations ... no data
    ]

    for tds in tdss:
        inner_test(tds)

def inner_test(tds, force=True, debug=True):
    log.info(f'running {tds}')
    dataset_path = (Path('~/files/sparc-datasets-test/').expanduser() / tds / 'dataset').resolve()
    cs = list(dataset_path.rchildren)
    dataset_id = dataset_path.cache_identifier

    def all_paths(c):
        return [
            extract_export_path(dataset_id, c.cache_identifier),

            combine_version_export_path(dataset_id, c.cache_identifier),
            index_combine_latest_export_path(dataset_id, c.cache_identifier),
            index_combine_archive_export_path(dataset_id, c.cache_identifier),

            index_obj_path(c.cache_identifier),
            index_obj_symlink_latest(dataset_id, c.cache_identifier),
            index_obj_symlink_archive(dataset_id, c.cache_identifier),
        ]

    aps = [all_paths(c) for c in cs]
    if False:
        [print(p) for paths in aps for p in paths]

    create_current_version_paths()

    updated_cache_transitive, object_id_types, some_failed = from_dataset_path_extract_object_metadata(dataset_path, force=force, debug=debug)
    drps, drp_index = from_dataset_id_object_id_types_combine(dataset_id, object_id_types, updated_cache_transitive, keep_in_mem=True)

    missing_from_inds = [d for d, h, ty, ta, ne, mb in drps if not h and
                         # we don't usually expect manifest entires for themselves
                         # and we don't expect them for folders though they can be
                         # used to apply metadata to folders
                         ty not in ('inode/directory', *_metadata_file_types)]
    if missing_from_inds:
        log.error('missing_from_inds:\n' + '\n'.join(sorted([d.as_posix() for d in missing_from_inds])))
    if drp_index:
        # e.g. manifest records with no corresponding path
        log.error('drp_index:\n' + '\n'.join(sorted([k.as_posix() for k in drp_index.keys()])))
        # TODO embed somewhere somehow, probably in the dataset-uuid/dataset-uuid record?
        # i.e. combine_export_path(dataset_id, datset_id)

    blobs = [d[-1] for d in drps]
    with_xml_ref = [b for b in blobs if 'external_records' in b and 'xml' in b['external_records']]
    xml_files = [b for b in blobs if pathlib.PurePath(b['path_metadata']['dataset_relative_path']).suffix == '.xml']
    xml_sources = [b for b in blobs if 'extracted' in b and 'xml' == b['extracted']['extracted']['object_type']]  # FIXME annoying to query
    apparent_targets = [x['external_records']['manifest']['input']['description'][26:] for x in xml_sources
                        if 'external_records' in x and
                        'manifest' in x['external_records'] and
                        'description' in x['external_records']['manifest']['input']]
    maybe_matches = [b for b in blobs if b['path_metadata']['basename'] in apparent_targets]

    breakpoint()


def create_current_version_paths():
    ext_path = extract_path()
    com_path = combine_path()
    idx_path = index_path()
    archive_path = index_combine_archive_path()

    if not ext_path.exists():
        ext_path.mkdir()

    if not com_path.exists():
        com_path.mkdir()

    if not idx_path.exists():
        idx_path.mkdir()

    if not archive_path.exists():
        archive_path.mkdir()

    resymlink_index_combine_latest()


def resymlink_index_combine_latest(index_version=None):
    clp = index_combine_latest_path(index_version=index_version)
    cp = combine_path()
    target = cp.relative_path_from(clp)

    up_to_date = False
    if clp.exists() or clp.is_symlink():
        current_target = clp.readlink()
        if not (up_to_date := current_target == target):
            # unsymlink old
            clp.unlink()
        else:
            msg = f'index latest symlink is already up to date {target}'
            log.log(9, msg)

    if not up_to_date:
        clp.symlink_to(target)

    # FIXME TODO issue with when to run this
    # is it as soon as a new version shows up
    # or is it after have processed all datasets
    # I think it needs to be after we finish reprocessing
    # all files in all datasets that were present in the
    # previous version otherwise there will be a bunch of 404s
    # of course this is a bit harder to implement because you
    # have to know what is done


def index_move_and_resymlink_archive(index_version=None):
    # XXX there is no reason to resymlink the archive because there
    # is only ever one archive and it will always be on the latest version of the index
    # older versions of the index will symlink there

    # TODO
    # iterate over all index versions
    # find the archive that is a directory
    # move it to the latest version
    # relink all other versions to point directly
    pass


def archive_symlink_to_combine():
    # when objects are removed we will only have and old
    # version so it will have to be moved to archive and
    # better to symlink so that we can retrieve prov if needed
    # anything not linked into latest or archive can then be culled
    cap = combine_archive_path()  # index latest should always be the real directory


def index_resymlink_archive(object_uuid):
    pass


def _dataset_path_obj_path(dataset_id, object_id):
    dataset_path = dataset_id.uuid_cache_path_string(1, 1, use_base64=True)
    obj_path = object_id.uuid_cache_path_string(1, 1, use_base64=True)
    return dataset_path, obj_path


def pathmeta_version_path(pathmeta_version=None):
    pathmeta_version = str(__pathmeta_version__ if pathmeta_version is None else pathmeta_version)
    return sf_export_base / pathmeta_dir_name / pathmeta_version


def pathmeta_version_export_path(dataset_id, object_id, pathmeta_version=None):
    base_path = pathmeta_version_path(pathmeta_version=pathmeta_version)
    dataset_path, obj_path = _dataset_path_obj_path(dataset_id, object_id)
    return base_path / dataset_path / obj_path


def extract_path(extract_version=None):
    extract_version = str(__extract_version__ if extract_version is None else extract_version)
    return sf_export_base / extract_dir_name / extract_version


def extract_export_path(dataset_id, object_id, extract_version=None):
    base_path = extract_path(extract_version=extract_version)
    dataset_path, obj_path = _dataset_path_obj_path(dataset_id, object_id)
    return base_path / dataset_path / obj_path


def combine_path(combine_version=None):
    combine_version = str(__combine_version__ if combine_version is None else combine_version)
    return sf_export_base / combine_dir_name / combine_version


def combine_version_export_path(dataset_id, object_id, combine_version=None):
    base_path = combine_path(combine_version=combine_version)
    dataset_path, obj_path = _dataset_path_obj_path(dataset_id, object_id)
    return base_path / dataset_path / obj_path


def index_combine_latest_path(index_version=None):
    base_path = index_path(index_version=index_version)
    return base_path / combine_latest_dir_name


def index_combine_archive_path(index_version=None):
    base_path = index_path(index_version=index_version)
    return base_path / combine_archive_dir_name


def index_combine_latest_export_path(dataset_id, object_id, index_version=None):
    # we keep this in the index dir to save 3 bytes of ../
    base_path = index_combine_latest_path(index_version=index_version)
    dataset_path, obj_path = _dataset_path_obj_path(dataset_id, object_id)
    return base_path / dataset_path / obj_path


def index_combine_archive_export_path(dataset_id, object_id, index_version=None):
    # we keep this in the index dir to save 3 bytes of ../
    base_path = index_combine_archive_path(index_version=index_version)
    dataset_path, obj_path = _dataset_path_obj_path(dataset_id, object_id)
    return base_path / dataset_path / obj_path


def index_path(index_version=None):
    index_version = str(__index_version__ if index_version is None else index_version)
    return sf_export_base / index_dir_name / index_version


def index_obj_path(object_id, index_version=None):
    """ index_version is included in the path so that if there is
    a convention change e.g. from 16 ** 3 ** 2 to 16 ** 4 ** 2
    the change can be handled explicitly
    """
    obj_path = object_id.uuid_cache_path_string(3, 2)  # 4096 expansion ratio but needed to shave bytes to fit in i_block, can hold 1e11 w/ ~6k
    return index_path(index_version=index_version) / obj_path


def index_obj_symlink_latest(dataset_id, object_id, index_version=None, do_link=False):
    # FIXME design with the keyword is not the greatest ... but mimnizes dupe calls
    clep = index_combine_latest_export_path(dataset_id, object_id)
    iop = index_obj_path(object_id, index_version=index_version)
    target = clep.relative_path_from(iop)
    new = False
    if do_link:
        if not iop.parent.exists():
            iop.parent.mkdir(exist_ok=True, parents=True)

        if not iop.exists() and not iop.is_symlink():
            # can fail if we didn't also symlink the combine latest
            iop.symlink_to(target)
            new = True

    return target, new


def index_obj_symlink_archive(dataset_id, object_id, index_version=None, do_link=False):
    caep = index_combine_archive_export_path(dataset_id, object_id)
    iop = index_obj_path(object_id, index_version=index_version)
    target = caep.relative_path_from(iop)
    new = False
    if do_link:
        if not iop.parent.exists():
            iop.parent.mkdir(exist_ok=True, parents=True)

        if not iop.exists() and not iop.is_symlink():
            # can fail if we didn't also symlink the combine latest
            iop.symlink_to(target)
            new = True

    return target

def extract_fun_manifest(path):
    # FIXME this need the template schema version to work ... at least in theory
    # the internals of the implementation mean that if it is missing it isn't fatal
    extracted = {
        'type': 'extracted-manifest',
        'object_type': 'manifest',  # FIXME figure out how to enforce
    }
    mf = dat.ManifestFile(path)
    try:
        data = mf.data
    except Exception as e:
        he = HasErrors(pipeline_stage='objects.manifest.extract_fun')
        if he.addError(e, path=path):
            logd.exception(e)

        he.embedErrors(extracted)
        return extracted

    if 'manifest_records' in data:
        # FIXME does contents have a defined type and is an object
        # or can it very by object_type? at the moment it certainly
        # varies by object_type ...
        extracted['contents'] = data['manifest_records']
    else:
        # contents is a required property but it
        # can be anything including an empty list
        # since this phase of the pipeline is internal
        extracted['contents'] = []

    if 'errors' in data:
        extracted['errors'] = data['errors']

    return extracted


def extract_fun_xml(path):
    # TODO yes, in theory this could write the extracted bits
    # to disk directly, but this is a workaround for a memory leak
    # not a rearchitecting of the pipeline ...
    blob = pipes.XmlFilePipeline._do_subprocess(path, raise_on_error=True)
    extracted = {
        'type': 'extracted-xml',
        'object_type': 'xml',  # FIXME figure out how to enforce
    }

    if 'contents' in blob:
        if 'errors' in blob['contents']:
            log.warning('sigh errors in contents ???')
            extracted['errors'] = blob['contents'].pop('errors')

        extracted['contents'] = blob['contents']

    if 'mimetype' in blob:
        extracted['mimetype'] = blob['mimetype']

    if 'errors' in blob:
        if 'errors' not in extracted:
            extracted['errors'] = []

        extracted['errors'] += blob['errors']
        log.error(extracted['errors'])  # FIXME these inherit
        if 'contents' not in blob or not blob['contents']:
            #breakpoint()
            #extract_fun_xml_debug(path)

            # raise here to match non-subpipeline behavior
            # TODO find a way to forward this information
            # to combine, i think we can just check for things
            # that should have some extracted content since
            # their type matches by they have no extracted file
            # that sould be sufficient to elicit a log check
            raise Exception(extracted['errors'])

    return extracted


def extract_fun_xml_debug(path):
    extracted = {
        'type': 'extracted-xml',
        'object_type': 'xml',
    }
    # XXX adapted from sparcur.pipelines.XmlFilePipeline._path_to_json_meta
    try:
        e = exml.ExtractXml(path)
        if e.mimetype:
            extracted['contents'] = e.asDict(_fail=True, raise_on_error=True)
            extracted['mimetype'] = e.mimetype
    except NotImplementedError as e:
        he = HasErrors(pipeline_stage='objects.xml.extract_fun')
        if he.addError(e, path=path):
            logd.exception(e)

        he.embedErrors(extracted)
        raise e  # always fatal now
    except Exception as e:
        log.error(f'xml extraction failure for {path!r}')
        raise e

    return extracted


def do_actually_extract(path):  # FIXME naming
    # FIXME this is a horrible way to dispatch pipelines ...

    # TODO this determines whether a path should be extracted
    # fetch_fun and extract_fun are separate so that we can
    # run the fetch funs together outside the network sandbox

    fetch_fun = None

    if path.is_dir():
        expex_type = 'inode/directory'
        extract_fun = None

    elif path.stem == 'manifest':  # FIXME do a proper match, XXX reminder that any change here _WILL_ require reextraction unless we split path_metadata only blobs from the rest, which we probably should to avoid exact this kind of issue
        expex_type = 'manifest'
        extract_fun = extract_fun_manifest
        # FIXME need to match more name patterns
        # see sc.metadata_filename_pattern

    elif path.stem in _metadata_file_types:  # FIXME overly restrictive match
        # also not clear we need to differentiate the expex type really here?
        # but also kind of why not ?
        expex_type = path.stem
        extract_fun = None

    elif path.suffix == '.xml':
        expex_type = 'xml'
        extract_fun = extract_fun_xml

    else:
        expex_type = None
        extract_fun = None

    if expex_type not in _expex_types:
        msg = f'expex_type {expex_type} not in {_expex_types}'
        raise ValueError(msg)

    return expex_type, fetch_fun, extract_fun


register_type(None, 'extract-object-metadata')  # currently unmapped
def extract(dataset_uuid, path_string, expex_type=None, extract_fun=None):
    """ actually run extraction for a single path
    see also sparcur.pipelines.XmlFilePipeline._do_xml_metadata

    we convert back from dataset_uuid to simplify passing to subprocess
    """

    if extract_fun is None or expex_type is None:  # subprocess case
        expex_type, fetch_fun, extract_fun = do_actually_extract(path)
        if extract_fun is None:
            msg = f'no extract_fun known for {path}'
            raise TypeError(msg)

    he = HasErrors(pipeline_stage='objects.extract')

    dataset_id = RemoteId(dataset_uuid, type='dataset')
    path = Path(path_string)
    object_id = path.cache_identifier
    #if extract_fun: # no longer needed because extract is only called if extract_fun is NOT None since pathmeta has moved up a level

    if path.is_file():  # catch symlink case
        blob = {  # FIXME duplicate hierarchies here, all this holds is that there was a generic extract here allow individuale extract funs to return whatever they want ... and have differnt keys ...
            '__extract_version__': __extract_version__,
            'type': 'extract-object-metadata',
            'remote_id': object_id,
        }
        # FIXME TODO ... kind of external vs internal type ...
        try:
            extracted = extract_fun(path)  # FIXME really should be extract_fun(path)
            blob['extracted'] = extracted  # FIXME still doubled ...
        except Exception as e:
            # unhandled excpetions case
            if he.addError(e, path=path, pipeline_stage='objects.extract.extract_fun'):
                logd.exception(e)

            he.embedErrors(blob)
            # we return early becase any failure here means we didn't actually extract
            return

        export_path = extract_export_path(dataset_id, object_id)
        if not export_path.parent.exists():
            export_path.parent.mkdir(exist_ok=True, parents=True)

        # FIXME TODO depending on what the workloads look like in prod
        # we can optimize to avoid writes by taking an extra read in cases
        # where we ignore the version, if nothing changed for that file
        # in particular, in those cases we will want to create a symlink
        # from the new version to the old version which will take up much
        # less space, and provide a clear record of what actually changed
        # it also means that the embedded version number and the path can
        # differ, but if they do it means that the version update did not
        # affect that file ...
        previous_version = __extract_version__ - 1
        if previous_version >= 0:
            previous_path = extract_export_path(extract_version=previous_version)
            if previous_path.exists():
                previous_blob = path_json(previous_path)
                for_comparison = json.loads(json.dumps(blob, cls=JEncode))
                # FIXME TODO ... if this winds up being more efficient
                # then do we need to embed the version at all ????
                # the key question is about the cost of storage and
                # network calls required to rerun an extract and avoiding
                # doing that every time if we know nothing has changed ...
                # so yes, I think we still need versions to minimize
                # reruns, and we might need to roll with more inodes than
                # default, but otherwise the space cost for the symlinks
                # should be effectively zero because they are preallocated
                # by the filesystem (if in the ext lineage)

                # TODO a routine that detects old version folders where
                # all files are symlinks and relinks the folder as a whole
                # so that e.g. a whole dataset could be linked if all its
                # objects were links with no files
                previous_blob.pop('__extract_version__')
                for_comparison.pop('__extract_version__')
                if previous_blob == for_comparison:
                    fp = previous_path.resolve()
                    target = fp.relative_path_from(export_path)
                    breakpoint()
                    export_path.symlink_to(target)
                    msg = f'extract object metadata matches previous so linked to {target}'
                    log.log(9, msg)
                    return
                else:
                    breakpoint()

        validate_extract(blob)
        with open(export_path, 'wt') as f:
            json.dump(blob, f, sort_keys=True, cls=JEncode)

        msg = f'extract object metadata written to {export_path}'
        log.log(9, msg)

    else:
        # XXX FIXME TODO atm we only process files that
        # have been fetched because setting up the iterative
        # extract, fetch, extract, fetch process is too much
        msg = f'not extracting because not fetched {path.name}'
        log.warning(msg)


def validate_extract(blob):
    bads = []
    def ce(msg): raise ValueError(msg)  # LOL PYTHON can't raise in lambda ...
    for f in (
            (lambda : blob['type']),
            (lambda : blob['__extract_version__']),
            (lambda : blob['remote_id']),
            (lambda : blob['extracted']['type']),
            (lambda : blob['extracted']['object_type']),
            (lambda : blob['extracted']['contents']),
            (lambda : True if list(blob['extracted']['contents']) != ['errors'] else ce('ceo')),
    ):
        try:
            f()
        except Exception as e:
            bads.append(e)

    if bads:
        [log.exception(b) for b in bads]
        # this is an internal error because we control the format of everything we get here
        raise Exception(f'validate failed for\n{json.dumps(blob, sort_keys=True, indent=2, cls=JEncode)}\n')


def argv_extract(dataset_uuid, path, force=False):
    return sys.executable, '-c', (
        'from sparcur.objects import extract;'  # FIXME TODO move to another file to avoid import overhead
        f'extract({dataset_uuid!r}, {path.as_posix()!r}, force={force})')


path_log_base = auth.get_path('log-path')
path_log_objects = path_log_base / 'objs'


def object_logdir(object_id):
    return path_log_objects / object_id.uuid_cache_path_string(2, 3)


def pathmeta_ir(blob):
    keyf = {
        'dataset_id': RemoteId,
        'dataset_relative_path': Path,
        'parent_id': RemoteId,
        'size_bytes': aug.FileSize,
        'timestamp_updated': dateparser.parse,
    }
    nb = {k: (keyf[k](v) if k in keyf else v) for k, v in blob.items()}

    rid = 'remote_id'
    rii = 'remote_inode_id'
    nbrid = nb[rid]
    if rii in nb:
        nb[rid] = RemoteId(nbrid, file_id=nb[rii])
    else:
        nb[rid] = RemoteId(nbrid)

    return nb


class _pathmeta_ir():
    fromJson = staticmethod(pathmeta_ir)


register_type(_pathmeta_ir, 'pathmeta')


def pathmeta_refresh(dataset_id, object_id, path, force=False):

    blob = path._cache_jsonMetadata()
    blob['type'] = 'pathmeta'  # you can't actually invert everything back to Path because Path is only the name
    blob['__pathmeta_version__'] = __pathmeta_version__

    # XXX note that the inclusion of dataset_relative_path means that
    # any moves or renames that happen up the hierarchy will cause
    # pathmeta to change this is probably ok since it avoids the many
    # network calls that would otherwise be required to reconstruct
    # the drp

    export_path = pathmeta_version_export_path(dataset_id, object_id)
    changed = True  # default true so nothing -> something is changed
    if export_path.exists():
        blob_existing = path_json(export_path)
        ir_existing = fromJson(blob_existing)
        # FIXME ir vs raw json, easier to parse the raw I think?
        changed = blob != ir_existing
        #changed_keys = set(blob) != set(ir_existing)
        #if not changed_keys:
        #    changed_values = False
        #    for k, ev in ir_existing.items():
        #        if (nv := blob[k]) != ev:
        #            changed_values = True
        #            breakpoint()

        #    changed = changed_values

    if changed or force:
        if not export_path.parent.exists():
            export_path.parent.mkdir(exist_ok=True, parents=True)

        with open(export_path, 'wt') as f:
            json.dump(blob, f, sort_keys=True, cls=JEncode)

    return blob, changed


def subprocess_extract(dataset_id, path, time_now, force=False, debug=False, subprocess=False):
    # FIXME TODO probably wire this into sparcron more directly
    # to avoid calling this via Async deferred
    object_id = path.cache_identifier #RemoteId(path.cache_id)
    if path.is_dir():
        _updated = path.getxattr('bf.updated').decode()
        updated = aug.PathMeta(updated=_updated).updated
    else:
        updated = path.updated_cache_transitive()  # something of a hack


    pathmeta_blob, pathmeta_changed = pathmeta_refresh(dataset_id, object_id, path)  # FIXME TODO in point of fact this can run in parallel with extract because there is not any necessary interaction until combine

    export_path = extract_export_path(dataset_id, object_id)
    done = export_path.exists()  # and not pathmeta_changed  # FIXME we don't need to rerun the export if the pathmeta changed ... the id does not change
    # FIXME this is obviously wrong, e.g. what happens if the remote changes the file name or moves a file? we need a bit more info related update times or reparents, for now we will have to run time force=True to avoid stale, or we run with force=True when it is publication time, OR we don't store pathmeta at all and always rederive it during extract ... that seems better, yes ...
    expex_type, fetch_fun, extract_fun = do_actually_extract(path)
    # FIXME hard to pass extract_fun directly so would have to
    # rederive in subprocess version not that I think we will be using
    # that variant

    if done and not force:
        success = 'already-done'
        msg = f'{object_id} already done {path}'
        log.log(9, msg)
        return path, object_id, expex_type, success, updated

    elif extract_fun is None:
        success = 'no-extract-fun-and-pathmeta-already-written'
        msg = f'{object_id} pathmeta already written and no extract_fun {path}'
        log.log(9, msg)
        return path, object_id, expex_type, success, updated

    elif debug or not subprocess:
        # TODO or raw expex_type in IMPORTANT i.e. stuff that should be in memory ... but that is and optimization
        try:
            extract(dataset_id.uuid, path.as_posix(), expex_type, extract_fun=extract_fun)
            success = True
        except Exception as e:
            success = False
            log.exception(e)

        return path, object_id, expex_type, success, updated

    msg = "don't do this right now"
    # likely not needed
    # has resource issues, creates too many log files, etc.
    # the log issue can sort of be dealt with, but not easily
    # from inside python if you want all processes to write to
    # the same log file, have to log to socket or something
    raise NotImplementedError(msg)
    # XXX start the subprocess variant which we likely do not want
    # since it is usually the wrong cut point
    timestamp = time_now.START_TIMESTAMP_LOCAL_FRIENDLY
    logdir = object_logdir(object_id)
    logfile = logdir / timestamp / 'stdout.log'
    latest = logdir / 'LATEST'
    if not logfile.parent.exists():
        logfile.parent.mkdir(parents=True)

    if latest.exists():
        latest.unlink()

    latest.symlink_to(timestamp)

    argv = argv_extract(dataset_id.uuid, path)
    try:
        with open(logfile, 'wt') as logfd:
            try:
                p = subprocess.Popen(argv, stderr=subprocess.STDOUT, stdout=logfd)
                out = p.communicate()
                if p.returncode != 0:
                    raise exc.SubprocessException(f'oops objs return code was {p.returncode}')

                success = True
            except KeyboardInterrupt as e:
                p.send_signal(signal.SIGINT)
                raise e

        # FIXME the boundary for this needs to happend only when there is an actual
        # extract going on, not if we are just dumping path meta, and i think we aren't
        # going to subprocess this at all, so the logging will be in the parent dataset export log

        log.info(f'DONE: {object_id}')
    except exc.SubprocessException as e:
        log.critical(f'FAIL: {dataset_id} {object_id} | {dataset_id.base64uuid()} {object_id.base64uuid()}')
        log.exception(e)
        success = False

    return path, object_id, expex_type, success, updated


def from_dataset_path_get_path_to_fetch_and_extract():
    """ this is how the second fetch phase actually happens
    """


def from_paths_extract_object_metadata(paths, time_now, force=False, debug=False):
    """ we can't do the whole dataset at once because we need incremental information
    that we do not have until we do a first round of extraction, i.e. the manifests

    also look into using this process process all the metadata files in an iterative
    manner so that dataset description comes in first, then manifest etc, basic
    extraction is possible, but in some cases we can't actually process the manifest
    to determine which additional files to retrieve until we have processed the
    dataset description, ugh ...
    """

    # FIXME TODO, right now this makes way too many hardcore assumptions


def from_dataset_path_extract_object_metadata(dataset_path, time_now=None, force=False, debug=False, _Async=Async, _deferred=deferred):
    """ given a dataset_path extract path-metadata and extract-object-metadata

    this is a braindead implementation that ignores the existing way
    in which path-metadata.json populated and is a clean sheet impl

    that can be check for consistency with the current path-metadata.json
    process and then we can switch over one way or the other
    for reference: sparcur.pipelines.PathTransitiveMetadataPipeline
    and sparcur.paths.PathHelper._transitive_metadata
    """
    if time_now is None:
        time_now = GetTimeNow()

    dataset_id = dataset_path.cache_identifier
    #rfiles = transitive_files(dataset_path)  # XXX sadly can't use this becaues we have to review the symlinks
    # but in a sense that is ok because we also want to pull the path metadata here as well since it will end up
    # being more efficient to start from here
    rchildren = transitive_paths(dataset_path)

    # TODO need to sort out the right way to do this when running in sparcron ... especially wrt when the step is actually "done"
    # how do we handle the dataset itself (it would be nice to just put the metadata there ...)
    # also, how do we deal with organizations, there is nothing to extract so to speak ... maybe they are dealt with at the
    # combine level ... yes, I think we are ok to skip dataset and org at this point and deal with them at combine

    if debug:
        # breakpoint and Async still not cooperating
        def deferred(f): return f
        def Async(**kwargs):
            def i(gen):
                return list(gen)
            return i
    else:
        # LOL PYTHON what is even the point
        Async = _Async
        deferred = _deferred

    results = Async(rate=False)( # we need Async and deferred to prevent processing
        # of hundres/thousands of xmls files in a subprocess from taking an eternity
        # unfortunately this means that each spc export will use a full worker count
        # assuming that it is the only process on the system, which is already handled
        # by celery ... so yeah FIXME TODO sort out balacing how to run this becase
        # parallel execution requires coordinate to avoid resource exhaustion, pulling
        # it out of the spc export path is likely going to be part of the solution
        # since that was also mostly just to get a quick and dirty entrypoint
        deferred(subprocess_extract)
        (dataset_id, path, time_now, force=force, debug=debug)
        for path in rchildren)

    updated_cache_transitive = None
    bads = []
    object_id_types = []
    for path, id, type, ok, updated in results:
        if updated_cache_transitive is None:
            updated_cache_transitive = updated
        elif updated > updated_cache_transitive:
            updated_cache_transitive = updated

        if not ok:
            # pretend like the file doesn't exist
            # or at least doesn't have any relevant content
            # e.g. we don't not run export because there is no manifest ...
            # but do update cache transitive because that expects to be
            # calculated over all paths regardless
            bads.append((path, id))
            continue

        if updated_cache_transitive is None:
            updated_cache_transitive = updated
        elif updated > updated_cache_transitive:
            updated_cache_transitive = updated

        object_id_types.append((id, type))

    if bads:
        # {"" if debug else object_logdir(id) / "LATEST" / "stdout.log"}
        fails = '\n'.join(f'{id} {path}' for path, id in bads)
        msg = (f'{dataset_id} {len(bads)} object meta fails:\n{fails}\n'
               'those files will be treated as if they do not exist so '
               'proceed with caution')
        log.warning(msg)
        # should we try to continue anyway and run combine knowing that
        # we may have to rerun when a file is fixed? do we just pretend like it
        # never existed in the first place? I think it is safe to continue here
        # but callers should catch ... except for that thing about return values
        # and partial failures :/ so instead we return bads and the caller
        # has to check

    return updated_cache_transitive, object_id_types, bads


# these variants imply a certain amount of rework
def from_dataset_id_combine(dataset_id): pass
def from_dataset_path_combine(dataset_id): pass
def from_dataset_id_object_ids_combine(dataset_id): pass


def path_json(path):
    with open(path, 'rt') as f:
        return json.load(f)


def pex_manifests(dataset_id, oids, name_drp_index=None, **inds):
    # name_drp_index used at this point to look for similar files (but only those not procssed by a type that depends on manifest info)

    # manifest data
    # construct the individual manifest paths
    # merge into a single global manifest

    def add_drp(object_id, drp, parent, rec):
        # XXX ah yes, that's why I wanted to implement my own PurePath variant ...
        # who the heck leaves simplify/normpath out !?!?!
        if 'filename' in rec:
            target_drp = parent.__class__(os.path.normpath(parent / rec['filename']))
            out = {
                'type': 'manifest_record',  # a single manifest record for a file
                'prov': {
                    'source_id': object_id,
                    'source_drp': drp,
                },
                'dataset_relative_path': target_drp,  # make it easy to check consistency without fancy steps
                # FIXME if we want a singlular name don't use 'input' because it will confuse with dataset export 'inputs'
                'input': rec,  # FIXME vs inputs: [rec] to support multiple inputs technically allowed ... no, we need another way to handle that
            }
            return target_drp, out
        else:
            # might be an improperly filled out pattern manifest
            # the blob itself should already have an error though?
            # but TODO we might want to embed again to be sure?
            msg = ('manifest record is missing "filename" column !??!\n'
                   f'{drp} {rec}')
            log.warning(msg)
            return None, None

    # we don't use .utils.fromJson here, I don't think we need it? or at least don't need it yet?
    id_drp = {}
    drp_manifest_record_index = {}
    for object_id in oids:
        extract_path = extract_export_path(dataset_id, object_id)
        if not extract_path.exists():
            # TODO logging ?
            continue

        blob = path_json(extract_path)  # FIXME might not exist
        if 'extracted' not in blob:  # FIXME this should never happen now ...
            breakpoint()
            continue

        extracted = blob['extracted']

        if 'contents' not in extracted:
            continue  # TODO logging

        contents = extracted['contents']

        pathmeta_blob = path_json(pathmeta_version_export_path(dataset_id, object_id))
        drp = pathlib.PurePath(pathmeta_blob['dataset_relative_path'])
        #ir = fromJson(blob)  # indeed we can't load type: path
        #id_drp[object_id] = drp
        #manifests[drp] = blob  # FIXME memory issues
        parent = drp.parent  # FIXME top level

        for c in contents:
            target_drp, rec = add_drp(object_id, drp, parent, c)
            if target_drp is None:
                # log.error('a malformed manifest has made it through to this stage')
                # we logged above so just pass here or TODO embed an error in the
                # manifest blob itself to be written during manifest combine?
                pass
            elif target_drp in drp_manifest_record_index:
                msg = 'NotImplementedError TODO merge multiple to same file'
                log.info(msg)
                msg = (
                    f'multiple manifest records for {dataset_id}:{target_drp} '
                    f'from {drp} and {drp_manifest_record_index[target_drp]["prov"]["source_drp"]}')
                log.error(msg)
                # FIXME TODO embed error in rec
                breakpoint()
                continue
            else:
                drp_manifest_record_index[target_drp] = rec

    return id_drp, drp_manifest_record_index


def add_to_ind(dataset_id, drp, ind, key, value):
    if key not in ind:  # looks like we can't avoid multiple refs ...
        ind[key] = []

    ind[key].append(value)

    return 

    if key in ind:
        asdf = a, f = drp, ind[key]["prov"]["source_drp"]
        breakpoint()
        msg = 'NotImplementedError TODO multiple seg files per images is possible'
        log.info(msg)
        msg = (
            f'multiple xml refs for {dataset_id}:{key} '  # lol
            f'from {drp} and {ind[key]["prov"]["source_drp"]}')
        log.error(msg)
    else:
        ind[key] = value


def pex_xmls(dataset_id, oids, drp_index=None, name_drps_index=None, **inds):
    id_drp = {}
    unmapped = {}
    drp_xml_ref_index = {}

    for object_id in oids:

        extract_path = extract_export_path(dataset_id, object_id)

        if not extract_path.exists():
            continue

        pathmeta_blob = path_json(pathmeta_version_export_path(dataset_id, object_id))
        blob = path_json(extract_path)
        drp = pathlib.PurePath(pathmeta_blob['dataset_relative_path'])

        #xmls[drp] = blob  # FIXME memory usage issues here
        #id_drp[object_id] = drp

        if 'extracted' not in blob:
            breakpoint()
            # we're expecting embedded errors or something?
            msg = ('malformed data that should never have been writen'
                   f'{dataset_id} {object_id} {extract_path}\n{blob}')
            raise ValueError(msg)

        extracted = blob['extracted']  # FIXME could be missing ... ??
        mimetype = extracted['mimetype'] if 'mimetype' in extracted else None
        if 'contents' not in extracted:
            # FIXME TODO logging
            continue

        not_this_time = [False]
        if drp in drp_index:
            # we have a manifest record
            internal_references = drp_index[drp]
        else:
            internal_references = {}

        contents = extracted['contents']  # FIXME could be missing ...
        if mimetype == 'application/x.vnd.mbfbioscience.metadata+xml':
            if 'images' not in contents:
                # FIXME TODO logging and embedding errors in combined for review
                continue

            suffixes = set()
            mbf_path_strings = [p for i in contents['images'] if 'path_mbf' in i for p in i['path_mbf']]
            for target_mbf_path_string in mbf_path_strings:
                rec = {
                    'type': 'mbf_xml_path_ref',
                    'prov': {
                        'source_id': object_id,
                        'source_drp': drp,
                    },
                    #'dataset_relative_path': 'LOL',  # yeah ... no way this is going to work
                    'input': extracted,  # FIXME TODO don't embed the whole file, figure out what to extract or just leave it as a pointer
                }

                target_mbf_path = pathlib.PurePath(target_mbf_path_string)
                target_drp = None
                if target_mbf_path.name in name_drps_index:
                    candidates = name_drps_index[target_mbf_path.name]
                    if len(candidates) == 1:
                        rec['prov']['mapped_by'] = 'exact-pathname-match'
                        target_drp, = candidates
                    else:
                        log.log(9, f'multiple candidates for mbf path in {drp} {target_mbf_path} {candidates}')
                        breakpoint()
                else:

                    def numonly(name):
                        nums = [int(_) for _ in ''.join(c if c.isdigit() else ' ' for c in name).split()] # FIXME or c == '.' ?
                        nn = ' '.join(str(_) for _ in nums)
                        return nn, nums, set(nums)

                    def entcomp(nums1, nums2):
                        both = nums1 & nums2
                        return sum(log2(n) for n in both)

                    suffix = target_mbf_path.suffix
                    suffixes.add(suffix)
                    lev_subset = [(n, *numonly(n)) for n in name_drps_index if n.endswith(suffix)]
                    if lev_subset:
                        jn = [n[0] for n in lev_subset]
                        tmpname = target_mbf_path.name
                        tmp_no, tmp_nums, tmp_sn = numonly(tmpname)
                        tmp_self_comp = entcomp(tmp_sn, tmp_sn)
                        #[(n, no) for n, no in lev_subset if all([num in no.split() for num in tmp_no.split()])]
                        #hrm = sorted([(entcomp(tmp_nums, nums), n, nums) for n, no, nums in lev_subset], reverse=True)
                        log.log(9, f'tmp_self_comp {tmp_self_comp}')
                        levs = sorted((
                            1 - ((
                                #comp :=
                                entcomp(tmp_sn, snums)) / tmp_self_comp),
                            levenshteinDistance(tmpname, n),
                            #comp,
                            n, no) for n, no, nums, snums in lev_subset)

                        rep = [(0.0,
                                0.0,
                                #tmp_self_comp,
                                tmpname,
                                tmp_no,
                                )] + levs
                        rep[:10]
                        log.log(9, '\n' + pprint.pformat(
                            rep[:5],
                            #[(f'{a:1.2f}', f'{b:1.2f}', *r) for a, b, *r in rep[:5]],
                            width=120))
                    else:
                        # sometimes maybe they get the suffix wrong?
                        msg = ('somehow there are no files that even match the '
                               'extension of an mbf path !?!??! maybe your princess is in another dataset ??? '
                               f'{dataset_id} {object_id} {drp} {target_mbf_path}')
                        log.log(9, msg)

                if target_drp is None:
                    add_to_ind(dataset_id, drp, unmapped, target_mbf_path_string, rec)
                else:
                    add_to_ind(dataset_id, drp, drp_xml_ref_index, target_drp, rec)

            # end mbf path strings loop

            if internal_references:
                # TODO manifest mapping, FIXME do we do this here or do we do it when processing manifests?
                # here we have more information about the type of xml and that we might be expecting something
                # in the manifest as a workaround for incomplete metadata (that should be corrected)
                irs = internal_references
                if 'manifest' in irs and 'input' in irs['manifest']:
                    input = irs['manifest']['input']
                    if len(input) > 2:  # not just filename and file type
                        log.log(9, input.keys())
                        if 'description' in input:
                            for suffix in suffixes:
                                if suffix in input['description']:
                                    breakpoint()
                                    break
                        elif not_this_time[0]:
                            pass
                        else:
                            #breakpoint()
                            pass

    return id_drp, drp_xml_ref_index, unmapped  # the keys here are going to have to be matched or something


def pex_dirs(dataset_id, oids, **inds):
    # TODO build the parent lookup index to replace parent_drp
    # i think what we want to do is embed all the parents up to
    # the one just before primary/derivative etc. since we have
    # the sub- and sam- prefixes that can make it a single
    # lookup to pull full subject metadata, because the id
    # will literally be in the parents ... that turns out to
    # be quite useful
    id_drp = {}
    did_index = {}  # FIXME unused
    parent_index = {}
    for object_id in oids:
        blob = path_json(pathmeta_version_export_path(dataset_id, object_id))
        drp = pathlib.PurePath(blob['dataset_relative_path'])
        #dirs[drp] = blob  # FIXME check on memory sizes here
        #id_drp[object_id] = drp
        did_index[drp] = object_id
        parent_index[object_id] = RemoteId(blob['parent_id'])

    return id_drp, did_index, parent_index


def pex_default(dataset_id, oids, did_index=None, **inds):
    # at this stage the other inds are not populated so sadly
    # we have to read from disk twice, in service of reduced memory usage
    id_drp = {}
    for object_id in oids:
        blob = path_json(pathmeta_version_export_path(dataset_id, object_id))
        drp = pathlib.PurePath(blob['dataset_relative_path'])
        #other[drp] = blob  # FIXME check on memory sizes here
        id_drp[object_id] = drp

    return id_drp, None


def combine(dataset_id, object_id, type, drp_index, parent_index):
    blob = {}
    blob['type'] = 'combine-object-metadata'  #'combined-metadata'
    blob['__combine_version__'] = __combine_version__

    pathmeta_blob = path_json(pathmeta_version_export_path(dataset_id, object_id))
    blob['path_metadata'] = pathmeta_blob

    # embed the parent ids so that they can be used without
    # having to deconstruct dataset_relative_path and reconstruct
    # other indexes from scratch, this does mean that changes to
    # the file structure are reflected here, but that is expected
    # at this phase (we manage to avoid it for extract)
    _pids = [RemoteId(pathmeta_blob['parent_id'])]
    if parent_index is not None:  # can happen if there are only files in a dataset at the top level
        while _pids[-1] in parent_index:
            _pm1 = _pids[-1]
            _pid = parent_index[_pm1]
            _pids.append(_pid)
            if _pid == _pm1:
                break

    # parent_ids are indexed so that the the first
    # parent id is the immediate parent and the
    # last parent id is the highest in the hierachy
    blob['parent_ids'] = _pids

    extract_path = extract_export_path(dataset_id, object_id)
    if extract_path.exists():
        extract_blob = path_json(extract_path)
        blob['extracted'] = extract_blob  # FIXME duped keys

    drp = pathlib.PurePath(pathmeta_blob['dataset_relative_path'])
    if (has_recs := drp in drp_index):
        blob['external_records'] = drp_index.pop(drp)
    elif 'mimetype' in pathmeta_blob and pathmeta_blob['mimetype'] == 'inode/directory':
        pass  # we don't expect dirs to have metadata (though they can)
    elif type in _metadata_file_types:
        pass  # don't expect metadata for metadata files ... usually
    else:
        # manifests technically aren't required as of 2.1.0
        msg = f'{dataset_id} {object_id} {drp} has no records of any kind'
        log.warning(msg)  # TODO embed error

    combine_path = combine_version_export_path(dataset_id, object_id)
    if not combine_path.parent.exists():
        combine_path.parent.mkdir(exist_ok=True, parents=True)

    with open(combine_path, 'wt') as f:
        # JEncode minimally needed to deal with Path :/
        json.dump(blob, f, sort_keys=True, cls=JEncode)

    msg = f'combine object metadata written to {combine_path}'
    log.log(9, msg)

    return drp, has_recs, blob


pex_funs = {
    None: pex_default,
    'inode/directory': pex_dirs,
    'manifest': pex_manifests,
    'xml': pex_xmls,
    'dataset_description': lambda did, oids, **inds: (log.debug('TODO'), None),
    'samples': lambda did, oids, **inds: (log.debug('TODO'), None),
    'subjects': lambda did, oids, **inds: (log.debug('TODO'), None),
    'submission': lambda did, oids, **inds: (log.debug('TODO'), None),
    'sites': lambda did, oids, **inds: (log.debug('TODO'), None),
    'performances': lambda did, oids, **inds: (log.debug('TODO'), None),
    'code_description': lambda did, oids, **inds: (log.debug('TODO'), None),
    'submission': lambda did, oids, **inds: (log.debug('TODO'), None),
}

if (__spf := set(pex_funs)) != (__sxt := set(_expex_types)):
    raise ValueError(f'pex_funs != _expex_types {__spf} != {__sxt}')


def from_dataset_id_object_id_types_combine(dataset_id, object_id_types, updated_cache_transitive=None, keep_in_mem=False):
    types = {}
    for oid, type in object_id_types:
        # FIXME why do oids for packages have a file_id associated when we go to put them in source_id ???
        # we have to load all in the first past to populate the info we need

        if type not in types:
            types[type] = []

        types[type].append(oid)

    # we _have_ to load this first for everything, we can optimize out fs reads in the future as needed
    # use for trying to find potential matches from fuzzy data in manifests and xmls
    # XXX might not actually need this because I was being a dumb, but it does enable
    # intra-type references to be found and resolved immediately ...
    id_drp_all, _ = pex_default(dataset_id, [o for os in types.values() for o in os])
    name_drps_index = {}
    for _, drp in id_drp_all.items():
        # TODO see if we need prefix_drps_index using e.g.
        # [Path(*p.parts[n:]).as_posix() for n in range(len(p.parts))]
        if drp.name not in name_drps_index:
            name_drps_index[drp.name] = []

        name_drps_index[drp.name].append(drp)

    # FIXME TODO pathmeta_changed is useful for determining whether we need to rerun combine?
    # sort of no, because the reason why we blindly rerun combine right now is becase of the long range effects changes in manifests,
    # TODO to do the full changes properly we would have to know
    # three things
    # 1. whether pathmeta changed (drp change here captures any reparenting up the hierarchy)
    # 2. whether any manifests changed
    # 3. whether manifests that did change affected specific drps
    # I think we can short circuit this by dumping the joint manifest
    # index and the comparing to the previous version and that will
    # be sufficient, or rather dumping the full drp_index and comparing
    # but we can do better than that because for non-manifest stuff
    # we realy only care about additions and deletions, and maybe moves

    did_index = None
    parent_index = None
    xml_index = None
    xml_unmapped = None
    drp_index = {}
    process_order = 'inode/directory', None, 'manifest', 'xml'  # references within a type have to be resolved in a second pass in that pex
    # first pass
    for type in process_order:
        if type not in types:
            continue

        oids = types[type]
        process_extracted = pex_funs[type]
        inds = dict(
            #did_index=did_index,
            parent_index=parent_index,  # parent_index is only over dirs since files already have parent_id
            name_drps_index=name_drps_index,
            drp_index=drp_index,
            xml_index=xml_index,
        )
        id_drp, index, *rest = process_extracted(dataset_id, oids, **inds)
        if index is not None:
            if type == 'inode/directory':
                did_index = index
                parent_index, = rest
                continue
            elif type == 'xml':
                xml_index = index  # TODO figure out how we can get to the point where this can be included in drp index
                xml_unmapped, = rest  # FIXME wat ???
                if xml_unmapped:
                    xi = len(xml_index)
                    xu = len(xml_unmapped)
                    msg = f'mbf xml path mapping for :dataset {dataset_id} :total {xi + xu} :ok {xi} :unmapped {xu}'
                    log.info(msg)

                if not xml_index:
                    continue

            for drp, rec in index.items():
                if drp not in drp_index:
                    drp_index[drp] = {'type': 'internal_references'}  # FIXME what is the name for this type ...

                if type in drp_index[drp]:
                    breakpoint()
                    msg = ('TODO probably shouldn\'t happen, '
                           'should have been handled during e.g. pex_manifests')
                    raise NotImplementedError(msg)

                drp_index[drp][type] = rec

    # TODO processing of dataset and organization level ids
    drps = []
    news = 0
    for object_id, type in object_id_types:  # TODO sort these in reverse order so that errors can be added to e.g. manifests
        drp, has_recs, blob = combine(dataset_id, object_id, type, drp_index, parent_index)
        target, new_link = index_obj_symlink_latest(dataset_id, object_id, do_link=True)
        if new_link:
            news += 1
        drps.append((drp, has_recs, type, target, new_link, (blob if keep_in_mem else None)))

    log.info(f'finished combining and writing {len(drps)} and linking {news} object records for dataset {dataset_id}')
    # TODO flag the objects that were deleted since the previous run and probably stick an xattr on them to
    # make cleanup as simple as possible
    return drps, drp_index


def single_file_extract(
        dataset_id=None,
        dataset_path=None,
        path_metadata_path=None,
        path_metadata_blob=None,
):
    # needs
    # dataset
    # dataset_path
    # cloned dataset
    # cloned dataset matches
    # path-metadata
    # path-metadata updated-cache-transitive matches dataset_path updated-cache-transitive

    # OOOO! we can just use the index files? no, super slow unfortunately
    # but the logic there is sound for updated_cache_transitive
    # single-file/extract/{duuid}/packages/{extract-version}/{puuid[0:2]}/{puuid}
    # the file id will be inside, if there are multiple files in a package we skip for now
    # single-file/c/{duuid}/packages/{combine-version}/{puuid[0:2]}{puuid}

    # XXX {updated-cache-transitive}/ doesn't fit in here, we technically already
    # have that information in the path-metadata file and it will be embedded at
    # the top so that we have it for reference, and if we really need it we can
    # recover which paths were present at a given updated cache transitive

    # get list of already extracted files at the current extract version
    # identify all files that could be extracted
    # generate list of files that need to be extracted

    # parallelization happens in here as we do with the xml extraction right now
    pass


def single_file_combine(
        dataset_id=None,
        path_metadata_path=None,
        path_metadata_blob=None,
        updated_cache_transitive=None,  # if known in advance can be used to triple check
        path_single_file_extract=None,
):

    # TODO outward reference index in addition to just extracted I think? or do we do it in memory?

    # TODO do we check whether the data contents have changed for a specific referenced file
    # or just rerun everything? I think that is an optimization, but a single manifest change
    # will hit every file unless we diff against the previously extracted info, so I think we
    # probably keep 1 existing file

    # if we land below 60bytes for the symlink length it will be stored in the inode
    # FIXME we're missing a level of indirection because we don't rerun every time
    # symlink single-file/c/{duuid}/{updated-cache-transitive}/packages/{combine-version}/{puuid[0:2]}/{puuid}


    # if there is only an old version this means that it was removed from the latest version of the dataset
    # some time since the version bump, what we can do is provide an ARCHIVE endpoint where removed packages
    # can be found at their last version
    # single-file/c/{duuid}/packages/A/{puuid[0:2]}/{puuid}
    # is populated by comparing /L/ to /L-1/ and packages that are not in L get moved to /A/
    # so the meta version needs to be embedded for archived blobs
    # and updated cache transitive is maintained in the blob OR we could use updated extracted latest to target only files
    # that might affect other files, or better updated extracted making references latest ... that is more complicated
    # but in a sense more accurate for most files only the manifest and their own modified date will matter

    # single-file/c/{duuid}/packages/{combine-version}/{puuid[0:2]}/{puuid}

    # single-file/index/{puuid[0:2]}/{puuid[2:4]}/{puuid[4:6]}/{puuid}
    # ../../../c/{duuid}/L/packages/L/{puuid[0:2]}/{puuid}
    pass


def main():
    test()


if __name__ == '__main__':
    main()
