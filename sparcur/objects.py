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
import lzma
import base64
import pprint
import pathlib
import subprocess
from uuid import UUID
from math import log as log2
from datetime import datetime
from itertools import chain
from collections import defaultdict
import orthauth as oa
import augpathlib as aug
from dateutil import parser as dateparser
from pyontutils.asyncd import Async, deferred
from pyontutils.utils_fast import timeformat_friendly
from sparcur import datasets as dat
from sparcur import exceptions as exc
from sparcur import pipelines as pipes
from sparcur.core import HasErrors, JEncode, JApplyRecursive, get_nested_by_key
from sparcur.paths import Path
from sparcur.utils import fromJson, register_type, log as _log, logd as _logd
from sparcur.utils import transitive_paths, GetTimeNow, PennsieveId as RemoteId, levenshteinDistance
from sparcur.config import auth
from sparcur.extract import xml as exml

log = _log.getChild('sob')
logd = _logd.getChild('sob')

# TODO HAH, with the addition of errors aka failed we can reuse the
# sparcron run logic for whole datasets on individual objects now ...
__objmeta_version__ = 0
__fsmeta_version__ = 0  # fsmeta is a bit different and will have version embedded because fsmeta is the part that can change and is like a wal, but on a very rare occasion fsmeta might require a forced global update and this way we can avoid having to read every single file? probably embedded not in the path
__pathmeta_version__ = 0
__extract_version__ = 0
__errors_version__ = 0  # this one should probably never change
__combine_version__ = 0
__index_version__ = 0

sf_dir_name = 'objs'  # this really is more of an objects index ...
sf_export_base = Path(auth.get_path('export-path')) / sf_dir_name  # TODO ensure configable for testing switching versions

fsmeta_dir_name = 'fsmeta'
objmeta_dir_name = 'objmeta'
pathmeta_dir_name = 'pathmeta'
extract_dir_name = 'extract'
errors_dir_name = 'errors'
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
            fsmeta_version_path(),
            objmeta_version_path(),
            pathmeta_version_path(),
            extract_path(),  # FIXME these are really x_version_path ...
            errors_version_path(),
            combine_path(),
            index_path(),

            index_combine_latest_path(),
            index_combine_archive_path(),
            
         ]
    ]
    [log.debug(p) for p in top_paths[0]]

    tdss = [
        # bugs
        '0c4fc5e8-c332-47d8-9fd8-1fbeb624078c',  # dat norm values returning None??? also .DS_Store
        #'0967e3b7-09db-4b91-9caf-1090c9d0c437',  # jdec, log2(0) also giant 31k files XXX good bench for using sxpr ...
        '41ca18b1-c991-4709-892e-8ae98907549b',  # JEncode
        '5942f61b-03f0-4fd6-a116-1de400af7422',  # json decode
        'a996cdac-2d00-4ad4-9699-8a75ce29c2f1',  # datset description no alt
        'aa43eda8-b29a-4c25-9840-ecbd57598afc',  # ft reva ultrasound xml format TODO
        'c3072708-13f4-45ed-992c-b9a744f6a5f3',  # no nums in name for entcomp
        'd088ed64-4f9f-47ee-bf6d-7d53e1863fc4',  # another json decode

        'c549b42b-9a92-4b6d-bf35-4cc2a11f5352',  # xml with trailing whitespace in an identifier >_< ooo or better yet <<mbf instead of <mbf
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
        'd484110a-e6e3-4574-aab2-418703c978e2',  # many xmls ~200 files, but only the segmentations ... no data, also a csv that is actually a tsv
    ]


    parent_parent_path = Path('~/files/sparc-datasets-test/').expanduser()

    if False:
        for ecp in (
                'c549b42b-9a92-4b6d-bf35-4cc2a11f5352/SPARC/High-throughput segmentation of rat unmyelinated axons by deep learning/primary/sub-131/sam-12/131L_F7_metadata.xml',
        ):
            err_causing_path = parent_parent_path / ecp
            try:
                extract_fun_xml(err_causing_path)
                assert False, 'should have failed'
            except AssertionError as e:
                raise e
            except Exception as e:
                pass

            try:
                extract_fun_xml_debug(err_causing_path)
                assert False, 'should have failed'
            except AssertionError as e:
                raise e
            except Exception as e:
                pass

            #extract('c549b42b-9a92-4b6d-bf35-4cc2a11f5352', err_causing_path.as_posix(), expex_type='xml', extract_fun=extract_fun_xml_debug)

    for tds in tdss:
        inner_test(tds, parent_parent_path=parent_parent_path)


def inner_test(tds, force=True, debug=True, parent_parent_path=None):
    log.info(f'running {tds}')

    dataset_path = (parent_parent_path / tds / 'dataset').resolve()
    #cs = list(dataset_path.rchildren)  # XXX FIXME insanely slow
    cs = transitive_paths(dataset_path)
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
        [log.debug(p) for paths in aps for p in paths]

    create_current_version_paths()

    (updated_cache_transitive, indicies, some_failed
     ) = from_dataset_path_extract_object_metadata(
         dataset_path, force=force, debug=debug)

    if debug:
        # force loading from fsmeta + objmeta store to ensure that we got those codepaths right and compare to
        #object_id_types = [(o, t) for o, t, b in object_id_type_pathmeta_blobs]
        d_drps, d_drp_index = from_dataset_id_combine(dataset_id, updated_cache_transitive, keep_in_mem=True)

    drps, drp_index = from_dataset_id_indicies_combine(
        dataset_id, indicies, updated_cache_transitive, keep_in_mem=True)

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
        # i.e. combine_export_path(dataset_id, dataset_id)

    blobs = [d[-1] for d in drps]
    with_xml_ref = [b for b in blobs if 'external_records' in b and 'xml' in b['external_records']]
    xml_files = [b for b in blobs if pathlib.PurePath(b['path_metadata']['dataset_relative_path']).suffix == '.xml']
    xml_sources = [b for b in blobs if 'extracted' in b and 'xml' == b['extracted']['object_type']]
    apparent_targets = [x['external_records']['manifest']['input']['description'][26:] for x in xml_sources
                        if 'external_records' in x and
                        'manifest' in x['external_records'] and
                        'description' in x['external_records']['manifest']['input']]
    maybe_matches = [b for b in blobs if b['path_metadata']['basename'] in apparent_targets]
    if maybe_matches:
        breakpoint()


def object_source_cache_path(datset_id, object_id, parent_parent_path=None):  # TODO
    # FIXME should probably actually use auth.get_path('data-path') since that is what it use supposed to be used for ...
    # even if you don't have the drp handy you can still find the object in the data-path objects cache
    return parent_parent_path / datset_id.uuid / f'dataset/../.operations/objects/{object_id.uuid_cache_path_string(2, 1)}'


def create_current_version_paths():
    fsm_path = fsmeta_version_path()
    obj_path = objmeta_version_path()
    #pm_path = pathmeta_version_path()
    ext_path = extract_path()
    err_path = errors_version_path()
    com_path = combine_path()
    idx_path = index_path()
    archive_path = index_combine_archive_path()

    if not obj_path.exists():
        obj_path.mkdir(parents=True)

    if not fsm_path.exists():
        fsm_path.mkdir(parents=True)

    if not ext_path.exists():
        ext_path.mkdir(parents=True)

    if not err_path.exists():
        err_path.mkdir(parents=True)

    if not com_path.exists():
        com_path.mkdir(parents=True)

    if not idx_path.exists():
        idx_path.mkdir(parents=True)

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


def _dataset_path(dataset_id):
    return dataset_id.uuid_cache_path_string(1, 1, use_base64=True)


def _dataset_path_obj_path(dataset_id, object_id):
    dataset_path = _dataset_path(dataset_id)
    obj_path = object_id.uuid_cache_path_string(1, 1, use_base64=True)
    return dataset_path, obj_path


def _dump_path(dataset_id, object_id, blob, version_export_path):
    path = version_export_path(dataset_id, object_id)
    if not path.parent.exists():
        path.parent.mkdir(exist_ok=True, parents=True)

    with open(path, 'wt') as f:
        json.dump(blob, f, sort_keys=True, cls=JEncode)

    return path


def fsmeta_version_path(fsmeta_version=None):
    fsmeta_version = str(__fsmeta_version__ if fsmeta_version is None else fsmeta_version)
    return sf_export_base / fsmeta_dir_name / fsmeta_version


def fsmeta_version_export_path(dataset_id, name, fsmeta_version=None):
    base_path = fsmeta_version_path(fsmeta_version=fsmeta_version)
    dataset_path = _dataset_path(dataset_id)
    return base_path / dataset_path / name


def objmeta_version_path(objmeta_version=None):
    objmeta_version = str(__objmeta_version__ if objmeta_version is None else objmeta_version)
    return sf_export_base / objmeta_dir_name / objmeta_version


def objmeta_version_export_path(dataset_id, object_id, objmeta_version=None):
    base_path = objmeta_version_path(objmeta_version=objmeta_version)
    dataset_path, obj_path = _dataset_path_obj_path(dataset_id, object_id)
    return base_path / dataset_path / obj_path


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


def dump_extract_path(dataset_id, object_id, blob):
    return _dump_path(dataset_id, object_id, blob, extract_export_path)


def errors_version_path(errors_version=None):  # always the latest but put version there for consistency
    errors_version = str(__errors_version__ if errors_version is None else errors_version)
    return sf_export_base / errors_dir_name / errors_version


def errors_version_export_path(dataset_id, object_id, errors_version=None):
    base_path = errors_version_path(errors_version=errors_version)
    dataset_path, obj_path = _dataset_path_obj_path(dataset_id, object_id)
    return base_path / dataset_path / obj_path


def dump_error_path(dataset_id, object_id, blob):
    return _dump_path(dataset_id, object_id, blob, errors_version_export_path)


def combine_path(combine_version=None):
    combine_version = str(__combine_version__ if combine_version is None else combine_version)
    return sf_export_base / combine_dir_name / combine_version


def combine_version_export_path(dataset_id, object_id, combine_version=None):
    base_path = combine_path(combine_version=combine_version)
    dataset_path, obj_path = _dataset_path_obj_path(dataset_id, object_id)
    return base_path / dataset_path / obj_path


def dump_combine_path(dataset_id, object_id, blob):
    return _dump_path(dataset_id, object_id, blob, combine_version_export_path)


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
    extracted = {}
    mf = dat.ManifestFile(path)
    try:
        data = mf.data
    except Exception as e:
        # FIXME should not be embedding errors at this stage at all
        # however this is pretty much impossible for manifests?
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
    blob = pipes.XmlFilePipeline._do_subprocess(path, raise_on_error=True) # FIXME this pulls pathmeta again internally
    extracted = {}
    if 'contents' in blob:
        if 'errors' in blob['contents']:
            log.warning('sigh errors in contents ???')
            breakpoint()
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
    extracted = {}
    # XXX adapted from sparcur.pipelines.XmlFilePipeline._path_to_json_meta
    try:
        e = exml.ExtractXml(path)
        if e.mimetype:
            extracted['contents'] = e.asDict(_fail=True, raise_on_error=True)
            extracted['mimetype'] = e.mimetype
        elif isinstance(e, exml.NotXml):  # pretty sure this is exactly (not e.mimetype)
            raise exc.BadDataError(f'xml suffix but not xml data in {path}')
    except NotImplementedError as e:
        raise e  # always fatal now
    except Exception as e:
        log.error(f'xml extraction failure for {path!r}')
        raise e

    return extracted


def extract_fun_from_expex_type(expex_type):
    if expex_type not in _expex_types:
        msg = f'expex_type {expex_type} not in {_expex_types}'
        raise ValueError(msg)

    if expex_type == 'inode/directory':
        extract_fun = None
    elif expex_type == 'manifest':
        extract_fun = extract_fun_manifest
    elif expex_type in _metadata_file_types :
        extract_fun = None
    elif expex_type == 'xml':
        #extract_fun = extract_fun_xml_debug
        extract_fun = extract_fun_xml
    else:
        extract_fun = None

    return extract_fun


def expex_type_from_pathmeta_blob(pathmeta_blob):  # FIXME naming
    # FIXME this is a horrible way to dispatch pipelines ...

    # TODO this determines whether a path should be extracted
    # fetch_fun and extract_fun are separate so that we can
    # run the fetch funs together outside the network sandbox

    #fetch_fun = None

    path = pathmeta_blob['dataset_relative_path']

    if 'mimetype' in pathmeta_blob and pathmeta_blob['mimetype'] == 'inode/directory':
        expex_type = 'inode/directory'
        #extract_fun = None

    elif path.stem == 'manifest':  # FIXME do a proper match, XXX reminder that any change here _WILL_ require reextraction unless we split path_metadata only blobs from the rest, which we probably should to avoid exact this kind of issue
        expex_type = 'manifest'
        #extract_fun = extract_fun_manifest
        # FIXME need to match more name patterns
        # see sc.metadata_filename_pattern

    elif path.stem in _metadata_file_types:  # FIXME overly restrictive match
        # also not clear we need to differentiate the expex type really here?
        # but also kind of why not ?
        expex_type = path.stem
        #extract_fun = None

    elif path.suffix == '.xml':
        expex_type = 'xml'
        #extract_fun = extract_fun_xml_debug
        #extract_fun = extract_fun_xml

    else:
        expex_type = None
        #extract_fun = None

    if expex_type not in _expex_types:
        msg = f'expex_type {expex_type} not in {_expex_types}'
        raise ValueError(msg)

    return expex_type #, fetch_fun, extract_fun


register_type(None, 'extract-object-metadata')  # currently unmapped
def extract(dataset_uuid, path_string, expex_type, extract_fun=None):
    """ actually run extraction for a single path
    see also sparcur.pipelines.XmlFilePipeline._do_xml_metadata

    we convert back from dataset_uuid to simplify passing to subprocess
    """

    if extract_fun is None:  # subprocess case
        extract_fun = extract_fun_from_expex_type(expex_type)
        if extract_fun is None:
            msg = f'no extract_fun known for {path}'
            raise TypeError(msg)

    he = HasErrors(pipeline_stage='objects.extract')

    dataset_id = RemoteId(dataset_uuid, type='dataset')
    path = Path(path_string)
    object_id = path.cache_identifier

    if path.is_file():  # catch symlink case
        _blob = {
            '__extract_version__': __extract_version__,
            'type': 'extract-object-metadata',
            'remote_id': object_id,
            'object_type': expex_type,
        }

        # FIXME TODO ... kind of external vs internal type ...
        try:
            blob = extract_fun(path)
            blob.update(_blob)
        except KeyboardInterrupt as e:
            raise e
        except Exception as e:
            # unhandled excpetions case
            if he.addError(e, blame='submission', path=path, pipeline_stage='objects.extract.extract_fun'):
                logd.exception(e)

            he.embedErrors(_blob)
            error_path = dump_error_path(dataset_id, object_id, _blob)
            msg = f'extract error metadata written to {error_path}'
            log.debug(msg)  # can caused by user data
            # TODO it would be nice if we could unlink any existing export paths
            # here for convenience during development, but unfortunately the most
            # likely cause of a write to the error path is that there was a bug
            # in the code, so we should retain the export path, and during dev
            # just remove stale or known bad trees manually
            return

        try:
            validate_extract(blob)
        except exc.ExtractionValidationError as e:
            if he.addError(e, blame='pipeline', path=path, pipeline_stage='objects.extract.validate_extract'):
                log.exception(e)

            he.embedErrors(blob)
            error_path = dump_error_path(dataset_id, object_id, blob)
            msg = f'validate extract error written to {error_path}'
            log.info(msg)  # 99% of the time caused by us
            # raise e  # this one is on us but grep for theh pipeline stage
            return

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
                    export_path = extract_export_path(dataset_id, object_id)
                    if not export_path.parent.exists():
                        export_path.parent.mkdir(exist_ok=True, parents=True)

                    fp = previous_path.resolve()
                    target = fp.relative_path_from(export_path)
                    breakpoint()
                    export_path.symlink_to(target)
                    msg = f'extract object metadata matches previous so linked to {target}'
                    log.log(9, msg)
                    return
                else:
                    breakpoint()

        export_path = dump_extract_path(dataset_id, object_id, blob)

        # if we succeed in writing a blob unlink any previous
        # error blob to keep things tidy
        errors_path = errors_version_export_path(dataset_id, object_id)
        if errors_path.exists():
            errors_path.unlink()

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
    def ce(msg): raise KeyError(msg)  # LOL PYTHON can't raise in lambda ...
    for f in (
            (lambda : blob['type']),
            (lambda : blob['__extract_version__']),
            (lambda : blob['remote_id']),
            (lambda : blob['object_type']),
            (lambda : blob['contents']),
            (lambda : True if list(blob['contents']) != ['errors'] else ce('ceo')),
    ):
        try:
            f()
        except KeyError as e:
            bads.append(e)

    # FIXME TODO we need to remove all embedding of errors from the extraction
    # phase because either the conversion conforms or it does not and if it does
    # not then it is assumed that the data is malformed beyond our ability to
    # proceed in which case we log an error and don't write the file or there
    # is a bug in the code, using the json schema to detect issues with the
    # content that was extracted is exactly what should happen in the combine step
    collect = []
    JApplyRecursive(get_nested_by_key, blob, 'errors', collect=collect)
    if collect:
        # if remote_id is missing we might as well raise here anyway
        msg = f'errors key detected in extract for {blob["remote_id"]}!\n{collect}'
        log.warning(msg)

    if bads:
        [log.error(b) for b in bads]  # don't use exception because there is not traceback
        # this is an internal error because we control the format of everything we get here
        msg = f'validate failed for\n{json.dumps(blob, sort_keys=True, indent=2, cls=JEncode)}\n'
        raise exc.ExtractionValidationError(msg)


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
        'timestamp_created': dateparser.parse,
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


register_type(_pathmeta_ir, 'objmeta')
register_type(_pathmeta_ir, 'pathmeta')


def objmeta(dataset_id, object_id, path, force=False):
    """ the invariant portion of pathmeta """
    export_path = pathmeta_version_export_path(dataset_id, object_id)
    if export_path.exists() and not force:  # objmeta is invariant so if we have it we have it
        return

    # FIXME this could be sourced from the packages endpoint directly in many cases
    # so going through the filesystem is wasted step
    blob = path._cache_jsonMetadata(with_created=True)
    do_not_want = 'dataset_relative_path', 'timestamp_updated', 'basename', 'parent_id'
    [blob.pop(k) for k in do_not_want]
    blob['type'] = 'objmeta'  # you can't actually invert everything back to Path because Path is only the name
    blob['__objmeta_version__'] = __pathmeta_version__

    export_path.parent.mkdir(exist_ok=True, parents=True)

    with open(export_path, 'wt') as f:
        json.dump(blob, f, sort_keys=True, cls=JEncode)

    return blob


def mrecs(inrecs):
    def escn(n):
        # cannot use json.dumps because it escapes all unicode in a way that breaks sxpr
        if '"' in n:
            nn = n.replace('"', '\\"')
        else:
            nn = n
        return f'"{nn}"'

    recs = '\n'.join(
        f'({o.curie} {p.curie} "{timeformat_friendly(u)}" {escn(n)})'
        # put news changes at the top of the file
        for o, p, u, n in sorted(inrecs, key=kupd, reverse=True))
    return recs


# most of our change sets are a single file removed and added in curation
# TODO figure out how to tune this for performance, most changes are a single
# file at a time, which is fine, but at some point we will want to repack
# in addition to the current keyframe approach
fsmeta_max_delta = 40


def fsmeta(dataset_id, dataset_path=None, force=True, records=None, updated_cache_transitive=None, debug=False):
    """
(:type fsmeta :version 0 :dataset uuid :updated-transitive ut (:repack-from rfut :repack 0)? :delta 0 :from previous-ut)
(:remove ut object-uuid ...)
;:add
(id parent updated name) ...
;:change ; not clear we need to differentiate add and change I think, just do last one wins
;(id parent updated name) ...

for this particular use case we may want to use epoch instead of a full timestamp, though it will reduce debugability
    """
    # as it exists this would slot into from_dataset_path_extract_object_metadata

    # we don't check to see if the dataset updated has changed because the only reason
    # we would be running this is because the dataset updated date changed for some reason
    # so we have to check the file system to see whether there were any changes there
    # XXX unless we start embedding updated transitive in the cached metadata ... which
    # we could ?

    old_latest = fsmeta_version_export_path(dataset_id, 'OLD_LATEST')  # then atomic swap
    latest = fsmeta_version_export_path(dataset_id, 'LATEST')
    previous = latest.resolve() if latest.exists() else None
    new = None

    if records is None:
        if dataset_path is None:
            raise TypeError('dataset_path or records must be provided')

        rchildren = transitive_paths(dataset_path)
        # FIXME yes this duplicates part of subprocess extract, but let's ignore that for now
        records = []
        #updated_cache_transitive = None
        for path in chain((dataset_path,), rchildren):
            object_id = path.cache_identifier
            if path.is_dir() or path.is_file():
                xattrs = path.xattrs()
                _updated = xattrs[b'bf.updated'].decode()
                _parent_id = xattrs[b'bf.parent_id'].decode()
                meta = aug.PathMeta(updated=_updated, parent_id=_parent_id)
                updated = meta.updated
                cache_parent = RemoteId(meta.parent_id)
            elif path.is_broken_symlink():
                meta = aug.PathMeta.from_symlink(path)
                updated = meta.updated
                cache_parent = RemoteId(meta.parent_id)
            else:
                msg = f'wat {path}'
                raise NotImplementedError(msg)

            if object_id.type == 'dataset':
                # we want parent closed within dataset so we loop here
                # a similar fix is applied in pathmeta_objmeta
                cache_parent = object_id
            elif updated_cache_transitive is None:
                updated_cache_transitive = updated
            elif updated > updated_cache_transitive:
                updated_cache_transitive = updated

            rec = object_id, cache_parent, updated, path.name
            records.append(rec)

    elif updated_cache_transitive is None:
        if len(records) == 1:
            dud = timeformat_friendly(records[0][2])
            header = fsmeta_header(dataset_id, dud, 0, 1, 0)
            removes = f'(:remove "{dud}" :ids ())\n'  # leave it empty for regularity
            recs = mrecs(records)
            derp = fsmeta_version_export_path(dataset_id, dud)
            if not derp.parent.exists():
                derp.parent.mkdir(exist_ok=True, parents=True)

            # FIXME bad code duplication
            with open(derp, 'wt') as f:
                f.write(header)
                if removes:
                    f.write(removes)
                f.write(recs)

            if not latest.exists():
                # XXX I'm sure a race could happen here ...
                latest.symlink_to(derp.name)
            else:
                old_latest.symlink_to(derp.name)
                latest.swap(old_latest)
                old_latest.unlink()

            return

        raise TypeError('updated_cache_transitive must provided if records is provided')

    typecheck_fsmeta_records(records)

    uct_friendly = timeformat_friendly(updated_cache_transitive)
    # we want to write updated transitive in the header
    # so we either have to traverse the list twice or hold everything in memory
    # we choose to hold everything in memory since these usually aren't giant
    puct_friendly = None
    previous_updated_cache_transitive = None
    delta = 0
    for i in (range(2) if previous is not None else []):
        (previous_header, _, *previous_records), old_recs = path_fsmeta(previous, return_index=True)
        previous_updated_cache_transitive = previous_header['updated-transitive']
        puct_friendly = timeformat_friendly(previous_updated_cache_transitive)
        #assert previous.stem == puct_friendly, 'oops'  # don't really need to check this, and if we did it would be in path_fsmeta
        previous_updated_cache_transitive = dateparser.parse(puct_friendly)
        delta = previous_header['delta']
        if previous_updated_cache_transitive < updated_cache_transitive:
            # we do this before we check force so if force is set we need
            # to make sure that we don't increment delta because uct has
            # not actually changed
            delta += 1

        if previous_updated_cache_transitive == updated_cache_transitive:
            if not force:
                # we're done here
                return

            previous_updated_cache_transitive = previous_header['from']
            puct_friendly = timeformat_friendly(previous_updated_cache_transitive)
            previous = fsmeta_version_export_path(dataset_id, puct_friendly)
        else:
            break

    new = fsmeta_version_export_path(dataset_id, uct_friendly)
    newxz = new.with_suffix('.xz')
    do_repack = False
    # only xz if delta will be zero and records will serialize to something
    # larger than a single block (4096) (estimated of course since names vary in length)
    xz = (delta == 0 or delta >= fsmeta_max_delta) and len(records) > 28  # FIXME we can do better than guessing at 28
    test_xz = True
    if not force and (new.exists() or xz and newxz.exists()):
        breakpoint()
        raise Exception('sigh')

    n_records = len(records)
    if delta == 0 or delta >= fsmeta_max_delta:
        # >= if we are doing repack then we want repack = max_delta I think ...
        # and if we take len(prevs) since delta starts at zero it should be equal to max_delta ... but TODO check this
        repack = delta
        delta = 0
        if do_repack:
            raise NotImplementedError('TODO')
        else:  # new keyframe
            removes = f'(:remove "{uct_friendly}" :ids ())\n'  # leave it empty for regularity
            n_removed = 0
            recs = mrecs(records)
    else:
        new_recs = {v[0]: v for v in records}
        sor, snr = set(old_recs), set(new_recs)
        removed = sor - snr
        added = [new_recs[k] for k in (snr - sor)]
        both = (sor & snr)
        _old_new = [(k, old_recs[k], new_recs[k]) for k in both if old_recs[k] != new_recs[k]]
        changed = [new_recs[k] for k in both if old_recs[k] != new_recs[k]]
        unchanged = [new_recs[k] for k in both if old_recs[k] == new_recs[k]]
        diff_records = added + changed
        alt_n_records = len(diff_records) + len(unchanged)
        assert n_records == alt_n_records, f'{n_records} != {alt_n_records}'
        n_removed = len(removed)
        recs = mrecs(diff_records)
        if removed:
            remids = ' '.join(_.curie for _ in sorted(removed))
            # reminder: we embed uct_friendly in :remove to simplify the (future) repacking process
            removes = f'(:remove "{uct_friendly}" :ids ({remids}))\n'
        else:
            removes = f'(:remove "{uct_friendly}" :ids ())\n'  # leave it empty for regularity

    header = fsmeta_header(dataset_id, uct_friendly, n_removed, n_records, delta, puct_friendly=puct_friendly)
    out_string = header + removes + recs

    if test_xz:
        # we aren't set up to read multiple sexps right now, so hack it for the time being
        try:
            #test_read_split = split_sxpr(out_string)
            test_read_wrap = wrap_sxpr(out_string)
        except Exception as e:
            breakpoint()
            raise e

    if not new.parent.exists():
        # exists_ok due to race condition for other processes
        # normally shouldn't happen at the dataset level but it can for objects
        new.parent.mkdir(exist_ok=True, parents=True)

    try:
        if xz:
            # FIXME internal error on pypy3 ??? it is specific to athena ???
            out_bytes = out_string.encode()
            #if hasattr(sys, 'pypy_version_info'):
                # whenever I try to use lzma.compress on pypy3 I get an internal error
                #pass
            #else:

            filters = [{'id': lzma.FILTER_LZMA2, 'preset': 9 | lzma.PRESET_EXTREME}]
            try:
                with lzma.open(newxz, 'wb', filters=filters) as f:
                    f.write(out_bytes)
            except lzma.LZMAError as e:
                log.exception(e)
                log.critical('lzma internal error issue')
                # I have an insane system specific lzma internal error bug
                # I have no idea how to track it down right now, but since
                # it doesn't appear on either of my other systems work around it for now
                with open(new, 'wb') as f:
                    f.write(out_bytes)

                argv = ['xz', '--compress', '--extreme', '-9', '--force', new.as_posix()]
                p = subprocess.Popen(argv)
                out = p.communicate()
                if p.returncode != 0:
                    raise exc.SubprocessException(f'xz compress failed objs return code was {p.returncode}')

                if not debug:
                    # if this happens anywhere but in dev/debug we need to know and raise
                    raise e

            if test_xz:
                with lzma.open(newxz, 'rb') as f:
                    # ( + + ) hack for lack of multi expression reading support at the moment
                    # we can implement a version of read that will work with strings but not
                    # worth the effort at the moment since we don't need true stream reading atm
                    in_bytes = f.read()
                    in_string = in_bytes.decode()
                    test_read_xz = wrap_sxpr(in_string) # oa.utils.sxpr_to_python('(' + in_string + ')')
                    assert out_bytes == in_bytes, 'bytesfail'
                    assert out_string == in_string, 'stringfail'
                    assert test_read_wrap == test_read_xz, 'readfail'
        else:
            with open(new, 'wt') as f:
                f.write(header)
                if removes:
                    f.write(removes)
                f.write(recs)

        if old_latest.exists():
            # this problem solves itself now that finally is there
            # but still an eek if we encounter it in the future
            raise Exception(f'something very wrong {old_latest}')

        if not latest.exists():
            # XXX I'm sure a race could happen here ...
            latest.symlink_to((newxz if xz else new).name)
        else:
            old_latest.symlink_to((newxz if xz else new).name)
            latest.swap(old_latest)
            old_latest.unlink()

        return records  # we always return records because we need that to write the latest package-index file which doesn't care about history
    except Exception as e:
        # if we don't symlink to latest cleanup after
        if not latest.exists():
            if latest.is_symlink():
                msg = f'broken symlink someone has been messing about! {latest}'
                log.critical(msg)
            elif xz and newxz.exists():
                newxz.unlink()
            elif new.exists():
                new.unlink()

        raise e
    finally:
        # old_latest should never exist when we are done
        if old_latest.exists() or old_latest.is_symlink():
            try:
                old_latest.unlink()
            except FileNotFoundError as e:
                # race woo
                pass

    # so max_delta_count should probably be used to indicate the need for a repack of the delta data
    # and then the :remove ut can be used along with the updated for individual files
    # in the repack we allow multiple
    # repack will give the number of updates packed into that one and we then symlink all the individual
    # changes into a single file, eh, this is for the future, but knowing it is coming helps with the design
    # if delta
    # check if delta count is beyond max delta count (tune for performance based on cost of file reads probably)
    # reconstruct the full state of the file


def objmeta_write_from_pathmeta(dataset_id, object_id, pathmeta_blob, force=False):
    export_path = objmeta_version_export_path(dataset_id, object_id)
    if export_path.exists() and not force:  # objmeta is invariant so if we have it we have it
        return

    do_not_want = 'dataset_relative_path', 'timestamp_updated', 'basename', 'parent_id', '__pathmeta_version__', 'type'
    blob = {k: v for k, v in pathmeta_blob.items() if k not in do_not_want}
    blob['type'] = 'objmeta'
    blob['__objmeta_version__'] = __pathmeta_version__

    if not export_path.parent.exists():
        export_path.parent.mkdir(exist_ok=True, parents=True)

    with open(export_path, 'wt') as f:
        json.dump(blob, f, sort_keys=True, cls=JEncode)

    return blob


def pathmeta_objmeta(dataset_id, object_id, path, force=False):
    blob = path._cache_jsonMetadata(with_created=True)

    if object_id.type == 'dataset':
        # cut off the organization level at this point
        # otherwise we have to deal with it in multiple other locations
        # organization is retained but renamed FIXME TODO move this into _cache_jsonMetadata probably
        blob['access_control_group_id'] = blob['parent_id']
        blob['parent_id'] = object_id

    objmeta_write_from_pathmeta(dataset_id, object_id, blob, force=force)
    blob['type'] = 'pathmeta'  # you can't actually invert everything back to Path because Path is only the name
    blob['__pathmeta_version__'] = __pathmeta_version__
    return blob


def pathmeta_refresh(dataset_id, object_id, path, force=False):

    blob = path._cache_jsonMetadata()

    if object_id.type == 'dataset':
        # cut off the organization level at this point
        # otherwise we have to deal with it in multiple other locations
        # organization is retained but renamed FIXME TODO move this into _cache_jsonMetadata probably
        blob['access_control_group_id'] = blob['parent_id']
        blob['parent_id'] = object_id

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

    #maybe_objmeta_blob = objmeta(dataset_id, object_id, path)

    pathmeta_blob = pathmeta_objmeta(dataset_id, object_id, path, force=force)
    #pathmeta_blob, pathmeta_changed = pathmeta_refresh(dataset_id, object_id, path)  # FIXME TODO in point of fact this can run in parallel with extract because there is not any necessary interaction until combine

    export_path = extract_export_path(dataset_id, object_id)
    done = export_path.exists()  # and not pathmeta_changed  # FIXME we don't need to rerun the export if the pathmeta changed ... the id does not change
    # FIXME this is obviously wrong, e.g. what happens if the remote changes the file name or moves a file? we need a bit more info related update times or reparents, for now we will have to run time force=True to avoid stale, or we run with force=True when it is publication time, OR we don't store pathmeta at all and always rederive it during extract ... that seems better, yes ...
    expex_type = expex_type_from_pathmeta_blob(pathmeta_blob)
    extract_fun = extract_fun_from_expex_type(expex_type)
    # FIXME hard to pass extract_fun directly so would have to
    # rederive in subprocess version not that I think we will be using
    # that variant

    if done and not force:
        success = 'already-done'
        msg = f'{object_id} already done {path}'
        log.log(9, msg)
        return path, object_id, expex_type, success, updated, pathmeta_blob

    elif extract_fun is None:
        success = 'no-extract-fun-and-objmeta-already-written'
        msg = f'{object_id} objmeta already written and no extract_fun {path}'
        log.log(9, msg)
        return path, object_id, expex_type, success, updated, pathmeta_blob

    elif debug or not subprocess:
        # TODO or raw expex_type in IMPORTANT i.e. stuff that should be in memory ... but that is and optimization
        try:
            extract(dataset_id.uuid, path.as_posix(), expex_type, extract_fun=extract_fun)
            success = True
        except Exception as e:
            success = False
            log.exception(e)

        return path, object_id, expex_type, success, updated, pathmeta_blob

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

    return path, object_id, expex_type, success, updated, pathmeta_blob  # FIXME include pathmeta in the return ...


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
        for path in chain((dataset_path,), rchildren))

    updated_cache_transitive = None
    bads = []
    #object_id_type = []
    _type_oids = defaultdict(list)
    fsmeta_records = []
    pathmeta_blob_index = {}
    parent_index = {}
    id_drp = {}
    for path, id, expex_type, ok, updated, pathmeta_blob in results:
        if id.type == 'dataset':
            # do not include dataset when calculating updated transitive
            pass
        elif updated_cache_transitive is None:
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

        parent = pathmeta_blob['parent_id']
        # we have to drop file_id here, could be used as a cross
        # reference to pathmeta sourced id later, they are all derived
        # from the same source so no point really
        fsmeta_records.append((RemoteId(id.curie) if id.type == 'package' else id, parent, updated, path.name))
        _type_oids[expex_type].append(id)
        #object_id_type.append((id, expex_type))
        pathmeta_blob_index[id] = pathmeta_blob
        parent_index[id] = parent
        id_drp[id] = pathmeta_blob['dataset_relative_path']

    type_oids = dict(_type_oids)
    indicies = type_oids, pathmeta_blob_index, parent_index, id_drp
    fsmeta(dataset_id, records=fsmeta_records, updated_cache_transitive=updated_cache_transitive, debug=debug)

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

    return updated_cache_transitive, indicies, bads


# these variants imply a certain amount of rework
#def from_dataset_id_combine(dataset_id): pass
def from_dataset_path_combine(dataset_id): pass
def from_dataset_id_object_ids_combine(dataset_id): pass


def path_json(path):
    try:
        with open(path, 'rt') as f:
            return json.load(f)
    except Exception as e:
        log.critical(f'json load error for {path!r}')
        log.exception(e)
        raise e


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

        if 'contents' not in blob:
            continue  # TODO logging

        contents = blob['contents']

        #pathmeta_blob = path_json(pathmeta_version_export_path(dataset_id, object_id))
        #pathmeta_blob = inds['pathmeta_blob_index'][object_id]
        #drp = pathlib.PurePath(pathmeta_blob['dataset_relative_path'])
        drp = inds['id_drp_all'][object_id]
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
                other_rec = drp_manifest_record_index[target_drp]
                other_drp = other_rec['prov']['source_drp']
                other_id = other_rec['prov']['source_id']
                other_op = '/'.join(_dataset_path_obj_path(dataset_id, other_id))
                this_op = '/'.join(_dataset_path_obj_path(dataset_id, object_id))
                msg = (
                    f'multiple manifest records for {dataset_id}:{target_drp} '
                    f'from {object_id} at {drp} and {other_id} at {other_drp}'
                    f'\n{this_op}\n{other_op}')
                log.error(msg)
                if drp == other_drp:
                    # big wat (drp gen code was bad)
                    msg = f'{other_rec}\n{rec}'
                    log.error(msg)
                # FIXME TODO embed error in rec
                from collections import Counter
                ctr = Counter([c['filename'] for c in contents])
                mc = ctr.most_common()
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

        if 'contents' not in blob:
            breakpoint()
            # we're expecting embedded errors or something?
            msg = ('malformed data that should never have been writen'
                   f'{dataset_id} {object_id} {extract_path}\n{blob}')
            raise ValueError(msg)

        mimetype = blob['mimetype'] if 'mimetype' in blob else None
        if 'contents' not in blob:
            # FIXME TODO logging
            continue

        not_this_time = [False]
        if drp in drp_index:
            # we have a manifest record
            internal_references = drp_index[drp]
        else:
            internal_references = {}

        contents = blob['contents']  # FIXME could be missing ...
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
                    'input': blob,  # FIXME TODO don't embed the whole file, figure out what to extract or just leave it as a pointer
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
                        return sum(log2(1 if n == 0 else n) for n in both)

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
                            1 - (((
                                #comp :=
                                entcomp(tmp_sn, snums)) / tmp_self_comp) if tmp_self_comp else 1),
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
    return None, None

    # at this stage the other inds are not populated so sadly
    # we have to read from disk twice, in service of reduced memory usage
    id_drp = {}
    for object_id in oids:
        blob = path_json(pathmeta_version_export_path(dataset_id, object_id))
        drp = pathlib.PurePath(blob['dataset_relative_path'])
        #other[drp] = blob  # FIXME check on memory sizes here
        id_drp[object_id] = drp

    return id_drp, None


def combine_at_updated():
    """ TODO historical version """


def combine(dataset_id, object_id, type, drp_index, parent_index, pathmeta_blob=None):
    blob = {}
    blob['type'] = 'combine-object-metadata'  #'combined-metadata'
    blob['__combine_version__'] = __combine_version__

    #objmeta_blob = path_json(objmeta_version_export_path(dataset_id, object_id))
    # XXX I have this sense that we really only want objmeta and fsmeta for historical
    # reconstruction of pathmeta, and if we already have pathmeta then we write the
    # fsmeta and objmeta and move along, but we always load the pathmeta because we
    # read it into memory anyway
    if pathmeta_blob is None:
        # we don't cache pathmeta_blobs anymore, so fsmeta + objmeta has to happen
        # first for a whole dataset, I suppose we could revisit that decision and
        # still keep the pathmeta cache for the latest version, but stuff starts to
        # get annoyingly large at that point in time
        raise NotImplementedError('TODO')
        pathmeta_blob = path_json(pathmeta_version_export_path(dataset_id, object_id))

    blob['path_metadata'] = pathmeta_blob

    # embed the parent ids so that they can be used without
    # having to deconstruct dataset_relative_path and reconstruct
    # other indexes from scratch, this does mean that changes to
    # the file structure are reflected here, but that is expected
    # at this phase (we manage to avoid it for extract)

    # FIXME we have this already in the tpar index ...
    _pids = [pathmeta_blob['parent_id']]#[RemoteId(pathmeta_blob['parent_id'])]
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
        blob['extracted'] = extract_blob

    errors_path = errors_version_export_path(dataset_id, object_id)
    if errors_path.exists():
        errors_blob = path_json(errors_path)
        blob['extract_errors'] = errors_blob

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
        # FIXME this is way to verbose to log :/
        # TODO log an error once or something ... ?
        log.log(9, msg)  # TODO embed error

    combine_path = dump_combine_path(dataset_id, object_id, blob)
    msg = f'combine object metadata written to {combine_path}'
    log.log(9, msg)

    return drp, has_recs, blob


pex_funs = {
    None: pex_default,
    'inode/directory': pex_default,  # pex_dirs,
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


#from desc.prof import profile_me

#@profile_me
def split_sxpr(string):  # don't use this one it is slower and potentially incorrect
    raise NotImplementedError('NO USE PLS')
    f, *_recs, l = string.split(')\n(')
    return oa.utils.sxpr_to_python(f + ')'), *[oa.utils.sxpr_to_python('(' + r + ')') for r in _recs], oa.utils.sxpr_to_python('(' + l)


#@profile_me
def wrap_sxpr(string):
    # this seems faster but only by about a second, but ~20s for 31k on 3.11
    # apparently I actually did a good implementation, on pypy3 wrap is ~2.7 and split is ~3.5 (lol)
    # for reference racket reads this in < 300ms
    # (let-values  ([(_ a b c) (time-apply read (list (open-input-string in)))]) (values (caar _) a b c))
    return oa.utils.sxpr_to_python('(' + string + ')')


def path_sxpr(path):
    """ open path sxpr, auto handle xz, perhaps unwisely """
    if path.exists():
        fpath = path
        if path.suffix == '.xz':
            xz = True
        else:
            xz = False
    else:
        if path.suffix == '.xz':
            fpath = path.with_suffix('')
            xz = False
            if fpath.exists():
                msg = f'we have a file extension mixup {path} does not exist but {fpath} does, logic is off somewhere'
                raise Exception(msg)
        else:
            fpath = path.with_suffix('.xz')
            xz = True
            if fpath.exists():
                msg = f'we have a file extension mixup {path} does not exist but {fpath} does, logic is off somewhere'
                raise Exception(msg)

        if not fpath.exists():
            raise FileNotFoundError(fpath)

    fopen = lzma.open if xz else open
    with fopen(fpath, 'rb') as f:
        # ( + + ) hack for lack of multi expression reading support at the moment
        # we can implement a version of read that will work with strings but not
        # worth the effort at the moment since we don't need true stream reading atm
        try:
            string = f.read().decode()
            return wrap_sxpr(string)
            #return oa.utils.sxpr_to_python('(' + string + ')')
        except Exception as e:
            breakpoint()
            raise e


def kupd(t):
    o, p, u, n = t
    return u


def fsmeta_header(dataset_id, uct_friendly, n_removed, n_records, delta, puct_friendly=None):
    # XXX n_records is the final expected number of records AFTER resolving all changes (duh)

    # FIXME TODO ideally we also want to check to make sure that the uct is not
    # coming from the dataset, but if dataset updated == real uct in some other system
    # e.g. because of atomic ops on say, removing a folder, then we have a problem
    # XXX ARGH actually removing a top level folder or file could cause the updated transitive time to go _backwards_ !!!
    # FIXME FIXME FIXME must handle this case, otherwise we are going to have a _very_ bad time
    # TODO need a test case
    if puct_friendly is not None and puct_friendly >= uct_friendly:
        msg = ('invariant violation! previous time somehow >= current time? '
               f'{puct_friendly} >= {uct_friendly}')
        raise ValueError(msg)

    if delta > 0 and puct_friendly is None:
        msg = f'something has gone very wrong {dataset_id} {uct_friendly}, delta {delta} > 0 but puct_friendly is None'
        raise Exception(msg)

    hfrom = f' :from "{puct_friendly}"' if delta > 0 else ''
    header = (
        f'(:type fsmeta :version {__fsmeta_version__} :dataset {dataset_id.curie} '  # FIXME maybe decouple
        f':updated-transitive "{uct_friendly}" :removes {n_removed} :records {n_records} :delta {delta}{hfrom})\n')
    return header


def path_fsmeta(path, return_index=False):
    prevs = []
    previous = path
    cdelta = 1
    dataset_id = None
    delta = None
    n_seen = 0
    while cdelta > 0:
        try:
            if (cdelta - 1) == 0:
                # have to handle the case where the first previous was a keyframe
                previousxz = previous.with_suffix('.xz')
                if previousxz.exists():
                    previous = previousxz

            previous_sxpr = path_sxpr(previous)
        except Exception as e:
            breakpoint()
            raise e

        prev_header = previous_sxpr[0]
        log.log(9, prev_header)
        cdelta = int(prev_header['delta'])

        if not prevs:
            delta = cdelta
            _header = {**prev_header}
            dataset_id = RemoteId(prev_header['dataset'])
            #previous_updated_cache_transitive = prev_header['updated-transitive']
        else:
            n_seen += 1

        if n_seen > delta:
            msg = f'number of previous traversed exceeds expected {n_seen} > {delta}'
            breakpoint()
            raise ValueError(msg)

        prevs.append(previous_sxpr)
        if cdelta > 0:
            delta_from = prev_header['from']
            if delta_from >= (uct_friendly := prev_header['updated-transitive']):
                # malformed, likely written by something not enforcing the invariant on write
                msg = ('invariant violation! previous time somehow >= current time? '
                       f'{delta_from} >= {uct_friendly} in {previous}')
                raise ValueError(msg)

            previous = fsmeta_version_export_path(dataset_id, delta_from)

    old_recs = {}
    old_rems = set()
    for olr in prevs:
        oh = olr[0]
        _orem = olr[1]['ids']
        orem = [] if _orem is None else _orem
        old_rems.update(orem)
        _recs = olr[2:]

        for r in _recs:
            # we could reverse the list and pop stuff off
            # but because we go in reverse chronological order
            # we know that we will always see the latest first
            if r[0] not in old_recs and r[0] not in old_rems:
                _o, _p, _u, n = r
                o = RemoteId(_o)
                p = RemoteId(_p)
                u = dateparser.parse(_u)
                old_recs[o] = (o, p, u, n)

    #_header['delta'] = 0  # we can't do this here and we don't need to
    # because this is the only function that resolves deltas
    # and I don't think we care that something is reconstructed
    # but we can mark it as such or we could drop the from ... or something
    hcaste = (('version', int),
              ('dataset', RemoteId),
              ('updated-transitive', dateparser.parse),
              ('removes', int),
              ('records', int),
              ('delta', int),
              ('from', dateparser.parse),
              )
    for k, t in hcaste:
        if k in _header:
            _header[k] = t(_header[k])

    header = _header
    removed = {'remove': header['updated-transitive'], 'ids': None}
    records = list(old_recs.values()) if True else sorted(old_recs.values(), key=kupdt, reverse=True)
    if len(records) != header['records']:
        msg = f'number of records does not match expected {len(records)} {header["records"]}'
        raise ValueError(msg)

    fsmeta_blob = [header, removed, *records]
    typecheck_fsmeta_blob(fsmeta_blob)
    if return_index:
        return fsmeta_blob, old_recs
    else:
        return fsmeta_blob


def typecheck_fsmeta_records(recs):
    try:
        _typecheck_fsmeta_records(recs)
    except AssertionError as e:
        breakpoint()
        raise e


def _typecheck_fsmeta_records(recs):
    rbads = []
    for rec in recs:
        o, p, u, n = rec
        to = (type(o) == RemoteId and o.file_id is None) # this is what we want, drop the file id, we need the type to match for operational reasons
            #type(o) == RemoteId and o.type != 'package')
            #type(o) == str and o.startswith('package:') or
              # XXX ARHG the bad design continues to permeate everything it touches
              # let this be a reminder for any future person who thinks it is a good idea
              # to have "files" with multiple internal things that are actually folders
              # and then not give them the full folder abstraction >_<
              #(or type(o.file_id)== int)  # can't do this, fsmeta doesn't know about file_id at all
              # so if there is a file_id it literally cannot be fsmeta
        tp = type(p) == RemoteId
        tu = type(u) == datetime # datetime
        tn = type(n) == str
        viols = ''
        for var, ok in (('o', to), ('p', tp), ('u', tu), ('n', tn)):
            if not ok:
                viols += var

        if viols:
            rbads.append((rec, viols))

    assert not rbads, rbads


def typecheck_fsmeta_blob(blob):
    try:
        _typecheck_fsmeta_blob(blob)
    except AssertionError as e:
        breakpoint()
        raise e


def _typecheck_fsmeta_blob(blob):
    header, rem, *recs = blob

    assert type(header) == dict
    assert type(rem) == dict
    hkeys = (('type', str),
             ('version', int),
             ('dataset', RemoteId),
             ('updated-transitive', datetime),  # parse don't validate
             #('updated-transitive', str),  # FIXME uct_friendly is needed in both places, keeping str here seems less work? but risk of inconsistency?
             ('removes', int),
             ('records', int),
             ('delta', int),
             ('from', datetime),
             #('from', str),  # XXX same issue as with uct_friendly
             )
    hcond = {'from': lambda h: h['delta'] > 0}
    assert not (hbads := [k for k, t in hkeys if (k not in hcond
                                                  or k in hcond and hcond[k](header))
                          and k not in header]), hbads

    htbads = []
    for k, t in hkeys:
        if k in hcond and not hcond[k](header):
            continue

        v = header[k]
        if type(v) != t:
            htbads.append((k, v, t))

    assert not htbads, htbads

    typecheck_fsmeta_records(recs)


def indicies_from_dataset_id_fsmeta_blob(dataset_id, fsmeta_blob):
    # technically don't need dataset_id but can be used as a check
    typecheck_fsmeta_blob(fsmeta_blob)
    header, _rem, *recs = fsmeta_blob
    assert dataset_id == header['dataset'], f'dataset mismatch {dataset_id.curie!r} != {header["dataset"]!r}'

    idn = {}
    parent_index = {}
    for o, p, u, n in recs:
        idn[o] = n
        if o != p:  # skip the dataset case
            # no parent means datset is parent
            # which simplifies things later
            parent_index[o] = p

    dd = defaultdict(list)
    for o, p, u, n in recs:
        _pids = [p]
        while _pids[-1] in parent_index:
            _pm1 = _pids[-1]
            _pid = parent_index[_pm1]
            _pids.append(_pid)
            #if _pid == _pm1:  # was datset case which we deal with above
                #break

        dd[o] = _pids

    tpar = dict(dd)
    id_drp = {o: pathlib.Path(*[idn[p] for p in pars[-2::-1]],  # somehow tpar already excludes dataset?
                            (idn[o] if o in parent_index else ''))  # hack so that dataset doesn't have its full name and is the base
            for o, pars in tpar.items()}
    asstest = len(set(id_drp.values())) == len(id_drp)
    if not asstest:
        breakpoint()
    assert asstest, 'ut oh' + '\n'.join(p.as_posix() for p in sorted(id_drp.values()))
    #log.debug('\n'.join(p.as_posix() for p in sorted(id_drp.values())))
    _type_oids = defaultdict(list)
    pathmeta_blob_index = {}
    for _temp_object_id, parent_id, updated, name in recs:
        # FIXME issue with {dataset_id}/{dataset_id} case not having objmeta ... which it should? just for closure?
        blob = path_json(objmeta_version_export_path(dataset_id, _temp_object_id))  # XXX evidence that we should run fsmeta after objmeta
        ir = fromJson(blob)
        ir['dataset_relative_path'] = id_drp[_temp_object_id]
        ir['parent_id'] = parent_id
        ir['timestamp_updated'] = updated
        ir['basename'] = name
        object_id = ir['remote_id']  # XXX this is where fsmeta and objmeta are combined and we get package + file_id
        pathmeta_blob_index[object_id] = ir
        if object_id != _temp_object_id:
            # fsmeta does not include the file_id
            parent_index[object_id] = parent_index.pop(_temp_object_id)
            id_drp[object_id] = id_drp.pop(_temp_object_id)

        expex_type = expex_type_from_pathmeta_blob(ir)
        _type_oids[expex_type].append(object_id)

    type_oids = _type_oids
    return type_oids, pathmeta_blob_index, parent_index, id_drp


def indicies_from_dataset_id(dataset_id, updated_cache_transitive=None):
    # load fsmeta at updated_cache_transitive or latest
    _l = 'LATEST'
    name = _l if updated_cache_transitive is None else timeformat_friendly(updated_cache_transitive)
    fsmeta_path = fsmeta_version_export_path(dataset_id, name)
    if name != _l and not fsmeta_path.exists():
        # see if we saved as xz, handle it explicitly here
        fsmeta_pathxz = fsmeta_path.with_suffix('.xz')
        if fsmeta_pathxz.exists():
            fsmeta_path = fsmeta_pathxz

    # FIXME need a good warning here for case where the file does not exist
    # e.g. because something go messed up and no fsmeta was written
    fsmeta_blob = path_fsmeta(fsmeta_path)
    type_oids, pathmeta_blob_index, parent_index, id_drp = indicies_from_dataset_id_fsmeta_blob(dataset_id, fsmeta_blob)

    # FIXME type_oids is hard to come by due to lack of indirection ? no, because we derive the expex_type from the real fs right now
    # which is bad for this workflow

    #for oid, t in object_id_types:
        #if oid not in pathmeta_blob_index:
            #breakpoint()

    #object_id_type_pathmeta_blobs = [
        #(object_id, type, pathmeta_blob_index[object_id]) for object_id, type in object_id_types
    #]
    #return object_id_type_pathmeta_blobs
    return type_oids, pathmeta_blob_index, parent_index, id_drp


def from_dataset_id_combine(dataset_id, updated_cache_transitive=None, keep_in_mem=False):
    indicies = indicies_from_dataset_id(dataset_id, updated_cache_transitive=updated_cache_transitive)
    return from_dataset_id_indicies_combine(
        dataset_id, indicies, updated_cache_transitive=updated_cache_transitive, keep_in_mem=keep_in_mem)


def from_dataset_id_indicies_combine(dataset_id, indicies, updated_cache_transitive=None, keep_in_mem=False):
    type_oids, pathmeta_blob_index, parent_index, id_drp_all = indicies
    #for oid, type, pathmeta_blob in object_id_type_pathmeta_blobs:
        # FIXME why do oids for packages have a file_id associated when we go to put them in source_id ???
        # we have to load all in the first past to populate the info we need

        #if type not in types:
            #type_oids[type] = []

        #type_oids[type].append(oid)

    # we _have_ to load this first for everything, we can optimize out fs reads in the future as needed
    # use for trying to find potential matches from fuzzy data in manifests and xmls
    # XXX might not actually need this because I was being a dumb, but it does enable
    # intra-type references to be found and resolved immediately ...
    #if pathmeta_blobs is None:
        #id_drp_all, _ = pex_default(dataset_id, [o for os in types.values() for o in os])

    _name_drps_index = defaultdict(list)
    for drp in id_drp_all.values():
        # TODO see if we need prefix_drps_index using e.g.
        # [Path(*p.parts[n:]).as_posix() for n in range(len(p.parts))]
        _name_drps_index[drp.name].append(drp)

    name_drps_index = dict(_name_drps_index)
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
    #parent_index = None  # can remain None if there are only files at the top level
    xml_index = None
    xml_unmapped = None
    drp_index = {}
    process_order = 'inode/directory', None, 'manifest', 'xml'  # references within a type have to be resolved in a second pass in that pex
    # first pass
    for type in process_order:
        if type not in type_oids:
            continue

        oids = type_oids[type]
        process_extracted = pex_funs[type]
        inds = dict(
            #did_index=did_index,
            id_drp_all=id_drp_all,
            parent_index=parent_index,  # parent_index is only over dirs since files already have parent_id
            name_drps_index=name_drps_index,
            drp_index=drp_index,
            xml_index=xml_index,
            pathmeta_blob_index=pathmeta_blob_index,
        )
        id_drp, index, *rest = process_extracted(dataset_id, oids, **inds)
        if index is not None:
            if type == 'inode/directory':
                #did_index = index
                #parent_index, = rest
                continue
            if type == 'xml':
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
    for expex_type, oids in type_oids.items():  # TODO sort these in reverse order so that errors can be added to e.g. manifests
        for object_id in oids:
            pathmeta_blob = pathmeta_blob_index[object_id]
            drp, has_recs, blob = combine(dataset_id, object_id, type, drp_index, parent_index, pathmeta_blob)
            target, new_link = index_obj_symlink_latest(dataset_id, object_id, do_link=True)
            if new_link:
                news += 1
            drps.append((drp, has_recs, type, target, new_link, (blob if keep_in_mem else None)))

    log.info(f'finished combining and writing {len(drps)} and linking {news} object records for dataset {dataset_id}')
    # TODO flag the objects that were deleted since the previous run and probably stick an xattr on them to
    # make cleanup as simple as possible
    return drps, drp_index


def from_dataset_path_extract_combine(dataset_path, force=False, debug=False):
    create_current_version_paths()

    (updated_cache_transitive, indicies, some_failed
        ) = from_dataset_path_extract_object_metadata(
            dataset_path, force=force, debug=debug)

    dataset_id = dataset_path.cache_identifier

    if debug:
        _drps, _drp_index = from_dataset_id_combine(dataset_id, updated_cache_transitive)

    drps, drp_index = from_dataset_id_indicies_combine(
        dataset_id, indicies, updated_cache_transitive)


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
