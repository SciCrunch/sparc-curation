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
from sparcur import exceptions as exc
from sparcur.paths import Path
from sparcur.utils import fromJson, register_type, log as _log, logd as _logd
from sparcur.config import auth
from sparcur.core import JEncode, HasErrors
from sparcur.utils import transitive_paths, GetTimeNow, PennsieveId as RemoteId, levenshteinDistance

log = _log.getChild('sob')
logd = _logd.getChild('sob')

__pathmeta_version__ = 0
__extract_version__ = 0
__combine_version__ = 0
__index_version__ = 0

sf_dir_name = 'objs'  # this really is more of an objects index ...
sf_export_base = Path(auth.get_path('export-path')) / sf_dir_name

pathmeta_dir_name = 'pathmeta'
extract_dir_name = 'extract'
combine_dir_name = 'combine'

# this approach requires
combine_latest_dir_name = 'L'  # short to keep symlinks small enough to fit in inodes
combine_archive_dir_name = 'A'  # short to keep symlinks small enough to fit in inodes

index_dir_name = 'index'

latest_link_name = 'L'  # short to keep symlinks small enough to fit in inodes
archive_dir_name = 'A'  # short to keep symlinks small enough to fit in inodes

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

    tds = '645267cb-0684-4317-b22f-dd431c6de323'  # small dataset with lots of manifests
    # tds = '21cd1d1b-9434-4957-b1d2-07af75ad3546'  # big xml test sadly too many files (> 80k) to be practical for testing
    # tds = '46bcac1d-3e45-499b-82a8-0ee1852bcc4d'  # manifest column naming issues
    tds = '3bb4788f-edab-4f04-8e96-bfc87d69e4e5'  # much better, only 100ish files with xmls
    tds = '57466879-2cdd-4af2-8bd6-7d867423c709'  # has vesselucida files
    tds = '75133e23-a572-42ee-876c-7dc127d280db'  # some segmentation
    tds = 'a95b4304-c457-4fa3-9bc9-cc557a220d3e'  # many xmls ~2k files so good balance, but lol ZERO of the files actually traced are provided ???
    tds = 'd484110a-e6e3-4574-aab2-418703c978e2'  # many xmls ~200 files, but only the segmentations ... no data
    tds = 'f58c75a2-7d86-439a-8883-e9a4ee33d7fa'  # jpegs might correspond to subject id ??? ... ya subjects and samples but oof oof

    tds = 'bec4d335-9377-4863-9017-ecd01170f354'

    tds = 'ded103ed-e02d-41fd-8c3e-3ef54989da81'

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

    aps = top_paths + [all_paths(c) for c in cs]
    [print(p) for paths in aps for p in paths]

    create_current_version_paths()

    updated_cache_transitive, object_id_types = from_dataset_path_extract_object_metadata(dataset_path, force=True, debug=True)
    drps, drp_index = from_dataset_id_object_id_types_combine(dataset_id, object_id_types, updated_cache_transitive, keep_in_mem=True)

    missing_from_inds = [d for d, h, ty, ta, ne, mb in drps if not h and
                         # we don't usually expect manifest entires for themselves
                         # and we don't expect them for folders though they can be
                         # used to apply metadata to folders
                         ty not in ('inode/directory', 'manifest')]
    if missing_from_inds:
        log.error('missing_from_inds:\n' + '\n'.join(sorted([d.as_posix() for d in missing_from_inds])))
    if drp_index:
        # e.g. manifest records with no corresponding path
        log.error('drp_index:\n' + '\n'.join(sorted([k.as_posix() for k in drp_index.keys()])))
        # TODO embed somewhere somehow, probably in the dataset-uuid/dataset-uuid record?
        # i.e. combine_export_path(dataset_id, datset_id)

    blobs = [d[-1] for d in drps]
    #with_xml_ref = [b for b in blobs if 'extracted' in b and 'xml' in b['extracted']['type']]
    xml_files = [b for b in blobs if pathlib.PurePath(b['path_metadata']['dataset_relative_path']).suffix == '.xml']
    xml_sources = [b for b in blobs if 'extracted' in b and 'xml' == b['extracted']['extracted']['object_type']]  # FIXME annoying to query
    apparent_targets = [x['external_records']['manifest']['input']['description'][26:] for x in xml_sources
                        if 'description' in x['external_records']['manifest']['input']]
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


def do_actually_extract(path):
    # TODO this determines whether a path should be extracted
    # fetch_fun and extract_fun are separate so that we can
    # run the fetch funs together outside the network sandbox

    fetch_fun = None

    if path.is_dir():
        object_type_raw = 'inode/directory'
        extract_fun = None
    elif path.stem == 'manifest':  # FIXME do a proper match, XXX reminder that any change here _WILL_ require reextraction unless we split path_metadata only blobs from the rest, which we probably should to avoid exact this kind of issue
        object_type_raw = 'manifest'
        # FIXME need to match more name patterns
        # see sc.metadata_filename_pattern
        from sparcur import datasets as dat
        def extract_fun(path):
            # FIXME this need the template schema version to work ... at least in theory
            # the internals of the implementation mean that if it is missing it isn't fatal
            extracted = {
                'type': 'extracted-manifest',
                'object_type': 'manifest',
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

    elif path.suffix == '.xml':
        object_type_raw = 'xml'

        from sparcur.extract import xml as exml
        from sparcur import datasets as dat
        def extract_fun(path):
            extracted = {
                'type': 'extracted-xml',
                'object_type': 'xml',
            }
            # XXX adapted from sparcur.pipelines.XmlFilePipeline._path_to_json_meta
            try:
                e = exml.ExtractXml(path)
                if e.mimetype:
                    extracted['contents'] = e.asDict()
                    extracted['mimetype'] = e.mimetype
            except NotImplementedError as e:
                he = HasErrors(pipeline_stage='objects.xml.extract_fun')
                if he.addError(e, path=path):
                    logd.exception(e)

                he.embedErrors(extracted)
            except Exception as e:
                log.error(f'xml extraction failure for {path!r}')
                raise e

            return extracted

    else:
        object_type_raw = None
        extract_fun = None

    return object_type_raw, fetch_fun, extract_fun


register_type(None, 'extract-object-metadata')  # currently unmapped
def extract(dataset_uuid, path_string, object_type_raw=None, extract_fun=None):
    """ actually run extraction for a single path
    see also sparcur.pipelines.XmlFilePipeline._do_xml_metadata

    we convert back from dataset_uuid to simplify passing to subprocess
    """

    if extract_fun is None or object_type_raw is None:  # subprocess case
        object_type_raw, fetch_fun, extract_fun = do_actually_extract(path)
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

        # expects type, extracted_type, contents
        # and have "extracted": {"type": "extracted-metadata-type", "object_type": "mbfxml", "some_property": "derp", "contents": [{}...]}

        # XXX FIXME so much tangling in here ...
        #object_type, contents = extract_fun(path, blob)
        # FIXME where to put expected content type ...
        # e.g. that this is a manifest ... the current approach is not so great iirc
        # things we know how to process, manifest, xml, jpx, etc.
        #blob['object_type_raw'] = object_type_raw   # FIXME naming also expected vs post analysis re: types of xml
        #blob['object_type'] = object_type   # FIXME naming XXX or maybe object_type_raw can imply other top level keys?
        #blob['contents'] = contents

        export_path = extract_export_path(dataset_id, object_id)
        if not export_path.parent.exists():
            export_path.parent.mkdir(exist_ok=True, parents=True)

        # TODO almost certainly need parent_drp so that it can be resolved in combine
        # TODO likely need to modify the type to be "object_meta" instead of path
        # and there will be no reverse mapping to an object at the moment
        # the alternative would be to have a top level "path_metadata" property
        # that simply contained the path meta, and we can pull remote_id out as the
        # id for the object itself, then we can put "contents" for files that we
        # extract from and have another property for "derived_from_elsewhere" or whatever
        with open(export_path, 'wt') as f:
            json.dump(blob, f, sort_keys=True, indent=2, cls=JEncode)

        msg = f'extract object metadata written to {export_path}'
        log.log(9, msg)

    else:
        # XXX FIXME TODO right now we only process files that
        # we have already retrieved because setting up the
        # iterative extract, fetch, extract, fetch process is
        # too much
        msg = f'not extracting because not fetched {path.name}'
        log.warning(msg)


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
        #'dataset_id': RemoteId,  # FIXME for some reason _cache_jsonMeta doesn't convert this ???
        'dataset_relative_path': Path,
        #'remote_id': RemoteId,
        'size_bytes': aug.FileSize,
        'timestamp_updated': dateparser.parse,
        'parent_drp': Path, 
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
    p = blob['dataset_relative_path'].parent
    if p.name != '':  # we are at the root of the relative path aka '.'
        blob['parent_drp'] = p

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
            json.dump(blob, f, sort_keys=True, indent=2, cls=JEncode)

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


    pathmeta_blob, pathmeta_changed = pathmeta_refresh(dataset_id, object_id, path)

    export_path = extract_export_path(dataset_id, object_id)
    done = export_path.exists() and not pathmeta_changed  # FIXME this is obviously wrong, e.g. what happens if the remote changes the file name or moves a file? we need a bit more info related update times or reparents, for now we will have to run time force=True to avoid stale, or we run with force=True when it is publication time, OR we don't store pathmeta at all and always rederive it during extract ... that seems better, yes ...
    object_type_raw, fetch_fun, extract_fun = do_actually_extract(path)
    # FIXME hard to pass extract_fun directly so would have to
    # rederive in subprocess version not that I think we will be using
    # that variant

    if done and not force:
        success = 'already-done'
        msg = f'{object_id} already done {path}'
        log.log(9, msg)
        return path, object_id, object_type_raw, success, updated

    elif extract_fun is None:
        success = 'no-extract-fun-and-pathmeta-already-written'
        msg = f'{object_id} pathmeta already written and no extract_fun {path}'
        log.log(9, msg)
        return path, object_id, object_type_raw, success, updated

    elif debug or not subprocess:
        # TODO or raw object_type_raw in IMPORTANT i.e. stuff that should be in memory ... but that is and optimization
        try:
            extract(dataset_id.uuid, path.as_posix(), object_type_raw, extract_fun=extract_fun)
            success = True
        except Exception as e:
            success = False
            log.exception(e)

        return path, object_id, object_type_raw, success, updated

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

    return path, object_id, object_type_raw, success, updated


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


def from_dataset_path_extract_object_metadata(dataset_path, time_now=None, force=False, debug=False):
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
    if True:  # TODO need to sort out the right way to do this when running in sparcron ... especially wrt when the step is actually "done"
        results = [subprocess_extract(dataset_id, path, time_now, force=force, debug=debug, subprocess=False) for path in rchildren]
    else:
        # XXX FIXME LOL yeah, maybe don't run this with a completely open async because it will spawn n processes
        # where n = THE NUMBER OF FILES IN A DATASET EEEEE !??!?! pypy3 has non-trivial minimal memory usage
        # FIXME it is also quite likely that we don't actually need to run in a subprocess (except for the logging bit maybe)
        # because the overhead of spinning up a whole new pypy3 vm for a single file is silly, we already spin up one
        # for export which protects from the memory issues with e.g. lxml
        results = Async(rate=20)(deferred(subprocess_extract)(dataset_id, path, time_now, force=force) for path in rchildren)

    updated_cache_transitive = None
    bads = []
    object_id_types = []
    for path, id, type, ok, updated in results:
        if not ok:
            bads.append((path, id))
            updated_cache_transitive = None
            object_id_types = []
            continue
        elif bads:
            continue

        if updated_cache_transitive is None:
            updated_cache_transitive = updated
        elif updated > updated_cache_transitive:
            updated_cache_transitive = updated

        object_id_types.append((id, type))

    if bads:
        fails = '\n'.join(f'{id} {"" if debug else object_logdir(id) / "LATEST" / "stdout.log"} {path}' for path, id in bads)
        msg = f'{dataset_id} object meta fails:\n{fails}'
        log.error(msg)
        msg = f'cannot continue for {dataset_id}'
        breakpoint()
        raise ValueError(msg)  # FIXME error type
    else:
        return updated_cache_transitive, object_id_types


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
        if mimetype in (
                'application/x.vnd.mbfbioscience.metadata+xml',
                'application/x.vnd.mbfbioscience.neurolucida+xml',
                'application/x.vnd.mbfbioscience.vesselucida+xml',  # does have path_mbf
        ):
            if 'images' not in contents:
                # FIXME TODO logging
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
                    'input': extracted,  # FIXME not quite right, maybe prefer just the pointer and deref later? or maybe just the image section?
                    # TODO contours go here i think
                }

                target_mbf_path = pathlib.PurePath(target_mbf_path_string)
                target_drp = None
                if target_mbf_path.name in name_drps_index:
                    candidates = name_drps_index[target_mbf_path.name]
                    if len(candidates) == 1:
                        rec['prov']['mapped_by'] = 'exact-pathname-match'
                        target_drp, = candidates
                    else:
                        breakpoint()
                else:

                    def numonly(name):
                        nums = [int(_) for _ in ''.join(c if c.isdigit() else ' ' for c in name).split()] # FIXME or c == '.' ?
                        nn = ' '.join(str(_) for _ in nums)
                        return nn, nums, set(nums)

                    def entcomp(nums1, nums2):
                        both = set(nums1) & set(nums2)
                        return sum(log2(n) for n in both)

                    suffix = target_mbf_path.suffix
                    suffixes.add(suffix)
                    lev_subset = [(n, *numonly(n)) for n in name_drps_index if n.endswith(suffix)]
                    if lev_subset:
                        jn = [n[0] for n in lev_subset]
                        tmpname = target_mbf_path.name
                        tmp_no, tmp_nums, tmp_sn = numonly(tmpname)
                        #[(n, no) for n, no in lev_subset if all([num in no.split() for num in tmp_no.split()])]
                        #hrm = sorted([(entcomp(tmp_nums, nums), n, nums) for n, no, nums in lev_subset], reverse=True)

                        levs = sorted((
                            - entcomp(tmp_nums, snums),
                            #levenshteinDistance(tmp_no, no),
                            levenshteinDistance(tmpname, n),
                            n, no) for n, no, nums, snums in lev_subset)

                        rep = [(0, 0, 0, tmpname, tmp_no)] + levs
                        rep[:10]
                        log.log(9, '\n' + pprint.pformat(rep[:5]))
                    else:
                        # sometimes maybe they get the suffix wrong?
                        msg = ('somehow there are no files that even match the '
                               'extension of an mbf path !?!??! maybe your princess is in another dataset ??? '
                               f'{dataset_id} {object_id} {drp} {target_mbf_path}')
                        log.log(9, msg)

                    if False:
                        #cutoff = 10
                        # XXX super expensive
                        levdists = sorted((levenshteinDistance(tmpname, n), n) for n in levdist_subset)
                        if levdists[0][0] > cutoff and ' ' in tmpname and ' ' not in levdists[0][1]:
                            # try again with underscore
                            tmpname_us = tmpname.replace(' ', '_')
                            levdists_us = sorted((levenshteinDistance(tmpname_us, n), n) for n in levdist_subset)


                        # also not actually good if there are numbers, because those have
                        # super high weight compared to the rest, you can't just mutate them
                        lev_numonly = [numonly(n) for n in levdist_subset]
                        levno = sorted((levenshteinDistance(tmp_numonly, n), n) for n in lev_numonly)

                        breakpoint()

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
    did_index = {}
    for object_id in oids:
        blob = path_json(pathmeta_version_export_path(dataset_id, object_id))
        drp = pathlib.PurePath(blob['dataset_relative_path'])
        #dirs[drp] = blob  # FIXME check on memory sizes here
        #id_drp[object_id] = drp
        did_index[drp] = object_id

    return id_drp, did_index


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


def combine(dataset_id, object_id, type, drp_index):
    blob = {}
    blob['type'] = 'combine-object-metadata'  #'combined-metadata'
    blob['__combine_version__'] = __combine_version__

    pathmeta_blob = path_json(pathmeta_version_export_path(dataset_id, object_id))
    blob['path_metadata'] = pathmeta_blob

    extract_path = extract_export_path(dataset_id, object_id)
    if extract_path.exists():
        extract_blob = path_json(extract_path)
        blob['extracted'] = extract_blob  # FIXME duped keys

    drp = pathlib.PurePath(blob['path_metadata']['dataset_relative_path'])
    if (has_recs := drp in drp_index):
        blob['external_records'] = drp_index.pop(drp)
    elif 'mimetype' in blob['path_metadata'] and blob['path_metadata']['mimetype'] == 'inode/directory':
        pass  # we don't expect dirs to have metadata (though they can)
    else:
        # manifests technically aren't required as of 2.1.0
        msg = f'{dataset_id} {object_id} {drp} has no records of any kind'
        log.warning(msg)  # TODO embed error

    combine_path = combine_version_export_path(dataset_id, object_id)
    if not combine_path.parent.exists():
        combine_path.parent.mkdir(exist_ok=True, parents=True)

    with open(combine_path, 'wt') as f:
        # JEncode minimally needed to deal with Path :/
        json.dump(blob, f, sort_keys=True, indent=2, cls=JEncode)

    msg = f'combine object metadata written to {combine_path}'
    log.log(9, msg)

    return drp, has_recs, blob


def from_dataset_id_object_id_types_combine(dataset_id, object_id_types, updated_cache_transitive=None, keep_in_mem=False):
    types = {}
    for oid, type in object_id_types:
        # FIXME why do oids for packages have a file_id associated when we go to put them in source_id ???
        # we have to load all in the first past to populate the info we need

        if type not in types:
            types[type] = []

        types[type].append(oid)

    pex = {
        'inode/directory': pex_dirs,
        None: pex_default,
        'manifest': pex_manifests,
        'xml': pex_xmls,
    }

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

    did_index = None
    xml_index = None
    xml_unmapped = None
    drp_index = {}
    process_order = 'inode/directory', None, 'manifest', 'xml'  # references within a type have to be resolved in a second pass in that pex
    # first pass
    for type in process_order:
        if type not in types:
            continue

        oids = types[type]
        process_extracted = pex[type]
        inds = dict(
            did_index=did_index,
            name_drps_index=name_drps_index,
            drp_index=drp_index,
            xml_index=xml_index,
        )
        id_drp, index, *rest = process_extracted(dataset_id, oids, **inds)
        if index is not None:
            if type == 'inode/directory':
                did_index = index
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

                drp_index[drp][type] = rec

    #breakpoint()
    # TODO do something about the xml index here
    # passing object_id_type_drps into this function
    # would avoid the need to load all the jsons
    # to retrieve that information ...

    drps = []
    news = 0
    for object_id, type in object_id_types:  # TODO sort these in reverse order so that errors can be added to e.g. manifests
        drp, has_recs, blob = combine(dataset_id, object_id, type, drp_index)
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
