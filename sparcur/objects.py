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
import pathlib
import subprocess
from uuid import UUID
import augpathlib as aug
from pyontutils.asyncd import Async, deferred
from sparcur import exceptions as exc
from sparcur.paths import Path
from sparcur.utils import register_type, log as _log, logd as _logd
from sparcur.config import auth
from sparcur.core import JEncode, HasErrors
from sparcur.utils import transitive_paths, GetTimeNow, PennsieveId as RemoteId

log = _log.getChild('sob')
logd = _logd.getChild('sob')

__extract_version__ = 0
__combine_version__ = 0
__index_version__ = 0

sf_dir_name = 'objs'  # this really is more of an objects index ...
sf_export_base = Path(auth.get_path('export-path')) / sf_dir_name

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
    ttds = '7b2165ef-5153-4a0e-8476-10888d3bb1a5'
    ttds_b64 = 'eyFl71FTSg6EdhCIjTuxpQ'
    assert uuid_cache_branch(2, 3, ttds) == '7b/21/65/7b2165ef-5153-4a0e-8476-10888d3bb1a5'
    assert uuid_cache_branch(2, 1, ttds) == '7b/7b2165ef-5153-4a0e-8476-10888d3bb1a5'
    assert uuid_cache_branch(1, 5, ttds) == '7/b/2/1/6/7b2165ef-5153-4a0e-8476-10888d3bb1a5'

    assert uuid_cache_branch(2, 3, ttds, dob64=True) == 'ey/Fl/71/eyFl71FTSg6EdhCIjTuxpQ'
    assert uuid_cache_branch(2, 1, ttds, dob64=True) == 'ey/eyFl71FTSg6EdhCIjTuxpQ'
    assert uuid_cache_branch(1, 5, ttds, dob64=True) == 'e/y/F/l/7/eyFl71FTSg6EdhCIjTuxpQ'

    top_paths = [
        [
            extract_path(),  # FIXME these are really x_version_path ...
            combine_path(),
            index_path(),

            index_combine_latest_path(),
            index_combine_archive_path(),
            
         ]
    ]

    #tds = ttds
    tds = '645267cb-0684-4317-b22f-dd431c6de323'  # small dataset with lots of manifests
    # tds = '21cd1d1b-9434-4957-b1d2-07af75ad3546'  # big xml test sadly too many files (> 80k) to be practical for testing
    #tds = '46bcac1d-3e45-499b-82a8-0ee1852bcc4d'  # manifest column naming issues
    tds = '57466879-2cdd-4af2-8bd6-7d867423c709'  # has vesselucida files
    tds = '3bb4788f-edab-4f04-8e96-bfc87d69e4e5'  # much better, only 100ish files with xmls
    dataset_path = (Path('/home/tom/files/sparc-datasets/') / tds / 'dataset').resolve()
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
    xml_sources = [b for b in blobs if 'extracted' in b and 'xml' == b['extracted']['object_type']]  # FIXME annoying to query
    apparent_targets = [x['external_records']['manifest']['input']['description'][26:] for x in xml_sources]
    maybe_matches = [b for b in blobs if b['path_metadata']['basename'] in apparent_targets]

    breakpoint()


def create_current_version_paths():
    #log.debug('setup test')
    ext_path = extract_path()
    com_path = combine_path()
    idx_path = index_path()
    archive_path = index_combine_archive_path()

    #log.debug(('mkdir', ext_path))
    if not ext_path.exists():
        ext_path.mkdir()

    #log.debug(('mkdir', com_path))
    if not com_path.exists():
        com_path.mkdir()

    #log.debug(('mkdir', idx_path))
    if not idx_path.exists():
        idx_path.mkdir()

    #log.debug(('mkdir', archive_path))
    if not archive_path.exists():
        archive_path.mkdir()

    resymlink_index_combine_latest()  # FIXME should this always run?


def b64uuid(uuid):  # XXX moved onto the id class
    return base64.urlsafe_b64encode(UUID(uuid).bytes)[:-2].decode()


def uuid_cache_branch(chars, count, uuid, dob64=False):  # XXX moved onto the id class
    # XXX watch out for count * chars > 8
    # assert count * chars <= 8  # for raw uuid
    id = b64uuid(uuid) if dob64 else uuid
    parents = [
        id[start:stop] for start, stop in
        zip(range(0, (chars * count + 1), chars), range(chars, (chars * count + 1), chars))]
    #log.debug((chars, count, uuid, parents))
    prefix = '/'.join(parents)
    return f'{prefix}/{id}'


def resymlink_index_combine_latest(index_version=None):
    clp = index_combine_latest_path(index_version=index_version)
    cp = combine_path()
    target = cp.relative_path_from(clp)

    if clp.exists() or clp.is_symlink():
        # unsymlink old
        clp.unlink()

    #log.debug(('ln -s', target, clp))
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


def extract_path(extract_version=None):
    extract_version = str(__extract_version__ if extract_version is None else extract_version)
    #relative = f'{extract_dir_name}/{extract_version}'
    return sf_export_base / extract_dir_name / extract_version


def _dataset_path_obj_path(dataset_id, object_id):
    dataset_path = dataset_id.uuid_cache_path_string(1, 1, use_base64=True)
    obj_path = object_id.uuid_cache_path_string(1, 1, use_base64=True)
    return dataset_path, obj_path


def extract_export_path(dataset_id, object_id, extract_version=None):
    dataset_path, obj_path = _dataset_path_obj_path(dataset_id, object_id)
    base_path = extract_path(extract_version=extract_version)
    return base_path / dataset_path / obj_path


def combine_path(combine_version=None):
    combine_version = str(__combine_version__ if combine_version is None else combine_version)
    return sf_export_base / combine_dir_name / combine_version


def combine_version_export_path(dataset_id, object_id, combine_version=None):
    dataset_path, obj_path = _dataset_path_obj_path(dataset_id, object_id)
    base_path = combine_path(combine_version=combine_version)
    return base_path / dataset_path / obj_path


def index_combine_latest_path(index_version=None):
    base_path = index_path(index_version=index_version)
    return base_path / combine_latest_dir_name


def index_combine_archive_path(index_version=None):
    base_path = index_path(index_version=index_version)
    return base_path / combine_archive_dir_name


def index_combine_latest_export_path(dataset_id, object_id, index_version=None):
    dataset_path, obj_path = _dataset_path_obj_path(dataset_id, object_id)
    # we keep this in the index dir to save 3 bytes of ../
    base_path = index_combine_latest_path(index_version=index_version)
    return base_path / dataset_path / obj_path


def index_combine_archive_export_path(dataset_id, object_id, index_version=None):
    dataset_path, obj_path = _dataset_path_obj_path(dataset_id, object_id)
    # we keep this in the index dir to save 3 bytes of ../
    base_path = index_combine_archive_path(index_version=index_version)
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
    elif path.name == 'manifest.xlsx':
        object_type_raw = 'manifest'
        # FIXME need to match more name patterns
        # see sc.metadata_filename_pattern
        from sparcur import datasets as dat
        def extract_fun(path):
            # FIXME this need the template schema version to work ... at least in theory
            # the internals of the implementation mean that if it is missing it isn't fatal
            extracted = {
                'type': 'extracted-metadata',
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
                'type': 'extracted-metadata',
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


register_type(None, 'object-metadata-raw')  # currently unmapped
def extract(dataset_uuid, path_string, extract_fun=None):
    """ actually run extraction for a single path
    see also sparcur.pipelines.XmlFilePipeline._do_xml_metadata

    we convert back from dataset_uuid to simplify passing to subprocess
    """
    he = HasErrors(pipeline_stage='objects.extract')

    dataset_id = RemoteId(dataset_uuid, type='dataset')
    path = Path(path_string)
    object_id = path.cache_identifier

    cjm = path._cache_jsonMetadata()
    p = cjm['dataset_relative_path'].parent
    if p.name != '':  # we are at the root of the relative path aka '.'
        cjm['parent_drp'] = p

    blob = {
        '__extract_version__': __extract_version__,  # both wind up embedded at the end
        'type': 'object-metadata-raw',
        'path_metadata': cjm,
    }

    if extract_fun is None:  # subprocess case
        object_type_raw, fetch_fun, extract_fun = do_actually_extract(path)

    if extract_fun:
        if path.is_file():
            # FIXME TODO ... kind of external vs internal type ...
            try:
                extracted = extract_fun(path)  # FIXME really should be extract_fun(path)
                blob['extracted'] = extracted
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
        else:
            # XXX FIXME TODO right now we only process files that
            # we have already retrieved because setting up the
            # iterative extract, fetch, extract, fetch process is
            # too much
            msg = f'not extracting because not fetched {path.name}'
            log.warning(msg)

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
        # JEncode minimally needed to deal with Path :/
        json.dump(blob, f, sort_keys=True, indent=2, cls=JEncode)

    msg = f'extract object metadata written to {export_path}'
    log.log(9, msg)


def argv_extract(dataset_uuid, path, force=False):
    return sys.executable, '-c', (
        'from sparcur.objects import extract;'  # FIXME TODO move to another file to avoid import overhead
        f'extract({dataset_uuid!r}, {path.as_posix()!r}, force={force})')


path_log_base = auth.get_path('log-path')
path_log_objects = path_log_base / 'objs'


def object_logdir(object_id):
    return path_log_objects / object_id.uuid_cache_path_string(2, 3)


def subprocess_extract(dataset_id, path, time_now, force=False, debug=False, subprocess=False):
    # FIXME TODO probably wire this into sparcron more directly
    # to avoid calling this via Async deferred
    object_id = path.cache_identifier #RemoteId(path.cache_id)
    if path.is_dir():
        _updated = path.getxattr('bf.updated').decode()
        updated = aug.PathMeta(updated=_updated).updated
    else:
        updated = path.updated_cache_transitive()  # something of a hack

    export_path = extract_export_path(dataset_id, object_id)
    done = export_path.exists()
    object_type_raw, fetch_fun, extract_fun = do_actually_extract(path)
    # FIXME hard to pass extract_fun directly so would have to
    # rederive in subprocess version not that I think we will be using
    # that variant

    if done and not force:
        success = 'already-done'
        msg = f'{object_id} already done {path}'
        log.log(9, msg)
        return path, object_id, object_type_raw, success, updated

    if debug or not subprocess:  # TODO or raw object_type_raw in IMPORTANT i.e. stuff that should be in memory ... but that is and optimization
        try:
            extract(dataset_id.uuid, path.as_posix(), extract_fun=extract_fun)
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

    argv = argv_extract(dataset_id.uuid, path, force=force)
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
    """ given a dataset_path extract path-metadata and object-metadata

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


def pex_manifests(dataset_id, oids):
    # manifest data
    # construct the individual manifest paths
    # merge into a single global manifest

    def add_drp(manifest_drp, manifest_parent, rec):
        # XXX ah yes, that's why I wanted to implement my own PurePath variant ...
        # who the heck leaves simplify/normpath out !?!?!
        if 'filename' in rec:
            drp = manifest_parent.__class__(os.path.normpath(manifest_parent / rec['filename']))
            out = {
                'type': 'manifest_record',  # a single manifest record for a file
                'source': manifest_drp,
                'dataset_relative_path': drp,  # make it easy to check consistency without fancy steps
                'input': rec,  # FIXME vs inputs: [rec] to support multiple inputs technically allowed ... no, we need another way to handle that
            }
            return drp, out
        else:
            # might be an improperly filled out pattern manifest
            # the blob itself should already have an error though?
            # but TODO we might want to embed again to be sure?
            msg = ('manifest record is missing "filename" column !??!\n'
                   f'{manifest_drp} {rec}')
            log.warning(msg)
            return None, None

    # we don't use .utils.fromJson here, I don't think we need it? or at least don't need it yet?
    drp_manifest_record_index = {}
    for object_id in oids:
        blob = path_json(extract_export_path(dataset_id, object_id))
        #ir = fromJson(blob)  # indeed we can't load type: path
        manifest_drp = pathlib.PurePath(blob['path_metadata']['dataset_relative_path'])
        manifest_parent = manifest_drp.parent  # FIXME top level
        if 'extracted' not in blob:
            continue  # TODO logging

        extracted = blob['extracted']
        if 'contents' not in extracted:
            continue  # TODO logging

        contents = extracted['contents']
        for c in contents:
            drp, rec = add_drp(manifest_drp, manifest_parent, c)
            if drp is None:
                # log.error('a malformed manifest has made it through to this stage')
                # we logged above so just pass here or TODO embed an error in the
                # manifest blob itself to be written during manifest combine?
                pass
            elif drp in drp_manifest_record_index:
                msg = 'NotImplementedError TODO merge multiple to same file'
                log.info(msg)
                msg = (
                    f'multiple manifest records for {dataset_id}:{drp} '
                    f'from {manifest_drp} and {drp_manifest_record_index[drp]["source"]}')
                log.error(msg)
                # FIXME TODO embed error in rec
                continue
            else:
                drp_manifest_record_index[drp] = rec

    return drp_manifest_record_index


def pex_xmls(dataset_id, oids):
    drp_xml_ref_index = {}
    for object_id in oids:
        blob = path_json(extract_export_path(dataset_id, object_id))
        xml_drp = pathlib.PurePath(blob['path_metadata']['dataset_relative_path'])

        if 'extracted' not in blob:
            continue

        extracted = blob['extracted']  # FIXME could be missing ... ??
        mimetype = extracted['mimetype'] if 'mimetype' in extracted else None
        if 'contents' not in extracted:
            # FIXME TODO logging
            continue

        contents = extracted['contents']  # FIXME could be missing ...
        if mimetype in (
                'application/x.vnd.mbfbioscience.metadata+xml',
                'application/x.vnd.mbfbioscience.neurolucida+xml',
                'application/x.vnd.mbfbioscience.vesselucida+xml',  # does have path_mbf
        ):
            if 'images' not in contents:
                # FIXME TODO logging
                continue

            drps = [p for i in contents['images'] if 'path_mbf' in i for p in i['path_mbf']]
            for drp in drps:
                rec = {
                    'type': 'mbf_xml_path_ref',
                    'source': xml_drp,  # FIXME probably want source index id and other prov stuff as well for quick deref
                    #'dataset_relative_path': 'LOL',  # yeah ... no way this is going to work
                    'input': extracted,  # FIXME not quite right, maybe prefer just the pointer and deref later? or maybe just the image section?
                    # TODO contours go here i think
                }
                if drp in drp_xml_ref_index:
                    msg = 'NotImplementedError TODO multiple seg files per images is possible'
                    log.info(msg)
                    msg = (
                        f'multiple xml refs for {dataset_id}:{drp} '  # lol
                        f'from {xml_drp} and {drp_xml_ref_index[drp]["source"]}')
                    log.error(msg)
                    continue
                else:
                    drp_xml_ref_index[drp] = rec

    return drp_xml_ref_index  # the keys here are going to have to be matched or something


def pex_dirs(dataset_id, oids):
    # TODO build the parent lookup index to replace parent_drp
    # i think what we want to do is embed all the parents up to
    # the one just before primary/derivative etc. since we have
    # the sub- and sam- prefixes that can make it a single
    # lookup to pull full subject metadata, because the id
    # will literally be in the parents ... that turns out to
    # be quite useful
    return {}


def combine(dataset_id, object_id, type, drp_index):
    blob = path_json(extract_export_path(dataset_id, object_id))
    blob['type'] = 'object-metadata'  #'combined-metadata'
    blob['__combine_version__'] = __combine_version__
    drp = pathlib.PurePath(blob['path_metadata']['dataset_relative_path'])
    if (has_recs := drp in drp_index):
        blob['external_records'] = drp_index.pop(drp)
    elif blob['path_metadata']['mimetype'] == 'inode/directory':
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
        if type is None:
            continue

        if type not in types:
            types[type] = []

        types[type].append(oid)

    pex = {
        'manifest': pex_manifests,
        'xml': pex_xmls,
        'inode/directory': pex_dirs,
    }
    drp_index = {}
    xml_index = None
    parent_index = None
    # first pass
    for type, oids in types.items():
        process_extracted = pex[type]
        if process_extracted is not None:
            index = process_extracted(dataset_id, oids)
            if index is not None:
                if type == 'xml':
                    xml_index = index
                elif type == 'inode/directory':
                    parent_index = index
                else:
                    for drp, rec in index.items():
                        if drp not in drp_index:
                            drp_index[drp] = {'type': 'internal_references'}  # FIXME what is the name for this type ...

                        drp_index[drp][type] = rec

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
