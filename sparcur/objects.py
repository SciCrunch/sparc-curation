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
import base64
from uuid import UUID
import augpathlib as aug
from sparcur.paths import Path
from sparcur.utils import log as _log
from sparcur.config import auth

log = _log.getChild('sob')

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

    dataset_path = (Path('/home/tom/files/sparc-datasets/') / ttds / 'dataset').resolve()
    cs = list(dataset_path.rchildren)
    dataset_uuid = dataset_path.cache.identifier.uuid


    def all_paths(c):
        return [
            extract_export_path(dataset_uuid, c.cache.identifier.uuid),

            combine_version_export_path(dataset_uuid, c.cache.identifier.uuid),
            index_combine_latest_export_path(dataset_uuid, c.cache.identifier.uuid),
            index_combine_archive_export_path(dataset_uuid, c.cache.identifier.uuid),

            index_obj_path(c.cache.identifier.uuid),
            index_obj_symlink_latest(dataset_uuid, c.cache.identifier.uuid),
            index_obj_symlink_archive(dataset_uuid, c.cache.identifier.uuid),
        ]

    aps = top_paths + [all_paths(c) for c in cs]
    [print(p) for paths in aps for p in paths]
    #create_current_version_paths()

    from_dataset_path_extract_object_metadata(dataset_path, force=True, debug=True)

def create_current_version_paths():
    log.debug('setup test')
    ext_path = extract_path()
    com_path = combine_path()
    idx_path = index_path()
    archive_path = index_combine_archive_path()

    log.debug(('mkdir', ext_path))
    # ext_path.mkdir()

    log.debug(('mkdir', com_path))
    # com_path.mkdir()

    log.debug(('mkdir', idx_path))
    # idx_path.mkdir()

    log.debug(('mkdir', archive_path))  # archive_path.mkdir()

    resymlink_index_combine_latest()


def b64uuid(uuid):
    return base64.urlsafe_b64encode(UUID(uuid).bytes)[:-2].decode()


def uuid_cache_branch(chars, count, uuid, dob64=False):  # XXX watch out for count * chars > 8
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
    log.debug(('ln -s', target, clp))
    #if cp.exists()
    # clp.symlink_to(target)

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

def extract_export_path(dataset_uuid, object_uuid, extract_version=None):
    obj_path = uuid_cache_branch(1, 1, object_uuid, dob64=True)
    dataset_dir = uuid_cache_branch(1, 1, dataset_uuid, dob64=True)
    #relative = f'{dataset_dir}/{obj_path}'
    base_path = extract_path(extract_version=extract_version)
    return base_path / dataset_dir / obj_path

def combine_path(combine_version=None):
    combine_version = str(__combine_version__ if combine_version is None else combine_version)
    return sf_export_base / combine_dir_name / combine_version

def combine_version_export_path(dataset_uuid, object_uuid, combine_version=None):
    obj_path = uuid_cache_branch(1, 1, object_uuid, dob64=True)
    #dataset_dir = b64uuid(dataset_uuid)
    dataset_dir = uuid_cache_branch(1, 1, dataset_uuid, dob64=True)
    #relative = f'{combine_dir_name}/{combine_version}/{dataset_dir}/{obj_path}'
    base_path = combine_path(combine_version=combine_version)
    return base_path / dataset_dir / obj_path

def index_combine_latest_path(index_version=None):
    base_path = index_path(index_version=index_version)
    return base_path / combine_latest_dir_name


def index_combine_archive_path(index_version=None):
    base_path = index_path(index_version=index_version)
    return base_path / combine_archive_dir_name


def index_combine_latest_export_path(dataset_uuid, object_uuid, index_version=None):
    obj_path = uuid_cache_branch(1, 1, object_uuid, dob64=True)
    #dataset_dir = b64uuid(dataset_uuid)
    dataset_dir = uuid_cache_branch(1, 1, dataset_uuid, dob64=True)
    # we keep these in the index dir to save 3 bytes of ../
    base_path = index_combine_latest_path(index_version=index_version)
    #relative = f'{dataset_dir}/{obj_path}'
    return base_path / dataset_dir / obj_path


def index_combine_archive_export_path(dataset_uuid, object_uuid, index_version=None):
    #index_version = __index_version__ if index_version is None else index_version
    obj_path = uuid_cache_branch(1, 1, object_uuid, dob64=True)
    #dataset_dir = b64uuid(dataset_uuid)
    dataset_dir = uuid_cache_branch(1, 1, dataset_uuid, dob64=True)
    # we keep these in the index dir to save 3 bytes of ../
    base_path = index_combine_archive_path(index_version=index_version)
    return base_path / dataset_dir / obj_path
    #relative = f'{base_path}/{dataset_dir}/{obj_path}'
    #return sf_export_base / relative


def index_path(index_version=None):
    index_version = str(__index_version__ if index_version is None else index_version)
    #relative = f'{index_dir_name}/{index_version}'
    return sf_export_base / index_dir_name / index_version


def index_obj_path(object_uuid, index_version=None):
    """ index_version is included in the path so that if there is
    a convention change e.g. from 16 ** 3 ** 2 to 16 ** 4 ** 2
    the change can be handled explicitly
    """
    #index_version = __index_version__ if index_version is None else index_version
    obj_path = uuid_cache_branch(3, 2, object_uuid)  # 4096 expansion ratio but needed to shave bytes to fit in i_block, can hold 1e11 w/ ~6k
    #relative = f'{index_dir_name}/{index_version}/{obj_path}'
    return index_path(index_version=index_version) / obj_path


def index_obj_symlink_latest(dataset_uuid, object_uuid, index_version=None):
    clep = index_combine_latest_export_path(dataset_uuid, object_uuid)
    icp = index_obj_path(object_uuid, index_version=index_version)
    return clep.relative_path_from(icp)


def index_obj_symlink_archive(dataset_uuid, object_uuid, index_version=None):
    caep = index_combine_archive_export_path(dataset_uuid, object_uuid)
    icp = index_obj_path(object_uuid, index_version=index_version)
    return caep.relative_path_from(icp)


import json
import subprocess
from sparcur.core import JEncode
from sparcur.utils import transitive_files, GetTimeNow, PennsieveId as RemoteId
from pyontutils.asyncd import Async, deferred
from sparcur.sparcron.core import SubprocessException  # FIXME move to sparcur.exceptions


def extract(dataset_uuid, path_string, force=False):
    """ actually run extraction for a single path
    see also sparcur.pipelines.XmlFilePipeline._do_xml_metadata
    """
    path = Path(path_string)
    id = path.cache_identifier #RemoteId(path.cache_id)
    msg = f'yes we get here for {path}'
    log.info(msg)
    # first pass just dump the path json meta
    not_done = True
    if not_done or force:
        cjm = path._cache_jsonMetadata()
        path = extract_export_path(dataset_uuid, id.uuid)
        if not path.parent.exists():
            path.parent.mkdir(exist_ok=True, parents=True)

        # TODO almost certainly need parent_drp so that it can be resolved in combine
        # TODO likely need to modify the type to be "object_meta" instead of path
        # and there will be no reverse mapping to an object at the moment
        # the alternative would be to have a top level "path_metadata" property
        # that simply contained the path meta, and we can pull remote_id out as the
        # id for the object itself, then we can put "contents" for files that we
        # extract from and have another property for "derived_from_elsewhere" or whatever
        with open(path, 'wt') as f:
            # JEncode minimally needed to deal with Path :/
            json.dump(cjm, f, sort_keys=True, indent=2, cls=JEncode)

        msg = f'object metadata written to {path}'
        log.info(msg)


import sys

def argv_extract(dataset_uuid, path, force=False):
    return sys.executable, '-c', (
        'from sparcur.objects import extract;'  # FIXME TODO move to another file to avoid import overhead
        f'extract({dataset_uuid!r}, {path.as_posix()!r}, force={force})')


path_log_base = auth.get_path('log-path')
path_log_objects = path_log_base / 'objs'

def subprocess_extract(dataset_uuid, path, time_now, force=False, debug=False):
    # FIXME TODO probably wire this into sparcron more directly
    # to avoid calling this via Async deferred
    id = path.cache_identifier #RemoteId(path.cache_id)

    timestamp = time_now.START_TIMESTAMP_LOCAL_FRIENDLY
    logdir = path_log_objects / uuid_cache_branch(2, 3, id.uuid)
    logfile = logdir / timestamp / 'stdout.log'
    latest = logdir / 'LATEST'
    if not logfile.parent.exists():
        logfile.parent.mkdir(parents=True)

    if latest.exists():
        latest.unlink()

    latest.symlink_to(timestamp)

    if debug:
        try:
            extract(dataset_uuid, path.as_posix(), force=force)
            extracted = True
        except Exception as e:
            extracted = False
            log.exception(e)

        return path, extracted

    argv = argv_extract(dataset_uuid, path, force=force)
    try:
        with open(logfile, 'wt') as logfd:
            try:
                p = subprocess.Popen(argv, stderr=subprocess.STDOUT, stdout=logfd)
                out = p.communicate()
                if p.returncode != 0:
                    raise SubprocessException(f'oops objs return code was {p.returncode}')

                extracted = True
            except KeyboardInterrupt as e:
                p.send_signal(signal.SIGINT)
                raise e

        log.info(f'DONE: {id}')
    except SubprocessException as e:
        log.critical(f'FAIL: {RemoteId(type="dataset", uuid=dataset_uuid)} {id} | {b64uuid(dataset_uuid)}  {base64uuid(id.uuid)}')
        log.exception(e)
        extracted = False

    return path, extracted


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

    dataset_uuid = dataset_path.cache_identifier.uuid
    rfiles = transitive_files(dataset_path)
    if debug:
        result = [subprocess_extract(dataset_uuid, path, time_now, force=force, debug=debug) for path in rfiles]
    else:
        result = Async()(deferred(subprocess_extract)(dataset_uuid, path, time_now, force=force) for path in rfiles)

    breakpoint()


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
