import os
#import types
#from nibabel import nifti1
#from pydicom import dcmread
#from scipy.io import loadmat
import boto3
import botocore
import requests
#from requests.exceptions import HTTPError, ConnectionError
from joblib import Parallel, delayed
if 'BLACKFYNN_LOG_LEVEL' not in os.environ:
    # silence agent import warning
    os.environ['BLACKFYNN_LOG_LEVEL'] = 'CRITICAL'

from blackfynn import log as _bflog
# blackfynn.log sets logging.basicConfig which pollutes logs from
# other programs that are sane and do not use the root logger
# so we have to undo the damage done by basic config here
# we add the sparcur local handlers back in later
from sparcur.utils import log, silence_loggers
for __bflog in (_bflog.get_logger(), _bflog.get_logger("blackfynn.agent")):
    silence_loggers(__bflog)
    __bflog.addHandler(log.handlers[0])

from blackfynn import Blackfynn, Collection, DataPackage, Organization, File
from blackfynn import Dataset, BaseNode
from blackfynn import base as bfb
from blackfynn.api import agent
from blackfynn.api import transfers
from blackfynn.api.data import PackagesAPI
from pyontutils.utils import Async, deferred
from pyontutils.iterio import IterIO
from sparcur import monkey
from sparcur import exceptions as exc
from sparcur.utils import BlackfynnId, ApiWrapper


def id_to_type(id):
    #if isinstance(id, BlackfynnId):  # FIXME this is a bad place to do this (sigh)
        #return {'package': DataPackage,
                #'collection':Collection,
                #'dataset': Dataset,
                #'organization': Organization,}[id.type]

    if id.startswith('N:package:'):
        return DataPackage
    elif id.startswith('N:collection:'):
        return Collection
    elif id.startswith('N:dataset:'):
        return Dataset
    elif id.startswith('N:organization:'):
        return Organization


PackagesAPI._id_to_type = id_to_type


def upload_fileobj(
        file,  # aka Path
        s3_host,
        s3_port,
        s3_bucket,
        s3_keybase,
        region,
        access_key_id,
        secret_access_key,
        session_token,
        encryption_key_id,
        upload_session_id=None,
        ):
    """ streaming upload
        the object passed in as 'file'
        doesn't have to be Path at all
        it just needs to implement the following methods
        `name`, `size`, and `data`
    """
    local_path = file

    try:
        # account for dev connections
        resource_args = {}
        config_args = dict(signature_version='s3v4')
        if 'amazon' not in s3_host.lower() and len(s3_host)!=0:
            resource_args = dict(endpoint_url="http://{}:{}".format(s3_host, s3_port))
            config_args = dict(s3=dict(addressing_style='path'))

        # connect to s3
        session = boto3.session.Session()
        s3 = session.client('s3',
            region_name = region,
            aws_access_key_id = access_key_id,
            aws_secret_access_key = secret_access_key,
            aws_session_token = session_token,
            config = botocore.client.Config(**config_args),
            **resource_args
        )

        # s3 key
        s3_key = '{}/{}'.format(s3_keybase, local_path.name)

        # override seek to raise an IOError so
        # we don't get a TypeError
        # FIXME IterIO stores a buffer of the whole generator >_<
        f = IterIO(local_path.data, sentinel=b'')
        def _seek(self, *args):
            raise IOError('nope')
        f.seek = _seek

        # upload file to s3
        s3.upload_fileobj(
            Fileobj=f,  # FIXME checksumming wrapper probably ...
            Bucket=s3_bucket,
            Key=s3_key,
            #Callback=progress,
            ExtraArgs=dict(
                ServerSideEncryption="aws:kms",
                SSEKMSKeyId=encryption_key_id,
                #Metadata=checksums,  # hca does it this way
                # annoyingly this means that you have to read the file twice :/
            ))

        return s3_key

    except Exception as e:
        log.error(e)
        raise e


transfers.upload_file = upload_fileobj


def check_files(files):
    """ of course they don't exist """


transfers.check_files = check_files


#_orgid = auth.get('remote-organization')  # HACK
# this works, but then we have to do the multipart upload dance
# which the bf client does not implement at the moment
def get_preview(self, files, append):
    paths = files

    params = dict(
        append = append,
        # dataset_id=?,
    )

    payload = { "files": [
        {
            "fileName": p.name,
            "size": p.size,
            "uploadId": i,
            "processing": True,
        } for i, p in enumerate(paths)
    ]}

    # current web dance
    # /upload/preview/organizations/{_orgid}?append= append ?dataset_id= integer???
    # /upload/fineuploaderchunk/organizations/{_orgid}/id/{importId}?multipartId= from-prev-resp
    # /upload/complete/organizations/{_orgid}/id/{importId}?datasetId=
    response = self._post(
        endpoint=self._uri('/files/upload/preview'),
        #endpoint=self._uri(f'/upload/preview/organizations/{_orgid}'),
        params=params,
        json=payload,
        )

    import_id_map = dict()
    for p in response.get("packages", list()):
        import_id = p.get("importId")
        warnings = p.get("warnings", list())
        for warning in warnings:
            logger.warn("API warning: {}".format(warning))
        for f in p.get("files", list()):
            index = f.get("uploadId")
            import_id_map[paths[index]] = import_id
    return import_id_map


transfers.IOAPI.get_preview = get_preview


def init_file(self, filename):
    """ not using any of this """


agent.UploadManager.init_file = init_file


# monkey patches

# monkey patch to avoid session overflow during async
bfb.ClientSession.session = monkey.patch_session


# monkey patch Blackfynn so that it doesn't eat errors and hide their type
Blackfynn.get = monkey.Blackfynn_get


# monkey patch File.download to
File.download = monkey.File_download


# monkey patch for PackagesAPI.get
PackagesAPI.get = monkey.PackagesAPI_get


# monkey patch Dataset to implement packages endpoint
FakeBFile, _packages = monkey.bind_packages_File(File)
Dataset._dp_class = DataPackage
Dataset._packages = _packages
Dataset.packages = monkey.packages
Dataset.packagesByName = monkey.packagesByName
Dataset.packageTypeCounts = monkey.packageTypeCounts


# monkey patch Dataset to implement dataset users endpoint
Dataset.users = monkey.Dataset_users


# monkey patch Dataset to implement teams endpoint
Dataset.teams = monkey.Dataset_teams


# monkey patch Dataset to implement doi endpoint
Dataset.doi = monkey.Dataset_doi


# monkey patch Dataset to implement delete
Dataset.delete = monkey.Dataset_delete


# monkey patch Dataset to implement contributors endpoint
Dataset.contributors = monkey.Dataset_contributors


# monkey patch Dataset to implement banner endpoint
Dataset.banner = monkey.Dataset_banner


# monkey patch Dataset to implement readme endpoint
Dataset.readme = monkey.Dataset_readme


# monkey patch Dataset to implement status_log endpoint
Dataset.status_log = monkey.Dataset_status_log


# monkey patch Dataset to implement just the dataset metadata endpoint
Dataset.meta = monkey.Dataset_meta


# monkey patch Organization to implement teams endpoint
Organization.teams = monkey.Organization_teams


class PackageMeta:

    class PackageMetaError(Exception):
        """ Ya done goofed part 3 """

    class NotDataPackage(Exception):
        """ only packages have """

    class NoPackageMetaError(Exception):
        """ no package meta """

    def __init__(self, hbfo):
        bfobject = hbfo.bfobject
        if not type(bfobject) == DataPackage:
            msg = f'{bfobject} is not a DataPackage'
            raise self.NotDataPackage(msg)

        elif not hasattr(bfobject, '_json'):
            msg = f'{bfobject} was not spawned from /datasets/{{id}}/packages'
            raise self.NoPackageMetaError(msg)

        self.hbfo = hbfo
        self.bfobject = bfobject
        self.json = hbfo.bfobject._json

    @property
    def size(self):
        self.json['objects']['source']


class BFLocal(ApiWrapper):

    _id_class = BlackfynnId
    _api_class = Blackfynn
    _sec_remote = 'blackfynn'
    _dp_class = DataPackage


class FakeBFLocal(BFLocal):
    class bf:
        """ tricksy hobbitses """
        @classmethod
        def get_dataset(cls, id):
            #import inspect
            #stack = inspect.stack(0)
            #breakpoint()
            #return CacheAsDataset(id)
            class derp:
                """ yep, this is getting called way down inside added
                    in the extras pipeline :/ """
                doi = None
                status = 'FAKE STATUS ;_;'
            return derp

    def __init__(self, project_id, anchor):
        self.organization = CacheAsBFObject(anchor)  # heh
        self.project_name = anchor.name
        self.project_path = anchor.local


class CacheAsBFObject(BaseNode):
    def __init__(self, cache):
        self.cache = cache
        self.id = cache.id
        self.cache.meta

    @property
    def name(self):
        return self.cache.name

    @property
    def parent(self):
        parent_cache = self.cache.parent #.local.parent.cache
        if parent_cache is not None:
            return self.__class__(parent_cache)

    @property
    def parents(self):
        parent = self.parent
        while parent:
            yield parent
            parent = parent.parent

    def __iter__(self):
        for c in self.cache.local.children:
            cache = c.cache
            if cache.is_dataset():
                child = CacheAsDataset(cache)
            elif cache.is_organization():
                child = CacheAsOrganization(cache)
            elif cache.is_dir():
                child = CacheAsCollection(cache)
            elif cache.is_broken_symlink():
                child = CacheAsFile(cache)

            yield child

    @property
    def members(self):
        return []


class CacheAsFile(CacheAsBFObject, File): pass
class CacheAsCollection(CacheAsBFObject, Collection): pass
class CacheAsDataset(CacheAsBFObject, Dataset):
    """ yep, this is getting called way down inside added
        in the extras pipeline :/ """
    doi = None
    status = 'FAKE STATUS ;_;'
    @property
    def created_at(self): return self.cache.meta.created
    @property
    def updated_at(self): return self.cache.meta.updated
    @property
    def owner_id(self): return self.cache.meta.user_id
class CacheAsOrganization(CacheAsBFObject, Organization): pass
