import os
if 'PENNSIEVE_LOG_LEVEL' not in os.environ:
    # silence agent import warning
    os.environ['PENNSIEVE_LOG_LEVEL'] = 'CRITICAL'
from pennsieve import log as _pnlog
# blackfynn.log sets logging.basicConfig which pollutes logs from
# other programs that are sane and do not use the root logger
# so we have to undo the damage done by basic config here
# we add the sparcur local handlers back in later
from sparcur.utils import log, silence_loggers
for __pnlog in (_pnlog.get_logger(), _pnlog.get_logger("pennsieve.agent")):
    silence_loggers(__pnlog)
    __pnlog.addHandler(log.handlers[0])

from pennsieve import Pennsieve, DataPackage, BaseNode
from pennsieve import Organization, Dataset, Collection, File
from pennsieve import base as pnb
from pennsieve.api import agent
from pennsieve.api.data import PackagesAPI
from sparcur import monkey
from sparcur.utils import ApiWrapper, PennsieveId, make_bf_cache_as_classes


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


class PNLocal(ApiWrapper):

    _id_class = PennsieveId
    _api_class = Pennsieve
    _sec_remote = 'pennsieve'
    _dp_class = DataPackage
    _remotebase = pnb


monkey.bind_agent_command(agent)

FakeBFile, _packages = monkey.bind_packages_File(File)

# monkey patches

Dataset._dp_class = DataPackage
Dataset.delete = monkey.Dataset_delete
Dataset.meta = monkey.Dataset_meta
Dataset.packagesByName = monkey.packagesByName
Dataset.packageTypeCounts = monkey.packageTypeCounts
Dataset.publishedMetadata = monkey.publishedMetadata
Dataset.publishedVersionMetadata = monkey.publishedVersionMetadata
Dataset.readme = monkey.Dataset_readme
Dataset.contributors = monkey.Dataset_contributors
Dataset.doi = monkey.Dataset_doi
Dataset.status_log = monkey.Dataset_status_log  # XXX NOTE this overwrites a method
Dataset.packages = monkey.packages
Dataset._packages = _packages
Pennsieve.get = monkey.Blackfynn_get
#PackagesAPI.get = monkey.PackagesAPI_get


(FakeBFLocal, CacheAsBFObject, CacheAsFile,
 CacheAsCollection, CacheAsDataset, CacheAsOrganization
 ) = make_bf_cache_as_classes(BaseNode, File, Collection, Dataset, Organization)
