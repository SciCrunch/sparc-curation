from pennsieve import Pennsieve, DataPackage
from pennsieve import Organization, Dataset, Collection, File
from pennsieve import base as pnb
from pennsieve.api import agent
from pennsieve.api.data import PackagesAPI
from sparcur import monkey
from sparcur.utils import ApiWrapper, PennsieveId


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
Dataset.meta = monkey.Dataset_meta
Dataset.packagesByName = monkey.packagesByName
Dataset.packageTypeCounts = monkey.packageTypeCounts
Dataset.readme = monkey.Dataset_readme
Dataset.contributors = monkey.Dataset_contributors
Dataset.doi = monkey.Dataset_doi
Dataset.status_log = monkey.Dataset_status_log  # XXX NOTE this overwrites a method
Dataset.packages = monkey.packages
Dataset._packages = _packages
Pennsieve.get = monkey.Blackfynn_get
#PackagesAPI.get = monkey.PackagesAPI_get
