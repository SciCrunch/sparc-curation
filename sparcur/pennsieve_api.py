from pennsieve import Pennsieve, DataPackage
from pennsieve import Organization, Dataset, Collection, File
from pennsieve.api.data import PackagesAPI
from sparcur.utils import ApiWrapper, PennsieveId


Dataset._dp_class = DataPackage


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


class PNLocal(ApiWrapper):

    _id_class = PennsieveId
    _api_class = Pennsieve
    _sec_remote = 'pennsieve'
    _dp_class = DataPackage
