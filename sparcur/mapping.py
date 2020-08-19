# term mapping

from .core import OntTerm
from .utils import log


# TODO load from db/config ?
_species = {
    'canis lupus familiaris': OntTerm('NCBITaxon:9615',  label='Canis familiaris'),
    'felis catus':            OntTerm('NCBITaxon:9685',  label='Felis catus'),
    'guinea pig':             OntTerm('NCBITaxon:10141', label='Cavia porcellus'),
    'homo sapiens':           OntTerm('NCBITaxon:9606',  label='Homo sapiens'),
    'mus musculus':           OntTerm('NCBITaxon:10090', label='Mus musculus'),
    'mustela putorius furo':  OntTerm('NCBITaxon:9669',  label='Mustela putorius furo'),
    'rattus norvegicus':      OntTerm('NCBITaxon:10116', label='Rattus norvegicus'),
    'suncus murinus':         OntTerm('NCBITaxon:9378',  label='Suncus murinus'),
    'sus scrofa':             OntTerm('NCBITaxon:9823',  label='Sus scrofa'),
}


def species(string, __species=dict(_species)):
    lstr = string.lower()
    if lstr in __species:
        return __species[lstr]
    else:
        log.warning(f'No ontology mapping found for {string}')
        return string


_sex = {
    'female': OntTerm('PATO:0000383', label='female'),
    'male': OntTerm('PATO:0000384', label='male'),
}


def sex(string, __sex=dict(_sex)):
    lstr = string.lower()
    if lstr in __sex:
        return __sex[lstr]
    else:
        log.warning(f'No ontology mapping found for {string}')
        return string

