# term mapping

from functools import wraps
from idlib.from_oq import OntTerm as iOT
from .core import OntTerm as cOT
from .utils import log


# reminder that ontquery is irretrievably broken if used directly and
# should never be invoked at the top level because it hits the network
# during __init__, this is a fix that uses the neurondm simple from_oq
# implementation that defers retrieval until fetch is called
class OntTerm(iOT, cOT):
    skip_for_instrumentation = False


def tos(f):
    @wraps(f)
    def inner(v):
        if isinstance(v, str):
            return f(v)
        elif isinstance(v, tuple):
            return tuple(f(_) for _ in v)
        elif isinstance(v, list):
            return [f(_) for _ in v]

    return inner


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
    'sus scrofa domesticus':  OntTerm('NCBITaxon:9825',  label='Sus scrofa domesticus'),
}


@tos
def species(string, __species=dict(_species), __fetched=[False]):
    if not __fetched[0]:  # SIGH
        [v.fetch() for v in __species.values()]  # TODO parallel
        __fetched[0] = True

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


@tos
def sex(string, __sex=dict(_sex), __fetched=[False]):
    if not __fetched[0]:  # SIGH
        [v.fetch() for v in __sex.values()]  # TODO parallel
        __fetched[0] = True

    lstr = string.lower()
    if lstr in __sex:
        return __sex[lstr]
    else:
        log.warning(f'No ontology mapping found for {string}')
        return string
