import rdflib
import dicttoxml
from pysercomb.pyr.types import ProtcurExpression, Quantity
from sparcur.core import OntTerm, get_all_errors


def xml(dataset_blobs):
    import pathlib
    #datasets = []
    #contributors = []
    subjects = []
    errors = []
    resources = []

    def normv(v):
        if isinstance(v, str) and v.startswith('http'):
            # needed for loading from json that has been serialized
            # rather than from our internal representation
            # probably better to centralized the reload ...
            v = OntTerm(v)
            return v.tabular()

        if isinstance(v, rdflib.URIRef):  # FIXME why is this getting converted early?
            ot = OntTerm(v)
            return ot.tabular()
        if isinstance(v, ProtcurExpression):
            return str(v)  # FIXME for xml?
        if isinstance(v, Quantity):
            return str(v)
        elif isinstance(v, pathlib.Path):
            return str(v)
        else:
            #log.debug(repr(v))
            return v

    for dataset_blob in dataset_blobs:
        id = dataset_blob['id']
        dowe = dataset_blob
        #id = dataset.id
        #dowe = dataset.data
        if 'subjects' in dowe:
            for subject in dowe['subjects']:
                subject['dataset_id'] = id
                subject = {k:normv(v) for k, v in subject.items()}
                subjects.append(subject)

        if 'resources' in dowe:
            for res in dowe['resources']:
                res['dataset_id'] = id
                res = {k:normv(v) for k, v in res.items()}
                resources.append(res)

        if 'errors' in dowe:
            ers = get_all_errors(dowe)
            for er in ers:
                if er['pipeline_stage'] == 'SPARCBIDSPipeline.data':
                    continue

                er['dataset_id'] = id
                er = {k:normv(v) for k, v in er.items()}
                errors.append(er)

    xs = dicttoxml.dicttoxml({'subjects': subjects})
    xr = dicttoxml.dicttoxml({'resources': resources})
    xe = dicttoxml.dicttoxml({'errors': errors})
    return (('subjects', xs),
            ('resources', xr),
            ('errors', xe))
