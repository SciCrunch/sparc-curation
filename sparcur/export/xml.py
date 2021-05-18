import pathlib
import idlib
import rdflib
import dicttoxml
from pysercomb.pyr.types import ProtcurExpression, Quantity, AJ as AsJson
from sparcur.core import OntTerm, get_all_errors
from sparcur.utils import loge, is_list_or_tuple
from sparcur import pipelines as pipes


def xml(dataset_blobs):
    #datasets = []
    #contributors = []
    subjects = []
    resources = []
    errors = []
    error_reports = []

    def normv(v):
        if is_list_or_tuple(v):
            return [normv(_) for _ in v]
        elif isinstance(v, dict):
            return {k:normv(v) for k, v in v.items()}
        elif isinstance(v, str) and v.startswith('http'):
            # needed for loading from json that has been serialized
            # rather than from our internal representation
            # probably better to centralized the reload ...

            # XXX NOTE these days this will only happen if someone
            # supplies us with a uri in a field where we aren't
            # expecting one, in which case we should just return it
            try:
                v = OntTerm(v)
                return v.asCell()
            except Exception as e:
                loge.error(f'something went wrong with {v}')
                loge.exception(e)
                return v
                #raise e
        elif isinstance(v, rdflib.URIRef):  # FIXME why is this getting converted early?
            ot = OntTerm(v)
            return ot.asCell()
        elif isinstance(v, ProtcurExpression):
            return str(v)  # FIXME for xml?
        elif isinstance(v, Quantity):
            return str(v)
        elif isinstance(v, AsJson):  # XXX returns value not tested, may be extremely strange
            return str(v)
        elif isinstance(v, pathlib.Path):
            return str(v)
        elif isinstance(v, idlib.Stream):
            return v.asCell()
        #elif isinstance(v, list) or isinstance(v, str):
            #return v
        elif isinstance(v, BaseException):
            return repr(v)
        else:
            #loge.debug(repr(v))
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
            for path, er in ers:
                if not isinstance(er, dict):
                    #breakpoint()
                    loge.critical(er)
                    continue

                if er['pipeline_stage'] in pipes.PipelineEnd._shadowed:
                    continue

                er['dataset_id'] = id
                er = {k:normv(v) for k, v in er.items()}
                errors.append(er)

        if 'status' in dowe:
            if 'path_error_report' in dowe['status']:
                error_reports.append(dowe['status']['path_error_report'])

    xs = dicttoxml.dicttoxml({'subjects': subjects})
    xr = dicttoxml.dicttoxml({'resources': resources})
    xe = dicttoxml.dicttoxml({'errors': errors})
    xer = dicttoxml.dicttoxml({'error_reports': error_reports})
    return (('subjects', xs),
            ('resources', xr),
            ('errors', xe),
            ('error_reports', xer),)
