import json
import idlib
import rdflib
from sparcur import schemas as sc
from .triples import TriplesExportDataset
from pyontutils.namespaces import (TEMP,
                                   isAbout,)
from sparcur.core import JEncode, OntId, OntTerm, get_all_errors
from sparcur.utils import want_prefixes, log, logd, loge


def disco(dataset_blobs, graphs):
    #dsh = sorted(MetaOutSchema.schema['allOf'][0]['properties'])
    dsh = ['acknowledgements',
           'additional_links',
           'award_number',
           'completeness_of_data_set',
           'contributor_count',
           'description',
           'dirs',
           'errors',
           'examples',
           'files',
           'funding',
           'keywords',
           'links',
           'modality',
           'name',  # -> title
           'organ',
           'originating_article_doi',
           'principal_investigator',
           'prior_batch_number',
           'protocol_url_or_doi',
           'sample_count',
           'size',
           'species',
           'subject_count',
           'title_for_complete_data_set',
           'uri_api',
           'uri_human',
           'error_index',  # (sum *_index)
           'dataset_completeness_index',  # dead
           'is_about',
           'involves_anatomical_region',
           'title',
           'folder_name',
           'timestamp_created',
           'timestamp_updated',
           'timestamp_updated_contents',
    ]
    chs = ['contributor_affiliation',
           'contributor_orcid',  # XXX note the change from with _id
           'contributor_role',
           'is_contact_person',
           'name',
           'first_name',
           'last_name',
           'middle_name',
           'id',
           'data_remote_user_id',]

    datasets = [['id', 'submission_index', 'curation_index'] + dsh]
    contributors = [['id'] + chs]
    subjects = [['id', 'blob']]
    errors = [['id', 'blob']]
    resources = [['id', 'blob']]
    error_reports = [['id', 'path', 'message']]

    #cje = JEncode()
    def normv(v):
        if isinstance(v, str) and v.startswith('http'):
            # needed for loading from json that has been serialized
            # rather than from our internal representation
            # probably better to centralized the reload ...
            oid = OntId(v)
            if oid.prefix in want_prefixes:
                return OntTerm(v).asCell()
            else:
                return oid.iri

        if isinstance(v, idlib.Stream):
            if hasattr(v, 'asCell'):
                return v.asCell()
            else:
                loge.debug(f'{type(v)} does not implement an asCell representation')
                return v.asType(str)

        if isinstance(v, OntId):
            if not isinstance(v, OntTerm):
                v = OntTerm(v)

            v = v.asCell()
        if isinstance(v, list) or isinstance(v, tuple):
            v = ','.join(json.dumps(_, cls=JEncode)
                         if isinstance(_, dict) else
                         normv(_)
                         for _ in v)
            v = v.replace('\n', ' ').replace('\t', ' ')
        elif any(isinstance(v, c) for c in (int, float, str)):
            v = str(v)
            v = v.replace('\n', ' ').replace('\t', ' ')  # FIXME tests to catch this

        elif isinstance(v, dict):
            v = json.dumps(v, cls=JEncode)

        return v

    for dataset_blob, graph in zip(dataset_blobs, graphs):
        id = dataset_blob['id']
        dowe = dataset_blob
        is_about = [OntTerm(o) for s, o in graph[:isAbout:] if isinstance(o, rdflib.URIRef)]
        involves = [OntTerm(o) for s, o in graph[:TEMP.involvesAnatomicalRegion:]]

        inv = ','.join(i.asCell() for i in involves)
        ia = ','.join(a.asCell() for a in is_about)
        #row = [id, dowe['error_index'], dowe['submission_completeness_index']]  # FIXME this doubles up on the row
        row = [id, dowe['status']['submission_index'], dowe['status']['curation_index']]  # FIXME this doubles up on the row
        if 'meta' in dowe:
            meta = dowe['meta']
            for k in dsh:
                if k in meta:
                    v = meta[k]
                    v = normv(v)
                elif k == 'is_about':
                    v = ia
                elif k == 'involves_anatomical_region':
                    v = inv
                else:
                    v = None

                row.append(v)

        else:
            row += [None for k in sc.MetaOutSchema.schema['properties']]

        datasets.append(row)

        # contribs 
        if 'contributors' in dowe:
            cs = dowe['contributors']
            for c in cs:
                row = [id]
                for k in chs:
                    if k in c:
                        v = c[k]
                        v = normv(v)
                        row.append(v)
                    else:
                        row.append(None)

                contributors.append(row)

        if 'subjects' in dowe:
            for subject in dowe['subjects']:
                row = [id]
                row.append(json.dumps(subject, cls=JEncode))
                subjects.append(row)

            # moved to resources if exists already
            #if 'software' in sbs:
                #for software in sbs['software']:
                    #row = [id]
                    #row.append(json.dumps(software, cls=JEncode))
                    #resources.append(row)

        if 'resources' in dowe:
            for res in dowe['resources']:
                row = [id]
                row.append(json.dumps(res, cls=JEncode))
                resources.append(row)

        if 'errors' in dowe:
            ers = get_all_errors(dowe)
            for er in ers:
                row = [id]
                row.append(json.dumps(er, cls=JEncode))
                errors.append(row)

        if 'status' in dowe:
            if 'path_error_report' in dowe['status']:
                per = dowe['status']['path_error_report']
                for path, report in sorted(per.items(), key=lambda kv: kv[0]):
                    for message in report['messages']:
                        error_reports.append([id, path, message])

    # TODO samples resources
    return (('datasets', datasets),
            ('contributors', contributors),
            ('subjects', subjects),
            ('resources', resources),
            ('errors', errors),
            ('error_reports', error_reports),)
