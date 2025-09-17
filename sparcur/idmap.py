import idlib
from sparcur.utils import PennsieveId as RemoteId, log
from sparcur.config import auth
from sparcur.simple.utils import backend_pennsieve


def get_projects_datasets_from_project_ids(project_ids):
    datasets = []
    projects = []
    for project_id in project_ids:
        PennsieveRemote = backend_pennsieve(project_id)
        root = PennsieveRemote(project_id)
        projects.append(root)
        datasets.extend(root.children)

    return projects, datasets


def identifier_indexes():
    """ one shot process to retrieve all identifier mappings
        use to sync with the remote, these mappings are immutable
        and only the dataset uuid -> dois mapping can change if
        new versions are published
    """
    project_id = auth.get('remote-organization')
    project_ids = auth.get_list('remote-organizations')
    if not project_ids:
        project_ids = [project_id]

    projects, datasets = get_projects_datasets_from_project_ids(project_ids)

    oiid_ouid = {p.id_int: p.identifier for p in projects}
    ouid_oiid = {v: k for k, v in oiid_ouid.items()}
    all_iid_did = {(d.organization.id_int, d.id_int): d.identifier for d in datasets}

    import requests
    api_base = 'https://api.pennsieve.io/'
    url_pd = f'{api_base}discover/datasets?limit=9999'  # FIXME paginate at some point

    session = requests.Session()
    try:
        resp = session.get(url_pd)
        p_datasets = resp.json()

        big_did = {}
        pid_did = {}
        iid_did = {}
        for d in p_datasets['datasets']:
            oiid = d['organizationId']
            iid = d['sourceDatasetId']
            pid = d['id']
            if oiid not in oiid_ouid:
                continue

            ouid = oiid_ouid[oiid]

            if (oiid, iid) not in all_iid_did:
                if (oiid, iid) ==  (367, 258):
                    # Functional recordings from the pig intrinsic cardiac nervous system (ICN)
                    did = iid_did[oiid, iid] = RemoteId('N:dataset:0d9454ca-43d9-4fdb-ac79-01c3574e8565')
                else:
                    # likely cases that have been unshared and that we need historical data to map
                    # e.g. 0d945 has never been run on the single dataset pipelines as far as i can
                    # tell and only exists in export archives now
                    log.error((pid, oiid, iid))
            else:
                did = iid_did[oiid, iid] = all_iid_did[oiid, iid]

            pid_did[pid] = did
            big_did[did] = {
                'id': did,
                'id_int': iid,
                'id_organization': ouid,
                'id_organization_int': oiid,
                'id_published': pid,
                'dois': tuple(),  # fill in next step
            }

        pid_versions = {}
        for pid in pid_did:
            url = f'{api_base}discover/datasets/{pid}/versions'
            resp = session.get(url)
            j = resp.json()
            pid_versions[pid] = j

    finally:
        session.close()

    doi_did = {}
    doi_pat = {}
    for did, ids in big_did.items():
        pid = ids['id_published']
        for version in pid_versions[pid]:
            fragment = version['doi']
            doi = idlib.Doi(f'https://doi.org/{fragment}')
            ids['dois'] += (doi,)
            doi_did[doi] = did
            doi_pat[doi] = version['versionPublishedAt']

    indexes = (
        oiid_ouid,
        ouid_oiid,
        iid_did,
        pid_did,
        doi_did,
        doi_pat,
    )

    return big_did, *indexes


def to_rkt(big_did, as_hash=False):
    """ simplify consuption by racket """
    # TODO probably normalize the representation of at least the org uuids
    out = '#hash(' if as_hash else ''
    for bd in sorted(big_did.values(), key=lambda d: frozenset(d.items())):
        id = bd['id']
        int = bd['id_int']
        org = bd['id_organization']
        org_int = bd['id_organization_int']
        pub = bd['id_published']
        dois = ' '.join([f'"{d.identifier.asStr()}"' for d in bd['dois']])
        if as_hash:
            line = f'({id.uuid} . #hash((id . "{id.uuid}")(org . "{org.uuid}")(org-int . {org_int})(int . {int})(pub . {pub})(dois . ({dois}))))'
        else:
            line = f'("{id.uuid}" "{org.uuid}" {org_int} {int} {pub} ({dois}))'

        out += '\n' + line

    if as_hash:
        out += '\n)'

    return out
