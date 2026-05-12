from sparcur.config import auth
from sparcur.idmap import get_projects_datasets_from_project_ids


def events():
    """ one shot process to retrieve all identifier mappings
        use to sync with the remote, these mappings are immutable
        and only the dataset uuid -> dois mapping can change if
        new versions are published
    """
    _project_id = auth.get('remote-organization')
    project_ids = auth.get_list('remote-organizations')
    project_ids = [(list(ro.keys())[0], list(ro.values()[0]))
                   if isinstance(ro, dict) else ('pennsieve', ro)  # backward compat
                   for ro in auth.get_list('remote-organizations')]
    if not project_ids:
        project_ids = [('pennsieve', _project_id)]

    projects, datasets = get_projects_datasets_from_project_ids(project_ids)
    timelines = {}
    for d in datasets:
        t = d.bfobject.timeline
        timelines[d.identifier.id] = t

    return timelines


def main():
    import json
    from pyontutils.utils_fast import utcnowtz, timeformat_friendly
    tff = timeformat_friendly(utcnowtz())
    dt = tff.split(',')[0] + tff[24:]
    timelines = events()
    with open(f'/tmp/pennsieve-event-data-{dt}.json', 'wt') as f:
        json.dump(timelines, f, indent=2)


if __name__ == '__main__':
    main()
