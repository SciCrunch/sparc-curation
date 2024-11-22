""" rerun all datasets """

import sys
from augpathlib.meta import isoformat
from sparcur.sparcron import get_redis_conn, _none
from datetime import timedelta
from dateutil import parser as dateparser
from sparcur.utils import PennsieveId
from sparcur.sparcron.core import (
    project_ids,
    datasets_remote_from_project_ids,
    mget_all,
    export_single_dataset
)
from sparcur.sparcron.status import dataset_fails, dataset_running

us = timedelta(microseconds=1)


def reset_dataset(conn, dataset):
    """ sometimes datasets get stuck """
    # somehow datasets get stuck running, possibly because their
    # runner exits without decrementing sid or something?
    dataset_id = dataset.id
    updated, qupdated, *_, rq, running, queued = mget_all(dataset_id)
    sid = 'state-' + dataset_id
    # if we only reset to queued then for some reason the logic in the
    # main loop will not restart the export, possibly due to matching
    # updated dates or something? therefore we reset all the way to none
    conn.set(sid, _none)
    # if the dataset is still in the todo list at this point then
    # it should automatically be rerun in the next loop


def rerun_dataset(conn, dataset):
    dataset_id = dataset.id
    updated, qupdated, *_, rq, running, queued = mget_all(dataset_id)
    if not (rq or running or queued):
        sid = 'state-' + dataset_id
        uid = 'updated-' + dataset_id
        qid = 'queued-' + dataset_id
        if updated:
            #if len(updated) < 27:  # some are missing micros entirely
            udt = dateparser.parse(updated)
            nudt = udt - us
            n_updated = isoformat(nudt)
            conn.set(uid, n_updated)

        conn.incr(sid)
        conn.set(qid, dataset.updated)
        export_single_dataset.delay(dataset_id, dataset.updated)


def main():
    conn = get_redis_conn()
    all_datasets = datasets_remote_from_project_ids(project_ids)
    args = sys.argv[1:]
    if args:
        if '--all' in args:
            to_run = dataset_fails(conn)
        else:
            to_run = [PennsieveId('dataset:' + rawid.split(':')[-1]) for rawid in args]

        datasets = [d for d in all_datasets if d.identifier in to_run]
    else:
        datasets = all_datasets

    _ = [rerun_dataset(conn, dataset) for dataset in datasets]


if __name__ == '__main__':
    main()
