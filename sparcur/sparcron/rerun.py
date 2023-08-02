""" rerun all datasets """

from augpathlib.meta import isoformat
from sparcur.sparcron import get_redis_conn
from datetime import timedelta
from dateutil import parser as dateparser
from sparcur.sparcron.core import (
    project_id,
    datasets_remote_from_project_id,
    mget_all,
    export_single_dataset
)

us = timedelta(microseconds=1)


def rerun_dataset(conn, dataset):
    updated, qupdated, *_, rq, running, queued = mget_all(dataset)
    if not (rq or running or queued):
        dataset_id = dataset.id
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
    datasets = datasets_remote_from_project_id(project_id)
    _ = [rerun_dataset(conn, dataset) for dataset in datasets]


if __name__ == '__main__':
    main()
