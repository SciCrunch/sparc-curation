"""
services
/etc/init.d/rabbitmq start
/etc/init.d/redis start

test with
python cron.py

run with

in one process
celery --app sparcron worker --beat --queues cron --schedule-filename ./sparcur-cron-schedule --concurency=2

in another process
PYTHONBREAKPOINT=0 celery --app sparcron worker --queues export,default --loglevel=INFO

#                   celery --app sparcron worker --beat --schedule-filename ./sparcur-cron-schedule
#PYTHONBREAKPOINT=0 celery --app sparcron worker --beat --schedule-filename ./sparcur-cron-schedule --loglevel=INFO


# to clean up
celery -A cron purge

rabbitmqctl list_queues
rabbitmqctl purge_queue celery
rabbitmqctl delete_queue cron
rabbitmqctl delete_queue export
rabbitmqctl delete_queue default
"""

# FIXME need a way to separate the periodic tasks and avoid double setup
# because celery will create as many as you want for top level ...
# alternatvely figure out how to pin workers to queus within a single
# top level process ...

# FIXME really bad because redis and rabbit get completely out of sync ...

# TODO get the latest export dates from the file system when we start and/or don't purge redis
# FIXME TODO need a way to poll google sheets to check for updates without reading

import sys
import json
import pprint

from docopt import parse_defaults

from kombu import Queue, Exchange
from celery import Celery, Task
from celery.signals import worker_process_init, worker_process_shutdown
from celery.schedules import crontab

from sparcur.cli import Main, Options, __doc__ as clidoc
from sparcur.utils import PennsieveId, GetTimeNow, log
from sparcur.paths import Path
from sparcur.config import auth
from sparcur.simple.utils import backend_pennsieve
from sparcur.simple.retrieve import main as retrieve
from sparcur.sheets import Sheet


# we set only cache here to avoid hitting rate limits the cache should
# be updated in another process to keep things simple
Sheet._only_cache = True


class _Dataset:  # XXX not a real class, just represents the values that we sync via redis
    def __init__(self, id):
        self.id = id
        self.last_remote_updated_datetime = None
        self.last_export_updated_datetime = None
        self.queued = False
        self.fetching = False
        self.exporting = False
        self.last_export_failed = None


defaults = {o.name:o.value if o.argcount else None
            for o in parse_defaults(clidoc)}
args = {**defaults, 'export': True, '--jobs': 1, 'schemas': False, 'protcur': False,
        '--no-network': True,  # XXX FIXME we need a way to fetch the data once and then reuse
        '--i-know-what-i-am-doing': True,
        'report': False, 'protocols': False,}  # FIXME separate args for protcur export
options = Options(args, defaults)

project_id = auth.get('remote-organization')
path_source_dir = Path('~/files/sparc-datasets-test').expanduser().resolve()  # FIXME hardcoded  XXX resolve required to avoid mismatches
if not path_source_dir.exists():
    path_source_dir.mkdir(parents=True)

cel = Celery('sparcur-cron',)

cel.conf.worker_hijack_root_logger = False

log.info(f'STATUS sparcur :id {project_id} :path {path_source_dir}')

# FIXME needed a dedicated worker for the cron queue
cel.conf.task_queues = (
    Queue('cron', Exchange('cron'), routing_key='task.cron',
          #max_priority=100,
          queue_arguments={'x-max-priority': 4},
          ),
    Queue('export', Exchange('export'), routing_key='task.export',
          #max_priority=5,
          queue_arguments={'x-max-priority': 3},
          ),
    Queue('default', Exchange('default'), routing_key='task.default',
          #max_priority=1,
          queue_arguments={'x-max-priority': 1},
          ),  # fallthrough
)

cel.conf.task_default_exchange = 'default'
#cel.conf.task_default_exchange_type = 'topic'
cel.conf.task_default_routing_key = 'task.default'

def route(name, args, kwargs, options, task=None, **kw):
    if name in ('cron.check_for_updates', 'cron.heartbeat'):
        out = {'exchange': 'cron', 'routing_key': 'task.cron', 'priority': 3}
    elif name == 'cron.export_single_dataset':
        out = {'exchange': 'export', 'routing_key': 'task.export', 'priority': 2}

    #print('wat', out)
    return out

cel.conf.task_routes = (route,)

# cron consumer to ensure timely completion
cel.control.add_consumer(
    queue='cron',
    exchange='cron',
    routing_key='task.cron',
)

cel.control.add_consumer(
    queue='export',
    exchange='export',
    routing_key='task.export',
)


def get_redis_conn():
    rc = Celery(backend='redis://',
                broker='redis://')
    return rc.backend.client


conn = get_redis_conn()


def reset_redis_keys(conn):
    names = [k for k in conn.keys() if b'N:dataset:' in k]
    if names:
        conn.delete(*names)


def populate_existing_redis(conn):
    """ Set the initial state for exports from the file system. """
    # we intentionally do not go to network here because that will
    # be done by check_for_updates
    datasets_export_base = Path(options.export_path) / 'datasets'
    uuids = [c.name for c in datasets_export_base.children if c.is_dir()]
    for uuid in uuids:
        dataset_id = 'N:dataset:' + uuid
        try:
            # catch potentially malformed ids
            did = PennsieveId(dataset_id)
        except idlib.exc.MalformedIdentifierError as e:
            log.error(f'strange dir in dataset export: {uuid}\n{e}')
            continue

        # FIXME hardcoded convention
        latest = (datasets_export_base /
                  uuid / 'LATEST' / 'curation-export.json')
        if latest.exists():
            with open(latest, 'rt') as f:
                # we don't bother to use fromJson here because we just
                # need the raw values not the sparcur ir
                blob = json.load(f)
            updated = blob['meta']['timestamp_updated']
            #prov_commit = blob['prov']['commit']  # TODO need to be able to detect software changes and rerun
            sid = 'state-' + dataset_id
            uid = 'updated-' + dataset_id
            fid = 'failed-' + dataset_id
            conn.set(sid, _none)
            conn.set(uid, updated)
            conn.set(fid, '')

    log.info(pprint.pformat({k:conn.get(k) for k in
                             sorted(conn.keys()) if b'N:dataset' in k}))


iscron = (
    '--queues' in sys.argv and 'cron' in sys.argv and
    sys.argv.index('--queues') == sys.argv.index('cron') - 1)

if iscron:
    # some time between 5.0.2 and 5.2.3 on_after_connect stopped working for this, so have to use finalize?
    #@cel.on_after_configure.connect
    @cel.on_after_finalize.connect
    def cleanup_redis(sender, **kwargs):
        """ For the time being ensure that any old data about process
            state is wiped when we restart. """
        log.info('cleaning up old redis connection ...')
        reset_redis_keys(conn)
        populate_existing_redis(conn)


    sync_interval = 60.0 * .25  # seconds
    heartbeat_interval = 2.0  # seconds


    # some time between 5.0.2 and 5.2.3 on_after_connect stopped working for this, so have to use finalize?
    #@cel.on_after_configure.connect
    @cel.on_after_finalize.connect
    def setup_periodic_tasks(sender, **kwargs):
        log.info('setting up periodic tasks ...')
        sender.add_periodic_task(sync_interval, check_for_updates.s(project_id), name='check for updates')
        sender.add_periodic_task(heartbeat_interval, heartbeat.s(), name='heartbeat')


_none = 0
_qed = 1
_run = 2
_qed_run = 3


@cel.task
def heartbeat():  # FIXME this has to run in a separate priority queue with its own worker
    #print(shared_state)
    #print(dataset_status)
    keys = conn.keys()
    vals = [int(conn.get(k)) for k in keys if b'state-N:dataset:' in k]
    fails = [k for k in keys if b'failed-' in k and conn.get(k)]
    #vals = shared_state.values()
    ln = len([1 for n in vals if not n or n == _none])
    lf = len(fails)
    lq = len([1 for n in vals if n == _qed])
    lr = len([1 for n in vals if n == _run])
    lqr = len([1 for n in vals if n == _qed_run])
    log.info(f'HEARTBEAT :n {ln} :f {lf} :q {lq} :r {lr} :qr {lqr}')
    with open('/tmp/cron-log', 'at') as f:
        f.write(f':n {ln} :f {lf} :q {lq} :r {lr} :qr {lqr}\n')

    #i = cel.control.inspect()
    #log.info(i.scheduled())
    #log.info(i.active())
    #log.info(i.reserved())
    #eff = f''
    #log.info(eff)


pid = PennsieveId(project_id)
def ret_val_exp(dataset_id, updated, time_now):
    log.info(f'START {dataset_id}')
    did = PennsieveId(dataset_id)
    uid = 'updated-' + dataset_id
    fid = 'failed-' + dataset_id

    dataset_path = retrieve(
        id=did, dataset_id=did, project_id=pid,
        sparse_limit=-1, parent_parent_path=path_source_dir,
        #extensions=('xml',),
    )
    # FIXME getting file exists errors for pull in here
    # in upstream.mkdir()

    # FIXME we need to track/check the state here too in the event
    # that retrieve succeeds but validate or export fails
    # FIXME getting no paths to fetch errors

    with dataset_path:
        main = Main(options, time_now)
        try:
            blob_ir, intr, dump_path, latest_path = main.export()
            conn.set(uid, updated)
            conn.set(fid, '')
            log.info(f'DONE: u: {uid} {updated}')
        except Exception as e:
            log.critical(f'FAIL: {fid} {updated}')
            # retain old updated, I think? failed should mask correctly
            #old_updated = conn.get(uid)
            conn.set(fid, updated)
            # TODO log the error
            log.exception(e)


@cel.task
def export_single_dataset(dataset_id, updated):  # FIXME this is being called way too often, that message queue is super hefty
    sid = 'state-' + dataset_id
    time_now = GetTimeNow()
    conn.incr(sid)
    ret_val_exp(dataset_id, updated, time_now)
    state = conn.get(sid)
    # FIXME I'm seeing -1 and -2 in here somehow
    conn.decr(sid)  # we always go back 2 either to none or quened
    conn.decr(sid)
    if state == _qed_run:
        export_single_dataset.delay(dataset_id)


@cel.task
def check_for_updates(project_id):
    datasets = datasets_remote_from_project_id(project_id)
    #datasets = sorted(datasets, key=lambda r:r.id)[:3]
    for dataset in datasets:
        dataset_id = dataset.id
        sid = 'state-' + dataset_id
        uid = 'updated-' + dataset_id
        fid = 'failed-' + dataset_id
        qid = 'queued-' + dataset_id
        
        _updated = conn.get(uid)
        updated = _updated.decode() if _updated is not None else _updated

        _qupdated = conn.get(qid)
        qupdated = _qupdated.decode() if _qupdated is not None else _qupdated

        _failed = conn.get(fid)
        failed = _failed.decode() if _failed is not None else _failed
        _state = conn.get(sid)
        state = int(_state) if _state is not None else _state
        rq = state == _qed_run
        running = state == _run or rq
        queued = state == _qed or rq
            
        log.info(f'STATUS :id {dataset_id} :u {updated} :f {failed} :q {queued} :r {running}')
        # All the logic for whether to run a particular dataset
        # timestamp_updated or timestamp_updated_contents whichever is greater
        # NOTE we populate updated values into redis at startup from
        # the latest export of each individual dataset
        # TODO also need to check sparcur code changes to see if we need to rerun
        if (not (updated or failed) or
            failed and dataset.updated > failed or
            not failed and updated and dataset.updated > updated):
            # FIXME export code change also needs to factor in here
            # FIXME the logic here is bad and is resulting in way too many jobs being enqueued
            log.info((f'LOGIC SAYS DO SOMETHING :id {dataset_id} du: '
                      f'{dataset.updated} u: {updated} f: {failed}'))
            if queued:
                pass
            elif running and updated and qupdated and updated > qupdated:
                conn.incr(sid)
            else:
                conn.incr(sid)
                conn.set(qid)
                export_single_dataset.delay(dataset_id, dataset.updated)


PennsieveRemote = backend_pennsieve(project_id)
root = PennsieveRemote(project_id)
def datasets_remote_from_project_id(project_id):
    datasets_no = auth.get('datasets-no')
    datasets = [c for c in root.children if c.id not in datasets_no]
    return datasets


def test():
    reset_redis_keys(conn)
    populate_existing_redis(conn)

    datasets = datasets_remote_from_project_id(project_id)
    datasets = sorted(datasets, key=lambda r:r.id)[:3]
    dataset = datasets[1] # 0, 1, 2 # ok, unfetched xml children, ok
    dataset_id = dataset.id
    updated = dataset.updated
    time_now = GetTimeNow()
    ret_val_exp(dataset_id, updated, time_now)


if __name__ == '__main__':
    test()
