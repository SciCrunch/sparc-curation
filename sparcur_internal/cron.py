"""
run with
                   celery -A cron worker -B -s ./sparcur-cron-schedule
PYTHONBREAKPOINT=0 celery -A cron worker -B -s ./sparcur-cron-schedule --loglevel=INFO

# to clean up celery -A cron purge
# rabbitmqctl list_queues
# rabbitmqctl purge_queue celery
# rabbitmqctl delete_queue cron
# rabbitmqctl delete_queue export
# rabbitmqctl delete_queue default
"""

# TODO get the latest export dates from the file system when we start and/or don't purge redis
# FIXME TODO need a way to poll google sheets to check for updates without reading

from docopt import parse_defaults

from kombu import Queue, Exchange
from celery import Celery, Task
from celery.signals import worker_process_init, worker_process_shutdown
from celery.schedules import crontab

from sparcur.cli import Main, Options, __doc__ as clidoc
from sparcur.utils import PennsieveId, GetTimeNow
from sparcur.paths import Path
from sparcur.config import auth
from sparcur.simple.utils import backend_pennsieve
from sparcur.simple.retrieve import main as retrieve


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
        '--no-google': True,  # XXX FIXME we need a way to fetch the data once and then reuse
        '--i-know-what-i-am-doing': True,
        'report': False, 'protocols': False,}  # FIXME separate args for protcur export
options = Options(args, defaults)

project_id = auth.get('remote-organization')
path_source_dir = Path('~/files/sparc-datasets-test').expanduser().resolve()  # FIXME hardcoded  XXX resolve required to avoid mismatches
if not path_source_dir.exists():
    path_source_dir.mkdir(parents=True)

cel = Celery('sparcur-cron',)

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


def get_redis_conn():
    rc = Celery(backend='redis://',
                broker='redis://')
    return rc.backend.client

conn = get_redis_conn()

# {k:conn.get(k) for k in sorted(conn.keys()) if b'N:dataset' in k}  


def reset_redis_keys(conn):
    conn.delete(*[k for k in conn.keys() if b'N:dataset:' in k])


@cel.on_after_configure.connect
def cleanup_redis(sender, **kwargs):
    """ For the time being ensure that any old data about process
        state is wiped when we restart. """
    reset_redis_keys(conn)


sync_interval = 60.0 * .25  # seconds
heartbeat_interval = 2.0  # seconds


@cel.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(sync_interval, check_for_updates.s(project_id), name='check for updates')
    sender.add_periodic_task(heartbeat_interval, heartbeat.s(), name='heartbeat')


@cel.task
def heartbeat():  # FIXME this has to run in a separate priority queue
    #print(shared_state)
    #print(dataset_status)
    keys = conn.keys()
    vals = [int(conn.get(k)) for k in keys if b'state-N:dataset:' in k]
    #vals = shared_state.values()
    ln = len([1 for n in vals if not n])
    lq = len([1 for n in vals if n == 1])
    lr = len([1 for n in vals if n == 2])
    lqr = len([1 for n in vals if n == 3])
    print(f'{ln} {lq} {lr} {lqr}')
    with open('/tmp/cron-log', 'at') as f:
        f.write(f'{ln} {lq} {lr} {lqr}\n')


pid = PennsieveId(project_id)
def ret_val_exp(dataset_id, updated, time_now):
    did = PennsieveId(dataset_id)
    uid = 'updated-' + dataset_id
    fid = 'failed-' + dataset_id

    dataset_path = retrieve(
        id=did, dataset_id=did, project_id=pid,
        sparse_limit=-1, parent_parent_path=path_source_dir)
    # FIXME we need to track/check the state here too in the event
    # that retrieve succeeds but validate or export fails
    # FIXME getting no paths to fetch errors

    with dataset_path:
        main = Main(options, time_now)
        try:
            blob_ir, intr, dump_path, latest_path = main.export()
            conn.set(uid, updated)
            conn.set(fid, '')
        except Exception as e:
            old_updated = conn.getset(uid, '')
            conn.set(uid, '' if old_updated is None else old_updated)
            conn.set(fid, updated)
            # TODO log the error
            pass


_none = 0
_qed = 1
_run = 2
_qed_run = 3

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
    for dataset in datasets:
        dataset_id = dataset.id
        sid = 'state-' + dataset_id
        uid = 'updated-' + dataset_id
        fid = 'failed-' + dataset_id
        
        _updated = conn.getset(uid, '')
        updated = _updated.decode() if _updated else _updated
        _failed = conn.getset(fid, '')
        failed = _failed.decode() if _failed else _failed
        state = conn.getset(sid, 0)
        rq = state == _qed_run
        running = state == _run or rq
        queued = state == _qed or rq
            
        print(f'{dataset_id} {updated} {queued} {running}')
        if not updated and not failed or (
                failed and dataset.updated > failed or
                not failed and updated and dataset.updated > updated):
            # FIXME export code change also needs to factor in here
            # FIXME the logic here is bad and is resulting in way too many jobs being enqueued
            if queued:
                pass
            elif running:
                conn.incr(sid)
            else:
                conn.incr(sid)
                export_single_dataset.delay(dataset_id, dataset.updated)


PennsieveRemote = backend_pennsieve(project_id)
root = PennsieveRemote(project_id)
def datasets_remote_from_project_id(project_id):
    datasets_no = auth.get('datasets-no')
    datasets = [c for c in root.children if c.id not in datasets_no]
    return datasets

