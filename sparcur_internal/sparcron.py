"""
services
/etc/init.d/rabbitmq start
/etc/init.d/redis start

test with
python sparcron.py

# run with
PYTHONBREAKPOINT=0 celery --app sparcron worker --beat --schedule-filename ./sparcur-cron-schedule --loglevel=INFO

# --queues cron:2,export:4,default:1  # SIGH https://github.com/celery/celery/issues/1599
#celery multi start cron export default -c:cron 2 -c:export 5 -c:default 1

# old run
## in one process
celery --app sparcron worker --beat --queues cron --schedule-filename ./sparcur-cron-schedule
#--concurrency=2

## in another process
PYTHONBREAKPOINT=0 celery --app sparcron worker --beat --queues export,default --loglevel=INFO

#                   celery --app sparcron worker --beat --schedule-filename ./sparcur-cron-schedule
#PYTHONBREAKPOINT=0 celery --app sparcron worker --beat --schedule-filename ./sparcur-cron-schedule --loglevel=INFO


# to clean up
celery -A sparcron purge

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
import signal
import subprocess

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
from sparcur.sheets import Sheet, Organs, Affiliations


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
cel.conf.worker_prefetch_multiplier = 1

log.info(f'STATUS sparcur :id {project_id} :path {path_source_dir}')

# FIXME needed a dedicated worker for the cron queue
cel.conf.task_queues = (
    Queue('cron', Exchange('cron'), routing_key='task.cron',
          #max_priority=100,
          queue_arguments={'x-max-priority': 10},
          ),
    Queue('export', Exchange('export'), routing_key='task.export',
          #max_priority=5,
          queue_arguments={'x-max-priority': 1},
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
    if name == 'sparcron.check_for_updates':
        out = {'exchange': 'cron', 'routing_key': 'task.cron', 'priority': 10, 'queue': 'cron'}
    elif name == 'sparcron.check_sheet_updates':
        out = {'exchange': 'cron', 'routing_key': 'task.cron', 'priority': 10, 'queue': 'cron'}
    elif name == 'sparcron.heartbeat':
        out = {'exchange': 'cron', 'routing_key': 'task.cron', 'priority': 3, 'queue': 'cron'}
    elif name == 'sparcron.export_single_dataset':
        out = {'exchange': 'export', 'routing_key': 'task.export', 'priority': 1, 'queue': 'export'}
    elif 'celery' in name:
        out = options
    else:
        oops = (name, args, kwargs, options, task, kw)
        log.error(oops)
        raise NotImplementedError(oops)

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
                             sorted(conn.keys()) if b'N:dataset' in k},
                            width=120))


@cel.on_after_finalize.connect
def cleanup_redis(sender, **kwargs):
    """ For the time being ensure that any old data about process
        state is wiped when we restart. """
    log.info('cleaning up old redis connection ...')
    reset_redis_keys(conn)
    populate_existing_redis(conn)


sync_interval = 17  # seconds
sync_sheet_interval = 23  # seconds
heartbeat_interval = 5  # seconds


@cel.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    log.info('setting up periodic tasks ...')
    sender.add_periodic_task(sync_interval,
                             check_for_updates.s(project_id),
                             name='check for updates',
                             expires=0.68333)

    sender.add_periodic_task(sync_sheet_interval,
                             check_sheet_updates.s(),
                             name='check sheet for updates',
                             expires=0.88333)

    if False:
        # FIXME remove the heartbeat and dump a log status line after
        # begin/end of every dataset export
        sender.add_periodic_task(heartbeat_interval,
                                    heartbeat.s(),
                                    name='heartbeat',
                                    expires=0.2)


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
    time_now = GetTimeNow()
    log.info(f'HEARTBEAT :n {ln} :f {lf} :q {lq} :r {lr} :qr {lqr}')
    with open('/tmp/cron-log', 'at') as f:
        f.write(f':t {time_now.START_TIMESTAMP_LOCAL} :n {ln} :f {lf} :q {lq} :r {lr} :qr {lqr}\n')


def argv_simple_retrieve(dataset_id):
    return [
        sys.executable,
        '-m',
        'sparcur.simple.retrieve',
        '--sparse-limit',
        '-1',
        '--dataset-id',
        dataset_id,
        '--parent-parent-path',
        path_source_dir]

argv_spc_find_meta = [
    sys.executable,
    '-m',
    'sparcur.cli',
    "find",
    "--name", "*.xlsx",
    "--name", "*.xml",
    "--name", "submission*",
    "--name", "code_description*",
    "--name", "dataset_description*",
    "--name", "subjects*",
    "--name", "samples*",
    "--name", "manifest*",
    "--name", "resources*",
    "--name", "README*",
    '--no-network',
    "--limit", "-1",
    "--fetch"]

argv_spc_export = [
    sys.executable,
    '-m',
    'sparcur.cli',
    'export',
    '--no-network']

pid = PennsieveId(project_id)
def ret_val_exp(dataset_id, updated, time_now):
    log.info(f'START {dataset_id}')
    did = PennsieveId(dataset_id)
    uid = 'updated-' + dataset_id
    fid = 'failed-' + dataset_id

    # FIXME detect cases where we have already pulled the latest and don't pull again
    # FIXME TODO smart retrieve so we don't pull if we failed during
    # export instead of pull, should be able to get it from the
    # cached metadata on the dataset

    # FIXME getting file exists errors for pull in here
    # in upstream.mkdir()

    # FIXME we need to track/check the state here too in the event
    # that retrieve succeeds but validate or export fails
    # FIXME getting no paths to fetch errors

    # FIXME detect cases where it appears that a new dataset is in the process of being
    # uploaded and don't run for a while if it is being continually modified
    try:
        try:
            p1 = subprocess.Popen(argv_simple_retrieve(dataset_id))
            out1 = p1.communicate()
            if p1.returncode != 0:
                raise Exception(f'oops return code was {p1.returncode}')
        except KeyboardInterrupt as e:
            p1.send_signal(signal.SIGINT)
            raise e

        dataset_path = (path_source_dir / did.uuid / 'dataset').resolve()
        try:
            p2 = subprocess.Popen(argv_spc_find_meta, cwd=dataset_path)
            out2 = p2.communicate()
            if p2.returncode != 0:
                raise Exception(f'oops return code was {p2.returncode}')
        except KeyboardInterrupt as e:
            p2.send_signal(signal.SIGINT)
            raise e

        try:
            p3 = subprocess.Popen(argv_spc_export, cwd=dataset_path)
            out3 = p3.communicate()
            if p3.returncode != 0:
                raise Exception(f'oops return code was {p3.returncode}')
        except KeyboardInterrupt as e:
            p3.send_signal(signal.SIGINT)
            raise e

        conn.set(uid, updated)
        conn.delete(fid)
        log.info(f'DONE: u: {uid} {updated}')
    except Exception as e:
        log.critical(f'FAIL: {fid} {updated}')
        conn.set(fid, updated)
        log.exception(e)


@cel.task
def export_single_dataset(dataset_id, updated):  # FIXME this is being called way too often, that message queue is super hefty
    sid = 'state-' + dataset_id
    time_now = GetTimeNow()
    conn.incr(sid)
    ret_val_exp(dataset_id, updated, time_now)
    state = conn.get(sid)
    # FIXME I'm seeing -1 and -2 in here somehow
    conn.decr(sid)  # we always go back 2 either to none or queued
    conn.decr(sid)
    if state == _qed_run:
        export_single_dataset.delay(dataset_id)

    status_report()


@cel.task
def check_sheet_updates():
    # TODO see if we need locking
    for sheetcls in (Organs, Affiliations):
        old_s = sheetcls()
        old = old_s.metadata_file()['modifiedTime']
        s = sheetcls()
        s._only_cache = False
        s._setup()
        new = s.metadata_file()['modifiedTime']
        #log.info(f':old {old} :new {new}')
        if new != old:
            # something has changed
            s._do_cache = True
            s._re_cache = True
            s._setup()
            s.fetch()
            log.info(f'spreadsheet cache updated for {s.name}')
            # TODO check which datasets (if any) changed
            # and put them in the queue to rerun


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
            
        #log.debug(f'STATUS :id {dataset_id} :u {updated} :f {failed} :q {queued} :r {running}')
        # All the logic for whether to run a particular dataset
        # timestamp_updated or timestamp_updated_contents whichever is greater
        # NOTE we populate updated values into redis at startup from
        # the latest export of each individual dataset
        # TODO also need to check sparcur code changes to see if we need to rerun
        if (not (updated or failed) or
            failed and dataset.updated > failed or
            not failed and updated and dataset.updated > updated):
            log.debug((f'MAYBE ENQUEUE :id {dataset_id} du: '
                      f'{dataset.updated} u: {updated} f: {failed}'))
            if queued:
                pass
            elif running and updated and qupdated and updated > qupdated:
                conn.incr(sid)
            else:
                conn.incr(sid)
                conn.set(qid, dataset.updated)
                export_single_dataset.delay(dataset_id, dataset.updated)


PennsieveRemote = backend_pennsieve(project_id)
root = PennsieveRemote(project_id)
def datasets_remote_from_project_id(project_id):
    datasets_no = auth.get_list('datasets-no')
    datasets = [c for c in root.children if c.id not in datasets_no]
    return datasets


def status_report():
    datasets = datasets_remote_from_project_id(project_id)

    todo = []
    fail = []
    run = 0
    que = 0
    for dataset in datasets:
        dataset_id = dataset.id
        sid = 'state-' + dataset_id
        uid = 'updated-' + dataset_id
        fid = 'failed-' + dataset_id

        _updated = conn.get(uid)
        updated = _updated.decode() if _updated is not None else _updated

        _failed = conn.get(fid)
        failed = _failed.decode() if _failed is not None else _failed

        _state = conn.get(sid)
        state = int(_state) if _state is not None else _state

        rq = state == _qed_run
        running = state == _run or rq
        queued = state == _qed or rq

        if not updated or dataset.updated > updated:
            todo.append(dataset)
        if failed:
            fail.append(dataset)
        if running:
            run += 1
        if queued:
            que += 1

    fails = '\n  '.join(sorted([f.id for f in fail]))
    todos = '\n  '.join(sorted([f.id for f in todo]))
    report = (
        '\nStatus Report\n'
        f'Datasets: {len(datasets)}\n'
        f'Failed:   {len(fail)}\n'
        f'Todo:     {len(todo)}\n'
        #f'Done:     {}\n'
        f'Running:  {run}\n'
        f'Queued:   {que}\n'
        '\n'
        f'Failed:\n  {fails}\n'
        f'Todo:\n  {todos}\n'
    )
    log.info(report)
    #log.info('TODO:\n' + pprint.pformat(todo))


def test():
    check_sheet_updates()
    return
    status_report()
    #breakpoint()
    return
    reset_redis_keys(conn)
    populate_existing_redis(conn)

    datasets = datasets_remote_from_project_id(project_id)
    datasets = sorted(datasets, key=lambda r:r.id)[:3]
    dataset = datasets[0] # 0, 1, 2 # ok, unfetched xml children, ok
    dataset_id = dataset.id
    updated = dataset.updated
    time_now = GetTimeNow()
    ret_val_exp(dataset_id, updated, time_now)


if __name__ == '__main__':
    test()
