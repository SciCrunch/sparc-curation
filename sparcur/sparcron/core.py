"""
services
/etc/init.d/rabbitmq start
/etc/init.d/redict start

test with
python -m sparcur.sparcron

# run with
PYTHONBREAKPOINT=0 celery --app sparcur.sparcron worker -n wcron -Q cron,default --concurrency=1 --detach --beat --schedule-filename ./sparcur-cron-schedule --loglevel=INFO
PYTHONBREAKPOINT=0 celery --app sparcur.sparcron worker -n wexport -Q export --loglevel=INFO

# to clean up
celery -A sparcur.sparcron purge

rabbitmqctl list_queues
rabbitmqctl purge_queue celery;\
rabbitmqctl delete_queue cron;\
rabbitmqctl delete_queue export;\
rabbitmqctl delete_queue default

"""

# FIXME split this in half to avoid high memory usage
# from all the imports needed for the cron half that
# are not needed in the export section because it is
# all in the subprocess

# FIXME need a way to separate the periodic tasks and avoid double setup
# because celery will create as many as you want for top level ...
# alternatvely figure out how to pin workers to queus within a single
# top level process ...

# FIXME really bad because redis and rabbit get completely out of sync ...

# TODO get the latest export dates from the file system when we start and/or don't purge redis

import os
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
from pyontutils.utils_fast import logToFile

import sparcur
from sparcur import exceptions as exc
from sparcur.cli import Main, Options, __doc__ as clidoc
from sparcur.utils import PennsieveId as RemoteId, GetTimeNow, log as _log
from sparcur.paths import Path, PennsieveCache
from sparcur.config import auth
from sparcur.backends import PennsieveDatasetData as RemoteDatasetData, BlackfynnRemote as Remote
from sparcur.simple.utils import backend_pennsieve
from sparcur.simple.retrieve import main as retrieve
from sparcur.sheets import Sheet, Organs, Affiliations
from sparcur.sparcron import get_redis_conn
from sparcur.sparcron import _none, _qed, _run, _qed_run

log = _log.getChild('cron')

dev_hack = False

if dev_hack:
    log.debug('dev_hack enabled: fetch will always happen')

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

project_ids = auth.get_list('remote-organizations')
data_path = auth.get_path('data-path')
path_source_dir = (
    Path('~/files/sparc-datasets-test').expanduser().resolve()
    if data_path is None else
    data_path)
path_log_base = auth.get_path('log-path')
path_log_datasets = path_log_base / 'datasets'

if not path_source_dir.exists():
    path_source_dir.mkdir(parents=True)
if not path_log_datasets.exists():
    path_log_datasets.mkdir(parents=True)


def ensure_caches():
    # make sure that all other values that should
    # be cached from the network are cached
    from sparcur import datasources as d
    #od = d.OrganData()


ensure_caches()

# celeary setup

cel = Celery('sparcur-cron',)

cel.conf.worker_hijack_root_logger = False
cel.conf.worker_prefetch_multiplier = 1

log.info(f'STATUS sparcur :ids {project_ids} :path {path_source_dir}')

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
    if name == 'sparcur.sparcron.core.check_for_updates':
        out = {'exchange': 'cron', 'routing_key': 'task.cron', 'priority': 10, 'queue': 'cron'}
    elif name == 'sparcur.sparcron.core.check_sheet_updates':
        out = {'exchange': 'cron', 'routing_key': 'task.cron', 'priority': 10, 'queue': 'cron'}
    elif name == 'sparcur.sparcron.core.heartbeat':
        out = {'exchange': 'cron', 'routing_key': 'task.cron', 'priority': 3, 'queue': 'cron'}
    elif name == 'sparcur.sparcron.core.export_single_dataset':
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

# don't add export to the core control consumer, we need a separate process
# that the work pool is separate and does not block the cron pool

#cel.control.add_consumer(
#    queue='export',
#    exchange='export',
#    routing_key='task.export',
#)


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
    initd = {}
    for uuid in uuids:
        dataset_id = 'N:dataset:' + uuid
        try:
            # catch potentially malformed ids
            did = RemoteId(dataset_id)
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
            internal_version = (int(blob['prov']['sparcur_internal_version'])
                                if not dev_hack and 'sparcur_internal_version' in blob['prov']
                                else 0)
            fs_version = (int(blob['prov']['sparcur_fs_translation_version'])
                          if 'sparcur_fs_translation_version' in blob['prov']
                          else 0)
            rd_version = (int(blob['inputs']['remote_dataset_metadata']['_meta_version'])
                          if ('inputs' in blob and
                              'remote_dataset_metadata' in blob['inputs'] and
                              '_meta_version' in blob['inputs']['remote_dataset_metadata'])
                          else 0)

            sid = 'state-' + dataset_id
            uid = 'updated-' + dataset_id
            fid = 'failed-' + dataset_id
            vid = 'verpi-' + dataset_id
            vfid = 'verfs-' + dataset_id
            vrid = 'verrd-' + dataset_id
            eid = 'sheet-' + dataset_id
            initd.update(
                {sid: _none,
                 uid: updated,
                 fid: '',
                 vid: internal_version,
                 vfid: fs_version,
                 vrid: rd_version,
                 eid: 0,
                 })

    conn.mset(initd)

    _keys = [k for k in sorted(conn.keys()) if b'N:dataset' in k]
    _values = conn.mget(_keys)
    _inits = {k:v for k, v in zip(_keys, _values)}
    log.log(9, pprint.pformat(_inits, width=120))


@cel.on_after_finalize.connect
def cleanup_redis(sender, **kwargs):
    """ For the time being ensure that any old data about process
        state is wiped when we restart. """
    i = cel.control.inspect()  # FIXME yes, still has a data race and can run twice
    if i.active_queues() is None:
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
                             check_for_updates.s(project_ids),
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



@cel.task
def heartbeat():  # FIXME this has to run in a separate priority queue with its own worker
    #print(shared_state)
    #print(dataset_status)
    keys = conn.keys()
    _skeys = [k for k in keys if b'state-N:dataset:' in k]
    _fkeys = [k for k in keys if b'failed-' in k]
    vals = [int(v) for v in conn.mget(_skeys)]
    fvals = [v for v in conn.mget(_fkeys)]
    fails = [k for k, v in zip(_fkeys, fvals) if v]

    #fails = [k for k in keys if b'failed-' in k and conn.get(k)]
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


dataset_project = {}
def argv_simple_retrieve(dataset_id):
    project_id = dataset_project[dataset_id]
    return [
        sys.executable,
        '-m',
        'sparcur.simple.retrieve',
        '--sparse-limit',
        '-1',
        '--no-index',
        '--project-id',
        project_id,
        '--dataset-id',
        dataset_id,
        '--parent-parent-path',
        path_source_dir]


def argv_simple_fetch_remote_metadata_all(dataset_id):
    project_id = dataset_project[dataset_id]
    return [
        sys.executable,
        '-m',
        'sparcur.simple.fetch_remote_metadata_all',
        '--project-id',
        project_id,
        '--dataset-id',
        dataset_id]


def argv_spc_find_meta(dataset_id):
    argv = [
        sys.executable,
        '-m',
        'sparcur.cli',
        "find",
        '--dataset-id', dataset_id,  # nop, debug memory issues
        "--name", "*.xml",
        "--name", "submission*",
        "--name", "curation*",
        "--name", "code_description*",
        "--name", "dataset_description*",
        "--name", "subjects*",
        "--name", "samples*",
        "--name", "sites*",
        "--name", "performances*",
        "--name", "manifest*",
        "--name", "resources*",
        "--name", "README*",
        '--no-network',
        "--limit", "-1",
        "--fetch"]
    return argv


def argv_spc_export(dataset_id):
    argv = [
        sys.executable,
        '-m',
        'sparcur.cli',
        'export',
        '--no-network',
        '--do-objects',
        # explicitly avoid joblib which induces absurd process overhead
        '--jobs', '1',
        '--dataset-id', dataset_id,  # nop, debug memory issues
    ]
    return argv


def ret_val_exp(dataset_id, updated, time_now, fetch=True, fetch_rmeta=True):
    timestamp = time_now.START_TIMESTAMP_LOCAL_FRIENDLY
    log.info(f'START {dataset_id} {timestamp}')
    did = RemoteId(dataset_id)
    uid = 'updated-' + dataset_id
    fid = 'failed-' + dataset_id
    vid = 'verpi-' + dataset_id
    vfid = 'verfs-' + dataset_id
    vrid = 'verrd-' + dataset_id

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

    # FIXME somehow rmeta is missing ??!?!?!

    logdir = path_log_datasets / did.uuid
    logfile = logdir / timestamp / 'stdout.log'
    latest = logdir / 'LATEST'
    if not logfile.parent.exists():
        logfile.parent.mkdir(parents=True)

    if latest.exists():
        latest.unlink()

    latest.symlink_to(timestamp)

    try:
        # FIXME urllib3.exceptions.MaxRetryError and requests.exceptions.RetryError
        # can happen, those cases currently return as failed and they are usually correlated
        # the follow command shows the times
        # cat <(grep -L 'docopt args' $(grep -rl RetryError ~/.cache/log/sparcur/datasets/)) | awk 'BEGIN { FS = "/" } ; { print $(NF-1) }' | sort
        # this one shows the logs
        # less -R $(grep -L 'docopt args' $(grep -rl RetryError ~/.cache/log/sparcur/datasets/))
        # ideally these cases should not be marked as failures of the local process
        # but instead stuck back into the queue after some wait period, not entirely
        # clear how to do this though ... exponential backoff etc.
        with open(logfile, 'wt') as logfd:
            if fetch:
                try:
                    p1 = subprocess.Popen(
                        argv_simple_retrieve(dataset_id),
                        stderr=subprocess.STDOUT, stdout=logfd)
                    out1 = p1.communicate()
                    if p1.returncode != 0:
                        raise exc.SubprocessException(f'oops retr return code was {p1.returncode}')
                except KeyboardInterrupt as e:
                    p1.send_signal(signal.SIGINT)
                    raise e

                dataset_path = (path_source_dir / did.uuid / 'dataset').resolve()
                try:
                    os.sync()  # try to avoid missing xattr metadata
                    p2 = subprocess.Popen(
                        argv_spc_find_meta(did.uuid), cwd=dataset_path,
                        stderr=subprocess.STDOUT, stdout=logfd)
                    out2 = p2.communicate()
                    if p2.returncode != 0:
                        raise exc.SubprocessException(f'oops find return code was {p2.returncode}')
                except KeyboardInterrupt as e:
                    p2.send_signal(signal.SIGINT)
                    raise e
            elif fetch_rmeta:
                # fetch_rmeta really means "fetch rmeta even if we didn't fetch all"
                try:
                    p1r = subprocess.Popen(
                        argv_simple_fetch_remote_metadata_all(dataset_id),
                        stderr=subprocess.STDOUT, stdout=logfd)
                    out1r = p1r.communicate()
                    if p1r.returncode != 0:
                        raise exc.SubprocessException(f'oops retr return code was {p1r.returncode}')
                except KeyboardInterrupt as e:
                    p1r.send_signal(signal.SIGINT)
                    raise e
            else:
                # sheet updated case
                dataset_path = (path_source_dir / did.uuid / 'dataset').resolve()

            try:
                p3 = subprocess.Popen(argv_spc_export(did.uuid), cwd=dataset_path,
                                      stderr=subprocess.STDOUT, stdout=logfd)
                out3 = p3.communicate()
                if p3.returncode != 0:
                    raise exc.SubprocessException(f'oops expr return code was {p3.returncode}')
            except KeyboardInterrupt as e:
                p3.send_signal(signal.SIGINT)
                raise e

        toset = {
            uid: updated,
            # XXX for vid some weird environment thing could mean that the internal
            # version might not match what was embedded in prov, but really that
            # should never happen, we just don't want to have to open the json file
            # in this process
            # XXX one way this can happen is if the subprocess calls run a new
            # version of the code when the celery process has not been restarted
            vid: sparcur.__internal_version__,
            # XXX similar issues with these if there is some inherited class that
            # overwrites so don't do that
            vfid: Remote._translation_version,
            vrid: RemoteDatasetData._translation_version,
        }
        conn.mset(toset)
        conn.delete(fid)
        log.info(f'DONE: u: {uid} {updated}')
    except exc.SubprocessException as e:
        log.critical(f'FAIL: {fid} {updated}')
        conn.set(fid, updated)
        log.exception(e)
    except Exception as e:
        with open(logfile, 'at') as logfd:
            logfd.write('\nunhandled error occured while processing this dataset\n')

        # use the uuid to ensure that if multiple datasets encounter an error
        # at the same time that they will not try to use the same logger
        # and wind up logging messages to incorrect files which would induce
        # all sorts of creeping insanity when trying to debug
        flog = log.getChild(did.uuid)
        handler = logToFile(flog, logfile)
        try:
            flog.critical(f'ERROR: {fid} {updated}')
            flog.exception(e)
        finally:
            flog.removeHandler(handler)


@cel.task
def export_single_dataset(dataset_id, qupdated_when_called):
    (updated, qupdated, _, _, _, fs_version, rd_version,
     _, _, _, _, _) = mget_all(dataset_id)

    if qupdated is None:
        #  conn.set(qid, dataset.updated) is called IMMEDIATELY before
        # export_single_dataset.delay, so it should ALWAYS find a value
        # ... if _qupdated is None this means redis and rabbit are out of
        # sync and yes, rabbitmqctl list_queues shows export 187 because
        # we clear redis on restart when those mesages get sent there is
        # nothing in redis
        msg = (f'Task to run {dataset_id} was called when qupdated was None! '
               'Doing nothing. Usual cause is stale messages in rabbit.')
        log.info(msg)
        return

    if qupdated_when_called < qupdated:
        log.debug(f'queue updated changed between call and run')

    # have to calculate fetch here because a sheet change may arrive
    # immediately before a data change and fetch is based on all events
    # that trigger a run, not just the first
    # < can be false if only the sheet changed
    fetch = (((updated < qupdated) if updated is not None else True) or
             dev_hack or
             (fs_version < Remote._translation_version))

    rdd = RemoteDatasetData(dataset_id)
    fetch_rmeta = (
        # rmeta cache can be missing even if fs data is fresh
        not rdd.cache.exists() or
        rd_version < RemoteDatasetData._translation_version)

    sid = 'state-' + dataset_id
    eid = 'sheet-' + dataset_id
    time_now = GetTimeNow()
    conn.incr(sid)
    conn.set(eid, 0)
    #log.critical(f'STATE INCRED TO: {conn.get(sid)}')
    status_report()

    ret_val_exp(dataset_id, qupdated, time_now, fetch, fetch_rmeta)
    _sid = conn.get(sid)
    state = int(_sid) if _sid is not None else None
    conn.decr(sid)  # we always go back 2 either to none or queued
    conn.decr(sid)
    if state == _qed_run:
        qid = 'queued-' + dataset_id
        _qupdated = conn.get(qid)
        # there are race conditions here, but we don't care because we
        # can't get a sane file list snapshot anyway
        qupdated = _qupdated.decode() if _qupdated is not None else None
        export_single_dataset.delay(dataset_id, qupdated)

    status_report()


def diff_sheet(old_s, new_s):
    old_r0 = old_s.row_object(0)
    new_r0 = new_s.row_object(0)
    old_rh = old_r0.header
    new_rh = new_r0.header
    valid_columns = [h for h in new_rh if
                     h.startswith('modality') or
                     h.startswith('technique') or
                     h in ('organ_term', 'award_manual')]
    old_sheet_ids = [i for i in getattr(old_r0, old_s.index_columns[0])().column.values[1:] if i]
    new_sheet_ids = [i for i in getattr(new_r0, old_s.index_columns[0])().column.values[1:] if i]
    for did in new_sheet_ids:
        new_row, _didn = new_s._row_from_index('id', did)

        if did not in old_sheet_ids:
            log.debug(f'new dataset {did}')
            # pretend like it was already in the sheet
            old_row, _dido = type('FakeRow', (object,),
                                  {'values': ['' for _ in new_row],
                                   'award_manual': None,
                                   },), did
        else:
            old_row, _dido = old_s._row_from_index('id', did)

        if old_row.values != new_row.values:
            # using valid columns elides checking header ordering changes
            log.debug(f'sheet row changed for {did}')
            #log.debug(f'{old_row.values}, {new_row.values}')
            #log.debug(f'{list(zip(old_row.values, new_row.values))}')
            for v in valid_columns:
                old_c = getattr(old_row, v)()
                new_c = getattr(new_row, v)()
                if old_c.value != new_c.value:
                    log.debug(f'sheet value {v} changed for {did}: {old_c.value!r} {new_c.value!r}')
                    eid = 'sheet-' + RemoteId(did).id
                    conn.incr(eid)


@cel.task
def check_sheet_updates():
    # XXX NOTE this is oblivious to the semantics of the changes to the sheets
    # so ANY time a change happens for a dataset it will trigger another run
    # even if those changes went back to a previous state
    for sheetcls in (Affiliations, Organs):
        old_s = sheetcls(fetch=False)
        try:
            assert old_s._only_cache
            old_s.fetch()
            old = old_s._values
        except AttributeError as e:
            log.exception(e)
            # FIXME risk of some other source of AttributeError
            # triggering this which this is specifically trying to
            # handle the case where the cache does not exist
            old = None

        s = sheetcls(fetch=False)
        s._only_cache = False
        s._setup_saf()
        s._setup()
        s._saf = None
        assert not s._do_cache
        assert not s._only_cache
        s.fetch(fetch_meta=False)
        new = s._values
        #log.debug(pprint.pformat(s._meta_file, width=120))
        #log.info(f':sheet {sheetcls.__name__} :old {old} :new {new}')
        if new != old:
            # something has changed
            # see if old exists
            old_exists = not (old is None and not hasattr(old_s, '_values'))
            if not old_exists:
                log.info(f'No existing cache for {sheetcls}')

            # fetch the latest changes
            s._do_cache = True
            s._re_cache = True
            s._setup()
            s.fetch()  # NOTE metadata_file will often be stale
            log.info(f'spreadsheet cache updated for {s.name}')
            if old_exists:
                # avoid trying to call diff on a nonexistent state
                # we can't call fetch again because old_s will access
                # the cache populated by the call to s.fetch() below

                # if there is no cache then there is no point in
                # running anything because we don't know what changed
                # also this usually only happens on the very first run
                # in which case all datasets are going in the queue
                try:
                    diff_sheet(old_s, s)  # enqueues changed datasets
                except Exception as e:
                    log.exception(e)


@cel.task
def check_discover_updates():  # TODO
    pass


def mget_all(dataset_id):
    sid = 'state-' + dataset_id
    uid = 'updated-' + dataset_id
    fid = 'failed-' + dataset_id
    vid = 'verpi-' + dataset_id
    vfid = 'verfs-' + dataset_id
    vrid = 'verrd-' + dataset_id
    qid = 'queued-' + dataset_id
    eid = 'sheet-' + dataset_id

    (_updated, _qupdated, _failed, _state, _internal_version, _fs_version, _rd_version, _sheet_changed
     ) = conn.mget(uid, qid, fid, sid, vid, vfid, vrid, eid)
    updated = _updated.decode() if _updated is not None else None
    qupdated = _qupdated.decode() if _qupdated is not None else None
    failed = _failed.decode() if _failed is not None else None
    state = int(_state) if _state is not None else None
    internal_version = (int(_internal_version.decode())
                        if _internal_version is not None
                        else 0)  # FIXME new dataset case?
    fs_version = (int(_fs_version.decode())
                  if _fs_version is not None
                  else 0)  # FIXME new dataset case?
    rd_version = (int(_rd_version.decode())
                  if _rd_version is not None
                  else 0)  # FIXME new dataset case?
    sheet_changed = int(_sheet_changed) if _sheet_changed is not None else None
    ivc = internal_version < sparcur.__internal_version__
    fvc = fs_version < Remote._translation_version
    rvc = rd_version < RemoteDatasetData._translation_version
    log.log(9, (
        ivc,
        fvc,
        rvc,
        internal_version, sparcur.__internal_version__,
        fs_version, Remote._translation_version,
        rd_version, RemoteDatasetData._translation_version,
    ))
    pipeline_changed = (ivc or fvc or rvc)
    rq = state == _qed_run
    running = state == _run or rq
    queued = state == _qed or rq
    return (updated, qupdated, failed, state, internal_version, fs_version, rd_version,
            sheet_changed, pipeline_changed, rq, running, queued)


def check_single_dataset(dataset):
    dataset_id = dataset.id
    sid = 'state-' + dataset_id
    qid = 'queued-' + dataset_id
    # FIXME there are so many potential race conditions here ...
    (updated, qupdated, failed, state, internal_version, fs_version, rd_version,
     sheet_changed, pipeline_changed, rq, running, queued) = mget_all(dataset_id)

    # XXX race condition if export_single_dataset conn.incr(sid) is called
    # while this function is after the call conn.get(sid) and the logic below

    #log.debug(f'STATUS :id {dataset_id} :u {updated} :f {failed} :q {queued} :r {running}')
    # All the logic for whether to run a particular dataset
    # timestamp_updated or timestamp_updated_contents whichever is greater
    # NOTE we populate updated values into redis at startup from
    # the latest export of each individual dataset
    # TODO also need to check sparcur code changes to see if we need to rerun
    if ((pipeline_changed or sheet_changed) and not failed or
        not (updated or failed) or
        failed and dataset.updated > failed or
        not failed and updated and dataset.updated > updated):

        log.debug((f'MAYBE ENQUEUE :id {dataset_id} s: {state} du: '
                   f'{dataset.updated} u: {updated} f: {failed}'))
        if queued:
            if dataset.updated > qupdated:
                conn.set(qid, dataset.updated)
        elif running and dataset.updated == qupdated and not sheet_changed:
            pass
        elif running and (dataset.updated > qupdated or sheet_changed):
            conn.incr(sid)
            conn.set(qid, dataset.updated)
        else:
            conn.incr(sid)
            conn.set(qid, dataset.updated)
            export_single_dataset.delay(dataset_id, dataset.updated)


@cel.task
def check_for_updates(project_ids):
    # yes we want to pass project ids in here even though it means
    # that in the current implementation that we have to restart
    # to change the set of projects that we run over
    for dataset in datasets_remote_from_project_ids(project_ids):
        check_single_dataset(dataset)


def get_roots(project_ids):
    roots = {}
    for project_id in project_ids:
        # have to construct remote specific Local and Cache
        # so that the mapping stays 1:1 otherwise we can get
        # some really wonky behavior
        class LPath(Path): pass
        LPath._bind_flavours()
        class Cache(PennsieveCache): pass
        Cache._bind_flavours()

        PennsieveRemote = backend_pennsieve(
            project_id,
            Local=LPath,
            Cache=Cache,)
        root = PennsieveRemote(project_id)
        roots[project_id] = root

    return roots


_org_src_to_dataset = {}
_dataset_to_org_src = {}
roots = get_roots(project_ids)
def datasets_remote_from_project_id(project_id, datasets_no):
    root = roots[project_id]
    datasets = [c for c in root.children if c.id not in datasets_no]
    _rii = root.bfobject.int_id
    for d in datasets:
        new = d.id not in dataset_project
        if new:
            dataset_project[d.id] = project_id
            _dii = d.bfobject.int_id
            _did = d.identifier
            _org_src_to_dataset[_rii, _dii] = _did
            _dataset_to_org_src[_did] = _rii, _dii
            conn.mset(
                {f'didos-{_rii}-{_dii}': _did.id,
                 f'org-src-{_did.uuid}': f'{_rii}/{_dii}',})

    return datasets


def datasets_remote_from_project_ids(project_ids):
    datasets_no = auth.get_list('datasets-no')
    datasets = []
    for project_id in project_ids:  # FIXME might want to spread these out in time?
        datasets.extend(datasets_remote_from_project_id(project_id, datasets_no))

    return datasets


def dataset_to_org_src(did):
    # local only, e.g. server should not use
    org, src = _dataset_to_org_src[did]
    return org, src


def org_src_to_dataset(org, src):
    # local only, e.g. server should not use
    did = _org_src_to_dataset[org, src]
    return did


def rd_dataset_to_org_src(did):
    # conn.keys('org-src-*')
    raw = conn.get(f'org-src-{did.uuid}')
    if not raw:
        raise KeyError(did)

    o, s = raw.split(b'/')
    return int(o), int(s)


def rd_org_src_to_dataset(org, src):
    # conn.keys('didos-*')
    raw = conn.get(f'didos-{org}-{src}')
    if not raw:
        raise KeyError((org, src))

    did = RemoteId(raw.decode())
    return did


def any_to_did(dataset_uuid):
    try:
        did = RemoteId(dataset_uuid, type='dataset')
    except Exception as e:
        ldu = len(dataset_uuid)
        if ldu == 24 and dataset_uuid[1] == ':':
            maybe_compact_id = dataset_uuid
        elif ldu == 22:
            maybe_compact_id = 'd:' + dataset_uuid
        else:
            raise e

        try:
            did = RemoteId.fromCompact(maybe_compact_id)
        except Exception as e2:
            raise e2 from e

    return did


def status_report(verbose_report=False):
    try:
        datasets = datasets_remote_from_project_ids(project_ids)
    except Exception as e:
        log.exception(e)
        return

    todo = []
    fail = []
    run = []
    que = 0
    for dataset in datasets:
        dataset_id = dataset.id
        sid = 'state-' + dataset_id
        uid = 'updated-' + dataset_id
        fid = 'failed-' + dataset_id
        vid = 'verpi-' + dataset_id
        vfid = 'verfs-' + dataset_id
        vrid = 'verrd-' + dataset_id

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
            run.append(dataset)
        if queued:
            que += 1

    runs  = '\n  '.join(sorted([RemoteId(f.id).uuid for f in run]))
    fails = '\n  '.join(sorted([RemoteId(f.id).uuid for f in fail]))
    todos = '\n  '.join(sorted([RemoteId(f.id).uuid for f in todo]))
    report = (
        '\nStatus Report\n'
        f'Datasets: {len(datasets)}\n'
        f'Failed:   {len(fail)}\n'
        f'Todo:     {len(todo)}\n'
        #f'Done:     {}\n'
        f'Running:  {len(run)}\n'
        f'Queued:   {que}\n'
        )

    if verbose_report:
        # since we can run python -m sparcur.sparcron.status to get
        # the detailed report on demand don't include these by default
        report += (
            '\n'
            f'Running:\n  {runs}\n'
            f'Failed:\n  {fails}\n'
            f'Todo:\n  {todos}\n'
        )

    log.info(report)
    #log.info('TODO:\n' + pprint.pformat(todo))


def test():
    check_sheet_updates()
    return
    datasets = datasets_remote_from_project_ids(project_ids)
    org, src = dataset_to_org_src(datasets[0].identifier)
    org1, src1 = dataset_to_org_src(any_to_did(datasets[0].identifier.base64uuid()))
    org5, src5 = dataset_to_org_src(any_to_did('d:' + datasets[0].identifier.base64uuid()))
    org2, src2 = dataset_to_org_src(any_to_did(datasets[0].identifier.uuid))
    org3, src3 = dataset_to_org_src(any_to_did(datasets[0].identifier.id))
    org4, src4 = dataset_to_org_src(any_to_did(datasets[0].identifier.curie))
    did = org_src_to_dataset(org, src)
    #breakpoint()
    return
    status_report()
    #breakpoint()
    return
    reset_redis_keys(conn)
    populate_existing_redis(conn)

    datasets = datasets_remote_from_project_ids(project_ids)
    datasets = sorted(datasets, key=lambda r:r.id)[:3]
    dataset = datasets[0] # 0, 1, 2 # ok, unfetched xml children, ok
    dataset_id = dataset.id
    updated = dataset.updated
    time_now = GetTimeNow()
    ret_val_exp(dataset_id, updated, time_now)


if __name__ == '__main__':
    test()
