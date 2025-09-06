from sparcur.utils import PennsieveId, log as _log
from sparcur.sparcron import get_redis_conn, state_lut, _qed, _run, _qed_run

log = _log.getChild('cron.status')


def dataset_status(conn, rawid):
    pid = PennsieveId(('dataset:' + rawid.split(':')[-1]))
    prefixes = 'state', 'updated', 'failed', 'sheet', 'verpi'
    keys = [f'{prefix}-{pid.id}' for prefix in prefixes]
    values = conn.mget(keys)
    out = {p:v for p, v in zip(prefixes, values)}
    out['id'] = pid.id
    out['state'] = state_lut[int(out['state'])]
    f = out['failed']
    out['failed'] = f.decode() if f else False
    out['sheet'] = None if out['sheet'] is None else int(out['sheet'])
    out['pipeline_internal_version'] = None if out['verpi'] is None else int(out.pop('verpi'))
    if out['updated'] is not None:
        out['updated'] = out['updated'].decode()
    if out['failed'] and out['updated'] and out['failed'] < out['updated']:
        out['failed'] = False

    return out


def dataset_fails(conn):
    _fkeys = list(conn.scan_iter('failed-*'))
    fvals = [v for v in conn.mget(_fkeys)]
    _fails = [(PennsieveId(('dataset:' + k.split(b':')[-1].decode())), v)
              for k, v in zip(_fkeys, fvals) if v]
    _ukeys = ['updated-N:dataset:' + i.uuid for i, _ in _fails]
    uvals = [v for v in conn.mget(_ukeys)]
    fails = [i for (i, f), u in zip(_fails, uvals) if not u or f > u]
    refails = [i for (i, f), u in zip(_fails, uvals) if not u or f <= u]
    # there should never be f < u cases, it means a state machine invariant was
    # violated but for sanity we use f <= u so we will see them if they happen
    dangerzone = [i for (i, f), u in zip(_fails, uvals) if not u or f < u]
    if dangerzone:
        log.error(f'fail clearing invariant violated for {dangerzone}')

    return fails, refails


def _dataset_thinging(conn, thing):
    _skeys = list(conn.scan_iter('state-*'))
    svals = [v for v in conn.mget(_skeys)]
    running = [PennsieveId(('dataset:' + k.split(b':')[-1].decode()))
               for k, v in zip(_skeys, svals) if int(v) in thing]
    return running


def dataset_running(conn):
    return _dataset_thinging(conn, (_run, _qed_run))


def dataset_queued(conn):
    return _dataset_thinging(conn, (_qed, _qed_run))


def main():
    import sys
    from pprint import pprint
    conn = get_redis_conn()
    fails, refails = dataset_fails(conn)
    running = dataset_running(conn)
    queued = dataset_queued(conn)
    if '--summary' in sys.argv:
        print(
            f':n-fails {len(fails)} + {len(refails)}\n'
            f':n-running {len(running)}\n'
            f':n-queued {len(queued)}'
        )
        return
    elif '--fail-logs' in sys.argv:
        if fails:
            from sparcur.config import auth
            lp = auth.get_path('log-path') / 'datasets'
            paths = '\n'.join((lp / f.uuid / 'LATEST/stdout.log').as_posix()
                                 for f in fails)
            print(paths)
        return

    if fails:
        _f = '\n'.join(sorted([f.uuid for f in fails]))
        print(f':fails (\n{_f}\n)')
        pprint(dataset_status(conn, fails[0].uuid))

    if refails:
        _f = '\n'.join(sorted([f.uuid for f in refails]))
        print(f':refails (\n{_f}\n)')
        pprint(dataset_status(conn, refails[0].uuid))

    if running:
        _r = '\n'.join(sorted([r.uuid for r in running]))
        print(f':running (\n{_r}\n)')
        pprint(dataset_status(conn, running[0].uuid))

    if queued:
        _r = '\n'.join(sorted([q.uuid for q in queued]))
        print(f':queued (\n{_r}\n)')
        pprint(dataset_status(conn, queued[0].uuid))


if __name__ == '__main__':
    main()
