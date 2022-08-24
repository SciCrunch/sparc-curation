from sparcur.utils import PennsieveId
from sparcur.sparcron import get_redis_conn, state_lut


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
    out['sheet'] = int(out['sheet'])
    out['pipeline_internal_version'] = int(out.pop('verpi'))
    out['updated'] = out['updated'].decode()
    if out['failed'] and out['updated'] and out['failed'] < out['updated']:
        out['failed'] = False

    return out


def dataset_fails(conn):
    keys = conn.keys()
    _fkeys = [k for k in keys if b'failed-' in k]
    fvals = [v for v in conn.mget(_fkeys)]
    _fails = [(PennsieveId(('dataset:' + k.split(b':')[-1].decode())), v)
              for k, v in zip(_fkeys, fvals) if v]
    _ukeys = ['updated-N:dataset:' + i.uuid for i, _ in _fails]
    uvals = [v for v in conn.mget(_ukeys)]
    fails = [i for (i, f), u in zip(_fails, uvals) if not u or f > u]
    return fails


def main():
    from pprint import pprint
    conn = get_redis_conn()
    fails = dataset_fails(conn)
    if fails:
        pprint(fails)
        pprint(dataset_status(conn, fails[0].uuid))

    if False:
        pprint(dataset_status(
            conn, keys[0].split(b':')[-1].decode()))


if __name__ == '__main__':
    main()
