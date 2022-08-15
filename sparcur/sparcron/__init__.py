from celery import Celery

_none = 0
_qed = 1
_run = 2
_qed_run = 3

state_lut = {
    _none: 'idle',
    _qed: 'queued',
    _run: 'running',
    _qed_run: 'running-queued',
}


def get_redis_conn():
    rc = Celery(backend='redis://',
                broker='redis://')
    return rc.backend.client


if __name__ == 'sparcur.sparcron':
    import sys
    if (sys.argv[0].endswith('celery') or
        'celery' in sys.argv):
        import sparcur.sparcron.core as celery
