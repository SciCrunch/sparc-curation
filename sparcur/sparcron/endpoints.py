from flask import Flask, abort
from sparcur.utils import log
from .status import dataset_status, dataset_fails


def make_app(conn, name='sparcron-status-server'):
    app = Flask(name)
    yield app

    @app.route('/status/<id>')
    def route_status(id):
        try:
            return dataset_status(conn, id), 200, {'Content-Type': 'application/json'}
        except Exception as e:
            log.exception(e)
            return abort(404)

    @app.route('/failed')
    def route_failed():
        failed = dataset_fails(conn)
        return {'failed': failed}, 200, {'Content-Type': 'application/json'}
