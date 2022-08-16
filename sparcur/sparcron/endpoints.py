from flask import Flask, abort
from sparcur.utils import log
from .status import dataset_status, dataset_fails


def make_app(conn, name='sparcron-status-server'):
    app = Flask(name)
    yield app

    ctaj = {'Content-Type': 'application/json'}

    @app.route('/status/<id>')
    def route_status(id):
        try:
            return dataset_status(conn, id), 200, ctaj
        except Exception as e:
            log.exception(e)
            return abort(404)

    @app.route('/failed')
    def route_failed():
        _failed = dataset_fails(conn)
        failed = [f.id for f in _failed]  # explicit id instead of JEncode
        return {'failed': failed}, 200, ctaj
