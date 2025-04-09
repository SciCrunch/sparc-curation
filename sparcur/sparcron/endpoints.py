from flask import Flask, abort
from sparcur.utils import log
from .status import dataset_status, dataset_fails
from .core import rd_dataset_to_org_src, rd_org_src_to_dataset, any_to_did


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
            abort(404)

    @app.route('/failed')
    def route_failed():
        _failed = dataset_fails(conn)
        failed = [f.id for f in _failed]  # explicit id instead of JEncode
        return {'failed': failed}, 200, ctaj

    @app.route('/id-map/dataset/<dataset_uuid>')
    def route_id_map_uuid(dataset_uuid):
        # convert from whatever representation we have
        try:
            did = any_to_did(dataset_uuid)
        except Exception as e:
            log.exception(e)
            abort(404)

        # lookup ord_id and src_id
        try:
            org, src = rd_dataset_to_org_src(did)
        except KeyError as e:
            log.exception(e)
            abort(404)

        #return {'org': org, 'src': src}, 200, ctaj
        return f'{org}/{src}'

    @app.route('/id-map/org-src/<org>/<src>')
    def route_id_map_pub(org, src):
        # TODO might also want to return the internal org id?
        try:
            o = int(org)
            s = int(src)
        except ValueError as e:
            log.exception(e)
            abort(404)

        try:
            did = rd_org_src_to_dataset(o, s)
        except KeyError as e:
            log.exception(e)
            abort(404)

        #return {'uuid': did}, 200, ctaj
        return did.id
