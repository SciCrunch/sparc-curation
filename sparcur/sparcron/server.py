from sparcur.sparcron import get_redis_conn
from .endpoints import make_app

conn = get_redis_conn()
app, *_ = make_app(conn)

if __name__ == '__main__':
    app.run(host='localhost', port=7252, threaded=True)
