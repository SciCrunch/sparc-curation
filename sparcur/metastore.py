import sqlite3


class MetaStore:
    """ A local backup against accidental xattr removal """
    _attrs = ('bf.id',
              'bf.file_id',
              'bf.size',
              'bf.created_at',
              'bf.updated_at',
              'bf.checksum',
              'bf.error')
    attrs = 'xattrs',
    # FIXME horribly inefficient 1 connection per file due to the async code ... :/
    def __init__(self, db_path):
        self.db_path = db_path
        self.setup()

    def conn(self):
        return sqlite3.connect(self.db_path.as_posix())

    def setup(self):
        if not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True)

        sqls = (('CREATE TABLE IF NOT EXISTS path_xattrs'
                 '('
                 'id TEXT PRIMARY KEY,'  # for hypothesis ids this can be string(??)
                 'xattrs BLOB'  # see path meta for the packed representation
                 ');'),
                ('CREATE UNIQUE INDEX IF NOT EXISTS path_xattrs_u_id ON path_xattrs (id);'))
        conn = self.conn()
        with conn:
            for sql in sqls:
                conn.execute(sql)

    def bulk(self, id_blobs):  # FIXME no longer a dict really ...
        cols = ', '.join(_.replace('.', '_') for _ in self.attrs)
        values_template = ', '.join('?' for _ in self.attrs)
        sql = ('INSERT OR REPLACE INTO path_xattrs '
               f'(id, {cols}) VALUES (?, {values_template})')
        conn = self.conn()
        with conn:
            for id, blob in id_blobs:
                conn.execute(sql, args)
            return
            for path, attrs in pdict.items():
                args = path.as_posix(), *self.convert_attrs(attrs)
                conn.execute(sql, args)

    def remove(self, path):
        sql = 'DELETE FROM path_xattrs WHERE id = ?'
        args = path.as_posix(),
        conn = self.conn()
        with conn:
            return conn.execute(sql, args)

    def convert_attrs(self, attrs):
        for key in self.attrs:
            if key in attrs:
                yield attrs[key]
            else:
                yield None

    def xattrs(self, path):
        sql = 'SELECT xattrs FROM path_xattrs WHERE id = ?'
        args = path.as_posix(),
        conn = self.conn()
        with conn:
            cursor = conn.execute(sql, args)
            blob = cursor.fetchone()
            if blob:
                return PathMeta.from_metastore(blob)
            #print(values)
            #if values:
                #return 
                #keys = [n.replace('_', '.', 1) for n, *_ in cursor.description]
                #print(keys, values)
                #return {k:v for k, v in zip(keys, values) if k != 'path' and v is not None}  # skip path itself
            #else:
                #return {}

    def setxattr(self, path, key, value):
        return self.setxattrs(path, {key:value})

    def setxattrs(self, path, attrs):
        # FIXME skip nulls on replace
        cols = ', '.join(attrs)
        values_template = ', '.join('?' for _ in self.attrs)
        sql = (f'INSERT OR REPLACE INTO path_xattrs (id, {cols}) VALUES (?, {values_template})')
        args = path.as_posix(), *self.convert_attrs(attrs)
        conn = self.conn()
        with conn:
            return conn.execute(sql, args)

    def getxattr(self, path, key):
        if key in self.attrs:
            col = key.replace('.', '_')
            sql = f'SELECT {col} FROM path_xattrs WHERE id = ?'
            args = path.as_posix(),
            conn = self.conn()
            with conn:
                return conn.execute(sql, args)
        else:
            print('WARNING unknown key', key)
