import hashlib
from pathlib import PosixPath
import xattr
import sqlite3
import subprocess


class Path(PosixPath):
    """ pathlib Path augmented with xattr support """

    def setxattr(self, key, value, namespace=xattr.NS_USER):
        if not isinstance(value, bytes):  # checksums
            bytes_value = str(value).encode()  # too smart? force pre encoded?
        else:
            bytes_value = value

        xattr.set(self.as_posix(), key, bytes_value, namespace=namespace)

    def setxattrs(self, xattr_dict, namespace=xattr.NS_USER):
        for k, v in xattr_dict.items():
            self.setxattr(k, v, namespace=namespace)

    def getxattr(self, key, namespace=xattr.NS_USER):
        # we don't deal with types here, we just act as a dumb store
        return xattr.get(self.as_posix(), key, namespace=namespace)

    def xattrs(self, namespace=xattr.NS_USER):
        # only decode keys ?
        return {k.decode():v for k, v in xattr.get_all(self.as_posix(), namespace=namespace)}

    def xopen(self):
        """ open file using xdg-open """
        subprocess.Popen(['xdg-open', self.as_posix()],
                         stdout=subprocess.DEVNULL,
                         stderr=subprocess.STDOUT)

    def checksum(self, cypher=hashlib.sha256):
        if self.is_file():
            m = cypher()
            chunk_size = 4096
            with open(self, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if chunk:
                        m.update(chunk)
                    else:
                        break

            return m.digest()


class MetaStore:
    """ A local backup against accidental xattr removal """
    attrs = ('bf.id',
             'bf.file_id',
             'bf.size',
             'bf.created_at',
             'bf.updated_at',
             'bf.checksum',
             'bf.error')
    # FIXME horribly inefficient 1 connection per file due to the async code ... :/
    def __init__(self, db_path):
        self.db_path = db_path
        self.setup()

    def conn(self):
        return sqlite3.connect(self.db_path.as_posix())

    def setup(self):
        if not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True)

        sqls = (('CREATE TABLE IF NOT EXISTS fsxattrs '
                 '(path TEXT PRIMARY KEY,'
                 'bf_id TEXT NOT NULL,'
                 'bf_file_id INTEGER,'
                 'bf_size INTEGER,'
                 'bf_created_at DATETIME,'
                 'bf_updated_at DATETIME,'
                 'bf_checksum BLOB,'
                 'bf_error INTEGER);'),
                ('CREATE UNIQUE INDEX IF NOT EXISTS fsxattrs_u_path ON fsxattrs (path);'))
        conn = self.conn()
        with conn:
            for sql in sqls:
                conn.execute(sql)

    def bulk(self, pdict):
        cols = ', '.join(attrs)
        values_template = ', '.join('?' for _ in self.attrs)
        sql = ('INSERT OR REPLACE INTO fsxattrs '
               f'(path, {cols}) VALUES (?, {values_template})')
        conn = self.conn()
        with conn:
            for path, attrs in pdict.items():
                args = path.as_posix(), *self.convert_attrs(attrs)
                conn.execute(sql, args)

    def remove(self, path):
        sql = 'DELETE FROM fsxattrs WHERE path = ?'
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
        sql = 'SELECT * FROM fsxattrs WHERE path = ?'
        args = path.as_posix(),
        conn = self.conn()
        with conn:
            cursor = conn.execute(sql, args)
            values = cursor.fetchone()
            print(values)
            if values:
                keys = [n.replace('_', '.', 1) for n, *_ in cursor.description]
                #print(keys, values)
                return {k:v for k, v in zip(keys, values) if k != 'path' and v is not None}  # skip path itself
            else:
                return {}

    def setxattr(self, path, key, value):
        return self.setxattrs(path, {key:value})

    def setxattrs(self, path, attrs):
        # FIXME skip nulls on replace
        cols = ', '.join(attrs)
        values_template = ', '.join('?' for _ in self.attrs)
        sql = (f'INSERT OR REPLACE INTO fsxattrs (path, {cols}) VALUES (?, {values_template})')
        args = path.as_posix(), *self.convert_attrs(attrs)
        conn = self.conn()
        with conn:
            return conn.execute(sql, args)

    def getxattr(self, path, key):
        if key in self.attrs:
            col = key.replace('.', '_')
            sql = f'SELECT {col} FROM fsxattrs WHERE path = ?'
            args = path.as_posix(),
            conn = self.conn()
            with conn:
                return conn.execute(sql, args)
        else:
            print('WARNING unknown key', key)
