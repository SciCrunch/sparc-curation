"""
Classes for storing and converting metadata associated with a path or path like object.
"""
import pickle
import struct
from pathlib import PurePosixPath
from datetime import datetime
from dateutil import parser


class FileSize(int):
    @property
    def md(self):
        return self / 1024 ** 2

    @property
    def hr(self):
        """ human readable file size """

        def sizeof_fmt(num, suffix=''):
            for unit in ['','K','M','G','T','P','E','Z']:
                if abs(num) < 1024.0:
                    return "%0.0f%s%s" % (num, unit, suffix)
                num /= 1024.0
            return "%.1f%s%s" % (num, 'Yi', suffix)

        if self is not None and self >= 0:
            return sizeof_fmt(self)
        else:
            return '??'  # sigh


class PathMeta:
    """ Internal representation that every other format converts through. """

    # TODO register xattr prefixes

    @classmethod
    def from_metastore(cls, blob, prefix=None):
        """ db entry """
        xattrs = pickle.loads(blob)
        return cls.from_xattrs(xattrs, prefix)


    def __init__(self,
                 size=None,
                 created=None,
                 updated=None,
                 checksum=None,
                 id=None,
                 file_id=None,
                 old_id=None,
                 gid=None,  # needed to determine local writability
                 user_id=None,
                 mode=None,
                 errors=None,
                 **kwargs):
        self.size = size if size is None else FileSize(size)
        self.created = created
        self.updated = updated
        self.checksum = checksum
        self.id = id
        self.file_id = file_id
        self.old_id = old_id
        self.gid = gid
        self.user_id = user_id
        self.mode = mode
        self.errors = errors
        if kwargs:
            log.warning(f'Unexpected meta values! {kwargs}')
            self.__kwargs = kwargs  # roundtrip values we don't explicitly handle

    #def as_xattrs(self, prefix=None):
        #log.debug(f'{self} {prefix}')
        #embed()
        #return self._as_xattrs(self, prefix=prefix)


    def items(self):
        return self.__dict__.items()  # FIXME nonfields?
        #for field in self.fields:
            #yield field, getattr(self, field)

    def as_metastore(self, prefix=None):
        # FIXME prefix= is a bad api ...
        """ db entry """  # TODO json blob in sqlite? can it index?
        return pickle.dumps(self.as_xattrs(prefix))

    def __iter__(self):
        yield from (k for k in self.__dict__ if not k.startswith('_'))

    def __reduce__(self):
        return (self.__class__, {k:v for k, v in self.items() if v is not None})

    def __repr__(self):
        _dict = {k:v for k, v in self.__dict__.items() if not k.startswith('_')}
        return f'{self.__class__.__name__}({_dict})'

    def __eq__(self, other):
        if isinstance(other, PathMeta):
             for field, value in self.__dict__.items():
                  if value != getattr(other, field):
                       return False
             else:
                  return True

    def __bool__(self):
        for v in self.__dict__.values():
            if v is not None:
                return True
        else:
            return False


class _PathMetaConverter:
    """ Base class for implementing PathMeta format converts.
        
        I haven't figured out how to properly abstract registering
        as_format_name and from_format_name yet, so look at one of
        the __init__ methods below to see how it is done. """

    pathmetaclass = PathMeta
    format_name = None


class _PathMetaAsSymlink(_PathMetaConverter):
    """ Convert to and from symlinks to nowhere.
        You can prepend whatever other info you want in front of this """

    format_name = 'symlink'
    empty = '#'
    fieldsep = '.'  # must match the file extension splitter for Path ...
    subfieldsep = ';'  # only one level, not going recursive in a filename ...
    order = ('size',
             'created',
             'updated',
             'checksum',
             'old_id',
             'gid',
             'user_id',
             'mode',
             'errors',)
    extras = 'size.hr',
    order_all = order + extras

    def __init__(self):
        # register functionality on PathMeta
        def as_symlink(self, _as_symlink=self.as_symlink):
            return _as_symlink(self)

        @classmethod
        def from_symlink(cls, symlink_path, _from_symlink=self.from_symlink):
            return _from_symlink(symlink_path)

        self.pathmetaclass.as_symlink = as_symlink
        self.pathmetaclass.from_symlink = from_symlink

    def encode(self, field, value):
        if value is None:
            return self.empty

        if field in ('errors',):
            return self.subfieldsep.join(value)

        value = str(value)
        value = value.replace(self.fieldsep, ',')
        return value

    def decode(self, field, value):
        value = value.strip(self.fieldsep)

        if value == self.empty:
            return None

        if field == 'errors':
            return [_ for _ in value.split(self.subfieldsep) if _]

        elif field in ('created', 'updated'):
            return parser.parse(value)

        elif field == 'checksum':  # FIXME checksum encoding ...
            #return value.encode()
            return value.encode()

        elif field == 'user_id':
            try:
                return int(value)
            except ValueError:  # FIXME :/ uid vs owner_id etc ...
                return value

        elif field in ('id', 'mode'):
            return value

        else:
            return int(value)

        return value

    def as_symlink(self, pathmeta):
        """ encode meta as a relative path to be appended as a child, not a sibbling """

        class __ignoreme:
            """ I don't exist I swear. HAHA! You can never accidentally pass me in! """
            # lol what a hack

        def multigetattr(object, attr, default=__ignoreme):
            rest = None
            if '.' in attr:
                first, attr = attr.split('.')
                object = multigetattr(object, first, default=default)

            if default != __ignoreme:
                return getattr(object, attr, default)
            else:
                return getattr(object, attr)


        gen = (self.encode(field, multigetattr(pathmeta, field))
               for field in self.order_all)

        return PurePosixPath(pathmeta.id + '/.meta.' + self.fieldsep.join(gen))

    def from_symlink(self, symlink_path):
        """ contextual portion to make sure something weird isn't going on
            e.g. that a link got switched to point to another name somehow """
        pure_symlink = symlink_path.readlink()
        msg = (symlink_path.name, pure_symlink.parts[0])
        assert symlink_path.name == pure_symlink.parts[0], msg
        return self.from_pure_symlink(pure_symlink)

    def from_pure_symlink(self, pure_symlink):
        kwargs = {field:self.decode(field, value)
                  for field, value in zip(self.order, pure_symlink.suffixes)}
        kwargs['id'] = str(pure_symlink.parent)
        return self.pathmetaclass(**kwargs)

    #@classmethod
    #def from_symlink(cls, symlink_path):
        #if not symlink_path.is_symlink():
            #raise TypeError(f'Not a symlink! {symlink_path}')
        #return cls.symlink.from_symlink(symlink_path)


class _PathMetaAsXattrs(_PathMetaConverter):
    """ Convert to and from unix xattrs. """

    format_name = 'xattrs'

    fields = ('size', 'created', 'updated',
              'checksum', 'id', 'file_id', 'old_id',
              'gid', 'user_id', 'mode', 'errors')

    encoding = 'utf-8'

    def __init__(self):
        # register functionality on PathMeta
        def as_xattrs(self, prefix=None, _as_xattrs=self.as_xattrs):
            #log.debug(f'{self} {prefix}')
            return _as_xattrs(self, prefix=prefix)

        # as a note: information hiding in python is ... weird ...
        # even when that isn't what you set out to do ...
        @classmethod
        def from_xattrs(cls, xattrs, prefix=None, path_object=None, _from_xattrs=self.from_xattrs):
            # FIXME cls(**kwargs) vs calling self.pathmetaclass
            return _from_xattrs(xattrs, prefix=prefix, path_object=path_object)

        self.pathmetaclass.as_xattrs = as_xattrs
        self.pathmetaclass.from_xattrs = from_xattrs

    @staticmethod
    def deprefix(string, prefix):
        if string.startswith(prefix):
            string = string[len(prefix):]

        # deal with normalizing old form here until it is all cleaned up
        if prefix == 'bf.':
            if string.endswith('_at'):
                string = string[:-3]

        return string

    def from_xattrs(self, xattrs, prefix=None, path_object=None):
        """ decoding from bytes """
        _decode = getattr(path_object, 'decode_value', None)
        if path_object and _decode is not None:
            # some classes may need their own encoding rules _FOR NOW_
            # we will remove them once we standardize the xattrs format
            def decode(field, value, dv=_decode):
                out = dv(field, value)
                if value is not None and out is None:
                    out = self.decode(field, value)

                return out
                
        else:
            decode = self.decode


        if prefix:
            prefix += '.'
            kwargs = {k:decode(k, v)
                      for kraw, v in xattrs.items()
                      for k in (self.deprefix(kraw.decode(self.encoding), prefix),)}
        else:  # ah manual optimization
            kwargs = {k:decode(k, v)
                      for kraw, v in xattrs.items()
                      for k in (kraw.decode(self.encoding),)}

        return self.pathmetaclass(**kwargs)

    def as_xattrs(self, pathmeta, prefix=None):
        """ encoding to bytes """
        #log.debug(pathmeta)
        out = {}
        for field in self.fields:
            value = getattr(pathmeta, field)
            if value:
                value_bytes = self.encode(field, value)
                if prefix:
                    key = prefix + '.' + field
                else:
                    key = field

                key_bytes = key.encode(self.encoding)
                out[key_bytes] = self.encode(field, value)

        return out

    def encode(self, field, value):
        #if field in ('created', 'updated') and not isinstance(value, datetime):
            #field.replace(cls.path_field_sep, ',')  # FIXME hack around iso8601
            # turns it 8601 isnt actually standard >_< with . instead of , sigh

        if field == 'errors':
            value = ';'.join(value)
            
        if isinstance(value, datetime):  # FIXME :/ vs iso8601
            #value = value.isoformat().isoformat().replace('.', ',')
            value = value.timestamp()  # I hate dealing with time :/

        if isinstance(value, int):
            # this is local and will pass through here before move?
            #out = value.to_bytes(value.bit_length() , sys.byteorder)
            out = str(value).encode(self.encoding)  # better to have human readable
        elif isinstance(value, float):
            out = struct.pack('d', value) #= bytes(value.hex())
        elif isinstance(value, str):
            out = value.encode(self.encoding)
        else:
            raise exc.UnhandledTypeError(f'dont know what to do with {value!r}')

        return out

    def decode(self, field, value):
        if field in ('created', 'updated'):  # FIXME human readable vs integer :/
            value, = struct.unpack('d', value)
            return datetime.fromtimestamp(value)
        elif field == 'checksum':
            return value
        elif field == 'errors':
            value = value.decode(self.encoding)
            return tuple(_ for _ in value.split(';') if _)
        elif field == 'user_id':
            try:
                return int(value)
            except ValueError:  # FIXME :/ uid vs owner_id etc ...
                return value.decode()
        elif field in ('id', 'mode'):
            return value.decode(self.encoding)
        elif field not in self.fields:
            log.warning(f'Unhandled field {field}')
            return value
        else:
            try:
                return int(value)
            except ValueError as e:
                print(field, value)
                raise e

# register helpers
_PathMetaAsSymlink()
_PathMetaAsXattrs()
