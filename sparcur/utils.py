from pyontutils.utils import makeSimpleLogger, python_identifier  # FIXME update imports
from pyontutils.namespaces import sparc
from sparcur.config import config

log = makeSimpleLogger('sparcur')
logd = makeSimpleLogger('sparcur-data')


class FileSize(int):
    @property
    def mb(self):
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

    def __repr__(self):
        return f'{self.__class__.__name__} <{self.hr} {self}>'
