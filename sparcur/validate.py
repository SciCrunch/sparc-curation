import re
from functools import wraps
from sparcur import schemas as sc

class HasSchema:
    def __init__(self, schema=None, normalize=False):
        self.schema = schema
        self.normalize = normalize

    def mark(self, cls):
        """ note that this runs AFTER all the methods """
        if self.schema is not None:
            cls._output = cls.output
            @property
            def output(_self):
                return self.schema.validate(cls._output)

            cls.output = output

        return cls

    def __call__(self, schema_class):
        # TODO switch for normalized output if value passes?
        schema = schema_class()
        def decorator(function):
            @wraps(function)
            def schema_wrapped_property(_self):
                data = function(_self)
                ok, norm_or_error, data = schema.validate(data)
                if not ok:
                    data['errors'] = norm_or_error.json()
                elif self.normalize:
                    return norm_or_error

                return data
            return schema_wrapped_property
        return decorator


hasSchema = HasSchema()
@hasSchema.mark
class Stage:
    # NOTE any iteration on these should be generator based
    # TODO do we need the def _step -> def step indirection?
    # TODO are there things we might want to persist here?
    def __init__(self, data=None):
        self._data = data

    @property
    def data(self):
        """ The 'raw' input, may be passed in at run time """
        # NOTE put io here for some stages
        # TODO what about lifting? where does that fit? restructuring?
        return self._data

    @property
    def normalized(self):
        data = self.data
        # rename column headings
        # rename row headings

        # local mappings of field values cat -> Felis Cattus
        return data

    @property
    def restructured(self):
        norm = self.normalized
        # reorder columns
        # reorder rows

        # convert column schema -> key value
        # convert a horizontal from a column schema -> list of key value

        # convert a row schema -> key value
        # convert a vertical from a row schema -> lit of key value

        # copy a value from one path to another
        # move a value from one path to another
        # convert a list of unique objects -> object

        # convert key value -> tabular (oof)

        rest = norm
        return rest

    #@schema(MyAugmentedSchema)
    # TODO consider additional decorators for schema validation?
    @property
    def augmented(self):
        rest = self.restructured
        # add the id field
        # compute the number of elements expected
        aug = rest
        return aug

    @property
    @hasSchema(sc.JSONSchema)
    def output(self):
        aug = self.augmented
        out = aug
        return out


HasSchema(sc.HeaderSchema).mark
class Header(Stage):
    """ generic header normalization for python """
    def __init__(self, first_row_or_column):
        super().__init__(first_row_or_column)

    @property
    def normalized(self):
        orig_header = self.data
        header = []
        for i, c in enumerate(orig_header):
            if c:
                c = (c.strip()
                     .replace('(', '')
                     .replace(')', '')
                     .replace(' ', '_')
                     .replace('+', '')
                     .replace('…','')
                     .replace('.','_')
                     .replace(',','_')
                     .replace('/', '_')
                     .replace('?', '_')
                     .replace('#', 'number')
                     .replace('-', '_')
                     .lower()  # sigh
                )
                if any(c.startswith(str(n)) for n in range(10)):
                    c = 'n_' + c

            if not c:
                c = f'TEMP_{i}'

            if c in header:
                c = c + f'_TEMP_{i}'

            header.append(c)

        return header

