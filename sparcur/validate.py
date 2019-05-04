import re
from functools import wraps
from sparcur import schemas as sc
from sparcur.core import python_identifier
from scibot.utils import mproperty

class HasSchema:
    """ decorator for classes with methods whose output can be validated by jsonschema """
    def __init__(self, input_schema_class=None, fail=True, normalize=False):
        self.input_schema = input_schema_class() if input_schema_class is not None else None
        self.fail = fail
        self.normalize = normalize
        self.schema = None  # deprecated output schema ...

    def mark(self, cls):
        """ note that this runs AFTER all the methods """

        if self.input_schema is not None:
            # FIXME probably better to do this as types
            # and check that schemas match at class time
            cls._pipeline_start = cls.pipeline_start
            @mproperty
            def pipeline_start(self, schema=self.input_schema, fail=self.fail):
                data = self._pipeline_start
                ok, norm_or_error, data = schema.validate(data)
                if not ok and fail:
                    raise norm_or_error

                return data

            pipeline_start.schema = self.input_schema
            cls.pipeline_start = pipeline_start

        return cls

        # pretty sure this functionality is no longer used
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
            @mproperty
            @wraps(function)
            def schema_wrapped_property(_self):
                data = function(_self)
                ok, norm_or_error, data = schema.validate(data)
                if not ok:
                    data['errors'] = norm_or_error.json()
                elif self.normalize:
                    return norm_or_error

                return data

            schema_wrapped_property.schema = schema
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
                c = python_identifier(c)
            if not c:
                c = f'TEMP_{i}'

            if c in header:
                c = c + f'_TEMP_{i}'

            header.append(c)

        return header
