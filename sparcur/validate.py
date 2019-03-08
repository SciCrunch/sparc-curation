'''
# this is not really what we want
# we want to just use properties for each of the steps
# an still have the classes correspond to the files/structures
# we are dealing with
class pipeline:
    """ class decorator for pipelines to mark phases """
    def __init__(self):
        self._normalize = tuple()
        self._key_normalization = None  # TODO I don't think this goes here
    
    def mark(self, cls):
        """ note that this runs AFTER all the methods """
        if not hasattr(cls, name):
            setattr(cls, name)

        return cls

    def normalization(self, function):
        """ decorate functions that normalize values in a column/key
        that has the same name as the function """

        self._normalization

class Stage:
    def __init__(self):
        pass

    def normalize(self):
        """ """

class PipelineStage:
    """ baseclass for curation pipeline stages """
    schema_class = None
    def __new__(cls):
        if cls.schema_class is not None:
            cls.schema = cls.schema_class()

        return super().__new__(cls)

    def __init__(self, data):  # FIXME should this instead accept previous stages?
        self.data = data

    @property
    def validated(self):
        ok, normlized_or_error, data = self.schema.validate(self.data)
        if not ok:
            data['errors'] = normalized_or_error.json()

        return data


class StartStage(PipelineStage):
    """ A stage that manages IO for data all pipelines should start with
    one of these classes, since __init__ takes no intputs. """

    def __init__(self):
        pass

    @property
    def data(self):
        """ Put your IO code here. """
        raise NotImplemented('Implement this in the child classes.')




class LoadingStage(PipelineStage):
    schema_in_class = None
    schema_out_class = None
    # FIXME I think we only need to check on output?
    # I think we want these to be more than just dumb functions?

    def __new__(cls):
        if cls.schema_in_class is not None:
            cls.schema = cls.schema_class()

        if cls.schema_out_class is not None:
            cls.schema_out = cls.schema_out_class()

        if cls.schema_class is not None:
            cls.schema = cls.schema_class()

        return super().__new__(cls)


    def __init__(self):

    def validate_in(self):
        if self.schema_in:
        self.schema_in.validate()

    def normalize(self):

    def restructure(self):

    def data_with_errors(self):
        
    def validate_out(self):
        pass



class NormalizeStage(PipelineStage):
    """ always normalize before restructure so that it is easy to
    communicate changes back to the previous stage in the structure
    that it knows about. e.g. if a human gives a spreadsheet
    normalize should be able to hand data back to the previous
    stage with the changes made.

    This is more difficult to do when dealing with the pre-json data,
    but still possible.
"""

class RestructureStage(PipelineStage):
    """ restructure and combline multiple previous stages """
    moves = 
    copies = 

    def __init__(self, **previous_stages):
        """ Implement a function per previous stage. """
        self.previous = previous_stages

    @property
    def data(self):
        for previous_stage_processing_function_name, previous_stage in


class AugmentStage(PipelineStage):
    # add meta

# the actual stages that we have

class DatasetPath(PipelineStage):
    def __init__(self, path, curation=True):
        """ curation just marks that we are downloading the dataset
        not uploading it, so the assumptions are different """
        self.path = path

    def data(self):


    def validated(self):
        ok = self.path.exists()
        out = {'path': self.path}
        if not ok:
            out['errors'] = [{f'{self.path} does not exist!'}]
        elif 


class MetadatasetStructure():
    """ static information and rules about where to find
    files givebb
    """
    def __init__(self, dataset_path):

class Loading(PipelineStage):
    # put the classes that we know at specification time
    # produce prior information that we need here
    metadataset_structure_class
    def __init__(self, metafile_location_getter_instance=None):


class NormalizeFileSuffix(PipelineStage):

class NormalizeEncoding(PipelineStage):

class Normalize(PipelineStage):
'''

##############
# A second attempt
##############
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

