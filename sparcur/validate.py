from sparcur import schemas as sc
from sparcur import normalization as nml


hasSchema = HasSchema()
@hasSchema.mark
class Stage:
    # NOTE any iteration on these should be generator based
    # TODO do we need the def _step -> def step indirection?
    # TODO are there things we might want to persist here?
    def __init__(self, data=None):
        self._data = data

    @property
    def pipeline_start(self):
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
    def data(self):
        aug = self.augmented
        out = aug
        return out
