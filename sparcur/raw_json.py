import json
from sparcur import schemas as sc


class RawJson:
    def __init__(self, path):
        self.path = path

    @property
    def data(self):
        with open(self.path, 'rt') as f:
            return json.load(f)


hasSchema = sc.HasSchema()
@hasSchema.mark
class RawJsonSubmission(RawJson):

    @hasSchema(sc.SubmissionSchema)
    def data(self):
        class RawSubmissionSchema(sc.JSONSchema):
            schema = sc.SubmissionSchema.schema['properties']['submission']

        rss = RawSubmissionSchema()
        blob = super().data
        try:
            rss.validate_strict(blob)
            # TODO this needs to be an error with an easy fix
            blob = {'submission': blob}
        except:
            pass

        return blob


hasSchema = sc.HasSchema()
@hasSchema.mark
class RawJsonDatasetDescription(RawJson):

    @hasSchema(sc.DatasetDescriptionSchema)
    def data(self):
        blob = super().data
        # TODO lift everything we can back to the ir
        class RawDatasetDescriptionSchema(sc.JSONSchema):
            schema = sc.DatasetDescriptionSchema.schema

        rds = RawDatasetDescriptionSchema()
        blob = super().data
        try:
            rds.validate_strict(blob)
        except:
            pass

        if not isinstance(blob['contributors'], list):
            # TODO this needs to be an error with an easy fix
            blob['contributors'] = [blob['contributors']]
            breakpoint()

        return blob


hasSchema = sc.HasSchema()
@hasSchema.mark
class RawJsonSubjects(RawJson):

    @hasSchema(sc.SubjectsSchema)
    def data(self):
        class RawSubjectsSchema(sc.JSONSchema):
            schema = sc.SubjectsSchema.schema['properties']['subjects']

        rss = RawSubjectsSchema()
        blob = super().data
        if isinstance(blob, list):
            # TODO this needs to be an error with an easy fix

            # try to do the right thing
            blob = {'subjects': blob}

        return blob
