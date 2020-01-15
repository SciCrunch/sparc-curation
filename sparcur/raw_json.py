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
class RawJsonSubjects(RawJson):

    @hasSchema(sc.SubjectsSchema)
    def data(self):
        class RawSubjectsSchema(sc.JSONSchema):
            schema = sc.SubjectsSchema.schema['properties']['subjects']

        rss = RawSubjectsSchema()
        blob = super().data
        try:
            rss.validate_strict(blob)
            # TODO this needs to be an error with an easy fix
            blob = {'subjects': blob}
        except:
            pass

        return blob
