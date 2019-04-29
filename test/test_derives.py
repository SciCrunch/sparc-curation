import unittest
from sparcur import schemas as sc
from sparcur.derives import Derives as De

class TestDerives(unittest.TestCase):
    def test_dsci(self):
        schema = sc.DatasetSchema.schema
        subschemas = {'dataset_description':sc.DatasetDescriptionSchema.schema,
                        'submission':sc.SubmissionSchema.schema,
                        'subjects':sc.SubjectsSchema.schema,}
        {'inputs':
         {'errors': [1, 2, 3],
         }}
        De.submission_completeness_index(schema, subschemas, inputs)
