import os

class Config(object):
    PENNSIEVE_API_HOST = os.environ.get("PENNSIEVE_API_HOST", "https://api.pennsieve.io")
    PENNSIEVE_API_SECRET = os.environ.get("PENNSIEVE_API_SECRET", "local-secret-key")
    PENNSIEVE_API_TOKEN = os.environ.get("PENNSIEVE_API_TOKEN", "local-api-key")
    PENNSIEVE_ORGANIZATION_ID = os.environ.get("PENNSIEVE_ORGANIZATION")
    BIOLUCIDA_ENDPOINT = os.environ.get("BIOLUCIDA_ENDPOINT", "https://sparc.biolucida.net/api/v1")
    BIOLUCIDA_USERNAME = os.environ.get("BIOLUCIDA_USERNAME", "major-user")
    BIOLUCIDA_PASSWORD = os.environ.get("BIOLUCIDA_PASSWORD", "local-password")
    TEST_DATASET_ID = os.environ.get("TEST_DATASET_ID", "")
    TEST_PACKAGE_ID = os.environ.get("TEST_PACKAGE_ID", "")
    SPARC_API = os.environ.get("SPARC_API", "https://api.sparc.science/")
