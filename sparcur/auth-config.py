{'config-search-paths': ['{:user-config-path}/sparcur/config.yaml',],
 'auth-variables':
 {'data-path': {
     'default': None,
     'environment-variables': 'SPARCUR_DATA_PATH SPARC_DATA_PATH DATA_PATH'},
  'export-path': {
     'default': '{:user-data-path}/sparcur/export',
     'environment-variables':
     'SPARCUR_EXPORT_PATH SPARC_EXPORTS EXPORT_PATH'},
  'cache-path': {
      'default': '{:user-cache-path}/sparcur',
      'environment-variables': 'SPARCUR_CACHE_PATH CACHE_PATH'},
  'cleaned-path': {
     'default': '{:user-data-path}/sparcur/cleaned',
     'environment-variables':
     'SPARCUR_CLEANED_PATH SPARC_CLEANED CLEANED_PATH'},
  'log-path': {
      'default': '{:user-log-path}/sparcur',
      'environment-variables': 'SPARCUR_LOG_PATH LOG_PATH'},
  'resources': {
      'default': [
          '../resources/',  # git
          '{:cwd}/share/sparcur/resources',  # ebuild testing
          '{:user-data-path}/sparcur/resources',  # pip install --user
          '{:prefix}/share/sparcur/resources',  # system
          '/usr/share/sparcur/resources',],  # pypy3
      'environment-variables': 'SPARCUR_RESOURCES'},
  'export-url': {
      'default': None,
      'environment-variables': 'SPARCUR_EXPORT_URL'},
  'remote-cli-path': {
      'default': None,
      'environment-variables': 'REMOTE_CLI_PATH'},
  'remote-organization': {  # FIXME cryptic error if this is not set
      # idlib.exceptions.MalformedIdentifierError: b'None' matched no known pattern
      'environment-variables':
      'BLACKFYNN_ORGANIZATION PENNSIEVE_ORGANIZATION REMOTE_ORGANIZATION'},
  'remote-backoff-factor': {
      'default': 1,
      'environment-variables': 'BLACKFYNN_BACKOFF_FACTOR'},
  'google-api-service-account-file-readonly': None,
  'google-api-service-account-file-rw': None,
  'hypothesis-api-key': {'environment-variables': 'HYP_API_KEY HYP_API_TOKEN'},
  'hypothesis-group': {'environment-variables': 'HYP_GROUP'},
  'hypothesis-user': {'environment-variables': 'HYP_USER'},
  'preview': {
      'default': False,
      'environment-variables': 'SPARCUR_PREVIEW'},
  'never-update': False,
  'datasets-noexport': None,
  'datasets-sparse': None,
  'datasets-no': None,
  'datasets-test': None,
  'sparse-limit': {
      'default': 10000,
      'environment-variables': 'SPARCUR_SPARSE_LIMIT SPARSE_LIMIT'},}}
