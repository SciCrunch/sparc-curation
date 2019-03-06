from sparcur import config
from sparcur.core import Path
this_file = Path(__file__)
template_root = this_file.parent.parent / 'resources/DatasetTemplate'
print(template_root)
project_path = this_file.parent / 'test_local/test_project'

config.local_storage_prefix = project_path.parent
