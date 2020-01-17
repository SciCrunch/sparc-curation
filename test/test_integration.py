import os
from pathlib import Path
from pyontutils.utils import get_working_dir
from pyontutils.integration_test_helper import _TestScriptsBase as TestScripts
from .common import project_path, project_path_real, test_organization, onerror
import sparcur

only = tuple()
skip = ('dashboard_server',)
ci_skip = tuple()

working_dir = get_working_dir(__file__)
if working_dir is None:
    # python setup.py test will run from the module_parent folder
    working_dir = Path(__file__).parent.parent

post_load = lambda : None
post_main = lambda : None


mains = {'cli-real': [['pushd', project_path_real.parent.as_posix(),
                       '&&', 'spc', 'clone', test_organization],
                      ['spc', 'pull'],
                      ['spc', 'refresh'],
                      ['spc', 'fetch']],
         'cli': [['spc', 'find', '--name', '*.xlsx'],
                 ['spc', 'find', '--limit', '3'],

                 ['spc', 'status'],
                 ['spc', 'meta'],

                 ['spc', 'export'],
                 ['spc', 'export', 'datasets'],
                 ['spc', 'export', 'ttl'],
                 ['spc', 'export', 'json'],

                 ['spc', 'report', 'completeness'],
                 ['spc', 'report', 'contributors'],
                 ['spc', 'report', 'filetypes'],
                 ['spc', 'report', 'keywords'],
                 ['spc', 'report', 'subjects'],
                 ['spc', 'report', 'samples'],
                 ['spc', 'report', 'pathids'],
                 ['spc', 'report', 'errors'],
                 ['spc', 'report', 'access'],
                 ['spc', 'report', 'stats'],
                 ['spc', 'report', 'size'],
                 ['spc', 'report', 'test'],

                 ['spc', 'tables'],
                 ['spc', 'missing'],
                 #['spc', 'xattrs'],  # deprecated
                 ['spc', 'annos'],
                 ['spc', 'annos', 'export'],
         ],
         'curation': None,
}

mains['cli'] = [args + ['--project-path', project_path.as_posix(), '-N']
                for args in mains['cli']]
_cli_real = mains.pop('cli-real')
if 'CI' not in os.environ:
    mains['cli'].extend([args + ['--project-path', project_path_real.as_posix(), '-N']
                         for args in _cli_real])

    # if the real project path exists then remove it so that we can test cloning
    # and keep the cloned directory around until the next time we run the tests
    if project_path_real.exists():
        project_path_real.rmtree(onerror=onerror)

print(skip)
TestScripts.populate_tests(sparcur, working_dir, mains, skip=skip,
                           post_load=post_load, post_main=post_main,
                           only=only, do_mains=True)
