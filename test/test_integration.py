import os
import sys
import unittest
from pathlib import Path
from importlib import import_module
import git
from pyontutils.utils import TermColors as tc, get_working_dir
from pyontutils.config import devconfig
from pyontutils.integration_test_helper import _TestScriptsBase as TestScripts
from test.common import project_path
import sparcur

only = tuple()
skip = tuple()
ci_skip = tuple()

working_dir = get_working_dir(__file__)
if working_dir is None:
    # python setup.py test will run from the module_parent folder
    working_dir = Path(__file__).parent.parent

post_load = lambda : None
post_main = lambda : None

mains = {'cli':[['spc', 'clone'],  # TODO
                ['spc', 'pull'],
                ['spc', 'refresh'],
                ['spc', 'fetch'],

                ['spc', 'find', '-n', '*.xlsx'],
                ['spc', 'find', '-l', '3'],

                ['spc', 'status'],
                ['spc', 'meta'],

                ['spc', 'report', 'completeness'],
                ['spc', 'report', 'filetypes'],
                ['spc', 'report', 'subjects'],
                ['spc', 'report', 'keywords'],
                ['spc', 'report', 'stats'],
                ['spc', 'report', 'size'],
                ['spc', 'report', 'test'],
                ['spc', 'export'],
                ['spc', 'export', 'datasets'],
                ['spc', 'export', 'ttl'],
                ['spc', 'export', 'json'],

                ['spc', 'tables'],
                ['spc', 'missing'],
                #['spc', 'xattrs'],  # deprecated
                ['spc', 'annos'],
                ['spc', 'annos', 'export'],
],
         'curation':None,
}

mains['cli'] = [args + ['--project-path', project_path.as_posix(), '-N'] for args in mains['cli']]

print(skip)
TestScripts.populate_tests(sparcur, working_dir, mains, skip=skip,
                           post_load=post_load, post_main=post_main,
                           only=only, do_mains=True)
