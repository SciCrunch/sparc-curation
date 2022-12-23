import os
from pathlib import Path
from pyontutils.utils import get_working_dir
from pyontutils.integration_test_helper import _TestScriptsBase as TestScripts
from .common import project_path, project_path_real, test_organization, onerror
from .common import fake_organization
import sparcur
import sparcur.cli
import sparcur.paths
import sparcur.backends
from sparcur.utils import log
from sparcur.pennsieve_api import FakeBFLocal


def fake_setup(self, *args, **kwargs):
    """ replace _setup_bfl with a version that handles repated invocation of
        cli.Main.__init__ as occurs during testing """
    # FIXME obviously the whole init process should be reworked to avoid the
    # utter insanity that cli.Main.__init__ is at the moment ...

    if self.options.clone or self.anchor.id != fake_organization:
        self.Remote = self._remote_class._new(
            self._cache_class._local_class, self._cache_class)
        if (hasattr(self.Remote, '_api') and
            not isinstance(self.Remote._api, self.Remote._api_class)):
            log.warning(f'stale _api on remote {self.Remote._api}')
            for cls in self.Remote.mro():
                if hasattr(cls, '_api'):
                    try:
                        del cls._api
                    except AttributeError as e:
                        pass

        self._old_setup_bfl()
    else:
        self._cache_class._cache_anchor = self.anchor  # don't trigger remote lookup
        self.bfl = self._remote_class._api = FakeBFLocal(self.anchor.id, self.anchor)


sparcur.cli.Main._old_setup_bfl = sparcur.cli.Main._setup_bfl
sparcur.cli.Main._setup_bfl = fake_setup


only = tuple()
skip = ('dashboard_server',)
ci_skip = tuple()

working_dir = get_working_dir(__file__)
if working_dir is None:
    # python setup.py test will run from the module_parent folder
    working_dir = Path(__file__).parent.parent

post_load = lambda : None
def post_main():
    # just wipe out the state of these after every test
    # there are countless strange and hard to debug errors
    # that can occur because of mutation of class aka global state
    # they really don't teach the fact that class level variables
    # are actually global variables and should be treated with fear
    sparcur.backends.PennsieveRemote._new(sparcur.paths.Path,
                                          sparcur.paths.PennsieveCache)


mains = {'cli-real': [['spc', 'clone', test_organization],
                      ['spc', 'pull'],
                      #['spc', 'refresh'],  # XXX insanely slow and no longer used due to brokeness
                      ['spc', 'fetch'],
                      ['spc', 'fetch', '--mbf'],  # FIXME abstract --mbf
                      #['spc', 'report', 'access'],  # TODO no easy way to test this ...
                      ['spc', 'rmeta'],],
         'cli': [['spc', 'find', '--name', '*.xlsx'],
                 ['spc', 'find', '--name', '*', '--limit', '3'],

                 ['spc', 'status'],
                 ['spc', 'meta'],

                 ['spc', 'export'],

                 ['spc', 'report', 'completeness'],
                 ['spc', 'report', 'contributors'],
                 ['spc', 'report', 'filetypes'],
                 ['spc', 'report', 'keywords'],
                 ['spc', 'report', 'subjects'],
                 ['spc', 'report', 'samples'],
                 ['spc', 'report', 'pathids'],
                 ['spc', 'report', 'errors'],
                 ['spc', 'report', 'size'],
                 ['spc', 'report', 'test'],

                 ['spc', 'tables'],
                 ['spc', 'missing'],
                 #['spc', 'annos'],  # XXX insanely slow
                 #['spc', 'annos', 'export'],  # XXX insanely slow
         ],
}

mains['cli'] = [args +
                ['--project-path', project_path.as_posix(), '-N', '--local', '--jobs', '1'] +
                (['--raw'] if 'report' in args else [])
                for args in mains['cli']]
_cli_real = mains.pop('cli-real')
if 'CI' not in os.environ:
    mains['cli'].extend([args + ['--project-path', project_path_real.as_posix(), '-N', '--jobs', '1']
                         for args in _cli_real])

    # if the real project path exists then remove it so that we can test cloning
    # and keep the cloned directory around until the next time we run the tests
    if project_path_real.exists():
        project_path_real.rmtree(onerror=onerror)

print(skip)
TestScripts.populate_tests(sparcur, working_dir, mains, skip=skip,
                           post_load=post_load, post_main=post_main,
                           only=only, do_mains=True)
