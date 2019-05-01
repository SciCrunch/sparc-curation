import os
import sys
import unittest
from pathlib import Path
from importlib import import_module
import git
from pyontutils.utils import TermColors as tc
from pyontutils.config import devconfig
from test.common import project_path


class TestScripts(unittest.TestCase):
    """ Import everything and run main() on a subset of those
        NOTE If you are debugging this. Most of the functions in this
        class are defined dynamically by populate_tests, and you will not
        find their code here. """
    # NOTE printing issues here have to do with nose not suppressing printing during coverage tests

    def setUp(self):
        super().setUp()
        if not hasattr(self, '_modules'):
            self.__class__._modules = {}

        if not hasattr(self, '_do_mains'):
            self.__class__._do_mains = []
            self.__class__._do_tests = []

    def notest_mains(self):
        failed = []
        if not self._do_mains:
            raise ValueError('test_imports did not complete successfully')
        for script, argv in self._do_mains:
            pass
        assert not failed, '\n'.join('\n'.join(str(e) for e in f) for f in failed)

    def notest_tests(self):
        failed = []
        for script in self._do_tests:
            try:
                script.test()
            except BaseException as e:
                failed.append((script, e))

        assert not failed, '\n'.join('\n'.join(str(e) for e in f) for f in failed)


def populate_tests(only=tuple(), do_mains=True):
    skip = tuple()
    if 'TRAVIS' in os.environ:
        skip += tuple()

    mains = {'cli':[['spc', 'tables'],
                    ['spc', 'missing'],
                    ['spc', 'xattrs'],
                    ['spc', 'stats'],
                    ['spc', 'report', 'completeness'],
                    ['spc', 'report', 'filetypes'],
                    ['spc', 'report', 'subjects'],
                    ['spc', 'report', 'keywords'],
                    ['spc', 'annos'],
                    ['spc', 'annos', 'export'],
                    ['spc', 'export'],
                    ['spc', 'export', 'ttl'],
                    ['spc', 'export', 'json'],
                    ['spc', 'find', '-n', '*.xlsx'],
                    ['spc', 'find', '-l', '3'],
                   ],
             'curation':None,
            }

    mains['cli'] = [args + ['-p', project_path.as_posix(), '-N'] for args in mains['cli']]
    if 'CI' not in os.environ:
        pass

    if only:
        mains = {k:v for k, v in mains.items() if k in only}

    if not do_mains:
        mains = {}

    tests = tuple()  # moved to mains --test

    _do_mains = []
    _do_tests = []
    try:
        repo = git.Repo(Path(__file__).resolve().parent.parent.as_posix())
        paths = sorted(f.rsplit('/', 1)[0] if '__main__' in f else f
                       for f in repo.git.ls_files().split('\n')
                       if f.endswith('.py') and
                       f.startswith('sparcur') and
                       '__init__' not in f and
                       (True if not only else any(_ + '.py' in f for _ in only)))

        npaths = len(paths)
        print(npaths)
        for i, path in enumerate(paths):
            ppath = Path(path).absolute()
            #print('PPATH:  ', ppath)
            pex = ppath.as_posix().replace('/', '_').replace('.', '_')
            fname = f'test_{i:0>3}_' + pex
            stem = ppath.stem
            #if not any(f'pyontutils/{p}.py' in path for p in neurons):
                #print('skipping:', path)
                #continue
            rp = ppath.relative_to(repo.working_dir)
            module_path = (rp.parent / rp.stem).as_posix().replace('/', '.')
            if stem not in skip:
                def test_file(self, module_path=module_path, stem=stem):
                    try:
                        print(tc.ltyellow('IMPORTING:'), module_path)
                        module = import_module(module_path)  # this returns the submod
                        self._modules[module_path] = module
                        if hasattr(module, '_CHECKOUT_OK'):
                            print(tc.blue('MODULE CHECKOUT:'), module, module._CHECKOUT_OK)
                            setattr(module, '_CHECKOUT_OK', True)
                            #print(tc.blue('MODULE'), tc.ltyellow('CHECKOUT:'), module, module._CHECKOUT_OK)
                    finally:
                        pass

                setattr(TestScripts, fname, test_file)

                if stem in mains:
                    argv = mains[stem]
                    if argv and type(argv[0]) == list:
                        argvs = argv
                    else:
                        argvs = argv,
                else:
                    argvs = None,

                for j, argv in enumerate(argvs):
                    mname = f'test_{i + npaths:0>3}_{j:0>3}_' + pex
                    #print('MPATH:  ', module_path)
                    def test_main(self, module_path=module_path, argv=argv, main=stem in mains, test=stem in tests):
                        try:
                            script = self._modules[module_path]
                        except KeyError:
                            return print('Import failed for', module_path, 'cannot test main, skipping.')

                        if argv and argv[0] != script:
                            os.system(' '.join(argv))  # FIXME error on this?

                        try:
                            if argv is not None:
                                sys.argv = argv
                            else:
                                sys.argv = self.argv_orig

                            if main:
                                print(tc.ltyellow('MAINING:'), module_path)
                                script.main()
                            elif test:
                                print(tc.ltyellow('TESTING:'), module_path)
                                script.test()  # FIXME mutex and confusion
                        except BaseException as e:
                            if isinstance(e, SystemExit):
                                return  # --help
                            raise e
                        finally:
                            pass

                    setattr(TestScripts, mname, test_main)

    except git.exc.InvalidGitRepositoryError:  # testing elsewhere
        import sparcur
        import pkgutil
        modinfos = list(pkgutil.iter_modules(sparcur.__path__))
        modpaths = [sparcur.__name__ + '.' + modinfo.name
                    for modinfo in modinfos]
        for modpath in modpaths:
            fname = 'test_' + modpath.replace('.', '_')
            def test_file(self, modpath=modpath):
                print(tc.ltyellow('IMPORTING:'), modpath)
                module = import_module(modpath)
                self._modules[modpath] = module

            setattr(TestScripts, fname, test_file)

    if not hasattr(TestScripts, 'argv_orig'):
        TestScripts.argv_orig = sys.argv

populate_tests(only=[], do_mains=True)
