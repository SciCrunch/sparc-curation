import re
import os
import sys
from pathlib import Path
from setuptools import setup


def find_version(filename):
    _version_re = re.compile(r"__version__ = ['\"](.*)['\"]")
    last = None  # match python semantics
    for line in open(filename):
        version_match = _version_re.match(line)
        if version_match:
            last = version_match.group(1)

    return last


__version__ = find_version('sparcur/__init__.py')


def tangle_files(*files):
    """ emacs org babel tangle blocks to files for release """

    argv = [
        'emacs',
        '--batch',
        '--quick',
        '--directory', '.',
        '--load', 'org',
        '--load', 'ob-shell',
        '--load', 'ob-python',
     ] + [arg
          for f in files
          for arg in ['--eval', f'"(org-babel-tangle-file \\"{f}\\")"']]

    os.system(' '.join(argv))


def fix_relative_links(md):
    group = 'SciCrunch'
    repo = 'sparc-curation'
    return md.replace('](./', f'](https://github.com/{group}/{repo}/blob/master/')


with open('README.md', 'rt') as f:
    long_description = fix_relative_links(f.read())

RELEASE = '--release' in sys.argv
NEED_SIMPLE = not Path('sparcur', 'simple').exists()
if RELEASE or NEED_SIMPLE:
    if RELEASE:
        sys.argv.remove('--release')

    tangle_files(
        './docs/developer-guide.org',)

cron_requires = ['celery', 'redis']
tests_require = ['pytest', 'pytest-runner'] + cron_requires
setup(name='sparcur',
      version=__version__,
      description='assorted',
      long_description=long_description,
      long_description_content_type='text/markdown',
      url='https://github.com/SciCrunch/sparc-curation',
      author='Tom Gillespie',
      author_email='tgbugs@gmail.com',
      license='MIT',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: 3.10',
          'Programming Language :: Python :: 3.11',
          'Programming Language :: Python :: Implementation :: CPython',
          'Programming Language :: Python :: Implementation :: PyPy',
          'Operating System :: POSIX :: Linux',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: Microsoft :: Windows',
      ],
      keywords='SPARC curation biocuration ontology pennsieve protc protocols hypothesis',
      packages=['sparcur', 'sparcur.export', 'sparcur.extract', 'sparcur.sparcron', 'sparcur.simple'],
      python_requires='>=3.6',
      tests_require=tests_require,
      install_requires=[
          'augpathlib>=0.0.25',
          'beautifulsoup4',
          'pennsieve',
          'dicttoxml',
          "ipython; python_version < '3.7'",
          'jsonschema>=3.0.1',  # need the draft 6 validator
          'ontquery>=0.2.8',
          'openpyxl',
          'protcur>=0.0.10',
          'pyontutils>=0.1.31',
          'pysercomb>=0.0.10',
          'terminaltables',
          'xlsx2csv',
          # sparcur code doesn't directly use protobuf, we restrict protobuf versions from pennsieve
          'protobuf<3.20.0',
      ],
      extras_require={'dev': ['wheel'],
                      'filetypes': ['nibabel', 'pydicom', 'scipy'],
                      'cron': cron_requires,
                      'test': tests_require},
      scripts=[],
      entry_points={
          'console_scripts': [
              'spc=sparcur.cli:main',
          ],
      },
      data_files=[('share/sparcur/resources/', ['resources/mimetypes.json']),],
)
