import os
from pathlib import Path
from setuptools import setup

with open('README.md', 'rt') as f:
    long_description = f.read()

setup(name='sparcur',
      version='0.0.0',
      description='assorted',
      long_description=long_description,
      long_description_content_type='text/markdown',
      url='https://github.com/tgbugs/sparc-curation',
      author='Tom Gillespie',
      author_email='tgbugs@gmail.com',
      license='MIT',
      classifiers=[],
      keywords='SPARC python curation ontology blackfynn',
      packages=['sparcur'],
      install_requires=[
          'blackfynn',
          'dicttoxml',
          'google-api-python-client',
          'jsonschema>=3.0.1',  # need the draft 6 validator
          'nibabel',
          'pexpect',  # >=4.7.0 probably once my changes are in
          #'protcur',  # TODO
          'pydicom',
          'pyontutils',
          #'pysercomb',  # TODO
          'python-magic',  # FIXME conflicts with sys-apps/file python bindings
          'pyxattr',
          'scipy',
          'terminaltables',
          'Xlib',
          'xlsx2csv',
      ],
      extras_require={'dev':['pytest', 'pytest-cov']},
      scripts=[],
      entry_points={
          'console_scripts': [
              'spc=sparcur.cli:main',
          ],
      },
      data_files=[]
     )
