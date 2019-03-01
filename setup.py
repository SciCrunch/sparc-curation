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
          'google-api-python-client',
          'nibabel',
          #'protcur',  # TODO
          'pydicom',
          'pyontutils',
          #'pysercomb',  # TODO
          'python-magic',  # FIXME conflicts with sys-apps/file python bindings
          'pyxattr',
          'scipy',
          'Xlib',
          'xlsx2csv',
      ],
      extras_require={'dev':[]},
      scripts=[],
      entry_points={
          'console_scripts': [
              'bfc=sparcur.bfcli:main',
          ],
      },
      data_files=[]
     )
