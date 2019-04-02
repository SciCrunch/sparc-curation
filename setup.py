import os
from pathlib import Path
from setuptools import setup

with open('README.md', 'rt') as f:
    long_description = f.read()

tests_require = ['pytest', 'pytest-runner']
setup(name='sparcur',
      version='0.0.0',
      description='assorted',
      long_description=long_description,
      long_description_content_type='text/markdown',
      url='https://github.com/tgbugs/sparc-curation',
      author='Tom Gillespie',
      author_email='tgbugs@gmail.com',
      license='MIT',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
      ],
      keywords='SPARC curation biocuration ontology blackfynn protc protocols hypothesis',
      packages=['sparcur'],
      python_requires='>=3.6',
      tests_require=tests_require,
      install_requires=[
          'blackfynn',
          'dicttoxml',
          'google-api-python-client',
          'jsonschema>=3.0.1',  # need the draft 6 validator
          'nibabel',
          'pexpect',  # >=4.7.0 probably once my changes are in
          #'protcur',  # TODO
          'pydicom',
          'pyontutils>=0.1.0',
          'pysercomb',
          'python-magic',  # FIXME conflicts with sys-apps/file python bindings
          'pyxattr',
          'scipy',
          'terminaltables',
          'Xlib',
          'xlsx2csv',
      ],
      extras_require={'test': tests_require},
      scripts=[],
      entry_points={
          'console_scripts': [
              'spc=sparcur.cli:main',
          ],
      },
      data_files=[]
     )
