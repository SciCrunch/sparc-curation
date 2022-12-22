# sparc-curation
[![PyPI version](https://badge.fury.io/py/sparcur.svg)](https://pypi.org/project/sparcur/)
[![Build Status](https://travis-ci.org/SciCrunch/sparc-curation.svg?branch=master)](https://travis-ci.org/SciCrunch/sparc-curation)
[![Coverage Status](https://coveralls.io/repos/github/SciCrunch/sparc-curation/badge.svg?branch=master)](https://coveralls.io/github/SciCrunch/sparc-curation?branch=master)

This repo contains `sparcur`, a python implementation of a validator for the SPARC Data Structure (SDS).

It also contains code, files, and documentation for curation and knowledge management workflows for SPARC datasets, protocols, and anatomical connectivity.

## SDS Validator
To use `sparcur` to validate an SDS formatted dataset run
```bash
pip install sparcur
pushd path/to/my/dataset
python -m sparcur.simple.validate
```
The result is written to `path/to/my/dataset/curation-export.json`.
General issues with the dataset can be found under the `path_error_report` property.

## Background
For a general introduction to the SPARC curpation process see [background.org](./docs/background.org).

For background on the SDS (with out-of-date technical details) see this [paper](https://doi.org/10.1101/2021.02.10.430563).

## Workflows
Documentation for curation workflows can be found in [workflows.org](./docs/workflows.org).

## Developer guide
See the [developer guide](./docs/developer-guide.org) for examples of how to reuse and develop sparcur.

## Setup
New developers or curators should start by following [setup.org](./docs/setup.org).

## Curation viewer
The [curation viewer](./sparcur_internal/sparcur/viewer.rkt) is a GUI application written in [Racket](https://racket-lang.org) that
streamlines the processes of downloading, validating, and correcting
SDS formatted datasets. The setup is currently quite involved because
it needs to run directly on the OS where curators work. It supports
windows, macos, and linux. Once the initial setup is complete there is
an update mechanism which simplifies keeping the pipelines in sync.

## SCKAN
This repo contains the core of the [SCKAN release pipelines](./docs/developer-guide.org#sckan) as well as the [documentation](./docs/sckan) for running and querying SCKAN.

## Related links
- [SODA](https://github.com/fairdataihub/SODA-for-SPARC) GUI app for creating, validating, and uploading SDS formatted datasets.  
- [SDS Viewer](https://github.com/MetaCell/sds-viewer) a web UI for SDS formatted datatsets via the SDS validator.  
- [dockerfiles/source.org](https://github.com/tgbugs/dockerfiles/blob/master/source.org#kg-dev-user) spec for developer docker image for this repo. Also has specs for the image that runs the [sparcron](./sparcur/sparcron/core.py) single dataset pipelines, SCKAN images, and more.  
- [tgbugs/musl](https://hub.docker.com/r/tgbugs/musl) dockerhub repo with latest build of images.  
- [open-physiology-viewer](https://github.com/open-physiology/open-physiology-viewer) code for converting ApiNATOMY models to OWL/RDF needed for [apinatomy pipelines](./docs/apinatomy.org).
