
# CMD Common Python Library

## Development Use

For development use the artifact should be installed using pip into either a virtual environment or anaconda.

## Customer Use

The process for using the artifact for customers will depend on the tooling needed.  Individual services are defined below:

### Amazon Elastic Map Reduce (EMR)

For EMR the artifact should be deployed to S3 and referenced during step execution.  This allows for different versions of the artifact to be used for different executions.  Examples of this are available in the EMR repository.

## Release Process

There are three types of release for this project:

* __Alpha Build__ - A build from a development workstation
* __Beta Build__ - A build from CI Server
* __Version Build__ - A build from a CI Server that involves a tag push.

## Local Environment Setup

Installing a local environment to support this library is done as follows:

* Install conda.
* Create a new environment with python 2.7 (currently that is the version of python that spark supports).
* Install this library (see below instructions for Alpha Build).
* Import the spark libraries as specified in [requirements.txt](requirements.txt).

## Alpha Build

To build the package you need to have the following `freetds` installed to allow the MS SQL components to be compiled (even if you don't use them).

On OSX run the following:

    brew unlink freetds;
    brew install freetds@0.91
    brew link --force --overwrite freetds@0.91

To then build the python library run the following.

    ./local_build.sh

To perform linting (which will be done a commit) run the following command:

    pylint --rcfile=pydistutils.cfg runcmd

## Beta Build

Beta versions are packaged using the `CI_JOB_ID` used during the pipeline run on GitLab.

## Version Build

Released versions use a version that matches the tags applied to the repository.  To release a versioned release perform the following:

1. Change the version number value of the variable `VERSION` in `setup.py` as `1.0.0`.
2. Then we need to create a tag and push it git repo.

        git tag '1.0.0'
        git push --tags
