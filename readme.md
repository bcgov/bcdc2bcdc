# Overview

This package will include code that can be used to migrate data back and
forth between BCDC instances.  During development and testing of new
versions of the BC Data Catalog we need to be able to test new features
and functionality using real data.

Being able to test using real data is a requirement for the CI/CD pipeline as
data can introduce different performance and outcomes in the application.

This repository contains code to support the migration of data between
CKAN instances in an incremental fashion...  Only change deltas as
opposed to a dump and replace strategy.

Design objective will be to try to keep the translation parameters
configurable and outside of code base.  Want to be able to easily modify
the data transformation parameters.

# Getting Started

## config/transformationConfig.json
This file currently contains the configuration that is being used at DataBC for
the transformation between prod and test.  A detailed description of this file
is available [here](./docs/transformationConfig.md)

## running

### Env vars:
configure the following env vars:

* export CKAN_API_KEY_TST=2jl4jslBoston_Bruins_Suck23kl4k
* export CKAN_URL_TST=https://destination.ckan.instance.com
* export CKAN_API_KEY_PRD=2liiow89jg0HAbSg0lsxnvzxvbw89sl
* export CKAN_URL_PRD=https://source.ckan.instance.com
* export CKAN_DO_NOT_WRITE_URL=<optional: source instance, adds checks to make sure not being written to>
* export CKAN_NEW_USER_PSWD=<default password to use for new users>


environment variables and how they get consumed is configured in the constants.py
file.

### Running

`python main.py`
