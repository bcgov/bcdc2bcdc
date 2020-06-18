# Overview

Utility to migrate data between two CKAN instances using the CKAN api.

# Getting Started

## config/transformationConfig.json

This file currently contains the configuration that is being used at DataBC for
the transformation between prod and test.  A detailed description of this file
is available [here](./docs/transformationConfig.md)

## Running

### Env vars:

Configure the following env vars:

* export CKAN_API_KEY_TST=2jl4jslBoston_Bruins_Suck23kl4k
* export CKAN_URL_TST=https://destination.ckan.instance.com
* export CKAN_API_KEY_PRD=2liiow89jg0HAbSg0lsxnvzxvbw89sl
* export CKAN_URL_PRD=https://source.ckan.instance.com
* export CKAN_DO_NOT_WRITE_URL=<optional: source instance, adds checks to make sure not being written to>
* export CKAN_NEW_USER_PSWD=<default password to use for new users>
* export CKAN_TRANSFORMATION_CONFIG=<config file in config dir you want to use>


environment variables and how they get consumed is configured in the constants.py
file.

### Running

`python main.py`
