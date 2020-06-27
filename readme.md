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

* export CKAN_API_KEY_DEST=<api key for the destination instance, should be for
   a ckan sysadmin user>
* export CKAN_URL_TST=<url to destination ckan instance>
* export CKAN_API_KEY_SRC=<api key for source destination instance, should be sysadmin>
* export CKAN_URL_SRC=<url to the source ckan instance>
* export CKAN_DO_NOT_WRITE_URL=<usually the source instance, adds checks to make no
      methods that make changes are not being called on this instance>
* export CKAN_NEW_USER_PSWD=<default password to use if new users are created>
* export CKAN_TRANSFORMATION_CONFIG=<config file in config dir you want to use>

Optional env vars, These are optional vars that should NOT be used in production/
deployed versions of this code.  They are parameters that help with the debugging
of this code.

* export LOG_FILE_PATH=<the path to where output log file should be located>
  if LOG_FILE_PATH is not set then logging will only go to console.
* export DUMP_DEBUG_DATA=<TRUE>
  When the debug param is set the objects returned by the rest api get cached.
  Subsequent runs of the script will re-use cached objects.  Also dumps comparison
  object data to help debug issues with change control.


Finally, environment variables are defined in the constants, making them easy
to change

### Running

After the module has been installed:

`pip install bcdc2bcdc`

and the the env vars described above have been set, you can run the script using

`python3 runBCDC2BCDC.py`
