# Overview

This script will include code that can be used to migrate data back and 
forth between BCDC instances.  During development and testing of new 
versions of the BC Data Catalog we need to be able to test new features
and functionality using real data.

Being able to test using real data gets complex when prod and test use 
different data models.

This repository contains code to support the migration of data between 
CKAN instances in an incremental fashion...  Only change deltas as 
opposed to a dump and replace strategy.

Design objective will be to try to keep the translation parameters 
configurable and outside of code base.  Want to be able to easily modify
the data transformation parameters.

