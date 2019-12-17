# Intro

Trying to sketch out the plan for how data is to be migrated from one 
instance to another with a configurable transformation in between.

# Steps:

## A) Get package list from prod

* get a package list from prod. (list of package names)
* get package list from test (list of package names)
* From that list identify deletes and adds.
* Do Deletes
* Do Adds
* For modifies iterate through list of packages
* Call package show for prd / tst - compare: metadata_modified dates
* If prod date is newer, do update.

Unknown: how will the data model translation work between prod and test.

* current thinking is a json file that describes the transformation.


The names of packages are unique. 
