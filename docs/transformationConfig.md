# file: config/transformationConfig.json

## description
This file describes how the transformation will take place for all the different
CKAN object types.  The first level of organization for this file is the object
type.  Valid types are identified in the constants.py file in the parameter: 
VALID_TRANSFORM_TYPES.

Things that can be configured in this file for each object type:
* unique field (unique_id_field): identifies the unique identifier used for the 
    object type.  type of this parameter is a string
* ignore list (ignore_list): Identifies values that are found for the object type
    in the field identified by 'unique_id_field' that should be ignored when it 
    comes to an update.  Idea is that there will be some custom configured objects
    in the destination that should not be deleted or modified.
* user_populated_properties: a dictionary where the keys are the properties for this
    data type and the corresponding values indicate whether they are user (true) or 
    auto generated fields (false).  This information is used to identify 
    differences between source and destination.  auto generated fields are not 
    included in the determination of differences.
