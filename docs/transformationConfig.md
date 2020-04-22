# file: config/transformationConfig.json

## Description
This file describes how the transformation will take place for all the different
CKAN object types.  The first level of organization for this file is the object
type.  Valid types are identified in the constants.py file in the parameter:
VALID_TRANSFORM_TYPES.

Things that can be configured in this file for each object type:
* **unique field (unique_id_field)**: identifies the unique identifier used for the
    object type.  type of this parameter is a string

* **ignore list (ignore_list):** Identifies values that are found for the object type
    in the field identified by 'unique_id_field' that should be ignored when it
    comes to an update.  Idea is that there will be some custom configured objects
    in the destination that should not be deleted or modified.

* **user_populated_properties:** a dictionary where the keys are the properties for this
    data type and the corresponding values indicate whether they are user (true) or
    auto generated fields (false).  This information is used to identify
    differences between source and destination.  auto generated fields are not
    included in the determination of differences.

* **update_fields_to_include:** For updates the record exists in both SRC and
    DEST. By default during an update the properties that are to be updated come
    from the source object.  In some cases where there are dependencies this is
    not always what should take place.  This parameter is populated with a list
    of properties that should be populated from the destination object instead
    of the source object.

* **required_default_values:** Describes properties that MUST be populated.  If
    they do not exist in the source object they get populated with these default
    values.

* **stringified_fields:** A simple list of the properties associated with an
    object type that should be "stringified" before they are sent to the api
    for either an update / add.  Currently only supports a simple struct of one
    dimension list.

* **data_type_enforcement:** Describes the strict types that a fields must comply
    with.  If the field is None / Null then will modify the type of the field so
    that it matches the expected type.

* **transformations:** under this property are a bunch of different data
    transformations supported by the script:

    * **id_fields:** These are fields that are equal to machine generated ids.
        this means they cannot be copied directly from source to destination as
        the id from src will likely not exist on dest, or point to a different
        object than it does for dest.  To resolve this problem the script
        requires the following properties:

        * **property:** The name of the property that contains a machine/auto
            populated value to establish a relationship.  For example owner_org
            is populated with an organization id to establish the relationship
            between a package the organization that owns it.
        * **obj_type:** refers to an object type.  See keys of root transformation
            config object.  (users, groups, organizations, packages)

        **ADDS**:

            1. the package is retrieved from the source CKAN instance
            1. auto / machine generated fields are removed.
            1. required fields are checked and populated if they do not exist
            1. **id_fields**: retrieves the value for the field on the source side,
                   then translates that value using the 'obj_type' and 'obj_field'
                   to figure out the equivalent id on the destination side and
                   add that field to this object.

