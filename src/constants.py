# Environment variable names used to retrieve urls and api keys
CKAN_APIKEY_DEST = "CKAN_API_KEY_TST"
CKAN_URL_DEST = "CKAN_URL_TST"
CKAN_APIKEY_SRC = "CKAN_API_KEY_PRD"
CKAN_URL_SRC = "CKAN_URL_PRD"

# name and expected location for the transformation configuration file.
TRANSFORM_CONFIG_FILE_NAME = "transformationConfig.json"
TRANSFORM_CONFIG_DIR = "config"


# Constants used to define different transformation types.  Refer to sections
# in the TRANSFORM_CONFIG_FILE_NAME
TRANSFORM_TYPE_USERS = 'users'
TRANSFORM_TYPE_GROUPS = 'groups'
TRANSFORM_TYPE_ORGS = 'organizations'
TRANSFORM_TYPE_RESOURCES = 'resources'
TRANSFORM_TYPE_PACKAGES = 'packages'
VALID_TRANSFORM_TYPES = [TRANSFORM_TYPE_USERS, TRANSFORM_TYPE_GROUPS,
                         TRANSFORM_TYPE_ORGS, TRANSFORM_TYPE_RESOURCES, 
                         TRANSFORM_TYPE_PACKAGES]
