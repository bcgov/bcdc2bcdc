import os.path


# Environment variable names used to retrieve urls and api keys
CKAN_APIKEY_DEST = "CKAN_API_KEY_DEST"
CKAN_URL_DEST = "CKAN_URL_DEST"
CKAN_APIKEY_SRC = "CKAN_API_KEY_SRC"
CKAN_URL_SRC = "CKAN_URL_SRC"

# default password to assign to newly generated users
CKAN_ONETIME_PASSWORD = "CKAN_NEW_USER_PSWD"

# name and expected location for the transformation configuration file.
TRANSFORM_CONFIG_FILE_NAME = "transformationConfig.json"
TRANSFORM_CONFIG_DIR = "config"

# transformation config sections, each of these sections in the config
# file will describe different information necessary for either the
# transformation of the data or the comparison
TRANSFORM_PARAM_USER_POPULATED_PROPERTIES = 'user_populated_properties'
TRANSFORM_PARAM_UNIQUE_ID_PROPERTY = 'unique_id_field'
TRANSFORM_PARAM_IGNORE_IDS = 'ignore_list'
TRANSFORM_PARAM_INCLUDE_FLDS_UPDATE = 'update_fields_to_include'

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

# LOGGING config file name
LOGGING_CONFIG_FILE_NAME = 'logger.config'
LOGGING_OUTPUT_DIR = 'logs'
LOGGING_OUTPUT_FILE_NAME = 'bcdc2bcdc.log'

# cached versions of data used by tests
CACHE_DATA_DIR = 'data'
CACHE_PROD_USERS_FILE = 'prod_users.json'
CACHE_TEST_USERS_FILE = 'test_users.json'
CACHE_PROD_ORG_FILE = 'prod_org.json'
CACHE_TEST_ORG_FILE = 'test_org.json'
CACHE_DEST_PKGS_FILE = 'dest_pkgs.json'
CACHE_SRC_PKGS_FILE = 'src_pkgs.json'


TEST_USER_DATA_FILE = "users_src.json" # defines dummy users that are used in testing
TEST_USER_DATA_POSITION = 0 # when a single user is required this is the one used.


def getCachedDir():
    """calculates the directory to use for cached data and returns the path
    :return: the path to the directory where cached data is to be located
    :rtype: str, path
    """
    curDir = os.path.dirname(__file__)
    cacheDirRelative = os.path.join(curDir, '..', CACHE_DATA_DIR)
    cacheDir = os.path.normpath(cacheDirRelative)
    return cacheDir
