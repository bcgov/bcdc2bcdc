import os.path
import enum


# Environment variable names used to retrieve urls and api keys
# ----------------------------------------------------------------
# src is where ckan objects will be read from
# dest is where ckan objects will be written to
# CKAN_DO_NOT_WRITE_URL - This is used to protect the the prod instance.
#                 in order to be able to read all the objects from
#                 the prod instance you may need to provide a superuser
#                 apikey.  This will give the script unlimited access
#                 to that instance.  All methods that they try to
#                 update a ckan instance check to make sure that the
#                 host does not align with the host in this env var.
#                 if this parameter does not get populated the update
#                 script will not have this check in place
CKAN_APIKEY_DEST = "CKAN_API_KEY_DEST"
CKAN_URL_DEST = "CKAN_URL_DEST"
CKAN_APIKEY_SRC = "CKAN_API_KEY_SRC"
CKAN_URL_SRC = "CKAN_URL_SRC"
CKAN_DO_NOT_WRITE_URL = "CKAN_DO_NOT_WRITE_URL"
CKAN_TRANS_CONF_FILE = 'CKAN_TRANSFORMATION_CONFIG'

# default password to assign to newly generated users
CKAN_ONETIME_PASSWORD = "CKAN_NEW_USER_PSWD"

REQUIRED_ENV_VARS = [CKAN_APIKEY_DEST, CKAN_URL_DEST, CKAN_APIKEY_SRC,
                     CKAN_URL_SRC, CKAN_DO_NOT_WRITE_URL]

# debugging env var... when this param is set to 'TRUE'
# a bunch of files will get dumped to the temp directory and a detailed
# directory called: 'details_<cnt>' is created, with comparison files to help
# debug why change control is getting triggered.
DUMP_DEBUG_DATA = "DUMP_DEBUG_DATA"

# -----------------END ENV VAR DEFS -----------------------------

# name and expected location for the transformation configuration file.
TRANSFORM_CONFIG_FILE_NAME = "transformationConfig_NewDataModel.json"
TRANSFORM_CONFIG_DIR = "bcdc2bcdc_config"

# transformation config sections, each of these sections in the config
# file will describe different information necessary for either the
# transformation of the data or the comparison
TRANSFORM_PARAM_USER_POPULATED_PROPERTIES = 'user_populated_properties'
TRANSFORM_PARAM_UNIQUE_ID_PROPERTY = 'unique_id_field'
TRANSFORM_PARAM_IGNORE_IDS = 'ignore_list'
TRANSFORM_PARAM_INCLUDE_FLDS_UPDATE = 'update_fields_to_include'
TRANSFORM_PARAM_INCLUDE_FLDS_ADD = 'add_fields_to_include'
TRANSFORM_PARAM_REQUIRED_FLDS_VALS = 'required_default_values'
TRANSFORM_PARAM_TRANSFORMATIONS = 'transformations'
TRANSFORM_PARAM_ID_FIELD_DEFS = 'id_fields'
TRANSFORM_PARAM_ID_AUTOGEN_FIELD_MAPPINGS = 'field_mapping'
TRANSFORM_PARAM_TYPE_ENFORCEMENT = 'data_type_enforcement'
TRANSFORM_PARAM_STRINGIFIED_FIELD = 'stringified_fields'
TRANSFORM_PARAM_CUSTOM_TRANFORMERS = 'custom_transformation_methods'

## subproperties of TRANSFORM_PARAM_CUSTOM_TRANFORMERS
CUSTOM_UPDATE_TYPE = 'UpdateType'
CUSTOM_UPDATE_METHOD_NAME = 'CustomMethodName'

# The enumeration of possible values for CUSTOM_UPDATE_TYPE
class UPDATE_TYPES(enum.Enum):
    ADD = 1
    UPDATE = 2
    COMPARE = 3

# other misc property references
# property's of field_mapping type
FIELD_MAPPING_AUTOGEN_FIELD = 'auto_populated_field'
FIELD_MAPPING_USER_FIELD = 'user_populated_field'


# Properties of id_fields
IDFLD_RELATION_PROPERTY = 'property'
IDFLD_RELATION_OBJ_TYPE = 'obj_type'
IDFLD_RELATION_FLDNAME = 'obj_field'

# all CKAN show methods use this parameter to identify a single record
# regardless of whether the query string is 'name' or 'id'
CKAN_SHOW_IDENTIFIER = 'id'

# TODO: field_remapping defined in the trans conf file but not coded yet

# keywords used to define whether data is from a source CKAN instance
# or a DESTINATION
# use enum instead of this
#SRC_ORIGIN = 'src'
#DEST_ORIGIN = 'dest'
#VALID_DATA_ORIGINS = [SRC_ORIGIN, DEST_ORIGIN]

# Constants used to define different transformation types.  Refer to sections
# in the TRANSFORM_CONFIG_FILE_NAME
# TODO: could refactor these into enumerations.. bit of work
TRANSFORM_TYPE_USERS = 'users'
TRANSFORM_TYPE_GROUPS = 'groups'
TRANSFORM_TYPE_ORGS = 'organizations'
TRANSFORM_TYPE_RESOURCES = 'resources' # not implemented yet
TRANSFORM_TYPE_PACKAGES = 'packages'
VALID_TRANSFORM_TYPES = [TRANSFORM_TYPE_USERS, TRANSFORM_TYPE_GROUPS,
                         TRANSFORM_TYPE_ORGS,
                         TRANSFORM_TYPE_PACKAGES]

# LOGGING config file name
LOGGING_CONFIG_FILE_NAME = 'logger.config'
LOGGING_OUTPUT_DIR = 'logs'
LOGGING_OUTPUT_FILE_NAME = 'bcdc2bcdc.log'
LOGGING_OUTPUT_FILE_ENV_VAR = 'LOG_FILE_PATH'

# cached versions of data used by tests
CACHE_DATA_DIR = 'data'
CACHE_TMP_DIR = 'temp'
CACHE_SRC_USERS_FILE = 'src_users.json'
CACHE_DEST_USERS_FILE = 'dest_users.json'
CACHE_SRC_ORG_FILE = 'src_org.json'
CACHE_DEST_ORG_FILE = 'dest_org.json'
CACHE_SRC_GROUPS_FILE = 'src_groups.json'
CACHE_DEST_GROUPS_FILE = 'dest_groups.json'
CACHE_DEST_PKGS_FILE = 'dest_pkgs.json'
CACHE_SRC_PKGS_FILE = 'src_pkgs.json'
CACHE_SCHEMING_FILE = 'scheming.json'

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

def isDataDebug():
    retVal = False
    if ((DUMP_DEBUG_DATA in os.environ ) and
        os.environ[DUMP_DEBUG_DATA].upper() == 'TRUE'):
        retVal = True
    return retVal



# TODO: Search code for 'src' and 'dest' and replace with references to enum
class DATA_SOURCE(enum.Enum):
    SRC = 1
    DEST = 2

USER_EMAIL_PROPERTY = 'email'
