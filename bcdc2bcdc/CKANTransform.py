"""
Used to transform data based on contents of the transformation configuration
file.

"""
import json
import logging
import os.path

import bcdc2bcdc.constants as constants

# pylint: disable=logging-format-interpolation, logging-not-lazy

LOGGER = logging.getLogger(__name__)


def validateType(dataType):
    """ Transformation types are different CKAN object types that can be
    configured for transformations.  They are referred to as strings.  This
    method verifies that the strings line up with a valid type.


    :param dataType: [description]
    :type dataType: [type]
    """
    if dataType not in constants.VALID_TRANSFORM_TYPES:
        msg = (
            f"a transformation type of: {dataType} was specified however "
            + "this is not a valid transformation type.  Valid options are: "
            + f"{constants.VALID_TRANSFORM_TYPES}"
        )
        raise InValidTransformationTypeError(msg)


def getTransformationConfig(transformConfigFile=None):
    """Loads the transformation configuration information from the config file

    :return transConfData: the transformation configuration data loaded from
        the transformation config file
    :rtype: dict
    """
    if transformConfigFile is None:
        transformConfigFile = os.path.join(
            os.path.dirname(__file__),
            "..",
            constants.TRANSFORM_CONFIG_DIR,
            constants.TRANSFORM_CONFIG_FILE_NAME,
        )
        if constants.CKAN_TRANS_CONF_FILE in os.environ:
            LOGGER.info(
                "using the transformation config file defined in env "
                + f"var {constants.CKAN_TRANS_CONF_FILE}"
            )
            transformConfigFile = os.path.join(
                os.path.dirname(transformConfigFile),
                os.environ[constants.CKAN_TRANS_CONF_FILE],
            )

    LOGGER.info(f"tranform config file being read: {transformConfigFile}")
    with open(transformConfigFile) as json_file:
        transConfData = json.load(json_file)
    return transConfData


class TransformationConfig:
    """Reads the transformation config file and provides methods to help
    retrieve the desired information from the configuration.
    """

    def __init__(self, transformationConfigFile=None):
        # LOGGER.debug(f"trans conf file: {transformationConfigFile}")
        self.transConf = getTransformationConfig(transformationConfigFile)

    def __parseNestForBools(self, data, boolVal, parsedData=None):
        """A recursive method that works its way through a transformation config
        record.  The method expects properties to be defined as either:

        * list
        * dict
        * bool

        returns back the same data structure but only with elements that are
        equal to 'boolVal'

        :param data: The input dataset that needs to be recursed through
        :type data: dict or list
        :param boolVal: Only properties that are equal to this value are included
            in the return dataset.
        :type boolVal: bool
        :param parsedData: Once recursion starts this property gets populated with
            the values that have already been parsed, subsequent recursions add
            to this datastruct
        :type parsedData: dict or list, optional
        :raises ValueError: Value errors are raised if the recusion runs into a
            type in input data structure that is not dict / list / bool
        :return: Same data structure but with values not meeting the 'boolval'
            removed.
        :rtype: dict or list
        """
        # TODO: could potentially implement using map. instead of if
        if isinstance(data, dict):
            parsedData = self.__parseNestDictForBools(data, boolVal, parsedData)

        elif isinstance(data, list):
            if parsedData is None:
                parsedData = []

            for item in data:
                if isinstance(item, bool):
                    parsedData.append(item)
                elif isinstance(item, (dict, list)):
                    parsedData.append(self.__parseNestForBools(item, boolVal))
                else:
                    msg = (
                        f"The type associated with the item {item} is not a "
                        + "dict / list or bool, fix the config file and rerun"
                    )
                    raise ValueError(msg)

        return parsedData

    def __parseNestDictForBools(self, data, boolVal, parsedData):
        if parsedData is None:
            parsedData = {}

        for key, value in data.items():
            if isinstance(value, bool):
                if value == boolVal:
                    # LOGGER.debug(f"key: {key} value: {value} value type: {type(value)}")
                    parsedData[key] = value
            elif isinstance(value, (dict, list)):
                parsedData[key] = self.__parseNestForBools(value, boolVal)
            else:
                # a type that shouldn't be, raise error.
                msg = (
                    f"The type associated with the key {key} is not a "
                    + "dict / list or bool, fix the config file and rerun"
                    + f"type is: {type(value)} value is {value}"
                )
                raise ValueError(msg)
        return parsedData

    def __getProperties(self, datatype, section, sectionValue):
        """using datatype and section as a dictionary keys, verifies that the
        requested sections exist in the transformation config document.  Values
        in the config all resolve to booleans.  In reading the doc returns

        :param datatype: the key value representing the data type to be retrieved
            from the transformation configuration
        :type datatype: str
        :param section: The section associated with the datatype that is to
            be retrieved
        :type section: str
        :param sectionValue: The boolean value associated with properties in
            the datastructure that should be included in the return dataset
        :type sectionValue: bool
        :return: Data Structure that has been filtered for values that match
            'sectionValue'
        :rtype: dict
        """
        retData = None
        if datatype in self.transConf:
            if section in self.transConf[datatype]:
                properties = self.transConf[datatype][section]
                # LOGGER.debug(f"properties: {properties}")
                retData = self.__parseNestForBools(properties, sectionValue)
        return retData

    def getUserPopulatedProperties(self, datatype):
        """ If the user populated parameters are defined in the config file for
        the given datatype they will be returned.  If they are not defined then
        will return None.

        user populated fields are the fields for the 'datatype' that are populated
        by users, vs auto-generated fields.

        :param datatype: a data type, needs to be included in 'constants.VALID_TRANSFORM_TYPES'
        :type datatype: str
        :return: a list of user populated fields for the 'datatype'
        :rtype: list
        """

        validateType(datatype)
        section = constants.TRANSFORM_PARAM_USER_POPULATED_PROPERTIES
        userPopulated = self.__getProperties(datatype, section, True)

        return userPopulated

    def getAutoPopulatedProperties(self, datatype):
        """retrieves from the transformation config file the fields that are
        defined as auto / machine generated.  These are fields that cannot
        be populated directory.  Example of a autogenerated field could be
        "Modification Date"

        :param datatype: The datatype. Valid data types are defined in the
            constants file in the parameter VALID_TRANSFORM_TYPES
        :type datatype: str
        :return: a list of fields that are defined in the config file as auto /
            machine populated.
        :rtype: list, str
        """
        validateType(datatype)

        section = constants.TRANSFORM_PARAM_USER_POPULATED_PROPERTIES
        autoPopulated = self.__getProperties(datatype, section, False)
        return autoPopulated

    def getUniqueField(self, datatype):
        """retrieves from the transformtion config file the field that has been
        identified as the unique identifier.  Usually will be 'name' but could
        be 'id' or some other field that has a unique constraint applied to it.

        :param datatype: The type of CKAN object. users, orgs, groups, packages,
            resources, etc...
        :type datatype: str
        :raises InvalidTransformationConfiguration: if the datatype is not a valid
            data type.
        :return: name of the field with the unique constraint
        :rtype: str
        """
        validateType(datatype)
        section = constants.TRANSFORM_PARAM_UNIQUE_ID_PROPERTY
        if section not in self.transConf[datatype]:
            msg = (
                "The transformation configuration file does for type "
                + f"{datatype} does not include a key for the section "
                + f"{section}.  This is a mandatory field"
            )
            raise InvalidTransformationConfiguration(msg)
        return self.transConf[datatype][section]

    def getIgnoreList(self, datatype):
        """retrieves from the config file the objects that should be ignored
        when it comes to any update actions.  Values in this field are values
        identified by the config files 'unique_id_field'.  This is an optional
        field.  If it does not exist then the update will assume that there are
        no values that should be ignored.
        """
        validateType(datatype)
        section = constants.TRANSFORM_PARAM_IGNORE_IDS
        retVal = []
        if section in self.transConf[datatype]:
            retVal = self.transConf[datatype][section]
            LOGGER.debug(
                f"found ignore list for the datatype {datatype},"
                + f"ignore values: {retVal}"
            )
        else:
            # LOGGER.info(f"no ignore values found for type: {datatype}")
            pass
        return retVal

    def getFieldsToIncludeOnUpdate(self, datatype):
        """sometimes there are machine generated fields that should be included
        in data used to update an object.  This method will return the names of
        those fields for the specified datatype

        :param datatype: The type of data who's machine generated fields should be
            included in an update method
        :type datatype: str
        :return: a list of fields that should be included for the specified data
            type when creating update data sets. (payload sent to update an object)
        :rtype: list
        """
        validateType(datatype)
        section = constants.TRANSFORM_PARAM_INCLUDE_FLDS_UPDATE
        retVal = []
        if section in self.transConf[datatype]:
            retVal = self.transConf[datatype][section]
            LOGGER.debug(
                f"found the fields to include for updates for the datatype {datatype},"
                + f"include fields are: {retVal}"
            )
        else:
            LOGGER.info(f"No fields to include for update defined for datatype: {datatype}")
        return retVal

    def getRequiredFieldDefaultValues(self, datatype):
        validateType(datatype)
        section = constants.TRANSFORM_PARAM_REQUIRED_FLDS_VALS
        retVal = []
        if section in self.transConf[datatype]:
            retVal = self.transConf[datatype][section]
            LOGGER.debug(
                f"found the fields to include for updates for the datatype {datatype},"
                + f"include fields are: {retVal}"
            )
        else:
            # LOGGER.info(f"no ignore values found for type: {datatype}")
            pass
        return retVal

    def getFieldsToIncludeOnAdd(self, datatype):
        validateType(datatype)

        section = constants.TRANSFORM_PARAM_INCLUDE_FLDS_ADD
        retVal = []
        if section in self.transConf[datatype]:
            retVal = self.transConf[datatype][section]
            LOGGER.debug(
                f"found the fields to include for updates for the datatype {datatype},"
                + f"include fields are: {retVal}"
            )
        else:
            LOGGER.info(f"no ignore values found for type: {datatype}")
        return retVal

    def getIdFieldConfigs(self, datatype):
        transKey = constants.TRANSFORM_PARAM_TRANSFORMATIONS
        idFields = constants.TRANSFORM_PARAM_ID_FIELD_DEFS
        retVal = []
        LOGGER.debug(f"transKey: {transKey}")
        LOGGER.debug(f"datatype: {datatype}")
        LOGGER.debug(f"transConf: {self.transConf[datatype]}")
        LOGGER.debug(f"idFields: {idFields}")
        if transKey in self.transConf[datatype]:
            if idFields in self.transConf[datatype][transKey]:
                retVal = self.transConf[datatype][transKey][idFields]
                LOGGER.debug(f"id field remapping: {retVal}")
        return retVal

    def getFieldMappings(self, datatype):
        """
        retrieves the autogenerated field unique identifier to user generated
        field unique identifier mapping.

        returns a list of dictionaries where each dictionary will have
        the properties:


        :param datatype: the data type for which the field mapping should be
            returned
        :type datatype: list of dict
        """

        retVal = None
        transKey = constants.TRANSFORM_PARAM_ID_AUTOGEN_FIELD_MAPPINGS
        if transKey in self.transConf[datatype]:
            retVal = self.transConf[datatype][transKey]
        return retVal

    def getTypeEnforcement(self, datatype):
        """retrieves the section that defines field type enforcement.  These are
        fields that CKAN expects to be a specific type.

        :param datatype: The object type, need to be a member of constants.VALID_TRANSFORM_TYPES
        :type datatype: str
        """
        retVal = None
        typeEnforceKey = constants.TRANSFORM_PARAM_TYPE_ENFORCEMENT
        if typeEnforceKey in self.transConf[datatype]:
            retVal = self.transConf[datatype][typeEnforceKey]
        return retVal

    def getStringifiedFields(self, datatype):
        """ Gets a list of field / property names who's values should be
        stringified before attempting to send the to the API for update or add

        :param datatype: The object type, need to be a member of constants.VALID_TRANSFORM_TYPES
        :type datatype: str
        :return: a list of property names who's values should be stringified before
            sending to the api
        :rtype: list
        """
        retVal = None
        stringifiedKey = constants.TRANSFORM_PARAM_STRINGIFIED_FIELD
        if stringifiedKey in self.transConf[datatype]:
            retVal = self.transConf[datatype][stringifiedKey]
        return retVal

    def getCustomTranformations(self, datatype):
        """Extracts a the information about the custom transformers for the
        specified datatype.

        return data will be a dict with the following properties:
            UpdateType: either UPDATE or ADD, identifies when the method
                        should be applied, if it should be applied for both
                        UPDATE and ADD create two custom transformer records
            CustomMethodName: The name of the method in the CustomTransformers.py
                        module.  The actual method should be placed in a class
                        with the same name as the datatype.  (organizations,
                        packages, etc)
            WhenToApply: The data read from the API gets transformed in two
                        different places.  Either for COMPARE or for UPDATE.

                        COMPARE means that the custom transformer is applied
                        to the source data before it is compared with the
                        destination data.

                        UPDATE means that the custom transformer is applied to
                        the source data before it is sent to the api for actual
                        update.

        :param datatype: The datatype who's custom transformers should be
            retrieved (add only operations), needs to be member of
            constants.VALID_TRANSFORM_TYPES
        :type datatype: str
        :raises InvalidTransformationConfiguration: Raised if the validation of the
            contents of the transformation config file fails.
        :return: a list of dictionaries with the custom transformations that
            are described in the transformation configuration file for the
            specified datatype.
        :rtype: list of dicts
        """
        retVal = None
        customTransParam = constants.TRANSFORM_PARAM_CUSTOM_TRANFORMERS
        if customTransParam in self.transConf[datatype]:
            retVal = []
            LOGGER.debug("found custom transformation entry in transconf file")
            customTransList = self.transConf[datatype][customTransParam]
            LOGGER.debug(f"customTransList: {customTransList}")
            for customTran in customTransList:
                LOGGER.debug(f"customTran: {customTran}")

                # make sure there is an entry for update type
                if constants.CUSTOM_UPDATE_TYPE not in customTran:
                    msg = (
                        f"The custom transformer for the data type {datatype} "
                        f"does not include an entry for {constants.CUSTOM_UPDATE_TYPE}"
                    )
                    raise InvalidTransformationConfiguration(msg)

                # ensure update type contains a valid value
                validTypesStrList = list(constants.UPDATE_TYPES.__members__)
                updateType = customTran[constants.CUSTOM_UPDATE_TYPE]

                if updateType not in validTypesStrList:
                    msg = (
                        f"The {constants.TRANSFORM_PARAM_CUSTOM_TRANFORMERS} transformer"
                        + "type found in the transformation config file defines an "
                        + f"update type that is unknown: ({updateType}) valid types "
                        + f"are: {validTypesStrList}"
                    )
                    LOGGER.error(msg)

                # validate that the whentoApply exists
                # if constants.CUSTOM_UPDATE_WHEN2APPLY not in customTran:
                #     msg = (
                #         f'The custom transformer for the data type {datatype} '
                #         f'does not include the property: {constants.CUSTOM_UPDATE_WHEN2APPLY}. '
                #     )
                #     LOGGER.error(msg)
                #     raise InvalidTransformationConfiguration(msg)

                # # validate that the whentoApply contains a valid value
                # validTypesStrList = list(constants.WHEN2APPLY_TYPES.__members__)
                # when2Apply = customTran[constants.CUSTOM_UPDATE_WHEN2APPLY]
                # if when2Apply not in validTypesStrList:
                #     msg = (
                #         "The custom transformation configuration for the datatype "
                #         f'{datatype} contains the value: {when2Apply} which is not '
                #         f'a valid value.  Valid values include: {validTypesStrList}'
                #     )
                #     raise InvalidTransformationConfiguration(msg)
                retVal.append(customTran)
        return retVal

    def getCustomUpdateTransformations(self, datatype):
        """ Gets a list of the method names that should be run on the
        data that is prepared for an ADD operation.

        :param datatype: The datatype who's custom transformers should be
            retrieved (add only operations), needs to be member of
            constants.VALID_TRANSFORM_TYPES
        :type datatype: str
        """
        retVal = None
        allCustomTransformations = self.getCustomTranformations(datatype)
        for customTransformation in allCustomTransformations:
            if (
                customTransformation[constants.CUSTOM_UPDATE_TYPE]
                == constants.UPDATE_TYPES.UPDATE.name
            ):
                if retVal is None:
                    retVal = []
                retVal.append(customTransformation)
        return retVal

    def getCustomAddTransformations(self, datatype):
        """
        """
        retVal = None
        allCustomTransformations = self.getCustomTranformations(datatype)
        for customTransformation in allCustomTransformations:
            if (
                customTransformation[constants.CUSTOM_UPDATE_TYPE]
                == constants.UPDATE_TYPES.ADD.name
            ):
                if retVal is None:
                    retVal = []
                retVal.append(customTransformation)
        return retVal


class InvalidTransformationConfiguration(AttributeError):
    """Raised when values cannot be found or incorrect values are found in the
    transformation configuration.

    :param AttributeError: [description]
    :type AttributeError: [type]
    """

    def __init__(self, message):
        self.message = message


class InValidTransformationTypeError(AttributeError):
    def __init__(self, message):
        self.message = message


class InValidTransformationData(AttributeError):
    def __init__(self, message):
        self.message = message
