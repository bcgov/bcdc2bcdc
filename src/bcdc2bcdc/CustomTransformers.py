"""Any transformers defined in the transformation config in the section
"custom_transformation_method" need to be added to this module in the
class that is associated with data type.

valid data types are described in constants.VALID_TRANSFORM_TYPES
"""


import logging
import constants
import sys
import os.path
import inspect
import json

# pylint: disable=logging-format-interpolation

LOGGER = logging.getLogger(__name__)


class MethodMapping:
    """used to glue together the method name described in the transformation
    config file and the method that will be described in this module
    """

    def __init__(self, dataType, customMethodNames, updateType):
        self.dataType = dataType
        self.customMethodNames = customMethodNames
        self.updateType = updateType
        self.validate()

    def validate(self):
        # verify that the datatype is in the valid data types defined in the
        # transform_types
        self.validateTransformerType()
        self.validateTransformerClass()
        self.validateTransformerMethods()
        self.validateUpdateType()

    def validateTransformerType(self):
        if self.dataType not in constants.VALID_TRANSFORM_TYPES:
            msg = (
                f"when attempting to map the the data type {self.dataType} "
                + f"with the methods: ({self.customMethodNames}), discovered that "
                + "the datatype is invalid"
            )
            LOGGER.error(msg)
            raise InvalidCustomTransformation(msg)

    def validateTransformerClass(self):

        # make sure there is a class in this module that aligns with the datatype
        classesInModule = self.getClasses()
        if self.dataType not in classesInModule:
            msg = (
                "you defined the following custom transformations methods: "
                + f"({self.customMethodNames}) for the data type: {self.dataType} "
                + " however there is no class in the "
                + f"{os.path.basename(__file__)} module for that data type."
            )
            LOGGER.debug(msg)
            raise InvalidCustomTransformation(msg)

    def validateTransformerMethods(self):
        # finally make sure the custom transformation method exists.
        # creating an object of type 'self.datatype'
        obj = globals()[self.dataType](self.updateType)
        # getting the methods
        methods = inspect.getmembers(obj, predicate=inspect.ismethod)
        # extracting just the names of the methods as strings
        methodNames = [i[0] for i in methods]
        # print(f'method names: {methodNames}')
        for customMethodName in self.customMethodNames:
            if customMethodName not in methodNames:
                msg = (
                    f"The custom method name {customMethodName} defined in the "
                    + f"transformation config file for the data type {self.dataType}"
                    + " does not exist"
                )
                raise InvalidCustomTransformation(msg)

        # validate that the method has the expected custom method name
        # LOGGER.debug(f"methods: {methods}")

    def validateUpdateType(self):
        """verifies that the updateType passed in the constructor is a
        constants.UPDATE_TYPES
        """
        if not isinstance(self.updateType, constants.UPDATE_TYPES):
            msg = (
                f"the update type specified is: {self.updateType} which "
                f"has a type of {type(self.updateType)}, expecting it to "
                "be of type: constants.UPDATE_TYPES"
            )
            raise ValueError(msg)


    def getClasses(self):
        classMembers = inspect.getmembers(sys.modules[__name__], inspect.isclass)
        clsNameAsStr = []
        for cls in classMembers:
            if cls[0] in constants.VALID_TRANSFORM_TYPES:
                clsNameAsStr.append(cls[0])
        return clsNameAsStr

    def getCustomMethodCall(self, methodName):
        """Takes the method name in as a string and returns a reference to the
        actual method with that name for the class that corresponds with
        the datatype property of this object

        :param methodName: the name of the method that should be returned
        :type methodName: str
        :return: a reference to the actual method
        :rtype: method reference
        """
        obj = globals()[self.dataType](self.updateType)
        method = getattr(obj, methodName)
        return method

class ckanObjectUpdateMixin:
    def getStructToUpdate(self, record):
        """using self.updateType parameter determines the update type
        that was defined for this custom transformation.

        Update types are described in constants.UPDATE_TYPES

        They essentially identify if the transformation is applied to the data
        that is used to compare two ckan objects, or alternatively if they should
        be run on the actual data that gets sent the API for an update / add
        operation

        :param record: The input record who's data structure that is to be
            updates will be returned
        :type record: CKANData.CKANRecord
        :return: a dict containing a reference to a datastructure that should
            get updated.
        :rtype: dict
        """
        # if compare then update the struct comparableJsonData
        # if add or update the update the struct updateableJsonData
        if self.updateType == constants.UPDATE_TYPES.COMPARE:
            updateStruct = record.comparableJsonData
        elif self.updateType == constants.UPDATE_TYPES.ADD or \
                self.updateType == constants.UPDATE_TYPES.UPDATE:
            updateStruct = record.updateableJsonData
        return updateStruct

class users(ckanObjectUpdateMixin):
    def __init__(self, updateType):
        self.updateType = updateType

    def removeNameField(self, record):
        """for comparison do not want to consider the name field

        :param record: [description]
        :type record: [type]
        :return: [description]
        :rtype: [type]
        """
        recordStruct = self.getStructToUpdate(record)
        if 'name' in  recordStruct:
            del recordStruct['name']

# names of specific classes need to align with the names in
# constants.VALID_TRANSFORM_TYPES
class packages(ckanObjectUpdateMixin):
    def __init__(self, updateType):
        self.customTransformations = []
        self.updateType = updateType

    def packageTransform(self, record):
        """ The custom transformer with misc logic to be applied to packages

        :param inputDataStruct: input data struct that will be sent to the api,
            this struct will be modified and returned by this method.
        :type inputDataStruct: dict
        """
        # LOGGER.debug("packageTransform has been called")
        self.fixResourceStatus(record)
        self.fixDownloadAudience(record)
        self.fixMoreInfo(record)
        self.fixSecurityClass(record)

        recordStruct = self.getStructToUpdate(record)

            # always set the type to 'bcdc_dataset'

        recordStruct["type"] = "bcdc_dataset"



    def fixSecurityClass(self, record):
        """ The security class for a dataset must be one of the following:
           * HIGH-CABINET
           * HIGH-CLASSIFIED
           * HIGH-SENSITIVITY
           * LOW-PUBLIC
           * LOW-SENSITIVITY
           * MEDIUM-PERSONAL
           * MEDIUM-SENSITIVITY

        HIGH-CONFIDENTIAL -> HIGH-CLASSIFIED
        not in set -> HIGH-SENSITIVITY

        :param record: [description]
        :type record: [type]
        """
        validSecurityClasses = [
            "HIGH-CABINET",
            "HIGH-CLASSIFIED",
            "HIGH-SENSITIVITY",
            "LOW-PUBLIC",
            "LOW-SENSITIVITY",
            "MEDIUM-PERSONAL",
            "MEDIUM-SENSITIVITY",
        ]
        defaultClass = "HIGH-SENSITIVITY"

        recordStruct = self.getStructToUpdate(record)

        if ("security_class" in record) and recordStruct["security_class"]:
            if recorecordStructrd["security_class"] not in validSecurityClasses:
                if recordStruct["security_class"] == "HIGH-CONFIDENTIAL":
                    recordStruct["security_class"] = "HIGH-CLASSIFIED"
                else:
                    recordStruct["security_class"] = defaultClass

    def fixResourceStatus(self, record):
        """ Records that have their properties 'resource_status' set to
        'historicalArchive' MUST also have a 'retention_expiry_date' date set.

        This method checks for this condition, modifies the record so it is
        compliant and returns the modified version.

        :param record: input package data struct that will be sent to the api
        :type record: dict
        """
        recordStruct = self.getStructToUpdate(record)

        if (
            ("resource_status" in recordStruct)
            and recordStruct["resource_status"] == "historicalArchive"
            and "retention_expiry_date" not in recordStruct
        ):

            recordStruct["retention_expiry_date"] = "2222-02-02"

    def fixDownloadAudience(self, record):
        """download_audience must be set to something other than null,
        if the download_audience is found to be set to null, will set to
        "Public"

        :param record: The input record (json struct) that is to be updated
        :type record: dict
        """
        recordStruct = self.getStructToUpdate(record)

        validDownloadAudiences = ["Government", "Named users", "Public"]
        defaultValue = "Public"
        if "download_audience" in recordStruct:
            if recordStruct["download_audience"] is None:
                recordStruct["download_audience"] = defaultValue
            elif recordStruct["download_audience"] not in validDownloadAudiences:
                recordStruct["download_audience"] = defaultValue

    def fixMoreInfo(self, record):
        """ fixes the 'more_info' field so that it can be consistently compared
        between instances.

        * If the 'more_info' field is string, it gets 'de-stringified'
        * Evaluate more_info for fields called 'link', and changes to url
        * re-stringifies with specific formatting parameters

        In theory after the stringified more_info field should be comparable
        accross instances.

        :param record: input CKAN package data structure
        :type record: dict, ckan package
        :return: CKAN package data structure, modified to resolve more_info issues
        :rtype: dict, ckan package
        """
        recordStruct = self.getStructToUpdate(record)

        if ("more_info") in recordStruct and recordStruct["more_info"] is None:
            recordStruct["more_info"] = "[]"
        # if more info has a value but is not a string, ie its a list
        if (("more_info" in recordStruct) and recordStruct["more_info"]) and \
                isinstance(recordStruct["more_info"], list):
            recordStruct["more_info"] = json.dumps(
                recordStruct["more_info"], sort_keys=True, separators=(",", ":")
            )
        elif (("more_info" in recordStruct) and recordStruct["more_info"]) and \
            isinstance(recordStruct["more_info"], str):
            # more info exists, has a value in it, and its a string.
            # in this situation code will:
            # * de-stringify
            # * parse
            # * convert link to url
            # * re-stringify with consistent format
            moreInfoRecord = json.loads(recordStruct["more_info"])
            if moreInfoRecord is None:
                moreInfoRecord = []
            for listPos in range(0, len(moreInfoRecord)): # noqa
                if "link" in moreInfoRecord[listPos]:
                    moreInfoRecord[listPos]["url"] = moreInfoRecord[listPos]["link"]
                    del moreInfoRecord[listPos]["link"]
            recordStruct["more_info"] = json.dumps(moreInfoRecord, sort_keys=True,
                    separators=(",", ":"))

    def noNullMoreInfo(self, record):
        """checks to see if moreInfo is set to Null, if it is then it removes
        the property from the object

        :param record: input CKAN package data structure
        :type record: dict, ckan package
        :return: CKAN package data structure, modified to resolve more_info issues
        :rtype: dict, ckan package
        """
        recordStruct = self.getStructToUpdate(record)

        if ("more_info" in recordStruct) and recordStruct['more_info'] \
                is None:
            del recordStruct['more_info']

    def addStrangeFields(self, record):
        """ These are fields that are "required" for update / add
        operations, however the values that these fields get set to

        :param record: CKAN package data structure, modified to resolve more_info issues
        :type record: dict, ckan package
        """
        # record is a CKANData.CKANRecord
        recordStruct = self.getStructToUpdate(record)

        if ("tag_string" not in recordStruct) or \
                recordStruct['tag_string'] is None:
            recordStruct['tag_string'] = 'dummy tag string'
        if ("iso_topic_string" not in recordStruct) or \
                recordStruct['iso_topic_string'] is None:
            recordStruct['iso_topic_string'] = 'TBD'

    def orgAndSubOrgToNames(self, record):
        """owner_org and sub_orgs are references to organization id values.
        id values are autogenerated so they are useless for change detection

        This method should be executed on the comparison dataset.  It will
        remap the org and sub_org id values to name fields.

        When the data is prepared for update, the remapping of ids will be
        run resulting in the autogenerated ids for org and sub_org to be
        populated.

        :param record: [description]
        :type record: [type]
        """
        dataCache = record.dataCache

        recordStruct = self.getStructToUpdate(record)

        existsMethodMap = {
            constants.DATA_SOURCE.DEST: dataCache.isAutoValueInDest,
            constants.DATA_SOURCE.SRC: dataCache.isAutoValueInSrc
        }
        existsMethod = existsMethodMap[record.origin]

        for orgTypeKey in ['owner_org', 'sub_org']:
            # type is 'organizataion'
            # org map field is 'id'
            # Get the struct value for orgs from the original unmodified data
            if orgTypeKey in record.jsonData:
                currentFieldValue = record.jsonData[orgTypeKey]

                if existsMethod('id', 'organizations', currentFieldValue):
                    userField = dataCache.getUserDefinedValue('id',
                                                                currentFieldValue,
                                                                'name',
                                                                'organizations',
                                                                record.origin)
                    # write the user defined value to the compare structure
                    recordStruct[orgTypeKey] = userField

        return record

class InvalidCustomTransformation(Exception):
    def __init__(self, message):
        LOGGER.error(message)
        self.message = message


# if __name__ == '__main__':
#     methMap = MethodMapping('packages', ['packageTransform'])
#     func = methMap.getCustomMethodCall('packageTransform')
#     func()
