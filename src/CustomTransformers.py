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

    def __init__(self, dataType, customMethodNames):
        self.dataType = dataType
        self.customMethodNames = customMethodNames
        self.validate()

    def validate(self):
        # verify that the datatype is in the valid data types defined in the
        # transform_types
        self.validateTransformerType()
        self.validateTransformerClass()
        self.validateTransformerMethods()

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
        obj = globals()[self.dataType]()
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

    def getClasses(self):
        classMembers = inspect.getmembers(sys.modules[__name__], inspect.isclass)
        clsNameAsStr = []
        for cls in classMembers:
            if cls[0] in constants.VALID_TRANSFORM_TYPES:
                clsNameAsStr.append(cls[0])
        return clsNameAsStr

    def getCustomMethodCall(self, methodName):
        obj = globals()[self.dataType]()
        method = getattr(obj, methodName)
        return method


# names of specific classes need to align with the names in
# constants.VALID_TRANSFORM_TYPES
class packages:
    def __init__(self):
        self.customTransformations = []

    def packageTransform(self, inputDataStruct):
        """ The custom transformer with misc logic to be applied to packages

        :param inputDataStruct: input data struct that will be sent to the api,
            this struct will be modified and returned by this method.
        :type inputDataStruct: dict
        """
        # LOGGER.debug("packageTransform has been called")
        if isinstance(inputDataStruct, list):
            iterObj = range(0, len(inputDataStruct))
        else:
            iterObj = inputDataStruct

        for iterVal in iterObj:
            # individual update record referred to: inputDataStruct[iterVal]
            inputDataStruct[iterVal] = self.fixResourceStatus(inputDataStruct[iterVal])
            inputDataStruct[iterVal] = self.fixDownloadAudience(
                inputDataStruct[iterVal]
            )
            inputDataStruct[iterVal] = self.fixMoreInfo(inputDataStruct[iterVal])
            inputDataStruct[iterVal] = self.fixSecurityClass(inputDataStruct[iterVal])

            # always set the type to 'bcdc_dataset'

            inputDataStruct[iterVal]["type"] = "bcdc_dataset"

        return inputDataStruct

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

        if ("security_class" in record) and record["security_class"]:
            if record["security_class"] not in validSecurityClasses:
                if record["security_class"] == "HIGH-CONFIDENTIAL":
                    record["security_class"] = "HIGH-CLASSIFIED"
                else:
                    record["security_class"] = defaultClass
        return record

    def fixResourceStatus(self, record):
        """ Records that have their properties 'resource_status' set to
        'historicalArchive' MUST also have a 'retention_expiry_date' date set.

        This method checks for this condition, modifies the record so it is
        compliant and returns the modified version.

        :param record: input package data struct that will be sent to the api
        :type record: dict
        """
        if (
            ("resource_status" in record)
            and record["resource_status"] == "historicalArchive"
            and "retention_expiry_date" not in record
        ):

            record["retention_expiry_date"] = "2222-02-02"
        return record

    def fixDownloadAudience(self, record):
        """download_audience must be set to something other than null,
        if the download_audience is found to be set to null, will set to
        "Public"

        :param record: The input record (json struct) that is to be updated
        :type record: dict
        """
        validDownloadAudiences = ["Government", "Named users", "Public"]
        defaultValue = "Public"
        if "download_audience" in record:
            if record["download_audience"] is None:
                record["download_audience"] = defaultValue
            elif record["download_audience"] not in validDownloadAudiences:
                record["download_audience"] = defaultValue
        return record

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
        if ("more_info") in record and record["more_info"] is None:
            record["more_info"] = "[]"
        # if more info has a value but is not a string, ie its a list
        if (("more_info" in record) and record["more_info"]) and isinstance(
            record["more_info"], list
        ):
            record["more_info"] = json.dumps(
                record["more_info"], sort_keys=True, separators=(",", ":")
            )
            record = self.fixMoreInfo(record)
        elif (("more_info" in record) and record["more_info"]) and isinstance(
            record["more_info"], str
        ):
            # more info exists, has a value in it, and its a string.
            # in this situation code will:
            # * de-stringify
            # * parse
            # * convert link to url
            # * re-stringify with consistent format
            moreInfoRecord = json.loads(record["more_info"])
            if moreInfoRecord is None:
                moreInfoRecord = []
            for listPos in range(0, len(moreInfoRecord)): # noqa
                if "link" in moreInfoRecord[listPos]:
                    moreInfoRecord[listPos]["url"] = moreInfoRecord[listPos]["link"]
                    del moreInfoRecord[listPos]["link"]
            record["more_info"] = json.dumps(
                moreInfoRecord, sort_keys=True, separators=(",", ":")
            )
        return record

    def noNullMoreInfo(self, record):
        """checks to see if moreInfo is set to Null, if it is then it removes
        the property from the object

        :param record: input CKAN package data structure
        :type record: dict, ckan package
        :return: CKAN package data structure, modified to resolve more_info issues
        :rtype: dict, ckan package
        """
        #if ("more_info") in record:
        for recCnt in range(0, len(record)):
            if ("more_info" in record[recCnt]) and record[recCnt]['more_info'] is None:
                del record[recCnt]['more_info']
        return record

    def addStrangeFields(self, record):
        """ These are fields that are "required" for update / add
        operations, however the values that these fields get set to

        :param record: CKAN package data structure, modified to resolve more_info issues
        :type record: dict, ckan package
        """
        for recCnt in range(0, len(record)):
            if ("tag_string" not in record[recCnt]) or record[recCnt]['tag_string'] is None:
                record[recCnt]['tag_string'] = 'dummy tag string'
            if ("iso_topic_string" not in record[recCnt]) or record[recCnt]['iso_topic_string'] is None:
                record[recCnt]['iso_topic_string'] = 'TBD'


        return record

class InvalidCustomTransformation(Exception):
    def __init__(self, message):
        LOGGER.error(message)
        self.message = message


# if __name__ == '__main__':
#     methMap = MethodMapping('packages', ['packageTransform'])
#     func = methMap.getCustomMethodCall('packageTransform')
#     func()
