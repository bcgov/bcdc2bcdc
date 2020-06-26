"""Any transformers defined in the transformation config in the section
"custom_transformation_method" need to be added to this module in the
class that is associated with data type.

valid data types are described in constants.VALID_TRANSFORM_TYPES
"""


import inspect
import json
import logging
import os.path
import sys
import urllib.parse

import bcdc2bcdc.constants as constants

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


class CkanObjectUpdateMixin:
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
        elif (
            self.updateType == constants.UPDATE_TYPES.ADD
            or self.updateType == constants.UPDATE_TYPES.UPDATE
        ):
            updateStruct = record.updateableJsonData
        return updateStruct

    def fixNoneAsString(self, record):
        """Looks at all values associated with all the resource properties.
        Replaces any values that are set to "None" to actual python None
        value that gets translated to json null.

        This method is implemented in the mixin because it needs to be available
        to be applied to all objects.

        :param record: The input record who's data structure that is to be
            updates will be returned
        :type record: CKANData.CKANRecord
        """
        recordStruct = self.getStructToUpdate(record)
        if "resources" in recordStruct:
            for resCnt in range(0, len(recordStruct["resources"])):
                for resourceKey in recordStruct["resources"][resCnt]:
                    if recordStruct["resources"][resCnt][resourceKey] == "None":
                        recordStruct["resources"][resCnt][resourceKey] = None


class users(CkanObjectUpdateMixin):
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
        if "name" in recordStruct:
            del recordStruct["name"]


class organizations(CkanObjectUpdateMixin):
    def __init__(self, updateType):
        self.updateType = updateType

    def remapUserNames(self, record):
        # if the record is a source record, get the corresponding dest record
        # swap out the username using the data cache,
        recordStruct = self.getStructToUpdate(record)
        if record.origin == constants.DATA_SOURCE.SRC:
            modifiedUsers = []
            users = recordStruct["users"]
            # iterate over each of the users
            #   retrieve the users email,
            #   map the email to the dest record and
            #   sub in that value for the comparable struct
            #
            #                "user_populated_field": "email",
            #                "auto_populated_field": "name"
            for user in users:
                currentName = user["name"]
                userSrcEmail = record.dataCache.getUserDefinedValue(
                    "name", currentName, "email", "users", constants.DATA_SOURCE.SRC
                )
                userDestName = record.dataCache.getAutoDefinedValue(
                    "name", userSrcEmail, "users", constants.DATA_SOURCE.DEST
                )
                # LOGGER.debug(f"userDestName: {userDestName}")
                if not userDestName:
                    LOGGER.error(
                        "Cannot find a corresponding user for the "
                        f"source user: {currentName}, email: {userSrcEmail}"
                    )
                user["name"] = userDestName
                modifiedUsers.append(user)
            recordStruct["users"] = modifiedUsers

    def revertUserName(self, record):
        # swap the username back to how it was
        recordStruct = self.getStructToUpdate(record)
        if record.origin == constants.DATA_SOURCE.SRC:
            modifiedUsers = []
            users = recordStruct["users"]
            for user in users:
                currentName = user["name"]
                # need to find current name in dest, translate to email
                # find the equivalent email in src, translate back to name

                userDestEmail = record.dataCache.getUserDefinedValue(
                    "name", currentName, "email", "users", constants.DATA_SOURCE.DEST
                )
                userSrcName = record.dataCache.getAutoDefinedValue(
                    "name", userDestEmail, "users", constants.DATA_SOURCE.SRC
                )
                user["name"] = userSrcName
                modifiedUsers.append(user)
            recordStruct["users"] = modifiedUsers


class groups(CkanObjectUpdateMixin):
    def __init__(self, updateType):
        self.updateType = updateType

    def remapUserNames(self, record):
        # if the record is a source record, get the corresponding dest record
        # swap out the username using the data cache,
        recordStruct = self.getStructToUpdate(record)
        if record.origin == constants.DATA_SOURCE.SRC:
            modifiedUsers = []
            users = recordStruct["users"]
            # iterate over each of the users
            #   retrieve the users email,
            #   map the email to the dest record and
            #   sub in that value for the comparable struct
            #
            #                "user_populated_field": "email",
            #                "auto_populated_field": "name"
            for user in users:
                currentName = user["name"]
                userSrcEmail = record.dataCache.getUserDefinedValue(
                    "name", currentName, "email", "users", constants.DATA_SOURCE.SRC
                )
                userDestName = record.dataCache.getAutoDefinedValue(
                    "name", userSrcEmail, "users", constants.DATA_SOURCE.DEST
                )
                # LOGGER.debug(f"userDestName: {userDestName}")
                if not userDestName:
                    LOGGER.error(
                        "Cannot find a corresponding user for the "
                        f"source user: {currentName}, email: {userSrcEmail}"
                    )
                user["name"] = userDestName
                modifiedUsers.append(user)
            recordStruct["users"] = modifiedUsers


# names of specific classes need to align with the names in
# constants.VALID_TRANSFORM_TYPES
class packages(CkanObjectUpdateMixin):
    def __init__(self, updateType):
        self.customTransformations = []
        self.updateType = updateType

    def fixPackageType(self, record):
        """older versions of bcdc used to have different types, now all
        records are bcdc_dataset, this transformer is making sure that all
        'type' values are set to bcdc_dataset

        this change is made on both the DEST and the SRC records

        :param record: The CKANRecord that is to be updated
        :type record: CKANData.CKANRecord
        """
        recordStruct = self.getStructToUpdate(record)
        recordStruct["type"] = "bcdc_dataset"

    def checkJsonTableSchemaForNone(self, record):
        """json table schema is getting returned as None on one instance and
        {} in another.  This difference should not result in the script thinking
        the object has changed.  This method will check for None and convert to
        {}

        :param record: The CKANRecord that is to be updated
        :type record: CKANData.CKANRecord
        """
        # apply this on both source and destination records
        self.__checkForNoneInResource(record, "json_table_schema", {})

    def fixOFI(self, record):
        """OFI was comming accross as a str bool on source and then an actual boolean
        field on the Dest.  This method checks the source side for 'true' or
        'false' values and converts them to bool,

        Not currently being used! Marked OFI to be a non user populated field.

        :param record: The CKANRecord that is to be updated
        :type record: CKANData.CKANRecord
        """
        if record.origin == constants.DATA_SOURCE.SRC:
            recordStruct = self.getStructToUpdate(record)
            if "ofi" in recordStruct:
                ofiValue = recordStruct["ofi"]
                if (isinstance(ofiValue, str)) and ofiValue.lower() in [
                    "true",
                    "false",
                ]:
                    if ofiValue.lower() == "true":
                        recordStruct["ofi"] = True
                    else:
                        recordStruct["ofi"] = False

    def adjustURLDomain(self, record):
        """Looks at the URL field that is part of each packages resources.  The
        If the URL field does not exist it is replaced with the "defaultURL" which
        is currently set to "https://www.zoomquilt.org/"

        :param record: The CKANRecord that is to be updated
        :type record: CKANData.CKANRecord
        """
        defaultURL = "https://www.zoomquilt.org/"
        # only apply on the source
        if record.origin == constants.DATA_SOURCE.SRC:
            recordStruct = self.getStructToUpdate(record)
            for resCnt in range(0, len(recordStruct["resources"])):
                if "url" in recordStruct["resources"][resCnt]:
                    # extract the domain, for the record and compare against
                    # the domain of the env var for SRC, if those are the
                    # same then swap it to DEST.
                    curUrl = recordStruct["resources"][resCnt]["url"]
                    curUrlParser = urllib.parse.urlparse(curUrl)

                    # src url parser
                    srcUrlParser = urllib.parse.urlparse(
                        os.environ[constants.CKAN_URL_SRC]
                    )
                    if curUrlParser.hostname == srcUrlParser.hostname:
                        # swap it to the DEST host as CKAN will do that once the
                        # record is updated.  This allows change detection to not
                        # flag a change.
                        destUrlParser = urllib.parse.urlparse(
                            os.environ[constants.CKAN_URL_DEST]
                        )
                        newUrl = curUrl.replace(
                            srcUrlParser.hostname, destUrlParser.hostname
                        )
                        LOGGER.debug(f"new url: {newUrl}")
                        recordStruct["resources"][resCnt]["url"] = newUrl
                else:
                    recordStruct["resources"][resCnt]["url"] = defaultURL

    def checkSpatialDatatypeForNone(self, record):
        self.__checkForNoneInResource(record, "spatial_datatype", "")

    def checkTemporalExtentForNone(self, record):
        self.__checkForNoneInResource(record, "temporal_extent", {}, otherNulls=[""])

    def checkIsoTopicCategoryForNone(self, record):
        self.__checkForNoneInResource(record, "iso_topic_category", [])

    def __checkForNoneInResource(
        self, record, property2Check, sub4NoneValue, otherNulls=None
    ):
        recordStruct = self.getStructToUpdate(record)
        if "resources" in recordStruct:
            for resCnt in range(0, len(recordStruct["resources"])):
                if (
                    property2Check not in recordStruct["resources"][resCnt]
                ) or recordStruct["resources"][resCnt][property2Check] is None:
                    recordStruct["resources"][resCnt][property2Check] = sub4NoneValue
                elif (
                    otherNulls is not None
                    and recordStruct["resources"][resCnt][property2Check] in otherNulls
                ):
                    recordStruct["resources"][resCnt][property2Check] = sub4NoneValue

    def fixResourceBCDC_TYPE(self, record):
        """ the property bcdc_type of a Resources that is part of a bcdc
        package can be can be populated with an incorrect value on the source
        side.  When this occurs and we try to update the package via the api
        it will generate an error.

        When this property is detected as having a value that is outside of its
        allowable range it will get populated with a default value.

        :param record: The CKANRecord that is to be updated
        :type record: CKANData.CKANRecord
        """
        # TODO: should really get these from the scheming end point
        propertyName = "bcdc_type"
        allowableValues = record.dataCache.scheming.getResourceDomain(propertyName)
        defaultValue = "geographic"
        if record.origin == constants.DATA_SOURCE.SRC:
            self.__validateResourceProperty(
                record, allowableValues, propertyName, defaultValue
            )

    def fixResourceAccessMethod(self, record):
        propertyName = "resource_access_method"
        allowableValues = record.dataCache.scheming.getResourceDomain(propertyName)
        defaultValue = "direct access"
        if record.origin == constants.DATA_SOURCE.SRC:
            self.__validateResourceProperty(
                record, allowableValues, propertyName, defaultValue
            )

    def fixResourceStorageFormat(self, record):
        """resource_storage_format
        """
        propertyName = "resource_storage_format"
        allowableValues = record.dataCache.scheming.getResourceDomain(propertyName)
        defaultValue = "oracle_sde"
        # example values for allowableValues:
        #   arcgis_rest", "atom","cded", "csv","e00","fgdb", "geojson",
        #   "georss", "gft", "html","json","kml","kmz",
        #   "openapi-json","oracle_sde", "other","pdf","rdf",
        #   "shp", "tsv", "txt","wms", "wmts", "xls", "xlsx",
        #   "xml","zip"
        if record.origin == constants.DATA_SOURCE.SRC:
            self.__validateResourceProperty(
                record, allowableValues, propertyName, defaultValue
            )

    def check4MissingProperties(self, record):
        """iterates over the source and destination resources looking for
        properties described in the fields2Check list.  If these properties
        exist but are set to False values the are removed.

        :param record: The CKANRecord that is to be updated
        :type record: CKANData.CKANRecord
        """
        fields2Check = ["mimetype", "name", "resource_description", "url_type"]
        recordStruct = self.getStructToUpdate(record)

        # only doing this for resources
        if "resources" in recordStruct:
            for resCnt in range(0, len(recordStruct["resources"])):
                for fld2Check in fields2Check:
                    if (
                        fld2Check in recordStruct["resources"][resCnt]
                    ) and not recordStruct["resources"][resCnt][fld2Check]:
                        del recordStruct["resources"][resCnt][fld2Check]

    def fixResourceType(self, record):
        propertyName = "resource_type"
        allowableValues = record.dataCache.scheming.getResourceDomain(propertyName)
        defaultValue = "data"
        if record.origin == constants.DATA_SOURCE.SRC:
            self.__validateResourceProperty(
                record, allowableValues, propertyName, defaultValue
            )

    def fixIsoTopicCategory(self, record):
        # Take any spaces out of the iso topics on the SRC side
        recordStruct = self.getStructToUpdate(record)
        if record.origin == constants.DATA_SOURCE.SRC:
            if "resources" in recordStruct:
                for resCnt in range(0, len(recordStruct["resources"])):
                    if "iso_topic_category" in recordStruct["resources"][resCnt]:
                        for isoTopicCnt in range(
                            0,
                            len(
                                recordStruct["resources"][resCnt]["iso_topic_category"]
                            ),
                        ):
                            recordStruct["resources"][resCnt]["iso_topic_category"][
                                isoTopicCnt
                            ] = recordStruct["resources"][resCnt]["iso_topic_category"][
                                isoTopicCnt
                            ].strip()

    def fixResourceStorageLocation(self, record):
        """Checks that the resource storage location (resource_storage_location)
        is populated and contains a valid value

        :param record: The CKANRecord that is to be updated
        :type record: CKANData.CKANRecord
        """
        # TODO: Should get these validations from the

        propertyName = "resource_storage_location"
        defaultValue = "bc geographic warehouse"
        allowableValues = record.dataCache.scheming.getResourceDomain(propertyName)

        # only perform if the record is a source object.
        if record.origin == constants.DATA_SOURCE.SRC:
            self.__validateResourceProperty(
                record, allowableValues, propertyName, defaultValue
            )

    def fixPublishState(self, record):
        """ checks to make sure that the packages 'publish_state' contains a
        valid value.

        :param record: The CKANRecord that is to be updated
        :type record: CKANData.CKANRecord
        """

        defaultValue = "PUBLISHED"
        propertyName = "publish_state"
        allowableValues = record.dataCache.scheming.getDatasetDomain(propertyName)
        # allowableValues = ['DRAFT', 'PUBLISHED', 'PENDING', 'ARCHIVE', 'REJECTED']
        # only perform if the record is a source object.
        if record.origin == constants.DATA_SOURCE.SRC:
            self.__validateProperty(record, allowableValues, propertyName, defaultValue)

    def __validateResourceProperty(
        self, record, validationDomainList, propertyName, defaultValue=None
    ):
        """a generic method that will check to see if the current value associated
        with a property of the resources that make up the current package are
        valid, and if not then assigns the default value.  If no default value
        is provided then the default value becomes the first value in the
        validationDomainList

        :param record: The CKANRecord that is to be updated
        :type record: CKANData.CKANRecord
        :param validationDomainList: a list of the values that are valid for the
            'propertyName'
        :type validationDomainList: list
        :param propertyName: name of the property that is to be validated
        :type propertyName: str
        :param defaultValue: Optional default value to set the 'propertyName' to
            if the current value violates the validationDomainList.  if no value
            is provided defaults to the first value in the validationDomainList
        :type defaultValue: str
        """
        recordStruct = self.getStructToUpdate(record)
        if defaultValue is None:
            defaultValue = validationDomainList[0]
        if defaultValue not in validationDomainList:
            msg = (
                f"method is configured with a default value of {defaultValue} "
                f"which is not part of the domain for the property {propertyName}. "
                f"allowable values: {validationDomainList}"
            )
            raise ValueError(msg)
        if "resources" in recordStruct:
            for resourceCnt in range(0, len(recordStruct["resources"])):
                if propertyName in recordStruct["resources"][resourceCnt]:
                    if (
                        recordStruct["resources"][resourceCnt][propertyName]
                        not in validationDomainList
                    ):
                        recordStruct["resources"][resourceCnt][
                            propertyName
                        ] = defaultValue
                else:
                    recordStruct["resources"][resourceCnt][propertyName] = defaultValue

    def __validateProperty(
        self, record, validationDomainList, propertyName, defaultValue=None
    ):
        """validate that the value associated with the packages 'propertyName'
        contains a valid value as defined by validationDomainList.

        :param record: The CKANRecord that is to be updated
        :type record: CKANData.CKANRecord
        :param validationDomainList: [description]
        :type validationDomainList: [type]
        :param propertyName: [description]
        :type propertyName: [type]
        :param defaultValue: [description], defaults to None
        :type defaultValue: [type], optional
        """
        recordStruct = self.getStructToUpdate(record)
        if defaultValue is None:
            defaultValue = validationDomainList[0]

        if propertyName in recordStruct:
            if recordStruct[propertyName] not in validationDomainList:
                recordStruct[propertyName] = defaultValue

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
        # only perform if the record is a source object.
        if record.origin == constants.DATA_SOURCE.SRC:
            if (
                ("security_class" in recordStruct) and recordStruct["security_class"]
            ) and recordStruct["security_class"] not in validSecurityClasses:
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
        if record.origin == constants.DATA_SOURCE.SRC:
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
        if record.origin == constants.DATA_SOURCE.SRC:
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
        if (("more_info" in recordStruct) and recordStruct["more_info"]) and isinstance(
            recordStruct["more_info"], list
        ):
            recordStruct["more_info"] = json.dumps(
                recordStruct["more_info"], sort_keys=True, separators=(",", ":")
            )
        if (("more_info" in recordStruct) and recordStruct["more_info"]) and isinstance(
            recordStruct["more_info"], str
        ):
            # more info exists, has a value in it, and its a string.
            # in this situation code will:
            # * de-stringify
            # * parse
            # * convert link to url
            # * re-stringify with consistent format
            recordStruct = self.__fixMoreInfoAsStr(recordStruct)

    def __fixMoreInfoAsStr(self, recordStruct):
        moreInfoRecord = json.loads(recordStruct["more_info"])
        if moreInfoRecord is None:
            moreInfoRecord = []
        for listPos in range(0, len(moreInfoRecord)):  # noqa
            if "link" in moreInfoRecord[listPos]:
                moreInfoRecord[listPos]["url"] = moreInfoRecord[listPos]["link"]
                del moreInfoRecord[listPos]["link"]

        recordStruct["more_info"] = json.dumps(
            moreInfoRecord, sort_keys=True, separators=(",", ":")
        )
        return recordStruct

    def noNullMoreInfo(self, record):
        """checks to see if moreInfo is set to Null, if it is then it removes
        the property from the object

        :param record: input CKAN package data structure
        :type record: dict, ckan package
        :return: CKAN package data structure, modified to resolve more_info issues
        :rtype: dict, ckan package
        """
        recordStruct = self.getStructToUpdate(record)
        if record.origin == constants.DATA_SOURCE.SRC:
            if ("more_info" in recordStruct) and recordStruct["more_info"] is None:
                del recordStruct["more_info"]

    def addStrangeFields(self, record):
        """ These are fields that are "required" for update / add
        operations, however the values that these fields get set to

        :param record: CKAN package data structure, modified to resolve more_info issues
        :type record: dict, ckan package
        """
        # record is a CKANData.CKANRecord
        recordStruct = self.getStructToUpdate(record)

        if ("tag_string" not in recordStruct) or recordStruct["tag_string"] is None:
            recordStruct["tag_string"] = "dummy tag string"
        if ("iso_topic_string" not in recordStruct) or recordStruct[
            "iso_topic_string"
        ] is None:
            recordStruct["iso_topic_string"] = "TBD"

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
            constants.DATA_SOURCE.SRC: dataCache.isAutoValueInSrc,
        }
        existsMethod = existsMethodMap[record.origin]

        for orgTypeKey in ["owner_org"]:
            # type is 'organization'
            # org map field is 'id'
            # Get the struct value for orgs from the original unmodified data
            if orgTypeKey in record.jsonData:
                currentFieldValue = record.jsonData[orgTypeKey]

                if existsMethod("id", "organizations", currentFieldValue):
                    userField = dataCache.getUserDefinedValue(
                        "id", currentFieldValue, "name", "organizations", record.origin
                    )
                    # write the user defined value to the compare structure
                    recordStruct[orgTypeKey] = userField

        return record


class InvalidCustomTransformation(Exception):
    def __init__(self, message):
        LOGGER.error(message)
        self.message = message
