"""CKAN module is a wrapper around API calls.  All of these methods will return
raw json objects.  The JSON that is returned can be used to construct CKANData
objects.

CKANData objects can be compared with one another.  They will use the
CKANTransform methods to identify fields that should and should not be
used to compare two objects.

CKANTransform will also be used to transform on CKANData object to a new
Schema allowing like for like comparisons.

CKANTransform module will work directly with CKAN Data objects.

"""
# pylint: disable=logging-format-interpolation

import json
import logging
import pprint
import sys

import CKANTransform
import constants
import CustomTransformers
import Diff

LOGGER = logging.getLogger(__name__)
TRANSCONF = CKANTransform.TransformationConfig()


def validateTypeIsComparable(dataObj1, dataObj2):
    """A generic function that can be used to ensure two objects are comparable.

    :param dataObj1: The first data object that is to be used in a comparison
    :type ckanDataSet:
    :raises IncompatibleTypesException: [description]
    """
    dataType1 = type(dataObj1)
    dataType2 = type(dataObj2)

    if hasattr(dataObj1, "dataType"):
        dataType1 = dataObj1.dataType
    if hasattr(dataObj2, "dataType"):
        dataType2 = dataObj2.dataType
    if dataType2 != dataType1:
        msg = (
            "You are attempting to compare two different types of objects "
            + f"that are not comparable. dataObj1 is type: {dataType1} and "
            + f"dataObj2 is type: with an object of type, {dataType2}"
        )
        raise IncompatibleTypesException(msg)


# ------------- Data Record defs -------------

# should spit CKANRecord functionality into a container for the data with
# some surrounding methods for data retrieval like unique id.
# then another class that extends that where the corresponding record can be
# embedded, along with references from datacache.

class CKANRecord:
    """A generic container to store information about an individual CKAN object
    Also provides a standard interface allowing the data to be transformed,
    searched, etc.

    :ivar jsonData: The original data structure.  This structure will remain
        unmodified.
    :ivar comparableJsonData: A modified version of jsonData.  This version
        is populated by the getComparableStruct() method.  It is a version of
        the data that allows for comparison accross CKAN instances
    :ivar updateableJsonData: Derived from "comparableJsonData".  This version
        includes additional modifications that are required for the data to be
        used in either an ADD or and UPDATE operation.
    :ivar operations: This list keeps track of the methods that have been run that
        transform the data.  Used to prevent tranformations that have already been
        run from being re-run

    """
    def __init__(self, jsonData, dataType, origin):
        self.jsonData = jsonData
        self.dataType = dataType
        self.origin = origin
        self.userPopulatedFields = TRANSCONF.getUserPopulatedProperties(self.dataType)

        self.comparableJsonData = None # populated when you call getComparableStruct()
        self.updateableJsonData = None # populated when you call getComparableStructUsedForAddUpdate()
        self.operations = []

        self.destRecord = None

    def getFieldValue(self, fieldName):
        return self.jsonData[fieldName]

    def getUniqueIdentifier(self):
        """returns the value in the field described in the transformation
        configuration file as unique.

        :return: value of unique field
        :rtype: any
        """
        # look up the name of the field in the transformation configuration
        # that describes the unique id field
        # get the unique id field value from the dict
        uniqueFieldName = TRANSCONF.getUniqueField(self.dataType)
        return self.jsonData[uniqueFieldName]

    def getComparableStruct(self):
        # this is getting the struct that can be used for comparison of two
        # data structure... should
        # a) remove the autogen fields / non user generated(populated) fields
        # b) remove embedded ignores
        # c) if its source populate the required fields
        # d) if its source: run custom transformations
        #
        # this struct will serve as the base structure for the generation
        # of the actual structure that is sent for the update
        # additional steps that would be run on the data that is used for
        # the update include:
        # a) run custom transformation
        # b) add either add / update fields
        # c) remap related id fields that use the autogen fields
        # d) run the custom update transformers

        # TODO: restructure all the data transformations methods so that
        #       they operate on the actual record, example removeEmbeddedIgnores
        #       should just be callable, and it will modify the underlying
        #       structure that sits behind the CKANRecord
        #       see applyRequiredFields() and applyCustomTransformations() as
        #       examples of the pattern
        methodName = sys._getframe().f_code.co_name
        if methodName not in self.operations:
            # removing the non user generated properties
            self.comparableJsonData = self.jsonData.copy()
            self.comparableJsonData = self.filterNonUserGeneratedFields()

            # remove embedded ignores
            dataCell = DataCell(self.comparableJsonData)
            dataCellNoIgnores = self.removeEmbeddedIgnores(dataCell)
            self.comparableJsonData = dataCellNoIgnores.struct

            if self.origin == constants.DATA_SOURCE.SRC:
                # add required fields to the source
                self.applyRequiredFields()

                # run the custom transformations
                self.applyCustomTransformations()

            self.operations.append(methodName)
        return self.comparableJsonData

    def filterNonUserGeneratedFields(self, struct=None, flds2Include=None):
        """Receives the data returned by one of the CKAN end points, recursively
        iterates over it returning a new data structure that contains only the
        fields that are user populated.  (removing auto generated fields).

        Field definitions are retrieved from the transformation configuration
        file.

        :param struct: The input CKAN data structure
        :type struct: list, dict
        :param flds2Include: Used internally during recursion to ensure the
            userfields line up with the current level of recursion, defaults to None
        :type flds2Include: list / dict, optional
        :return: The new data structure with only user generated fields
        :rtype: dict or list
        """
        if struct is None and flds2Include is None:
            if self.comparableJsonData is None:
                self.comparableJsonData = self.jsonData.copy()
            struct = self.comparableJsonData
            flds2Include = self.userPopulatedFields

        # LOGGER.debug(f"struct: {struct}, flds2Include: {flds2Include}")
        newStruct = None

        # only fields defined in this struct should be included in the output
        if isinstance(flds2Include, list):
            # currently assuming that if a list is found there will be a single
            # record in the flds2Include configuration that describe what to
            # do with each element in the list
            newStruct = []
            if isinstance(flds2Include[0], dict):
                for structElem in struct:
                    dataValue = self.filterNonUserGeneratedFields(structElem, flds2Include[0])
                    newStruct.append(dataValue)
                return newStruct
        elif isinstance(flds2Include, dict):
            newStruct = {}
            for key in flds2Include:
                # if the key is a datatype then:
                #   - get the unique-id for that data type
                #   - get the ignore list for that data type
                #   - check each value to make sure its not part
                #        of an ignore list.  If it is then do not
                #        include the data.
                # thinking this is part of a post process that should be run
                # after the comparable struct is generated.
                # LOGGER.debug(f'----key: {key}')
                # LOGGER.debug(f'flds2Include: {flds2Include}')
                # LOGGER.debug(f"flds2Include[key]: {flds2Include[key]}")
                # LOGGER.debug(f'struct: {struct}')
                # LOGGER.debug(f'newStruct: {newStruct}')
                if key not in struct:
                    # field is defined as being required but is not in the object
                    # that was returned.  Setting it equal to None
                    struct[key] = None
                # LOGGER.debug(f'struct[{key}]: {struct[key]}')

                newStruct[key] = self.filterNonUserGeneratedFields(
                    struct[key], flds2Include[key]
                )
            # LOGGER.debug(f"newStruct: {newStruct}")
            return newStruct
        elif isinstance(flds2Include, bool):
            # LOGGER.debug(f"-----------{struct} is {flds2Include}")
            return struct
        return newStruct

    def removeEmbeddedIgnores(self, dataCell):
        """many data structs in CKAN can contain embedded data types.  Example
        of data types in CKAN: users, groups, organizations, packages, resources

        An example of an embedded type... users are embedded in organizations.
        this impacts comparison as any datatype can be configured with an
        ignore_list.  The ignore list identifies the unique id of records that
        should be ignored for that data type.

        This is easy to handle for the actual data type.  Example for users, a
        delta object is generated that identifies all the differences even if
        they are in the ignore_list.  The update process however will ignore any
        differences that correspond with the ignore list.

        For embedded data we want to consider any data that is in the ignore
        list of embedded data types and not include these when differences between
        two objects are calculated.

        This method will recursively iterate through the data structure:
        * identify if a property is an embedded type.
        * If so remove any children that match the ignore_list defined for the
          type that is being embedded.

        :param struct: [description]
        :type struct: [type]
        """
        # need to figure out how to remove non
        # LOGGER.debug("---------  REMOVE EMBED IGNORES ---------")
        if isinstance(dataCell.struct, dict):
            for objProperty in dataCell.struct:
                # LOGGER.debug(f"objProperty: {objProperty}")
                newCell = dataCell.generateNewCell(objProperty)
                newCell = self.removeEmbeddedIgnores(newCell)
                dataCell.copyChanges(newCell)
        elif isinstance(dataCell.struct, list):
            positions2Remove = []
            for listPos in range(0, len(dataCell.struct)):
                # LOGGER.debug(f"listPos: {listPos} - {dataCell.struct[listPos]}")
                newCell = dataCell.generateNewCell(listPos)
                newCell = self.removeEmbeddedIgnores(newCell)
                if not newCell.include:
                    positions2Remove.append(listPos)
                    # LOGGER.debug("adding value: {listPos} to remove")
                # LOGGER.debug(f"include value: {dataCell.include}")
            if positions2Remove:
                # LOGGER.debug(f"removing positions: {positions2Remove}")
                pass
            dataCell.deleteIndexes(positions2Remove)
        # LOGGER.debug(f'returning... {dataCell.struct}, {dataCell.include}')
        # LOGGER.debug(f"ignore struct: {TRANSCONF.transConf['users']['ignore_list']}")
        return dataCell

    def __eq__(self, inputRecord):
        # LOGGER.debug("_________ EQ CALLED")
        diff = self.getDiff(inputRecord)

        # now need to evaluate the diff object to remove any
        # differences where type has changed but data continues to be
        # empty / false
        # example:
        #   None vs ""
        #   None vs []
        # diff.type_changes = list of dicts
        #   each dict: 'new_type': None, 'old_type': None
        #     and vise versa

        retVal = True
        if diff:
            retVal = False
        return retVal

    def isIgnore(self, inputRecord):
        """evaluates the current record to determine if it is defined in the
        transformation config as one that should be ignored

        :param inputRecord: a data struct (dict) for the current record type
        :type inputRecord: dict
        """
        retVal = False
        ignoreField = TRANSCONF.getUniqueField(self.dataType)
        ignoreList = TRANSCONF.getIgnoreList(self.dataType)
        if ignoreField in inputRecord.jsonData:
            if inputRecord.jsonData[ignoreField] in ignoreList:
                retVal = True
        return retVal

    def defineDestRecord(self, destRecord):
        """When calculating the data structure that will be sent to the api
        to update an CKAN object, most data will come from the soruce object,
        however some data needs to come from the destination object.  This
        method populates the CKANRecord object with the corresponding existing
        destination object if one exists.

        :param destRecord: a CKANRecord object for the corresponding record on
            the destination side
        :type destRecord: CKANRecord
        """
        self.destRecord = destRecord

    def getComparableStructUsedForAddUpdate(self, dataCache, operationType, destRecord=None):
        """ Starts by transforming the record into a comparable struct, then
        for update the following additonal operations need to be run on the
        json struct:

        a) Add back in the ADD / UPDATE fields.  These are usually
        b) add either add / update fields
        c) remap related id fields that use the autogen fields


        :param destRecord: This method gets called on the src record, but receives
                           a reference to a destination record as transformation
                           requires some data to be transferred from the
                           destination record to the record that is sent to
                           the api for an ADD / UPDATE
        :type destRecord: CKANRecord
        :param operationType: The operation type, either an ADD or UPDATE
        :type operationType: constants.UPDATE_TYPES
        :return: a data structure that can be sent to the API for the operation
                 type defined in "operationType"
        :rtype: dict
        """
        methodName = sys._getframe().f_code.co_name

        if methodName not in self.operations:
            if destRecord is None and operationType == constants.UPDATE_TYPES.ADD:
                # for adds don't need the dest record!
                destRecord = self
            #calls getcomparable which will remove all the autogenerated ids, then
            #for source data will apply the default field calculations
            thisComparable = self.getComparableStruct()

            # double check that this is being run on a source object
            if self.origin != constants.DATA_SOURCE.SRC:
                msg = "cannot do the requested transformations on a DEST record"
                LOGGER.warning(msg)
            else:
                if destRecord.origin != constants.DATA_SOURCE.DEST:
                    msg = ('the destination record provided to this method is not a'
                        "destination record")
                    LOGGER.error(msg)
                    raise ValueError(msg)

                # adds add fields if operationType is add
                # adds update fields if operationType is update
                self.applyAutoGenFields(destRecord, operationType)

                # id remapping
                self.applyIdRemapping(destRecord)

            self.operations.append(methodName)
        return self.updateableJsonData

    def applyIdRemapping(self, dataCache):
        LOGGER.debug("REMAP FIELDS")

        methodName = sys._getframe().f_code.co_name
        if methodName not in self.operations:
            idFields = TRANSCONF.getIdFieldConfigs(self.destCKANDataset.dataType)
            for idRemapObj in idFields:
                # properties of the idRemapObj, and some sample values
                #  * property": "owner_org",
                #  * obj_type": "organizations",
                #  * obj_field : "id"

                parentFieldName = idRemapObj[constants.IDFLD_RELATION_PROPERTY]
                childObjType = idRemapObj[constants.IDFLD_RELATION_OBJ_TYPE]
                childObjFieldName = idRemapObj[constants.IDFLD_RELATION_FLDNAME]

                parentFieldValue = self.updateableJsonData[parentFieldName]
                if not dataCache.isAutoValueInDest(childObjFieldName,
                        childObjType, parentFieldValue):

                    destAutoGenId = dataCache.src2DestRemap(
                        childObjFieldName, childObjType, parentFieldValue, origin
                    )
                    # last step is to write the value back to the data struct and
                    # return it
                    LOGGER.debug(
                        f"remapped autopop value from: {parentFieldValue} to {destAutoGenId}"
                    )
                    self.updateableJsonData[parentFieldName] = destAutoGenId
            self.operations.append(methodName)

    def applyAutoGenFields(self, destRecord, actionType):
        """Some fields that are autogenerated by the API are 'required' fields
        when adding a new dataset.  They can be defined in the transformation
        config in the parameter "add_fields_to_include".  This method will
        retrieve the values for 'add' fields from the destination object and
        add them to struct that sits behind this object

        :param destRecord: The corresponding destination record to this source
                           object.
        :type destRecord: CKANRecord
        """
        # TODO: complete adding logic to complete this operation
        methodName = sys._getframe().f_code.co_name
        if methodName not in self.operations:
            if actionType == constants.UPDATE_TYPES.ADD:
                fields2Add = TRANSCONF.getFieldsToIncludeOnAdd(self.dataType)
            elif actionType == constants.UPDATE_TYPES.UPDATE:
                fields2Add = TRANSCONF.getFieldsToIncludeOnUpdate(self.dataType)
            if fields2Add:
                for field2Add in fields2Add:
                    fieldValue = destRecord.getFieldValue(field2Add)
                    self.updateableJsonData[field2Add] = fieldValue
            self.operations.append(methodName)

    def applyCustomTransformations(self, customTransformationConfig):
        """[summary]
        """
        methodName = sys._getframe().f_code.co_name
        if methodName not in self.operations:

            if not customTransformationConfig:
                customTransformationConfig = TRANSCONF.getCustomUpdateTransformations(
                    self.destCKANDataset.dataType)
            if customTransformationConfig:
                LOGGER.debug(f"found custom transformers: {customTransformationConfig}")
                methMap = CustomTransformers.MethodMapping(
                    self.destCKANDataset.dataType,
                    customTransformers
                )
                for customTransformer in customTransformationConfig:
                    LOGGER.info(f"loading and running the custom transformer : {customTransformer}")
                    methodCall = methMap.getCustomMethodCall(customTransformer)
                    struct = methodCall([self.comparableJsonData])
                    self.comparableJsonData = struct.pop()
            self.operations.append(methodName)

    def applyRequiredFields(self):
        """retrieves the required field config if it exists for the current
        data/object type.  Then reads it and applies the default values.
        """
        methodName = sys._getframe().f_code.co_name
        if methodName not in self.operations:

            if self.comparableJsonData is None:
                self.comparableJsonData = self.jsonData.copy()

            defaultFields = TRANSCONF.getRequiredFieldDefaultValues(
                                        self.dataType)
            if defaultFields:
                for fieldName in defaultFields:
                    # LOGGER.debug(f"fieldName:  {fieldName}")
                    # fieldName will be the index to the current data set.
                    fieldValue = defaultFields[fieldName]

                    populator = DataPopulator(self.comparableJsonData)
                    currentDataset = populator.populateField(fieldName, fieldValue)
            self.comparableJsonData = currentDataset
            self.operations.append(methodName)

    def getDiff(self, inputRecord):
        # retrieve a comparable structure, and remove embedded data types
        # that have been labelled as ignores

        # TODO: before do anything check to see if this record is an
        diff = None
        # don't even go any further if the records unique id, usually name is in
        # the ignore list
        if not self.isIgnore(inputRecord):
            thisComparable = self.getComparableStruct(self)
            inputComparable = self.getComparableStruct(inputRecord)
            # thisComparable = self.getComparableStruct()
            # dataCell = DataCell(thisComparable)
            # dataCellNoIgnores = self.removeEmbeddedIgnores(dataCell)
            # thisComparable = self.runCustomTransformations(dataCellNoIgnores)
            # thisComparable = dataCellNoIgnores.struct

            # do the same thing for the input data structure
            # inputComparable = inputRecord.getComparableStruct()
            # dataCell = DataCell(inputComparable)
            # dataCellNoIgnores = self.removeEmbeddedIgnores(dataCell)
            # inputComparable = self.runCustomTransformations(dataCellNoIgnores)
            # inputComparable = dataCell.struct

            diffIngoreEmptyTypes = Diff.Diff(thisComparable, inputComparable)
            diff = diffIngoreEmptyTypes.getDiff()
            #diff = deepdiff.DeepDiff(thisComparable, inputComparable, ignore_order=True)

            if diff:
                pp = pprint.PrettyPrinter(indent=4)
                formatted = pp.pformat(inputComparable) # noqa
                formattedDiff = pp.pformat(diff) # noqa
                # LOGGER.debug("inputComparable: %s", pp.pformat(inputComparable))
                # LOGGER.debug('thisComparable: %s', pp.pformat(thisComparable))
                # LOGGER.debug(f"diffs are: {diff}")
        return diff

    def __ne__(self, inputRecord):
        # LOGGER.debug(f"__________ NE record CALLED: {type(inputRecord)}, {type(self)}")
        retVal = True
        if self == inputRecord:
            retVal = False
        # LOGGER.debug(f"retval from __ne__: {retVal}")
        return retVal

    def __str__(self):
        """string representation of obj

        :return: the json rep of self.jsonData property
        :rtype: str
        """
        return json.dumps(self.jsonData)

    # REPLACED by applycustomTransformation
    # def runCustomTransformations(self, dataCell):
    #     """Checks to see if a custom transformation has been defined for the
    #     data type.  If it has then retrieves a reference to the method and runs
    #     it, returning the resulting data transformation.

    #     :param dataCell: a datacell object to run
    #     :type dataCell: DataCell
    #     """
    #     updtTransConfigurations = TRANSCONF.getCustomUpdateTransformations(self.dataType)
    #     #LOGGER.debug(f"updtTransConfigurations: {updtTransConfigurations}")
    #     if updtTransConfigurations:
    #         methodMapper = CustomTransformers.MethodMapping(self.dataType, updtTransConfigurations)
    #         for customMethodName in updtTransConfigurations:
    #             #methodName = customTransformerConfig[constants.CUSTOM_UPDATE_METHOD_NAME]
    #             methodReference = methodMapper.getCustomMethodCall(customMethodName)
    #             # this is a bit cludgy.. the custom methods are designed to work with collections
    #             # so just putting the individual record into a collection so that the
    #             # method will work.
    #             dataCell.struct = methodReference([dataCell.struct])[0]
    #             #LOGGER.debug(f"called custom method: {customMethodName}")
    #     return dataCell

class DataCell:
    """an object that can be used to wrap a data value and other meta data
    about it from the perspective of a change
    """

    def __init__(self, struct, include=True):
        self.struct = struct
        self.include = include
        self.ignoreList = None
        self.ignoreFld = None
        self.parent = None
        self.parentType = None
        self.parentKey = None

    def copyChanges(self, childDataCell):
        self.struct[childDataCell.parentKey] = childDataCell.struct

    def deleteIndexes(self, positions):
        """gets a list of the position that are to be trimmed from the struct

        :param positions: a list of index positions for the self.struct list that
            are to be removed.
        :type positions: list of ints
        """
        # LOGGER.debug(f"remove positions: {positions}")
        newStruct = []
        for pos in range(0, len(self.struct)):
            if pos not in positions:
                newStruct.append(self.struct[pos])
            else:
                # LOGGER.debug(f"removing: {pos} {self.struct[pos]}")
                pass
        # LOGGER.debug(f"old struct: {self.struct}")
        # LOGGER.debug(f"new struct: {newStruct}")
        self.struct = newStruct
        # transfer changes to the parent

    def generateNewCell(self, key):
        """The current cell is a dict, generates a new cell for the position
        associated with the input key.

        :param key: a key of struct property
        :type key: str
        """
        newCell = DataCell(self.struct[key])
        newCell.parent = self
        newCell.parentKey = key
        # copy the attributes from parent to child
        newCell.include = self.include
        newCell.ignoreList = self.ignoreList
        newCell.ignoreFld = self.ignoreFld
        newCell.parentType = self.parentType

        # if the key is an embedded type, users, groups, etc...
        if key in constants.VALID_TRANSFORM_TYPES:
            newCell.ignoreList = TRANSCONF.getIgnoreList(key)
            newCell.ignoreFld = TRANSCONF.getUniqueField(key)
            newCell.parentType = key

        if newCell.parentType is not None:
            # if the key is equal to the name of the ignore field
            if (newCell.ignoreFld) and key == newCell.ignoreFld:
                # example key is 'name' and the ignore field is name
                # now check to see if the value is in the ignore list
                if newCell.struct in newCell.ignoreList:
                    # continue with example.. now the value for the key name
                    # is in the ignore list.  Set the enclosing object... self
                    # to not be included.
                    self.include = False
        return newCell


class CKANUserRecord(CKANRecord):
    def __init__(self, jsonData, origin):
        recordType = constants.TRANSFORM_TYPE_USERS
        CKANRecord.__init__(self, jsonData, recordType, origin)


class CKANGroupRecord(CKANRecord):
    def __init__(self, jsonData, origin):
        recordType = constants.TRANSFORM_TYPE_GROUPS
        CKANRecord.__init__(self, jsonData, recordType, origin)


class CKANOrganizationRecord(CKANRecord):
    def __init__(self, jsonData, origin):
        recordType = constants.TRANSFORM_TYPE_ORGS
        CKANRecord.__init__(self, jsonData, recordType, origin)


class CKANPackageRecord(CKANRecord):
    def __init__(self, jsonData, origin):
        recordType = constants.TRANSFORM_TYPE_PACKAGES
        CKANRecord.__init__(self, jsonData, recordType, origin)


# -------------------- DATASET DELTA ------------------


class CKANDataSetDeltas:
    """Class used to represent differences between two objects of the same
    type.  Includes all the information necessary to proceed with the update.

    :ivar adds: A list of CKANRecords from the source side that should be added
                to destination
    :ivar deletes: A list of the names or ids on the dest side of objects that
                should be deleted.
    :ivar updates: A list of CKANRecords from the source side that should be
                updated on the destination side
    :ivar srcCKANDataset: CKANDataset object with source CKAN information, maintain a reference to this
        object so that can request CKAN records in the dataset with only
        user generated fields included.
    :ivar destCKANDataset: CKANDataset object for destination data
    """

    def __init__(self, srcCKANDataset, destCKANDataset):
        self.adds = CKANRecordCollection(srcCKANDataset.dataType)
        self.deletes = CKANRecordCollection(srcCKANDataset.dataType)
        self.updates = CKANRecordCollection(srcCKANDataset.dataType)

        self.srcCKANDataset = srcCKANDataset
        self.destCKANDataset = destCKANDataset

        # self.transConf = self.srcCKANDataset.transConf

    # def setAddDataset(self, addDataObj):
    #     """Adds a object to the list of objects that are identified as adds

    #     Adds are objects that exist in the source but not the destination

    #     :param addDataObj: data that is to be added
    #     :type addDataObj: dict
    #     :raises TypeError: raised if the input data is not type dict
    #     """
    #     if not isinstance(addDataObj, CKANRecord):
    #         msg = (
    #             "addDataObj parameter needs to be type dict.  You passed "
    #             + f"{type(addDataObj)}"
    #         )
    #         raise TypeError(msg)
    #     self.adds.append(addDataObj)

    # def setDeleteDataset(self, deleteName):
    #     """Adds an object to the list of data that has been identified as a
    #     Delete.

    #     Deletes are records that exist in the destination but not the source.

    #     :param deleteName: [description]
    #     :type deleteName: [type]
    #     :raises TypeError: [description]
    #     """
    #     if not isinstance(deleteName, str):
    #         msg = (
    #             "deleteName parameter needs to be type str.  You passed "
    #             + f"{type(deleteName)}"
    #         )
    #         raise TypeError(msg)
    #     self.deletes.append(deleteName)

    # def setUpdateDataSet(self, updateObj):
    #     """Adds a new dataset that is to be updated.  When comparison of two
    #     objects identifies that there is a difference, the object that passed to
    #     this method is the src object with the data that should be copied to dest.

    #     Updates are datasets that exist in source and destination however not all
    #     the data is the same between them.

    #     :param updateObj: the data that is to be updated
    #     :type updateObj: dict
    #     :raises TypeError: object must be type 'dict', raise if it is not.
    #     :raises ValueError: object must have a 'name' property
    #     """
    #     if not isinstance(updateObj, CKANRecord):
    #         msg = (
    #             "updateObj parameter needs to be type dict.  You passed "
    #             + f"{type(updateObj)}"
    #         )
    #         raise TypeError(msg)
    #     LOGGER.debug(f"adding update for {updateObj.getUniqueIdentifier()}")
    #     self.updates.append(updateObj)


    def setAddCollection(self, addCollection, replace=True):
        """adds a list of data to the adds property.  The adds property
        gets populated with data that should be added to the destination
        ckan instance

        :param addList: input list of data that should be added to the dest
             instance
        :type addList: struct
        :param replace: if set to true, will replace any data that may already
            exist in the adds property if set to false then will append to the
            end of the struct, defaults to True
        :type replace: bool, optional
        """
        if replace:
            LOGGER.info(f"populate add list with {len(addCollection)} items")
            self.adds = addCollection
        else:
            LOGGER.info(f"adding {len(addCollection)} items to the add list")
            #self.adds.extend(addList)
            for addRecord in addCollection:
                if not self.adds.hasRecord(addRecord):
                    self.adds.addRecord(addRecord)

    def setDeleteCollection(self, deleteCollection, replace=True):
        """adds a list of data to the deletes property.  The deletes property
        gets populated with unique ids that should be removed from the destination
        ckan instance

        :param deleteList: input list of data that should be deleted from the dest
             ckan instance
        :type addList: struct
        :param replace: if set to true, will replace any data that may already
            exist in the deletes property, if set to false then will append to the
            end of the struct, defaults to True
        :type replace: bool, optional
        """
        if replace:
            LOGGER.info(f"populate delete list with {len(deleteCollection)} items")
            self.deletes = deleteCollection
        else:
            LOGGER.info(f"adding {len(deleteCollection)} items to the delete list")
            for deleteRecord in deleteCollection:
                if not self.deletes.hasRecord(deleteRecord):
                    self.deletes.addRecord(deleteRecord)

    def setUpdateCollection(self, updateCollection, replace=True):
        """Gets a list of data that should be used to update objects in the ckan
        destination instance and adds the data to this object

        :param updateList: list of data to be used to update the object
        :type updateList: list
        """
        if replace:
            LOGGER.info(f"populate update list with {len(updateCollection)} items")
            self.updates = updateCollection
        else:
            LOGGER.info(f"adding {len(updateCollection)} records to update")
            for updateRecord in updateCollection:
                if not self.updates.hasRecord(updateRecord):
                    self.updates.addRecord(updateRecord)

    def getAddData(self):

        # should just return self.adds

        # cacheObj = self.srcCKANDataset.dataCache
        # for addCKANRecord in self.adds:
        #     compStruct = addCKANRecord.getComparableStructUsedForAddUpdate(
        #         addCKANRecord, self.srcCKANDataset.dataCache, constants.UPDATE_TYPES.ADD)


        # LOGGER.debug(f"add data: {type(self.adds)} {len(self.adds)}")
        # adds = self.filterNonUserGeneratedFields(self.adds)

        # # these are fields that are defined as autogen, but should include these
        # # fields from the source when defining new data on the dest.
        # addFields = TRANSCONF.getFieldsToIncludeOnAdd(self.destCKANDataset.dataType)
        # defaultFields = TRANSCONF.getRequiredFieldDefaultValues(
        #     self.destCKANDataset.dataType
        # )
        # idFields = TRANSCONF.getIdFieldConfigs(self.destCKANDataset.dataType)
        # enforceTypes = TRANSCONF.getTypeEnforcement(self.destCKANDataset.dataType)

        # if addFields:
        #     LOGGER.debug("adding destination autogen fields")
        #     adds = self.addAutoGenFields(adds, addFields, constants.DATA_SOURCE.SRC)
        # if defaultFields:
        #     LOGGER.debug("adding required Fields")
        #     adds = self.addRequiredDefaultValues(adds, defaultFields)
        # if idFields:
        #     LOGGER.debug("Addressing remapping of ID fields")
        #     adds = self.remapIdFields(adds, idFields, constants.DATA_SOURCE.SRC)

        # if enforceTypes:
            LOGGER.debug("Addressing property type enforcement")
            adds = self.enforceTypes(adds, enforceTypes)

        return self.adds

    def enforceTypes(self, inputDataStruct, enforceTypes):
        """iterates through the data in the inputDataStruct, looking for
        fields that are defined in the enforceTypes struct, if any are
        found then checks to see if the expected types align, if they
        don't and there is no data in them then the value will be modified
        to match the expected type.

        If there is data in the propertly and the expected type does not
        align, will log an warning message.

        :param inputDataStruct: The input data struct, can be a dict where the
            values are the update data structs for individual CKAN objects, or
            can be just a list of CKAN objects to be updates/added
        :type inputDataStruct: list/dict
        :param enforceTypes: a dict where the key is the property and the value
            is an empty struct representing the type that is expected to be
            associated with this property,
                example: {'property_name' : [] }
        :type enforceTypes: dict
        :return: same inputDataStruct, but modified so that the types align.
        :rtype: dict
        """
        LOGGER.debug(f"enforcetypes: {enforceTypes}")
        if isinstance(inputDataStruct, list):
            iterObj = range(0, len(inputDataStruct))
        else:
            iterObj = inputDataStruct.keys()

        # iterate over each input data struct
        for iterVal in iterObj:
            # iterate over the different type enforcements,
            #    format = property: <type of object>
            for fieldName in enforceTypes:
                # does the field definition from enforcement types exist in the
                # add data struct
                if fieldName in inputDataStruct[iterVal]:
                    # do the types of the data in the field struct align with what
                    # we are expecting it to be.
                    if type(enforceTypes[fieldName]) is not type(
                        inputDataStruct[iterVal][fieldName]
                    ):
                        # only try to fix if the data is empty.
                        if not inputDataStruct[iterVal][fieldName]:
                            #LOGGER.info(f"fixing the data type for: {fieldName}")
                            inputDataStruct[iterVal][fieldName] = enforceTypes[
                                fieldName
                            ]
                        else:
                            LOGGER.warning(
                                f"the property {fieldName} has a type "
                                f"{type(inputDataStruct[iterVal][fieldName])}.  This "
                                f"conflicts with the expected type defined in "
                                f"the {constants.TRANSFORM_PARAM_TYPE_ENFORCEMENT}"
                                f"transformation config section.  The field "
                                f"currently has the following data in it: {inputDataStruct[iterVal][fieldName]}"
                            )
        return inputDataStruct

    def remapIdFields(self, inputDataStruct, idFields, origin=constants.DATA_SOURCE.DEST):
        """
        CKAN has user generated and autogenerated unique identifiers for every
        objects type.

        CKAN in some cases establishes relationship between two different object
        types using the autogenerated ids.

           * example: owner_org refers to an 'organization' using its autogenerated
             id

        When copying data between instances you need to remap the fields that
        establish relationships between two objects using autogenerated ids.

        This method does that remapping by going through the data structure
        defined in "inputAddStruct" and remaps the autogenerated fields defined
        in the dict "idFields".

        origin tells us whether the autogenerated id originates from the source
        object or the destination object.

        :param inputAddStruct: a list of dictionaries with data to be added to
            the ckan instance.
        :type inputAddStruct: list of dicts
        :param idFields: a dictionary with the following keys:
                * property: the name of the property in the inputAddStruct that
                            the id remapping should be applied to
                * obj_type: what type of object is this.  Corresponds to the
                            object types defined in the transformation config,
                            for example: (users, groups, organizations, packages,
                            resources)
                * obj_field: The field in the destination object that the unique
                            id maps to.  Ie if property value was owner_org and
                            this value was id, it says that the property,
                            owner_org relates to the id value defined in this
                            value
        :type idFields: dict
        """
        # TODO: The way that remap fields are handled is different between
        #       adds and updates.  With updates the inputDataset id field will
        #       be the id from the destination side.
        LOGGER.debug("REMAP FIELDS")
        dataCache = self.srcCKANDataset.dataCache

        if isinstance(inputDataStruct, list):
            iterObj = range(0, len(inputDataStruct))
        else:
            iterObj = inputDataStruct.keys()
        for iterVal in iterObj:
            LOGGER.debug(f"iterVal: {iterVal}")
            currentDataset = inputDataStruct[iterVal]

            jsonDatasetStr = json.dumps(currentDataset)
            LOGGER.debug(f"currentDataset {currentDataset['name']}:  {jsonDatasetStr[0:150]} ...")
            #LOGGER.debug(f"currentDataset:  {jsonDatasetStr} ")

            for idRemapObj in idFields:
                # properties of the idRemapObj, and some sample values
                #  * property": "owner_org",
                #  * obj_type": "organizations",
                #  * obj_field : "id"
                # get the value for owner_org

                parentFieldName = idRemapObj[constants.IDFLD_RELATION_PROPERTY]
                childObjType = idRemapObj[constants.IDFLD_RELATION_OBJ_TYPE]
                childObjFieldName = idRemapObj[constants.IDFLD_RELATION_FLDNAME]

                parentFieldValue = currentDataset[parentFieldName]

                # dest is not loaded
                if not dataCache.isAutoValueInDest(childObjFieldName,
                        childObjType, parentFieldValue):

                    destAutoGenId = dataCache.src2DestRemap(
                        childObjFieldName, childObjType, parentFieldValue, origin
                    )
                    # last step is to write the value back to the data struct and
                    # return it
                    LOGGER.debug(
                        f"remapped autopop value from: {parentFieldValue} to {destAutoGenId}"
                    )
                    inputDataStruct[iterVal][parentFieldName] = destAutoGenId
        return inputDataStruct

    def getDeleteData(self):
        return self.deletes

    def getUpdateData(self):
        """ creates and returns a structure that can be used to update the object
        in question.

        :return: a dictionary where the key values are the unique identifiers
            and the values are the actual struct that should be used to update
            the destination ckan instance.
        :rtype: dict
        """
        # TODO: the logic for this should be moved into the Dataset / record
        #       object types.  Then same logic can be used for comparison as is
        #       used to formulate the update data.
        # should return only fields that are user generated

        # read through the records in the update data set, make sure that
        # the api data has been calculated.


        # updateStruct = self.getComparableStructUsedForAddUpdate(self.destCKANDataset)
        # return updateStruct

        # updates = self.filterNonUserGeneratedFields(self.updates)
        # idFields = TRANSCONF.getIdFieldConfigs(self.destCKANDataset.dataType)

        # # retrieve transformation configs
        # defaultFields = TRANSCONF.getRequiredFieldDefaultValues(
        #                             self.destCKANDataset.dataType)
        # customTransformers = TRANSCONF.getCustomUpdateTransformations(
        #     self.destCKANDataset.dataType)
        # enforceTypes = TRANSCONF.getTypeEnforcement(self.destCKANDataset.dataType)
        # updateFields = TRANSCONF.getFieldsToIncludeOnUpdate(
        #     self.destCKANDataset.dataType)

        # if updateFields:
        #     # need to add these onto each record from the destination
        #     # instances data
        #     updates = self.addAutoGenFields(
        #         updates, updateFields, constants.DATA_SOURCE.DEST
        #     )

        # if defaultFields:
        #     LOGGER.debug("adding required Fields")
        #     updates = self.addRequiredDefaultValues(updates, defaultFields)

        # if enforceTypes:
        #     LOGGER.debug("Addressing property type enforcement")
        #     updates = self.enforceTypes(updates, enforceTypes)

        # if idFields:
        #     LOGGER.debug("Addressing remapping of ID fields")
        #     updates = self.remapIdFields(updates, idFields)

        # # if stringifiedFields:
        # #     LOGGER.debug("Addressing stringified fields")
        # #     updates = self.doStringify(updates, stringifiedFields)

        # if customTransformers:
        #     LOGGER.debug(f"found custom transformers: {customTransformers}")
        #     methMap = CustomTransformers.MethodMapping(
        #         self.destCKANDataset.dataType,
        #         customTransformers
        #     )
        #     for customTransformer in customTransformers:
        #         LOGGER.info(f"loading and running the custom transformer : {customTransformer}")
        #         methodCall = methMap.getCustomMethodCall(customTransformer)
        #         methodCall(updates)
        # return updates
        return self.updates

    def addRequiredDefaultValues(self, inputDataStruct, defaultFields):
        """

        """
        # TODO: this isn't gonna work

        dataRecordWithRequiredFields = CKANRecord(inputDataStruct,
                                                  self.destCKANDataset.dataType,
                                                  constants.DATA_SOURCE.SRC)
        dataRecordWithRequiredFields.applyRequiredFields()

        if isinstance(inputDataStruct, list):
            iterObj = range(0, len(inputDataStruct))
        else:
            iterObj = inputDataStruct
        for iterVal in iterObj:
            # 'iterObj' either a list of dict of data sets
            # LOGGER.debug(f"iterVal:  {iterVal}")
            currentDataset = inputDataStruct[iterVal]
            # LOGGER.debug(f"currentDataset:  {currentDataset}")
            for fieldName in defaultFields:
                # LOGGER.debug(f"fieldName:  {fieldName}")
                # fieldName will be the index to the current data set.
                fieldValue = defaultFields[fieldName]
                populator = DataPopulator(currentDataset)
                currentDataset = populator.populateField(fieldName, fieldValue)
                #currentDataset = self.__populateField(currentDataset, fieldName, fieldValue)
                # this line should not be necessary, instead should
                # be a double check
                if fieldName not in currentDataset:
                    currentDataset[fieldName] = defaultFields[fieldName]
        return inputDataStruct

    def doStringify(self, inputDataStruct, stringifiedFields):
        if isinstance(inputDataStruct, list):
            iterObj = range(0, len(inputDataStruct))
        else:
            iterObj = inputDataStruct
        cnt = 0
        for iterVal in iterObj:
            for stringifyField in stringifiedFields:
                if stringifyField in inputDataStruct[iterVal]:
                    if cnt < 5:
                        LOGGER.debug(f"stringify the field: {stringifyField}")
                    elif cnt == 10:
                        LOGGER.debug(f"stringify the field: {stringifyField} ... (repeating)")
                    inputDataStruct[iterVal][stringifyField] = json.dumps(inputDataStruct[iterVal][stringifyField])
                    cnt += 1
        return inputDataStruct

    def addAutoGenFields(self, inputDataStruct, autoGenFieldList,
            additionalFieldSource=constants.DATA_SOURCE.DEST):
        """dataDict contains the data that is to be used for the update that
        originates from the source ckan instance.  autoGenFieldList is a list of
        field names that should be added to the struct, additionalFieldSource
        then identifies where the autoGenFieldList should be populated from.

        :param dataDict: The update data struct which is a dictionary where the
            keys are the unique identifier, in most cases the keys are the name
            property.  The values in this struct are the values from the source
            ckan instance.
        :type dataDict: dict
        :param autoGenFieldList: a list of field names that should be added to
            the struct from the destination ckan instance
        :type autoGenFieldList: list
        :param additionalFieldSource: either DEST or SRC. Used to indicate WHERE
            the extra fields should get populated from.  The source data or the
            destination data
        :type additionalFieldSource: constants.DATA_SOURCE (enum)
        :return: The values in the dataDict with the destination instance fields
            defined in autoGenFieldList appended to the dictionary
        :rtype: dict
        """
        # verify the correct type was received as additionalFieldSource
        if not isinstance(additionalFieldSource, constants.DATA_SOURCE):
            msg = (
                f"arg: additionalFieldSource received a type "
                + f"{type(additionalFieldSource)} however it needs to be a "
                + f"constants.DATA_SOURCE type"
            )
            raise IllegalArgumentTypeError(msg)

        # create a map to where the data should originate from
        recordCalls = {
            constants.DATA_SOURCE.DEST: self.destCKANDataset,
            constants.DATA_SOURCE.SRC: self.srcCKANDataset,
        }

        LOGGER.debug(f"type of dataDict: {type(inputDataStruct)}")
        elemCnt = 0

        if isinstance(inputDataStruct, list):
            iterObj = range(0, len(inputDataStruct))
            uniqueIdField = TRANSCONF.getUniqueField(
                recordCalls[additionalFieldSource].dataType
            )
        else:
            iterObj = inputDataStruct

        for iterVal in iterObj:
            # record = self.destCKANDataset.getRecordByUniqueId(uniqueId)
            if isinstance(inputDataStruct, list):
                uniqueId = inputDataStruct[iterVal][uniqueIdField]
            else:
                uniqueId = iterVal

            # if isinstance(uniqueId, dict):
            #     uniIdField = TRANSCONF.getUniqueField(recordCalls[additionalFieldSource].dataType)
            #     lookup = uniqueId
            #     uniqueId = lookup[uniIdField]
            # else:
            #     lookup = dataDict

            # LOGGER.debug(f"uniqueId: {uniqueId}")
            record = recordCalls[additionalFieldSource].getRecordByUniqueId(uniqueId)
            for field2Add in autoGenFieldList:
                # if field2Add not in inputDataStruct[iterVal]:
                fieldValue = record.getFieldValue(field2Add)
                inputDataStruct[iterVal][field2Add] = fieldValue
                if field2Add == "owner_org":
                    LOGGER.debug(f"{field2Add}:  {fieldValue}")
                # LOGGER.debug(f"adding: {field2Add}:{fieldValue} to {uniqueId}")

            elemCnt += 1
        return inputDataStruct

    def __str__(self):
        # addNames = []
        # for add in self.adds:
        #     addNames.append(add['name'])
        # updateNames = self.updates.keys()
        msg = (
            f"add datasets: {len(self.adds)}, deletes: {len(self.deletes)} "
            + f"updates: {len(self.updates)}"
        )
        return msg

    # def filterNonUserGeneratedFields(self, ckanDataSet):
    #     """
    #     Receives either a dict or list:
    #        * dict: key is the unique id for the dataset
    #        * list: a list of dicts describing a list of data
    #                objects.

    #     Iterates over all the data in the ckanDataSet struct, removing non
    #     user generated fields and returns a json struct (dict) with only
    #     fields that are user defined

    #     :param ckanDataSet: a ckan data set
    #     :type ckanDataSet: CKANDataSet or an object that subclasses it
    #     """
    #     # get the unique id for this dataset type
    #     # uniqueIdentifier = self.srcCKANDataset.transConf.getUniqueField(
    #     #    self.srcCKANDataset.dataType)
    #     uniqueIdentifier = TRANSCONF.getUniqueField(self.srcCKANDataset.dataType)

    #     # if generating a dataset to be used to update a dataset, then check to
    #     # see if there are machine generated fields that should be included in the
    #     # update
    #     LOGGER.debug(f"uniqueIdentifier: {uniqueIdentifier}")

    #     if isinstance(ckanDataSet, dict):
    #         filteredData = {}
    #         uniqueIds = ckanDataSet.keys()
    #     elif isinstance(ckanDataSet, list):
    #         filteredData = []
    #         # below is wrong as it returns all unique ids, we only want the
    #         # unique ids provided in the struct ckanDataSet
    #         # uniqueIds = self.srcCKANDataset.getUniqueIdentifiers()
    #         uniqueIds = []
    #         for record in ckanDataSet:
    #             uniqueIds.append(record[uniqueIdentifier])
    #     else:
    #         msg = f"type received is {type(ckanDataSet)}, expecting list or dict"
    #         raise IncompatibleTypesException(msg)

    #     for uniqueId in uniqueIds:
    #         # LOGGER.debug(f"uniqueId: {uniqueId}")
    #         ckanRec = self.srcCKANDataset.getRecordByUniqueId(uniqueId)
    #         compStruct = ckanRec.getComparableStruct()

    #         # Adding this code in to accommodate resources in packages.  When
    #         # updating resources

    #         if isinstance(ckanDataSet, dict):
    #             filteredData[uniqueId] = compStruct
    #         elif isinstance(ckanDataSet, list):
    #             filteredData.append(compStruct)
    #     return filteredData
# -------------------- DATASETS --------------------


class CKANDataSet(CKANRecordCollection):
    """This class wraps a collection of datasets.  Includes an iterator that
    will return a CKANRecord object.

    :raises IncompatibleTypesException: This method is raised when comparing two
        incompatible types.
    """

    def __init__(self, jsonData, dataType, dataCache, origin):
        CKANRecordCollection.__init__(self, dataType)
        # json data isn't stored, it gets parsed info individual records

        self.dataCache = dataCache
        self.origin = origin

        # self.transConf = CKANTransform.TransformationConfig()
        self.userPopulatedFields = TRANSCONF.getUserPopulatedProperties(self.dataType)
        self.iterCnt = 0

        self.srcUniqueIdSet = None
        self.destUniqueIdSet = None

        self.parseDataIntoRecords(jsonData)

    def populateDataSets(self, destDataSet):
        if self.srcUniqueIdSet is None:
            self.srcUniqueIdSet = set(self.getUniqueIdentifiers())
        if self.destUniqueIdSet is None:
            self.destUniqueIdSet set(destDataSet.getUniqueIdentifiers())

    def calcDeleteCollection(self, destDataSet):
        """gets a set of unique ids from the source and destination ckan instances,
        compares the two lists and generates a list of ids that should be deleted
        from the destination instance.  Excludes any ids that are identified in
        the ignore list defined in the transformation configuration file.

        :param destUniqueIdSet: a set of unique ids found the destination ckan
            instance
        :type destUniqueIdSet: set
        :param srcUniqueIdSet: a set of the unique ids in the source ckan instance
        :type srcUniqueIdSet: set
        """
        self.populateDataSets(destDataSet)
        ignoreList = TRANSCONF.getIgnoreList(self.dataType)
        deleteDataCollection = CKANRecordCollection(self.dataType)

        deleteSet = self.destUniqueIdSet.difference(self.srcUniqueIdSet)
        deleteList = []
        for deleteUniqueName in deleteSet:
            # Check to see if the user is in the ignore list, only add if it is not
            if deleteUniqueName not in ignoreList:
                #deleteList.append(deleteUniqueName)
                record = self.getRecordByUniqueId(deleteUniqueName)
                deleteDataCollection.addRecord(record)
        return deleteDataCollection

    def calcAddCollection(self, destDataSet):
        """Gets a two sets of unique ids, one for the data on the source ckan
        instance and another for the destination ckan instance.  Using this
        information returns a list of unique ids that should be added to the
        destination instance

        :param destUniqueIdSet: a set of unique ids from the destination ckan
            instance.
        :type destUniqueIdSet: set
        :param srcUniqueIdSet: a set of unique ids from the source ckan instance
        :type srcUniqueIdSet: set
        :return: a CKANRecordCollection object
            ckan instance.  Will exclude any unique ids identified in the
            transformation configuration ignore list.
        :rtype: list
        """
        self.populateDataSets(destDataSet)

        # in source but not in dest, ie adds
        addSet = self.srcUniqueIdSet.difference(self.destUniqueIdSet)

        ignoreList = TRANSCONF.getIgnoreList(self.dataType)

        addList = []
        addCollection = CKANRecordCollection()

        for addRecordUniqueName in addSet:
            # LOGGER.debug(f"addRecord: {addRecordUniqueName}")
            if addRecordUniqueName not in ignoreList:
                addRecord = self.getRecordByUniqueId(addRecordUniqueName)
                #addDataStruct = addRecord.getComparableStruct()
                addCollection.addRecord(addRecord)
        return addCollection

    def calcUpdatesCollection(self, destDataSet):
        self.populateDataSets(destDataSet)

        ignoreList = TRANSCONF.getIgnoreList(self.dataType)

        chkForUpdateIds = self.srcUniqueIdSet.intersection(self.destUniqueIdSet)
        chkForUpdateIds = list(chkForUpdateIds)
        chkForUpdateIds.sort()

        updateCollection = CKANRecordCollection()

        for chkForUpdateId in chkForUpdateIds:
            # now make sure the id is not in the ignore list
            if chkForUpdateIds not in ignoreList:
                srcRecordForUpdate = self.getRecordByUniqueId(chkForUpdateId)
                destRecordForUpdate = destDataSet.getRecordByUniqueId(chkForUpdateId)


                # if they are different then identify as an update.  The __eq__
                # method for dataset is getting called here.  __eq__ will consider
                # ignore lists.  If record is in ignore list it will return as
                # equal.
                if srcRecordForUpdate != destRecordForUpdate:
                    # updateDataList.append(srcRecordForUpdate)
                    updateCollection.addRecord(srcRecordForUpdate)
        return updateCollection

    def parseDataIntoRecords(self, jsonData):
        LOGGER.debug("parsing list of dicts into CKANRecord objects")
        includeType = True
        constructor = CKANRecord
        if hasattr(self, 'recordConstructor'):
            constructor = self.recordConstructor
            includeType = False
        for recordJson in jsonData:
            if includeType:
                record = constructor(recordJson, self.dataType, self.origin)
            else:
                record = constructor(recordJson, self.origin)
            self.addRecord(record)

    def getDelta(self, destDataSet):
        """Compares this dataset with the provided 'ckanDataSet' dataset and
        returns a CKANDatasetDelta object that identifies
            * additions
            * deletions
            * updates

        Assumption is that __this__ object is the source dataset and the object
        in the parameter destDataSet is the destination dataset, or the dataset
        that is to be updated

        :param destDataSet: the dataset that is going to be updated so it
            matches the contents of the source dataset
        :type ckanDataSet: CKANDataSet
        """
        deltaObj = CKANDataSetDeltas(self, destDataSet)

        # populate the cache to allow quick remapping of fields that reference
        # autogenerated unique ids
        self.dataCache.addData(self, constants.DATA_SOURCE.SRC)
        self.dataCache.addData(destDataSet, constants.DATA_SOURCE.DEST)

        # calculates unique id SETS used to calculate actual deltas
        self.populateDataSets(destDataSet)

        deleteCollection = self.calcDeleteCollection(destDataSet)
        deltaObj.setDeleteCollection(deleteCollection)

        addCollection = self.calcAddCollection(destDataSet)
        deltaObj.setAddCollection(addCollection)

        updateCollection = self.calcUpdatesCollection(destDataSet)
        deltaObj.setUpdateCollection(updateCollection)

        return deltaObj

    def __eq__(self, ckanDataSet):
        """ Identifies if the input dataset is the same as this dataset

        :param ckanDataSet: The input CKANDataset
        :type ckanDataSet: either CKANDataSet, or a subclass of it
        """
        LOGGER.debug("DATASET EQ TEST")
        retVal = True
        # TODO: rework this, should be used to compare a collection
        validateTypeIsComparable(self, ckanDataSet)

        # get the unique identifiers and verify that input has all the
        # unique identifiers as this object
        inputUniqueIds = ckanDataSet.getUniqueIdentifiers()
        thisUniqueIds = self.getUniqueIdentifiers()

        LOGGER.debug(f"inputUniqueIds (subset): {inputUniqueIds[0:10]} ...")
        LOGGER.debug(f"thisUniqueIds (subset): {thisUniqueIds[0:10]}")

        LOGGER.debug(f"this unique ids count: {len(thisUniqueIds)}")
        LOGGER.debug(f"input data sets unique id count: {len(inputUniqueIds)}")

        if set(inputUniqueIds) == set(thisUniqueIds):
            # has all the unique ids, now need to look at the differences
            # in the data
            LOGGER.debug(f"iterate ckanDataSet: {ckanDataSet}")
            LOGGER.debug(f"ckanDataSet record count: {len(ckanDataSet)}")
            for inputRecord in ckanDataSet:
                #sampleString = (str(inputRecord))[0:65]
                # LOGGER.debug(f"iterating: {sampleString} ...")
                recordUniqueId = inputRecord.getUniqueIdentifier()
                compareRecord = self.getRecordByUniqueId(recordUniqueId)
                # LOGGER.debug(f"type 1 and 2... {type(inputRecord)} {type(compareRecord)}")
                if inputRecord != compareRecord:
                    LOGGER.debug(f"---------{recordUniqueId} doesn't have equal")
                    retVal = False
                    break
        else:
            LOGGER.debug(f"unique ids don't align")
            retVal = False
        return retVal

class CKANRecordCollection:
    """Used to store a bunch of CKAN Records
    """
    def __init__(self, dataType):
        self.recordList = []
        self.dataType = dataType

        # an index to help find records faster. constructed
        # the first time a record is requested
        self.uniqueidRecordLookup = {}

    def getUniqueIdentifiers(self):
        """Iterates through the records in the dataset extracting the values from
        the unique identifier field as defined in the config file.

        :return: list of values found in the datasets unique constrained field.
        :rtype: list
        """
        self.reset()
        uniqueIds = []
        for record in self:
            uniqueIds.append(record.getUniqueIdentifier())
        uniqueIds.sort()
        return uniqueIds

    def addRecord(self, record):
        self.recordList.append(record)

    def reset(self):
        """reset the iterator
        """
        self.iterCnt = 0

    def hasRecord(self, record):
        retVal = False
        self.populateUniqueIdLookup()
        recordUniqueId = record.getUniqueIdentifier()
        if recordUniqueId in self.uniqueidRecordLookup:
            retVal = True
        return retVal

    def populateUniqueIdLookup(self):
        if not self.uniqueidRecordLookup:
            for record in self:
                recordID = record.getUniqueIdentifier()
                self.uniqueidRecordLookup[recordID] = record


    def getRecordByUniqueId(self, uniqueValueToRetrieve):
        """Gets the record that aligns with this unique id.
        """
        retVal = None
        self.populateUniqueIdLookup()
        if uniqueValueToRetrieve in self.uniqueidRecordLookup:
            retVal = self.uniqueidRecordLookup[uniqueValueToRetrieve]
        return retVal


    def __iter__(self):
        return self

    def __next__(self):
        if self.iterCnt >= len(self.recordList):
            self.iterCnt = 0
            raise StopIteration
        ckanRecord = self.recordList[self.iterCnt]
        self.iterCnt += 1
        return ckanRecord

    def next(self):
        return self.__next__()

    def __len__(self):
        return len(self.recordList)

class CKANUsersDataSet(CKANDataSet):
    """Used to represent a collection of CKAN user data.

    :param CKANData: [description]
    :type CKANData: [type]
    """

    def __init__(self, jsonData, dataCache, origin):
        CKANDataSet.__init__(self, jsonData, constants.TRANSFORM_TYPE_USERS, dataCache, origin)
        self.recordConstructor = CKANUserRecord


class CKANGroupDataSet(CKANDataSet):
    def __init__(self, jsonData, dataCache, origin):
        CKANDataSet.__init__(self, jsonData, constants.TRANSFORM_TYPE_GROUPS, dataCache, origin)
        self.recordConstructor = CKANGroupRecord


class CKANOrganizationDataSet(CKANDataSet):
    def __init__(self, jsonData, dataCache, origin):
        CKANDataSet.__init__(self, jsonData, constants.TRANSFORM_TYPE_ORGS, dataCache, origin)
        self.recordConstructor = CKANGroupRecord


class CKANPackageDataSet(CKANDataSet):
    def __init__(self, jsonData, dataCache, origin):
        CKANDataSet.__init__(
            self, jsonData, constants.TRANSFORM_TYPE_PACKAGES, dataCache, origin
        )
        self.recordConstructor = CKANPackageRecord

class DataPopulator:
    def __init__(self, inputData):
        self.inputData = inputData

    def populateField(self, key, valueStruct):
        returnData = self.__populateField(
            self.inputData, key, valueStruct
        )
        return returnData

    def __populateField(self, inputData, key, valueStruct):
        """
        inputData is an input data struct, key refers to either an element in a
        list or an element in a dictionary that should exist.

        valueStruct is the value that the key should be equal to.  ValueStruct
        can be any native python type.  If its a list it identifies values that
        should exist in that list

        If valueStruct is a dict it identifies key value pairs, keys are keys
        that must be in the corresponding dict in inputData.

        :param inputData: The input data structure
        :type inputData: list or dict
        :param key: if the inputdata is expected to be a list then this will be
            populated to 0 by default, however if the input is a dict it will
            be a key that should exist in the dictionary
        :type key: int, str
        :param valueStruct: a structure that should be represented in the input
            Data the structure defined keys that must exist if its a dict, and
            the value to set the keys equal to if they do NOT exist in the
            inputData.
        :type valueStruct: any
        :raises ValueError: raised when an unexpected type is encountered.
        :return: the inputData struct with modifications
        :rtype: any
        """
        # LOGGER.debug(f"inputData: {inputData}")
        # LOGGER.debug(f"key: {key}")
        # LOGGER.debug(f"valueStruct: {valueStruct}")

        # test if valueStruct is a primitive
        if isinstance(valueStruct, (str, bool, int, float, complex)):
            inputData = self.populatePrimitive(key, inputData, valueStruct)
        elif isinstance(valueStruct, list):
            # inputData = package struct
            # key = resources
            # valueStruct = list of dict with keys for resources
            #               [ { key:val...}]
            # key always aligns with the data.  data and key = default values in valueStruct
            inputData = self.populateList(key, inputData, valueStruct)
        elif isinstance(valueStruct, dict):
            inputData = self.populateDict(inputData, valueStruct)
        return inputData

    def populateDict(self, inputData, valueStruct):
        for elemKey in valueStruct:
            elemValue = valueStruct[elemKey]
            if isinstance(inputData, list):
                for inputDataPosition in range(0, len(inputData)): #pylint: disable=consider-using-enumerate
                    inputData[inputDataPosition] = self.__populateField(
                        inputData[inputDataPosition], elemKey, elemValue
                    )
            elif isinstance(inputData, dict):
                for inputKey in inputData:
                    inputData[inputKey] = self.__populateField(
                        inputData[inputKey], elemKey, elemValue
                    )
        return inputData

    def populateList(self, key, inputData, valueStruct):
        if isinstance(inputData, dict):
            if key not in inputData:
                inputData[key] = []
            for nextKey in valueStruct:
                if isinstance(nextKey, dict) and not inputData[key]:
                    inputData[key].append({})
                inputData[key] = self.__populateField(inputData[key], 0, nextKey)
        elif isinstance(inputData, list) and valueStruct not in inputData:
            inputData.append([])
            for nextKey in valueStruct:
                inputData[-1] = self.__populateField(inputData[-1], 0, nextKey)
        return inputData

    def populatePrimitive(self, key, inputData, valueStruct):
        if isinstance(inputData, dict):
            if key not in inputData:
                inputData[key] = valueStruct
        elif isinstance(inputData, list):
            if valueStruct not in inputData:
                # example key would be a number, doesn't matter cause its not used
                # input data is a string that must be in the inputData list
                inputData.append(valueStruct)
        else:
            msg = (
                'expecting "inputData" to be a dict or a list, but its a '
                + f"{type(inputData)} type.  Don't know what to do! {inputData}"
            )
            raise ValueError(msg)
        return inputData


# ----------------- EXCEPTIONS


class UserDefinedFieldDefinitionError(Exception):
    """Raised when the transformation configuration encounters an unexpected
    value or type

    """

    def __init__(self, message):
        LOGGER.error(f"error message: {message}")
        self.message = message


class IncompatibleTypesException(Exception):
    def __init__(self, message):
        LOGGER.error(f"error message: {message}")
        self.message = message


class IllegalArgumentTypeError(ValueError):
    def __init__(self, message):
        LOGGER.error(f"error message: {message}")
        self.message = message
