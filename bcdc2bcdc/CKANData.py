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
import os
import pprint
import sys

import json_delta

import bcdc2bcdc.CacheFiles as CacheFiles
import bcdc2bcdc.CKANTransform as CKANTransform
import bcdc2bcdc.constants as constants
import bcdc2bcdc.CustomTransformers as CustomTransformers
import bcdc2bcdc.Diff as Diff

LOGGER = logging.getLogger(__name__)
TRANSCONF = CKANTransform.TransformationConfig()

# pylint: disable=protected-access


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

    def __init__(self, jsonData, dataType, origin, dataCache):
        self.jsonData = jsonData
        self.dataType = dataType
        self.origin = origin
        self.userPopulatedFields = TRANSCONF.getUserPopulatedProperties(self.dataType)

        self.comparableJsonData = None  # populated when you call getComparableStruct()
        self.updateableJsonData = (
            None  # populated when you call getComparableStructUsedForAddUpdate()
        )
        self.operations = []

        self.destRecord = None
        # after a diff has been run for update objects this will be
        self.diff = None
        self.dataCache = dataCache

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
            dataCell = DataCell(self.comparableJsonData, self.dataCache, self.origin)
            dataCellNoIgnores = self.removeEmbeddedIgnores(dataCell)
            self.comparableJsonData = dataCellNoIgnores.struct

            if self.origin == constants.DATA_SOURCE.SRC:
                # add required fields to the source
                self.applyRequiredFields()

            # determine if there are custom transformations that should be
            # run.
            #   conditions:
            #     - there is a custom transformation defined
            #     - WhenToApply should be COMPARE
            #
            # run the custom transformations, the method below will only
            # apply the custom transformation if they are configured for
            # the current situation
            self.applyCustomTransformations(constants.UPDATE_TYPES.COMPARE)

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

        newStruct = None

        # only fields defined in this struct should be included in the output
        if isinstance(flds2Include, list):
            # currently assuming that if a list is found there will be a single
            # record in the flds2Include configuration that describe what to
            # do with each element in the list
            return self.__filterUserGenFieldsList(struct, flds2Include)
        elif isinstance(flds2Include, dict):
            newStruct = self.__filterUserGenFieldsDict(struct, flds2Include)

            return newStruct
        elif isinstance(flds2Include, bool):
            return struct
        return newStruct

    def __filterUserGenFieldsList(self, struct, flds2Include):
        newStruct = []
        if isinstance(flds2Include[0], dict):
            for structElem in struct:
                dataValue = self.filterNonUserGeneratedFields(
                    structElem, flds2Include[0]
                )
                newStruct.append(dataValue)
            return newStruct

    def __filterUserGenFieldsDict(self, struct, flds2Include):
        """Used by the filterNonUserGeneratedFields method.
        filterNonUserGeneratedFields iterates recursively over the data found
        in particular object.  Looking for property definitions in the
        flds2Include structure.  If a field is either not found or the
        definition or the field in flds2Include is false then the property will
        not be included in the returned data structure.

        During the recursion if, an iteration encouters a dict data structure
        this method is called with that structure.  It iterates over the
        flds2Include properties.  If a field is found to be required in the
        flds2Include it is added to the struct. filterNonUserGeneratedFields
        then gets called on the actual key and corresponding value and the
        equivalent data structure in the flds2Include

        :param struct: The input dictionary to evalute
        :type struct: dict
        :param flds2Include: The input dictionary that describes the required
            properties for the 'struct'
        :type flds2Include: dict
        :return: a dictionary that only includes properties that are defined in
            the flds2Include dict
        :rtype: dict
        """
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
            if key not in struct:
                # field is defined as being required but is not in the object
                # that was returned.  Setting it equal to None
                struct[key] = None

            newStruct[key] = self.filterNonUserGeneratedFields(
                struct[key], flds2Include[key]
            )
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
        if isinstance(dataCell.struct, dict):
            for objProperty in dataCell.struct:
                newCell = dataCell.generateNewCell(objProperty)
                newCell = self.removeEmbeddedIgnores(newCell)
                dataCell.copyChanges(newCell)
        elif isinstance(dataCell.struct, list):
            positions2Remove = []
            for listPos in range(0, len(dataCell.struct)):
                newCell = dataCell.generateNewCell(listPos)
                newCell = self.removeEmbeddedIgnores(newCell)
                if not newCell.include:
                    positions2Remove.append(listPos)
            dataCell.deleteIndexes(positions2Remove)
        return dataCell

    def __eq__(self, inputRecord):
        diff = self.getDiff(inputRecord)

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
        if (ignoreField in inputRecord.jsonData) and inputRecord.jsonData[ignoreField] in ignoreList:
            retVal = True
        return retVal

    def setDestRecord(self, destRecord):
        """When calculating the data structure that will be sent to the api to
        update an CKAN object, most data will come from the source object,
        however some data needs to come from the destination object.  This
        method populates the CKANRecord object with the corresponding existing
        destination object if one exists.

        :param destRecord: a CKANRecord object for the corresponding record on
            the destination side
        :type destRecord: CKANRecord
        """
        self.destRecord = destRecord
        if self.destRecord.origin != constants.DATA_SOURCE.DEST:
            msg = (f"the dest record provided has a type of "
                "{self.destRecord.origin} needs to be a 'DEST' type")
            raise InvalidDataRecordOrigin(msg)

    def getComparableStructUsedForAddUpdate(self, dataCache, operationType,
                                            destRecord=None):
        """ Starts by transforming the record into a comparable struct, then
        for update the following additional operations need to be run on the
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
                # ADDS
                destRecord = self
            else:
                destRecord = self.destRecord

            # calls getcomparable which will remove all the autogenerated ids, then
            # for source data will apply the default field calculations

            # init the structure that will contain the updateable json
            # make sure this has the required fields in it
            self.updateableJsonData = self.getComparableStruct()

            # double check that this is being run on a source object
            if self.origin != constants.DATA_SOURCE.SRC:
                msg = "cannot do the requested transformations on a DEST record"
                LOGGER.warning(msg)
            else:
                if (
                    destRecord.origin != constants.DATA_SOURCE.DEST
                    and operationType == constants.UPDATE_TYPES.UPDATE
                ):
                    msg = (
                        "the destination record provided to this method is not a"
                        "destination record"
                    )
                    LOGGER.error(msg)
                    raise ValueError(msg)

                # determine if custom transformations should be run
                # run if: WhenToApply='UPDATE'
                # and if operationType = CUSTOM_UPDATE_TYPE

                # adds add fields if operationType is add
                # adds update fields if operationType is update
                self.applyAutoGenFields(destRecord, operationType)

                # id remapping
                self.applyIdRemapping(dataCache)

                # run the custom transformations, they will only run if they
                # are configured for ADD or UPDATE, otherwise they will already
                # have been run
                self.applyCustomTransformations(operationType)
            self.operations.append(methodName)
        return self.updateableJsonData

    def applyIdRemapping(self, dataCache):
        # LOGGER.debug("REMAP FIELDS")

        methodName = sys._getframe().f_code.co_name
        if methodName not in self.operations:
            idFields = TRANSCONF.getIdFieldConfigs(self.dataType)
            for idRemapObj in idFields:
                # properties of the idRemapObj, and some sample values
                #  * property": "owner_org",
                #  * obj_type": "organizations",
                #  * obj_field : "id"

                parentFieldName = idRemapObj[constants.IDFLD_RELATION_PROPERTY]
                childObjType = idRemapObj[constants.IDFLD_RELATION_OBJ_TYPE]
                childObjFieldName = idRemapObj[constants.IDFLD_RELATION_FLDNAME]

                # get the original value from the package, that has not been
                # modified in any way
                parentFieldValue = self.jsonData[parentFieldName]
                if not dataCache.isAutoValueInDest(
                    childObjFieldName, childObjType, parentFieldValue
                ):

                    destAutoGenId = dataCache.src2DestRemap(
                        childObjFieldName, childObjType, parentFieldValue, self.origin
                    )
                    # last step is to write the value back to the data struct and
                    # return it
                    # LOGGER.debug(f"remapped autopop value from: {parentFieldValue}"
                    #             " to {destAutoGenId}")
                    self.updateableJsonData[parentFieldName] = destAutoGenId
                else:
                    # the autogen id is dest already.  Make sure its added to the
                    # updateable object
                    self.updateableJsonData[parentFieldName] = parentFieldValue
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

    def applyCustomTransformations(self, applicationType,
                                   customTransformationConfig=None):
        """
        applicationType: either UPDATE or COMPARE.  The trans conf identifies
            whether the custom transformation should be run for comparison
            operations (Which means it gets run for comparison and for update)
            or whether it should be run for ONLY update operations.

        other considerations:
             - Should only be run on source object.
             - The operation, is it an ADD or and UPDATE, this only applies
               if the applicationType = 'UPDATE' and NOT 'COMPARE'

               if applicationType = 'COMPARE' then the custom transformation is
               applied

        :param applicationType: [description]
        :type applicationType: [type]
        :param customTransformationConfig: [description], defaults to None
        :type customTransformationConfig: [type], optional
        """
        # custom transformations can be called for different types of updates.
        # custom transformers are configured so that when they are called for
        # COMPARE they know to modify the structure that is used for comparison
        # and when called for ADD / UPDATE they modify a different structure.
        # Adding the name of the operation type to the method name to keep track
        # of which custom transformations have been run.
        #
        methodName = sys._getframe().f_code.co_name
        methodName = f"{methodName}.{applicationType.name}"
        # has this already been run on this record?
        if methodName not in self.operations:
            # if no customTransformationConfig provided then retrieve one from the
            # config file
            if not customTransformationConfig:
                # customTransformationConfig = TRANSCONF.getCustomUpdateTransformations(
                #    self.dataType)
                # gets all custom transformations for all types
                customTransformationConfig = TRANSCONF.getCustomTranformations(
                    self.dataType
                )

            # if there is a custom tranformation for the current datatype
            if customTransformationConfig:
                # create a method mapper, which will validate the custom tranformation names
                # LOGGER.debug(f"found custom transformers: {customTransformationConfig}")
                # get a list of the custom transformer method names
                customTransformerMethodNames = [
                    customTransDict[constants.CUSTOM_UPDATE_METHOD_NAME]
                    for customTransDict in customTransformationConfig
                ]

                methMap = CustomTransformers.MethodMapping(
                    self.dataType, customTransformerMethodNames, applicationType
                )


                for customTransformer in customTransformationConfig:
                    self.__runCustomTransformer(applicationType, methMap, customTransformer, methodName)

                    # # run the custom transformer that are configured for the current applicationType
                    # if (
                    #     customTransformer[constants.CUSTOM_UPDATE_TYPE]
                    #     == applicationType.name
                    # ):
                    #     # LOGGER.info(f"loading and running the custom transformer : {customTransformer}")
                    #     # CustomMethodName
                    #     customTransformerName = customTransformer[
                    #         constants.CUSTOM_UPDATE_METHOD_NAME
                    #     ]
                    #     methodCall = methMap.getCustomMethodCall(customTransformerName)
                    #     self.customTransformerParams = {"updateType": applicationType}
                    #     methodCall(self)
                    #     # struct = methodCall([self.comparableJsonData])
                    #     # self.comparableJsonData = struct.pop()
                    #     if methodName not in self.operations:
                    #         self.operations.append(methodName)

    def __runCustomTransformer(self, applicationType, methodMap, customTransformerDict, methodName):
        # run the custom transformer that are configured for the current applicationType
        if (
            customTransformerDict[constants.CUSTOM_UPDATE_TYPE]
            == applicationType.name
        ):
            # CustomMethodName
            customTransformerName = customTransformerDict[
                constants.CUSTOM_UPDATE_METHOD_NAME
            ]
            methodCall = methodMap.getCustomMethodCall(customTransformerName)
            self.customTransformerParams = {"updateType": applicationType}
            methodCall(self)
            if methodName not in self.operations:
                self.operations.append(methodName)


    def applyRequiredFields(self):
        """retrieves the required field config if it exists for the current
        data/object type.  Then reads it and applies the default values.
        """
        methodName = sys._getframe().f_code.co_name
        if methodName not in self.operations:

            if self.comparableJsonData is None:
                self.comparableJsonData = self.jsonData.copy()

            defaultFields = TRANSCONF.getRequiredFieldDefaultValues(self.dataType)
            if defaultFields:
                currentDataset = self.comparableJsonData
                for fieldName in defaultFields:
                    # LOGGER.debug(f"fieldName:  {fieldName}")
                    # fieldName will be the index to the current data set.
                    fieldValue = defaultFields[fieldName]

                    populator = DataPopulator(currentDataset)
                    currentDataset = populator.populateField(fieldName, fieldValue)
                self.comparableJsonData = currentDataset
            self.operations.append(methodName)

    def getResourceDiff(self, inputRecord):
        diff = None

        cacheFiles = None

        if not self.isIgnore(inputRecord):
            thisComparable = self.getComparableStruct()
            thisComparable = thisComparable.copy()
            inputComparable = inputRecord.getComparableStruct()
            inputComparable = inputComparable.copy()

            diff = None
            # remove resources and compare separately
            if "resources" in thisComparable and "resources" in inputComparable:
                resource1 = thisComparable["resources"].copy()
                resource2 = inputComparable["resources"].copy()

                resDiffIngoreEmptyTypes = Diff.Diff(resource1, resource2)
                diff = resDiffIngoreEmptyTypes.getDiff()

                if diff:
                    # This is all debugging code to help resolve change detection
                    # issues.
                    # debugging... writing the resources for closer examination
                    if cacheFiles is None:
                        cacheFiles = CacheFiles.CKANCacheFiles()
                    name = self.getUniqueIdentifier()

                    resPath1 = cacheFiles.getResourceFilePath(name, self.origin)
                    with open(resPath1, 'w') as fh1:
                        json.dump(resource1, fh1, sort_keys=True)

                    with open(resPath1, 'a') as fh2:
                        dumpStr = json.dumps(resource2, sort_keys=True)
                        fh2.write(f'\n{dumpStr}\n')

                    jsonDiff = json_delta.diff(resource1, resource2)
                    LOGGER.debug(f"jsonDiff 2: {jsonDiff}")
        return diff

    def getPackageDiff(self, inputRecord):
        """
        deepdiff does not seem to be working for list of dicts.  Package
        resources are effected by this.  Have modified how package diffs
        are calculated so diffs are first calculated on resources then on
        packages.

        links relateing to diff on complex data structures:

        * https://gist.github.com/samuraisam/901117
        * https://pypi.org/project/deep/

        :return: the diff data structure, empty if no diff is found
        :rtype: []
        """
        diff = None

        cacheFiles = None

        diff = self.getResourceDiff(inputRecord)
        if not diff:
            pkgDiff = None
            # don't even go any further if the records unique id, usually name is in
            # the ignore list
            if not self.isIgnore(inputRecord):
                thisComparable = self.getComparableStruct()
                thisComparable = thisComparable.copy()
                inputComparable = inputRecord.getComparableStruct()
                inputComparable = inputComparable.copy()

                diffIngoreEmptyTypes = Diff.Diff(thisComparable, inputComparable)
                pkgDiff = diffIngoreEmptyTypes.getDiff()
                jsonDiff = json_delta.diff(thisComparable, inputComparable)
                LOGGER.debug(f"package Diff: {jsonDiff}")
                diff = pkgDiff

            if pkgDiff and constants.isDataDebug():
                recordName = self.getUniqueIdentifier()
                LOGGER.debug(f"record name with diff: {recordName}")

                if cacheFiles is None:
                    cacheFiles = CacheFiles.CKANCacheFiles()

                pkgPath1 = cacheFiles.getDebugDataPath(recordName, self.origin, 'PKG')
                with open(pkgPath1, 'w') as fh1:
                    json.dump(thisComparable, fh1, sort_keys=True)

                with open(pkgPath1, 'a') as fh2:
                    pkgStr = json.dumps(inputComparable, sort_keys=True)
                    fh2.write(f'\n{pkgStr}\n')

        return diff

    def getDiff(self, inputRecord):
        # retrieve a comparable structure, and remove embedded data types
        # that have been labelled as ignores
        diff = None
        if inputRecord.dataType == "packages":
            diff = self.getPackageDiff(inputRecord)
        else:
            diff = self.getGenericDiff(inputRecord)
        return diff

    def getGenericDiff(self, inputRecord):
        cacheFiles = None

        diff = None
        if not self.isIgnore(inputRecord):
            thisComparable = self.getComparableStruct()
            inputComparable = inputRecord.getComparableStruct()

            diffIngoreEmptyTypes = Diff.Diff(thisComparable, inputComparable)
            diff = diffIngoreEmptyTypes.getDiff()

            if diff:
                if inputRecord.origin == constants.DATA_SOURCE.DEST:
                    updateStruct = self.getComparableStructUsedForAddUpdate(self.dataCache,
                        constants.UPDATE_TYPES.UPDATE, inputRecord)
                else:
                    updateStruct = inputRecord.getComparableStructUsedForAddUpdate(self.dataCache,
                        constants.UPDATE_TYPES.UPDATE, self)

                if constants.isDataDebug():
                    # TODO: come back and make code option either via arg or
                    #       env var.
                    if cacheFiles is None:
                        cacheFiles = CacheFiles.CKANCacheFiles()

                    name = self.getUniqueIdentifier()
                    dataDumpPath = cacheFiles.getDataTypeFilePath(name, self.dataType)
                    with open(dataDumpPath, 'w') as fh1:
                        json.dump(thisComparable, fh1, sort_keys=True)

                    with open(dataDumpPath, 'a') as fh2:
                        dumpStr = json.dumps(inputComparable, sort_keys=True)
                        fh2.write(f'\n{dumpStr}\n')

                    # now to help with debugging get the struct that will be used
                    # for update and dump it
                    # dataCache, operationType, destRecord

                    dataDumpPath = cacheFiles.getDebugDataPath(name, self.dataType, "UPDT")
                    with open(dataDumpPath, 'w') as fh1:
                        json.dump(updateStruct, fh1, sort_keys=True)

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


class DataCell:
    """an object that can be used to wrap a data value and other meta data
    about it from the perspective of a change
    """

    def __init__(self, struct, dataCache, origin, include=True):
        self.struct = struct
        self.dataCache = dataCache
        self.origin = origin
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
        newStruct = []
        for pos in range(0, len(self.struct)):
            if pos not in positions:
                newStruct.append(self.struct[pos])
        self.struct = newStruct

    def generateNewCell(self, key):
        """The current cell is a dict, generates a new cell for the position
        associated with the input key.

        :param key: a key of struct property
        :type key: str
        """
        newCell = DataCell(self.struct[key], self.dataCache, self.origin)
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
            if (  (newCell.ignoreFld) and
                      key == newCell.ignoreFld):
                # example key is 'name' and the ignore field is name
                # now check to see if the value is in the ignore list
                # also check that not in the datacache ignore list
                # dataType in parentType, value in struct
                if (
                    newCell.struct in newCell.ignoreList
                    or newCell.dataCache.ignores.isIgnored(
                        newCell.parentType, newCell.origin, newCell.struct
                    )
                ):
                    # continue with example.. now the value for the key name
                    # is in the ignore list.  Set the enclosing object... self
                    # to not be included.
                    self.include = False
        return newCell


class CKANUserRecord(CKANRecord):
    def __init__(self, jsonData, origin, dataCache):
        recordType = constants.TRANSFORM_TYPE_USERS
        CKANRecord.__init__(self, jsonData, recordType, origin, dataCache)
        self.duplicateEmail = False

    def isIgnore(self, inputRecord):
        # calls the parent isIgnore method then adds additional logic that
        # will check to see if the record's email is duplicated by other emails.
        isIgnore = super().isIgnore(inputRecord)
        if not isIgnore and self.duplicateEmail:
            isIgnore = True
        return isIgnore


class CKANGroupRecord(CKANRecord):
    def __init__(self, jsonData, origin, dataCache):
        recordType = constants.TRANSFORM_TYPE_GROUPS
        CKANRecord.__init__(self, jsonData, recordType, origin, dataCache)


class CKANOrganizationRecord(CKANRecord):
    def __init__(self, jsonData, origin, dataCache):
        recordType = constants.TRANSFORM_TYPE_ORGS
        CKANRecord.__init__(self, jsonData, recordType, origin, dataCache)


class CKANPackageRecord(CKANRecord):
    def __init__(self, jsonData, origin, dataCache):
        recordType = constants.TRANSFORM_TYPE_PACKAGES
        CKANRecord.__init__(self, jsonData, recordType, origin, dataCache)


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
            # self.adds.extend(addList)
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
                            # LOGGER.info(f"fixing the data type for: {fieldName}")
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

    def remapIdFields(
        self, inputDataStruct, idFields, origin=constants.DATA_SOURCE.DEST
    ):
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
            LOGGER.debug(
                f"currentDataset {currentDataset['name']}:  {jsonDatasetStr[0:150]} ..."
            )
            # LOGGER.debug(f"currentDataset:  {jsonDatasetStr} ")

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
                if not dataCache.isAutoValueInDest(
                    childObjFieldName, childObjType, parentFieldValue
                ):

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
        return self.updates

    def addRequiredDefaultValues(self, inputDataStruct, defaultFields):
        """

        """
        # TODO: this isn't gonna work

        dataRecordWithRequiredFields = CKANRecord(inputDataStruct,
            self.destCKANDataset.dataType, constants.DATA_SOURCE.SRC,
            self.destCKANDataset.dataCache
        )
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
                # currentDataset = self.__populateField(currentDataset, fieldName, fieldValue)
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
                        LOGGER.debug(
                            f"stringify the field: {stringifyField} ... (repeating)"
                        )
                    inputDataStruct[iterVal][stringifyField] = json.dumps(
                        inputDataStruct[iterVal][stringifyField]
                    )
                    cnt += 1
        return inputDataStruct

    def addAutoGenFields(
        self,
        inputDataStruct,
        autoGenFieldList,
        additionalFieldSource=constants.DATA_SOURCE.DEST,
    ):
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

        if isinstance(inputDataStruct, list):
            iterObj = range(0, len(inputDataStruct))
            uniqueIdField = TRANSCONF.getUniqueField(
                recordCalls[additionalFieldSource].dataType
            )
        else:
            iterObj = inputDataStruct

        for iterVal in iterObj:
            if isinstance(inputDataStruct, list):
                uniqueId = inputDataStruct[iterVal][uniqueIdField]
            else:
                uniqueId = iterVal

            record = recordCalls[additionalFieldSource].getRecordByUniqueId(uniqueId)
            for field2Add in autoGenFieldList:
                fieldValue = record.getFieldValue(field2Add)
                inputDataStruct[iterVal][field2Add] = fieldValue
                if field2Add == "owner_org":
                    LOGGER.debug(f"{field2Add}:  {fieldValue}")

        return inputDataStruct

    def __str__(self):
        msg = (
            f"add datasets: {len(self.adds)}, deletes: {len(self.deletes)} "
            + f"updates: {len(self.updates)}"
        )
        return msg


# -------------------- DATASETS --------------------


class CKANRecordCollection:
    """Used to store a bunch of CKAN Records
    """

    def __init__(self, dataType):
        self.recordList = []
        self.dataType = dataType

        # an index to help find records faster. constructed
        # the first time a record is requested
        self.uniqueidRecordLookup = {}
        self.iterCnt = 0

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

        self.userPopulatedFields = TRANSCONF.getUserPopulatedProperties(self.dataType)
        self.iterCnt = 0

        self.srcUniqueIdSet = None
        self.destUniqueIdSet = None

    def populateDataSets(self, destDataSet):
        if self.srcUniqueIdSet is None:
            self.srcUniqueIdSet = set(self.getUniqueIdentifiers())
        if self.destUniqueIdSet is None:
            self.destUniqueIdSet = set(destDataSet.getUniqueIdentifiers())

    def getIgnoreList(self):
        ignoreList = TRANSCONF.getIgnoreList(self.dataType)
        return ignoreList

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
        ignoreList = self.getIgnoreList()
        deleteDataCollection = CKANRecordCollection(self.dataType)

        deleteSet = self.destUniqueIdSet.difference(self.srcUniqueIdSet)
        for deleteUniqueName in deleteSet:
            # Check to see if the user is in the ignore list, only add if it is not
            if deleteUniqueName not in ignoreList:
                # deleteList.append(deleteUniqueName)
                LOGGER.debug(f"delete unique id: {deleteUniqueName}")
                # its a delete so the record needs to come from the destination
                # dataset where it exists, and is to be deleted
                record = destDataSet.getRecordByUniqueId(deleteUniqueName)
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

        ignoreList = self.getIgnoreList()
        addCollection = CKANRecordCollection(self.dataType)

        for addRecordUniqueName in addSet:
            # LOGGER.debug(f"addRecord: {addRecordUniqueName}")
            if addRecordUniqueName not in ignoreList:
                addRecord = self.getRecordByUniqueId(addRecordUniqueName)
                addCollection.addRecord(addRecord)
        return addCollection

    def calcUpdatesCollection(self, destDataSet):

        self.populateDataSets(destDataSet)

        ignoreList = self.getIgnoreList()
        chkForUpdateIds = self.srcUniqueIdSet.intersection(self.destUniqueIdSet)
        chkForUpdateIds = list(chkForUpdateIds)
        chkForUpdateIds.sort()
        LOGGER.info(f"evaluting {len(chkForUpdateIds)} overlapping ids for update")

        updateCollection = CKANRecordCollection(self.dataType)

        for chkForUpdateId in chkForUpdateIds:
            # now make sure the id is not in the ignore list
            if chkForUpdateIds not in ignoreList:
                srcRecordForUpdate = self.getRecordByUniqueId(chkForUpdateId)
                destRecordForUpdate = destDataSet.getRecordByUniqueId(chkForUpdateId)

                # when an update operation is required it uses both the
                # source and the destination objects to form the data
                # that is sent to the api.  The lines below add a reference
                # to the dest record in the source record so that it is available
                # later during the update.
                srcRecordForUpdate.setDestRecord(destRecordForUpdate)

                # if they are different then identify as an update.  The __eq__
                # method for dataset is getting called here.  __eq__ will consider
                # ignore lists.  If record is in ignore list it will return as
                # equal.
                if srcRecordForUpdate != destRecordForUpdate:
                    # updateDataList.append(srcRecordForUpdate)
                    LOGGER.debug(f"adding {chkForUpdateId} to update list")
                    # DEBUG: putting these lines in here so that we can test the
                    #        updates data, as for some reason updates are not
                    #        making the changes that they should or CKAN
                    #        is not accepting them even though it says they are
                    srcRecordForUpdate.getComparableStructUsedForAddUpdate(
                        self.dataCache, constants.UPDATE_TYPES.UPDATE
                    )
                    updateCollection.addRecord(srcRecordForUpdate)
        return updateCollection

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
                recordUniqueId = inputRecord.getUniqueIdentifier()
                compareRecord = self.getRecordByUniqueId(recordUniqueId)
                if inputRecord != compareRecord:
                    LOGGER.debug(f" src and dest for {recordUniqueId} are different")
                    retVal = False
                    break
        else:
            LOGGER.debug(f"unique ids don't align")
            retVal = False
        return retVal


class CKANRecordParserMixin:
    def parseDataIntoRecords(self, jsonData):
        LOGGER.debug("parsing list of dicts into CKANRecord objects")
        includeType = True
        constructor = CKANRecord
        if hasattr(self, "recordConstructor"):
            constructor = self.recordConstructor
            includeType = False
        for recordJson in jsonData:
            if includeType:
                record = constructor(
                    recordJson, self.dataType, self.origin, self.dataCache
                )
            else:
                record = constructor(recordJson, self.origin, self.dataCache)
            self.addRecord(record)


class CKANUsersDataSet(CKANRecordParserMixin, CKANDataSet):
    """Used to represent a collection of CKAN user data.

    :param CKANData: [description]
    :type CKANData: [type]
    """

    def __init__(self, jsonData, dataCache, origin):
        self.recordConstructor = CKANUserRecord
        CKANDataSet.__init__(
            self, jsonData, constants.TRANSFORM_TYPE_USERS, dataCache, origin
        )
        self.duplicateEmails = {}
        self.email2NameLUT = {}
        self.name2emailLUT = {}
        self.emailSet = None

        self.parseDataIntoRecords(jsonData)


    def getDuplicateEmailAddresses(self):
        """Will iterate over this dataset and search for records that have
        duplicate email addresses.

        populates duplicateEmails with a list of the emails that are duplicated
        by more than one record.

        individual CKANRecord objects are updated with a duplicate email flag.
        That flag is checked, and records with this flag get added to the ignore
        list.

        TODO: Should modify how ignores are handled so that they get tagged
               at the record level

        :return: [description]
        :rtype: [type]
        """
        # first scan through all the records looking for duplicate emails.
        # then create a list that omits any records that correspond with duplicate
        # email addresses, and finally replace the self.recordList with the
        # new list that does not include the duplicates
        #
        # find duplicates...

        # email addresses that are ignored ignores!
        cachedIgnores = self.dataCache.ignores
        ignoreEmailList = []
        if not self.duplicateEmails:
            for userRecord in self:
                emailProperty = userRecord.getFieldValue(constants.USER_EMAIL_PROPERTY)
                if (
                    emailProperty in self.duplicateEmails
                    and emailProperty not in ignoreEmailList
                ):
                    self.duplicateEmails[emailProperty] += 1
                else:
                    self.duplicateEmails[emailProperty] = 1

        # rebuild collection without duplicates
        newRecordList = []
        for userRecord in self:
            emailProperty = userRecord.getFieldValue(constants.USER_EMAIL_PROPERTY)
            if (emailProperty in self.duplicateEmails) and self.duplicateEmails[
                emailProperty
            ] >= 2:
                msg = (
                    f"found {self.duplicateEmails[emailProperty]} records with this email "
                    f"address: {emailProperty}, All records with this email "
                    "address will be omitted from the update."
                )
                LOGGER.warning(msg)
                userRecord.duplicateEmail = True
                recordName = userRecord.getUniqueIdentifier()
                cachedIgnores.addIgnore(self.dataType, userRecord.origin, recordName)
                newRecordList.append(recordName)
        return newRecordList

    def getIgnoreList(self):
        ignoreList = TRANSCONF.getIgnoreList(self.dataType)
        recordNamesWithDuplicateEmails = self.getDuplicateEmailAddresses()
        # for users need to get the duplicate email list and add that to the
        # ignore list
        for userId in recordNamesWithDuplicateEmails:
            if userId not in ignoreList:
                ignoreList.append(userId)
        LOGGER.debug(f"users 2 ignore: {ignoreList}")
        return ignoreList

    def calcDeleteCollection(self, destDataSet):
        """Over riding the default method because users need to work differently
        due to the way that openid authentication was implemented.

        This method uses email as the unique id between the source and
        destination objects.

        :param destUniqueIdSet: a set of unique ids found the destination ckan
            instance
        :type destUniqueIdSet: set
        :param srcUniqueIdSet: a set of the unique ids in the source ckan instance
        :type srcUniqueIdSet: set
        """
        self.populateDataSets(destDataSet)
        deleteDataCollection = CKANRecordCollection(self.dataType)

        # need to:
        #   1. create a dict with email as the key, unique id as the value
        #   2. create an email set
        #   3. identify the difference
        #   4. for the difference map emails back to original unique id.

        # create email dict
        self.calcEmailLut()
        destDataSet.calcEmailLut()

        # calculate diff
        emailDiff = destDataSet.emailSet.difference(self.emailSet)

        # now map emails back to uniqueids:
        for email in emailDiff:
            userName = destDataSet.email2NameLUT[email]
            record = destDataSet.getRecordByUniqueId(userName)
            if not record.isIgnore(record):
                LOGGER.debug(f"delete user email: {email} / name: {userName}")
                deleteDataCollection.addRecord(record)
        LOGGER.debug(f"records in delete collection: {len(deleteDataCollection)}")
        return deleteDataCollection

    def calcEmailLut(self):
        if not self.email2NameLUT:
            self.name2emailLUT = {}
            for userRecord in self:
                email = userRecord.getFieldValue(constants.USER_EMAIL_PROPERTY)
                uniId = userRecord.getUniqueIdentifier()
                if email is not None and uniId is not None:
                    self.email2NameLUT[email] = uniId
                    self.name2emailLUT[uniId] = email
                else:
                    LOGGER.warning(f"email or user is None: {email}, {uniId}")
            self.emailSet = set(list(self.email2NameLUT.keys()))

    def calcAddCollection(self, destDataSet):
        """Same issue as the Deletes...

        """
        self.populateDataSets(destDataSet)

        # create email dict
        self.calcEmailLut()
        destDataSet.calcEmailLut()

        # calculate diff
        emailDiff = self.emailSet.difference(destDataSet.emailSet)
        addCollection = CKANRecordCollection(self.dataType)

        # now map emails back to uniqueids:
        for email in emailDiff:
            userName = self.email2NameLUT[email]
            record = self.getRecordByUniqueId(userName)
            if not record.isIgnore(record):
                LOGGER.debug(f"add user email: {email} / name: {userName}")
                addCollection.addRecord(record)
        LOGGER.debug(f"records in add collection: {len(addCollection)}")
        return addCollection

    def calcUpdatesCollection(self, destDataSet):
        # TODO: working on this
        self.populateDataSets(destDataSet)

        # modify so the intersection between the two sets of data is
        # calculated based on email addresses.
        self.calcEmailLut()
        destDataSet.calcEmailLut()

        emailDiff = self.emailSet.intersection(destDataSet.emailSet)
        emails2Check4Update = list(emailDiff)
        LOGGER.debug(f'emails 2 check 4 update: {len(emails2Check4Update)}, {type(emails2Check4Update)}')

        emails2Check4Update.sort()

        updateCollection = CKANRecordCollection(self.dataType)

        for email in emails2Check4Update:
            srcUserName = self.email2NameLUT[email]
            destUserName = destDataSet.email2NameLUT[email]
            srcRecord = self.getRecordByUniqueId(srcUserName)
            destRecord = destDataSet.getRecordByUniqueId(destUserName)
            if not srcRecord.isIgnore(srcRecord):
                srcRecord.setDestRecord(destRecord)
                if srcRecord != destRecord:
                    updateStruct = srcRecord.getComparableStructUsedForAddUpdate(
                        self.dataCache, constants.UPDATE_TYPES.UPDATE
                    )
                    updtJson = json.dumps(updateStruct)
                    LOGGER.debug(f"update struct: {updtJson[0:100]} ...")

                    updateCollection.addRecord(srcRecord)

        return updateCollection


class CKANGroupDataSet(CKANRecordParserMixin, CKANDataSet):
    def __init__(self, jsonData, dataCache, origin):
        CKANDataSet.__init__(
            self, jsonData, constants.TRANSFORM_TYPE_GROUPS, dataCache, origin
        )
        self.recordConstructor = CKANGroupRecord
        self.parseDataIntoRecords(jsonData)


class CKANOrganizationDataSet(CKANRecordParserMixin, CKANDataSet):
    def __init__(self, jsonData, dataCache, origin):
        CKANDataSet.__init__(
            self, jsonData, constants.TRANSFORM_TYPE_ORGS, dataCache, origin
        )
        self.recordConstructor = CKANOrganizationRecord
        self.parseDataIntoRecords(jsonData)


class CKANPackageDataSet(CKANRecordParserMixin, CKANDataSet):
    def __init__(self, jsonData, dataCache, origin):
        CKANDataSet.__init__(
            self, jsonData, constants.TRANSFORM_TYPE_PACKAGES, dataCache, origin
        )
        self.recordConstructor = CKANPackageRecord
        self.parseDataIntoRecords(jsonData)


class DataPopulator:
    def __init__(self, inputData):
        self.inputData = inputData

    def populateField(self, key, valueStruct):
        returnData = self.__populateField(self.inputData, key, valueStruct)
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
                for inputDataPosition in range(
                    0, len(inputData)
                ):  # pylint: disable=consider-using-enumerate
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
            if ((key not in inputData) or inputData[key] is None) or not inputData[key]:
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


class InvalidDataRecordOrigin(ValueError):
    def __init__(self, message):
        LOGGER.error(f"error message: {message}")
        self.message = message
