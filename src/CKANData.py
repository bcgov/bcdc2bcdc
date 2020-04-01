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

import logging
import constants
import CKANTransform
import deepdiff
import pprint
import json

LOGGER = logging.getLogger(__name__)

def validateTypeIsComparable(dataObj1, dataObj2):
    """A generic function that can be used to ensure two objects are comparable.

    :param dataObj1: The first data object that is to be used in a comparison
    :type ckanDataSet:
    :raises IncompatibleTypesException: [description]
    """
    dataType1 = type(dataObj1)
    dataType2 = type(dataObj2)

    if hasattr(dataObj1, 'dataType'):
        dataType1 = dataObj1.dataType
    if hasattr(dataObj2, 'dataType'):
        dataType2 = dataObj2.dataType
    if dataType2 != dataType1:
        msg = 'You are attempting to compare two different types of objects ' + \
            f'that are not comparable. dataObj1 is type: {dataType1} and ' + \
            f'dataObj2 is type: with an object of type, {dataType2}'
        raise IncompatibleTypesException(msg)

# ------------- Data Record defs -------------

class CKANRecord:

    def __init__(self, jsonData, dataType):
        self.jsonData = jsonData
        self.dataType = dataType
        self.transConf = CKANTransform.TransformationConfig()
        self.userPopulatedFields = self.transConf.getUserPopulatedProperties(self.dataType)

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
        uniqueFieldName = self.transConf.getUniqueField(self.dataType)
        return self.jsonData[uniqueFieldName]

    def getComparableStruct(self, struct=None, flds2Include=None):
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
        # TODO: ckan obj can have embedded objects.  Example orgs contain users.
        # when looking at users should consider the users: ignore values
        # need to figure out how to implement this.

        if struct is None and flds2Include is None:
            struct = self.jsonData
            flds2Include = self.userPopulatedFields

        #LOGGER.debug(f"struct: {struct}, flds2Include: {flds2Include}")
        newStruct = None

        # only fields defined in this struct should be included in the output
        if isinstance(flds2Include, list):
            # currently assuming that if a list is found there will be a single
            # record in the flds2Include configuration that describe what to
            # do with each element in the list
            newStruct = []
            if isinstance(flds2Include[0], dict):
                for structElem in struct:
                    dataValue = self.getComparableStruct(structElem, flds2Include[0])
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
                #LOGGER.debug(f'----key: {key}')
                #LOGGER.debug(f'flds2Include: {flds2Include}')
                #LOGGER.debug(f"flds2Include[key]: {flds2Include[key]}")
                #LOGGER.debug(f'struct: {struct}')
                #LOGGER.debug(f'newStruct: {newStruct}')
                if key not in struct:
                    # field is defined as being required but is not in the object
                    # that was returned.  Setting it equal to None
                    struct[key] = None
                #LOGGER.debug(f'struct[{key}]: {struct[key]}')

                newStruct[key] = self.getComparableStruct(struct[key], flds2Include[key])
            #LOGGER.debug(f"newStruct: {newStruct}")
            return newStruct
        elif isinstance(flds2Include, bool):
            #LOGGER.debug(f"-----------{struct} is {flds2Include}")
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


        #LOGGER.debug("---------  REMOVE EMBED IGNORES ---------")
        if isinstance(dataCell.struct, dict):
            for objProperty in dataCell.struct:
                #LOGGER.debug(f"objProperty: {objProperty}")
                newCell = dataCell.generateNewCell(objProperty, self.transConf)
                newCell = self.removeEmbeddedIgnores(newCell)
                dataCell.copyChanges(newCell)
        elif isinstance(dataCell.struct, list):
            positions2Remove = []
            for listPos in range(0, len(dataCell.struct)):
                #LOGGER.debug(f"listPos: {listPos} - {dataCell.struct[listPos]}")
                newCell = dataCell.generateNewCell(listPos, self.transConf)
                newCell = self.removeEmbeddedIgnores(newCell)
                if not newCell.include:
                    positions2Remove.append(listPos)
                    #LOGGER.debug("adding value: {listPos} to remove")
                #LOGGER.debug(f"include value: {dataCell.include}")
            LOGGER.debug(f"removing positions: {positions2Remove}")
            dataCell.deleteIndexes(positions2Remove)
        #LOGGER.debug(f'returning... {dataCell.struct}, {dataCell.include}')
        #LOGGER.debug(f"ignore struct: {self.transConf.transConf['users']['ignore_list']}")
        return dataCell

    def __eq__(self, inputRecord):
        LOGGER.debug("_________ EQ CALLED")
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
        ignoreField = self.transConf.getUniqueField(self.dataType)
        ignoreList = self.transConf.getIgnoreList(self.dataType)
        if ignoreField in inputRecord.jsonData:
            if inputRecord.jsonData[ignoreField] in ignoreList:
                retVal = True
        return retVal

    def getDiff(self, inputRecord):
        # retrieve a comparable structure, and remove embedded data types
        # that have been labelled as ignores

        # TODO: before do anything check to see if this record is an
        diff = None
        # don't even go any further if the records unique id, usually name is in
        # the ignore list
        if not self.isIgnore(inputRecord):
            thisComparable = self.getComparableStruct()
            dataCell = DataCell(thisComparable)
            dataCellNoIgnores = self.removeEmbeddedIgnores(dataCell)
            thisComparable = dataCellNoIgnores.struct

            # do the same thing for the input data structure
            inputComparable = inputRecord.getComparableStruct()
            dataCell = DataCell(inputComparable)
            dataCellNoIgnores = self.removeEmbeddedIgnores(dataCell)
            inputComparable = dataCell.struct

            diff = deepdiff.DeepDiff(thisComparable,
                                    inputComparable,
                                    ignore_order=True)

            if diff:
                pp = pprint.PrettyPrinter(indent=4)
                pp.pformat(inputComparable)
                LOGGER.debug("inputComparable: %s", pp.pformat(inputComparable))
                LOGGER.debug('thisComparable: %s', pp.pformat(thisComparable))
                LOGGER.debug(f"diffs are: {diff}")
        return diff

    def __ne__(self, inputRecord):
        LOGGER.debug(f"__________ NE record CALLED: {type(inputRecord)}, {type(self)}")
        retVal = True
        if self == inputRecord:
            retVal = False
        LOGGER.debug(f"retval from __ne__: {retVal}")
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
        LOGGER.debug(f"remove positions: {positions}")
        newStruct = []
        for pos in range(0, len(self.struct)):
            if pos not in positions:
                newStruct.append(self.struct[pos])
            else:
                LOGGER.debug(f"removing: {pos} {self.struct[pos]}")
        LOGGER.debug(f"old struct: {self.struct}")
        LOGGER.debug(f"new struct: {newStruct}")
        self.struct = newStruct
        # transfer changes to the parent

    def generateNewCell(self, key, transConf):
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
            newCell.ignoreList = transConf.getIgnoreList(key)
            newCell.ignoreFld = transConf.getUniqueField(key)
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

    def __init__(self, jsonData):
        recordType = constants.TRANSFORM_TYPE_USERS
        CKANRecord.__init__(self, jsonData, recordType)

class CKANGroupRecord(CKANRecord):
    def __init__(self, jsonData):
        recordType = constants.TRANSFORM_TYPE_GROUPS
        CKANRecord.__init__(self, jsonData, recordType)

class CKANOrganizationRecord(CKANRecord):
    def __init__(self, jsonData):
        recordType = constants.TRANSFORM_TYPE_ORGS
        CKANRecord.__init__(self, jsonData, recordType)

class CKANPackageRecord(CKANRecord):

    def __init__(self, jsonData):
        recordType = constants.TRANSFORM_TYPE_PACKAGES
        CKANRecord.__init__(self, jsonData, recordType)


# -------------------- DATASET DELTA ------------------

class CKANDataSetDeltas:
    """Class used to represent differences between two objects of the same
    type.  Includes all the information necessary to proceed with the update.

    :ivar adds: A list of dicts containing the user defined properties that need
                to be populated to create an equivalent version of the src data
                in dest.
    :ivar deletes: A list of the names or ids on the dest side of objects that
                should be deleted.
    :ivar updates: Same structure as 'adds'. Only difference between these and
        adds is these will get added to dest using an update method vs a create
        method.
    :ivar srcCKANDataset: CKANDataset object, maintain a reference to this
        object so that can request CKAN records in the dataset with only
        user generated fields included.
    """
    def __init__(self, srcCKANDataset, destCKANDataset):
        self.adds = []
        self.deletes = []
        self.updates = {}
        self.srcCKANDataset = srcCKANDataset
        self.destCKANDataset = destCKANDataset

        self.transConf = self.srcCKANDataset.transConf

    def setAddDataset(self, addDataObj):
        """Adds a object to the list of objects that are identified as adds

        Adds are objects that exist in the source but not the destination

        :param addDataObj: data that is to be added
        :type addDataObj: dict
        :raises TypeError: raised if the input data is not type dict
        """
        if not isinstance(addDataObj, dict):
            msg = "addDataObj parameter needs to be type dict.  You passed " + \
                  f"{type(addDataObj)}"
            raise TypeError(msg)
        self.adds.append(addDataObj)

    def setAddDatasets(self, addList, replace=True):
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
            LOGGER.info(f"populate add list with {len(addList)} items")
            self.adds = addList
        else:
            LOGGER.info(f"adding {len(addList)} items to the add list")
            self.adds.extend(addList)

    def setDeleteDataset(self, deleteName):
        """Adds an object to the list of data that has been identified as a
        Delete.

        Deletes are records that exist in the destination but not the source.

        :param deleteName: [description]
        :type deleteName: [type]
        :raises TypeError: [description]
        """
        if not isinstance(deleteName, str):
            msg = "deleteName parameter needs to be type str.  You passed " + \
                  f"{type(deleteName)}"
            raise TypeError(msg)
        self.deletes.append(deleteName)

    def setDeleteDatasets(self, deleteList, replace=True):
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
            LOGGER.info(f"populate delete list with {len(deleteList)} items")
            self.deletes = deleteList
        else:
            LOGGER.info(f"adding {len(deleteList)} items to the delete list")
            self.deletes.extend(deleteList)

    def setUpdateDatasets(self, updateList):
        """Gets a list of data that should be used to update objects in the ckan
        destination instance and adds the data to this object

        :param updateList: list of data to be used to update the object
        :type updateList: list
        """
        LOGGER.info(f"adding {len(updateList)} records to update")
        for updateData in updateList:
            self.setUpdateDataSet(updateData)

    def setUpdateDataSet(self, updateObj):
        """Adds a new dataset that is to be updated.  When comparison of two
        objects identifies that there is a difference, the object that passed to
        this method is the src object with the data that should be copied to dest.

        Updates are datasets that exist in source and destination however not all
        the data is the same between them.

        :param updateObj: the data that is to be updated
        :type updateObj: dict
        :raises TypeError: object must be type 'dict', raise if it is not.
        :raises ValueError: object must have a 'name' property
        """
        if not isinstance(updateObj, dict):
            msg = "updateObj parameter needs to be type dict.  You passed " + \
                  f"{type(updateObj)}"
            raise TypeError(msg)
        if 'name' not in updateObj:
            msg = 'Update object MUST contain a property \'name\'.  Object ' + \
                  f'provided: {updateObj}'
            raise ValueError(msg)
        LOGGER.debug(f"adding update for {updateObj['name']}")
        self.updates[updateObj['name']] = updateObj

    def filterNonUserGeneratedFields(self, ckanDataSet):
        """
        Receives either a dict or list:
           * dict: key is the unique id for the dataset
           * list: a list of dicts describing a list of data
                   objects.

        Iterates over all the data in the ckanDataSet struct, removing non
        user generated fields and returns a json struct (dict) with only
        fields that are user defined

        :param ckanDataSet: a ckan data set
        :type ckanDataSet: CKANDataSet or an object that subclasses it
        """
        # get the unique id for this dataset type
        uniqueIdentifier = self.srcCKANDataset.transConf.getUniqueField(
            self.srcCKANDataset.dataType)

        # if generating a dataset to be used to update a dataset, then check to
        # see if there are machine generated fields that should be included in the
        # update
        LOGGER.debug(f"uniqueIdentifier: {uniqueIdentifier}")

        if isinstance(ckanDataSet, dict):
            filteredData = {}
            uniqueIds = ckanDataSet.keys()
        elif isinstance(ckanDataSet, list):
            filteredData = []
            # below is wrong as it returns all unique ids, we only want the
            # unique ids provided in the struct ckanDataSet
            #uniqueIds = self.srcCKANDataset.getUniqueIdentifiers()
            uniqueIds = []
            for record in ckanDataSet:
                uniqueIds.append(record[uniqueIdentifier])
        else:
            msg = f'type received is {type(ckanDataSet)}, expecting list or dict'
            raise IncompatibleTypesException(msg)

        for uniqueId in uniqueIds:
            #LOGGER.debug(f"uniqueId: {uniqueId}")
            ckanRec = self.srcCKANDataset.getRecordByUniqueId(uniqueId)
            compStruct = ckanRec.getComparableStruct()
            if isinstance(ckanDataSet, dict):
                filteredData[uniqueId] = compStruct
            elif isinstance(ckanDataSet, list):
                filteredData.append(compStruct)
        return filteredData

    def getAddData(self):
        LOGGER.debug(f'add data: {type(self.adds)} {len(self.adds)}')
        adds = self.filterNonUserGeneratedFields(self.adds)
        return adds

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
        # should return only fields that are user generated

        updates = self.filterNonUserGeneratedFields(self.updates)
        #LOGGER.debug(f'updates: {updates}')
        updateFields = self.transConf.getFieldsToIncludeOnUpdate(self.destCKANDataset.dataType)
        if updateFields:
            # need to add these onto each record from the destination
            # instances data
            updates = self.addUpdateAutoGenFields(updates, updateFields)
        return updates

    def addUpdateAutoGenFields(self, dataDict, autoGenFieldList):
        """dataDict contains the data that is to be used for the update that
        originates from the source ckan instance.  autoGenFieldList is a list of
        field names that should be added to the struct from the destination
        ckan instance

        :param dataDict: The update data struct which is a dictionary where the
            keys are the unique identifier, in most cases the keys are the name
            property.  The values in this struct are the values from the source
            ckan instance.
        :type dataDict: dict
        :param autoGenFieldList: a list of field names that should be added to
            the struct from the destination ckan instance
        :type autoGenFieldList: list
        :return: The values in the dataDict with the destination instance fields
            defined in autoGenFieldList appended to the dictionary
        :rtype: dict
        """
        for uniqueId in dataDict:
            record = self.destCKANDataset.getRecordByUniqueId(uniqueId)
            for field2Add in autoGenFieldList:
                fieldValue = record.getFieldValue(field2Add)
                dataDict[uniqueId][field2Add] = fieldValue
                LOGGER.debug(f"adding: {field2Add}:{fieldValue} to {uniqueId}")
        return dataDict


    def __str__(self):
        # addNames = []
        # for add in self.adds:
        #     addNames.append(add['name'])
        # updateNames = self.updates.keys()
        msg = f"add datasets: {len(self.adds)}, deletes: {len(self.deletes)} " + \
            f"updates: {len(self.updates)}"
        return msg
# -------------------- DATASETS --------------------


class CKANDataSet:
    """This class wraps a collection of datasets.  Includes an iterator that
    will return a CKANRecord object.

    :raises IncompatibleTypesException: This method is raised when comparing two
        incompatible types.
    """

    def __init__(self, jsonData, dataType):
        self.jsonData = jsonData
        self.dataType = dataType
        self.transConf = CKANTransform.TransformationConfig()
        self.userPopulatedFields = self.transConf.getUserPopulatedProperties(self.dataType)
        self.iterCnt = 0
        self.recordConstructor = CKANRecord

        # an index to help find records faster. constructed
        # the first time a record is requested
        self.uniqueidRecordLookup = {}

    def reset(self):
        """reset the iterator
        """
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
        return uniqueIds

    def getRecordByUniqueId(self, uniqueValueToRetrieve):
        """Gets the record that aligns with this unique id.
        """
        retVal = None
        if not self.uniqueidRecordLookup:
            self.reset()
            for record in self:
                recordID = record.getUniqueIdentifier()
                self.uniqueidRecordLookup[recordID] = record
                if  uniqueValueToRetrieve == recordID:
                    retVal = record
        else:
            if uniqueValueToRetrieve in self.uniqueidRecordLookup:
                retVal = self.uniqueidRecordLookup[uniqueValueToRetrieve]
        return retVal

    def getDeleteList(self, destUniqueIdSet, srcUniqueIdSet):
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
        ignoreList = self.transConf.getIgnoreList(self.dataType)

        deleteSet = destUniqueIdSet.difference(srcUniqueIdSet)
        deleteList = []
        for deleteUniqueName in deleteSet:
            #Check to see if the user is in the ignore list, only add if it is not
            if deleteUniqueName not in ignoreList:
                deleteList.append(deleteUniqueName)
        return deleteList

    def getAddList(self, destUniqueIdSet, srcUniqueIdSet):
        """Gets a two sets of unique ids, one for the data on the source ckan
        instance and another for the destination ckan instance.  Using this
        information returns a list of unique ids that should be added to the
        destination instance

        :param destUniqueIdSet: a set of unique ids from the destination ckan
            instance.
        :type destUniqueIdSet: set
        :param srcUniqueIdSet: a set of unique ids from the source ckan instance
        :type srcUniqueIdSet: set
        :return: a list of unique ids that should be added to the destination
            ckan instance.  Will exclude any unique ids identified in the
            transformation configuration ignore list.
        :rtype: list
        """
        # in source but not in dest, ie adds
        addSet = srcUniqueIdSet.difference(destUniqueIdSet)

        ignoreList = self.transConf.getIgnoreList(self.dataType)

        addList = []

        for addRecordUniqueName in addSet:
            #LOGGER.debug(f"addRecord: {addRecordUniqueName}")
            if addRecordUniqueName not in ignoreList:
                addDataSet = self.getRecordByUniqueId(addRecordUniqueName)
                addDataStruct = addDataSet.getComparableStruct()
                addList.append(addDataStruct)
        return addList

    def getUpdatesList(self, destUniqueIdSet, srcUniqueIdSet, destDataSet):

        ignoreList = self.transConf.getIgnoreList(self.dataType)

        chkForUpdateIds = srcUniqueIdSet.intersection(destUniqueIdSet)

        updateDataList = []

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
                    updateDataList.append(srcRecordForUpdate.jsonData)
        return updateDataList

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
        dstUniqueIds = set(destDataSet.getUniqueIdentifiers())
        srcUniqueids = set(self.getUniqueIdentifiers())

        deleteList = self.getDeleteList(dstUniqueIds, srcUniqueids)
        deltaObj.setDeleteDatasets(deleteList)

        addList = self.getAddList(dstUniqueIds, srcUniqueids)
        deltaObj.setAddDatasets(addList)

        updateList = self.getUpdatesList(dstUniqueIds, srcUniqueids, destDataSet)
        deltaObj.setUpdateDatasets(updateList)

        return deltaObj

    def __eq__(self, ckanDataSet):
        """ Identifies if the input dataset is the same as this dataset

        :param ckanDataSet: The input CKANDataset
        :type ckanDataSet: either CKANDataSet, or a subclass of it
        """
        LOGGER.debug("DATASET EQ")
        retVal = True
        # TODO: rework this, should be used to compare a collection
        validateTypeIsComparable(self, ckanDataSet)

        # get the unique identifiers and verify that input has all the
        # unique identifiers as this object
        inputUniqueIds = ckanDataSet.getUniqueIdentifiers()
        thisUniqueIds = self.getUniqueIdentifiers()

        LOGGER.debug(f"inputUniqueIds: {inputUniqueIds}")
        LOGGER.debug(f"thisUniqueIds: {thisUniqueIds}")

        LOGGER.debug(f"this unique ids count: {len(thisUniqueIds)}")
        LOGGER.debug(f"input data sets unique id count: {len(inputUniqueIds)}")

        if set(inputUniqueIds) == set(thisUniqueIds):
            # has all the unique ids, now need to look at the differences
            # in the data
            LOGGER.debug(f"iterate ckanDataSet: {ckanDataSet}")
            LOGGER.debug(f"ckanDataSet record count: {len(ckanDataSet)}")
            for inputRecord in ckanDataSet:
                LOGGER.debug(f"iterating: {inputRecord}")
                recordUniqueId = inputRecord.getUniqueIdentifier()
                compareRecord = self.getRecordByUniqueId(recordUniqueId)
                LOGGER.debug(f"type 1 and 2... {type(inputRecord)} {type(compareRecord)}")
                if inputRecord != compareRecord:
                    LOGGER.debug(f"---------{recordUniqueId} doesn't have equal")
                    retVal = False
                    break
        else:
            LOGGER.debug(f"unique ids don't align")
            retVal = False
        return retVal

    def next(self):
        return self.__next__()

    def __next__(self):
        if self.iterCnt >= len(self.jsonData):
            self.iterCnt = 0
            raise StopIteration
        ckanRecord = None
        # if the record constructor is a CKANRecord then use the two parameter
        # constructor, otherwise the type is already defined in subclass of the
        # CKANRecord
        if self.recordConstructor == CKANRecord:
            ckanRecord = self.recordConstructor(self.jsonData[self.iterCnt], self.dataType)
        else:
            ckanRecord = self.recordConstructor(self.jsonData[self.iterCnt]) # pylint: disable=no-value-for-parameter
        self.iterCnt += 1
        return ckanRecord

    def __iter__(self):
        return self

    def __len__(self):
        return len(self.jsonData)

class CKANUsersDataSet(CKANDataSet):
    """Used to represent a collection of CKAN user data.

    :param CKANData: [description]
    :type CKANData: [type]
    """

    def __init__(self, jsonData):
        CKANDataSet.__init__(self, jsonData, constants.TRANSFORM_TYPE_USERS)
        self.recordConstructor = CKANUserRecord

class CKANGroupDataSet(CKANDataSet):
    def __init__(self, jsonData):
        CKANDataSet.__init__(self, jsonData, constants.TRANSFORM_TYPE_GROUPS)
        self.recordConstructor = CKANGroupRecord

class CKANOrganizationDataSet(CKANDataSet):
    def __init__(self, jsonData):
        CKANDataSet.__init__(self, jsonData, constants.TRANSFORM_TYPE_ORGS)
        self.recordConstructor = CKANGroupRecord

class CKANPackageDataSet(CKANDataSet):
    def __init__(self, jsonData):
        CKANDataSet.__init__(self, jsonData, constants.TRANSFORM_TYPE_PACKAGES)
        self.recordConstructor = CKANPackageRecord

# ----------------- EXCEPTIONS

class UserDefinedFieldDefinitionError(Exception):
    """Raised when the transformation configuration encounters an unexpected
    value or type

    """
    def __init__(self, message):
        LOGGER.debug(f"error message: {message}")
        self.message = message

class IncompatibleTypesException(Exception):
    def __init__(self, message):
        LOGGER.debug(f"error message: {message}")
        self.message = message
