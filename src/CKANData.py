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

        LOGGER.debug(f"struct: {struct}")
        LOGGER.debug(f"flds2Include: {flds2Include}")

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
                LOGGER.debug(f'----key: {key}')
                #LOGGER.debug(f'flds2Include: {flds2Include}')
                #LOGGER.debug(f"flds2Include[key]: {flds2Include[key]}")
                #LOGGER.debug(f'struct: {struct}')
                #LOGGER.debug(f'newStruct: {newStruct}')
                #LOGGER.debug(f'struct[key]: {struct[key]}')
               
                newStruct[key] = self.getComparableStruct(struct[key], flds2Include[key])
            LOGGER.debug(f"newStruct: {newStruct}")
            return newStruct
        elif isinstance(flds2Include, bool):
            LOGGER.debug(f"-----------{struct} is {flds2Include}")
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
        if isinstance(dataCell.struct, dict):
            for objProperty in dataCell.struct:
                newCell = dataCell.generateNewCell(objProperty)
                newCell = self.removeEmbeddedIgnores(newCell)
            return dataCell
        elif isinstance(dataCell.struct, list):
            positions2Remove = []
            for listPos in range(0, len(dataCell.struct)):
                newCell = dataCell.generateNewCell(listPos)
                newCell = self.removeEmbeddedIgnores(newCell)
                
                if not dataCell.include:
                    positions2Remove.append(listPos)
            dataCell.deleteIndexes(positions2Remove)
            

        else:
            # datacell is a primitive, ie: str, int, num, etc, now time for the comparison
            # struct is just a value,
            return dataCell

    def __eq__(self, inputRecord):
        LOGGER.debug("_________ EQ CALLED")
        retVal = True
        thisComparable = self.getComparableStruct()
        dataCell = DataCell(thisComparable)
        dataCellNoIgnores = self.removeEmbeddedIgnores(dataCell)
        inputComparable = inputRecord.getComparableStruct()
        diff = deepdiff.DeepDiff(thisComparable, 
                                 inputComparable, 
                                 ignore_order=True)
        LOGGER.debug(f'diff value: {diff}')
        if diff:
            retVal = False
        return retVal

    def __ne__(self, inputRecord):
        LOGGER.debug(f"__________ NE record CALLED: {type(inputRecord)}, {type(self)}")
        retVal = True
        if self == inputRecord:
            retVal = False
        LOGGER.debug(f"retval from __ne__: {retVal}")
        return retVal

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

    def generateNewCell(self, key, transConf):
        """The current cell is a dict, generates a new cell for the position
        associated with the input key.
        
        :param key: a key of struct property
        :type key: str
        """
        newCell = DataCell(self.struct[key])
        newCell.parent = self
        # copy the attributes from parent to child
        newCell.include = self.include
        newCell.ignoreList = self.ignoreList
        newCell.ignoreFld = self.ignoreFld

        # if the key is an embedded type, users, groups, etc...
        if key in constants.VALID_TRANSFORM_TYPES:
            newCell.ignoreList = transConf.getIgnoreList(key)
            newCell.ignoreFld = transConf.getUniqueField(key)

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

    def setParent(self, oldCell):
        """The cell properties:
          * include
          * ignoreList
          * ignoreFld
          * assess 

        should be passed on to child cells, this method will copy these properties
        from a oldCell to the current cell
        
        :param oldCell: a parent datacell
        :type oldCell: 
        """
        self.parent = oldCell
        self.include = self.parent.include
        self.ignoreList = self.parent.ignoreList
        self.ignoreFld = self.parent.ignoreFld


        


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
                adds is these will get added to dest using an update method
                vs a create method.
    """
    def __init__(self):
        self.adds = []
        self.deletes = []
        self.updates = {}

    def setAddDataset(self, addDataObj):
        if not isinstance(addDataObj, dict):
            msg = "addDataObj parameter needs to be type dict.  You passed " + \
                  f"{type(addDataObj)}"
            raise TypeError(msg)
        self.adds.append(addDataObj)

    def setDeleteDataset(self, deleteName):
        if not isinstance(deleteName, str):
            msg = "deleteName parameter needs to be type str.  You passed " + \
                  f"{type(deleteName)}"
            raise TypeError(msg)
        self.deletes.append(deleteName)

    def setUpdateDataSet(self, updateObj):
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

    def getAddData(self):
        return self.adds

    def getDeleteData(self):
        return self.deletes

    def getUpdateData(self):
        return self.updates

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
        deltaObj = CKANDataSetDeltas()
        dstUniqueIds = set(destDataSet.getUniqueIdentifiers())
        srcUniqueids = set(self.getUniqueIdentifiers())

        # in dest but not in src, ie deletes
        deleteSet = dstUniqueIds.difference(srcUniqueids)
        for deleteUniqueName in deleteSet:
            deltaObj.setDeleteDataset(deleteUniqueName)

        # in source but not in dest, ie adds
        addSet = srcUniqueids.difference(dstUniqueIds)
        for addRecordUniqueName in addSet:
            LOGGER.debug(f"addRecord: {addRecordUniqueName}")
            addDataSet = self.getRecordByUniqueId(addRecordUniqueName)
            addDataStruct = addDataSet.getComparableStruct()
            deltaObj.setAddDataset(addDataStruct)

        # deal with id of updates
        chkForUpdateIds = srcUniqueids.intersection(dstUniqueIds)
        for chkForUpdateId in chkForUpdateIds:
            srcRecordForUpdate = self.getRecordByUniqueId(chkForUpdateId)
            destRecordForUpdate = destDataSet.getRecordByUniqueId(chkForUpdateId)
            if srcRecordForUpdate != destRecordForUpdate:
                deltaObj.setUpdateDataSet(srcRecordForUpdate.jsonData)
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
            LOGGER.debug(f"unique ids dont align")
            retVal = False
        return retVal

    def next(self):
        return self.__next__()

    def __next__(self):
        if self.iterCnt >= len(self.jsonData):
            self.iterCnt = 0
            raise StopIteration
        ckanRecord = None
        if self.recordConstructor == CKANRecord:
            ckanRecord = self.recordConstructor(self.jsonData[self.iterCnt], self.dataType)
        else:
            ckanRecord = self.recordConstructor(self.jsonData[self.iterCnt])
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

# ----------------- EXCEPTIONS 

class UserDefiniedFieldDefinitionError(Exception):
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








