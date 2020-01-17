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
                LOGGER.debug(f'----key: {key}')

                LOGGER.debug(f'flds2Include: {flds2Include}')
                LOGGER.debug(f"flds2Include[key]: {flds2Include[key]}")
                LOGGER.debug(f'struct: {struct}')
                #LOGGER.debug(f'newStruct: {newStruct}')
                LOGGER.debug(f'struct[key]: {struct[key]}')
                newStruct[key] = self.getComparableStruct(struct[key], flds2Include[key])
                LOGGER.debug(f"newStruct: {newStruct}")

            LOGGER.debug(f"newStruct: {newStruct}")
            return newStruct
        elif isinstance(flds2Include, bool):
            LOGGER.debug(f"-----------{struct} is {flds2Include}")
            return struct
        return newStruct

    def __eq__(self, inputRecord):
        LOGGER.debug("_________ EQ CALLED")
        retVal = True
        thisComparable = self.getComparableStruct()
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

# -------------------- DATASETS --------------------

class CKANUserRecord(CKANRecord):

    def __init__(self, jsonData):
        CKANRecord.__init__(self, jsonData, constants.TRANSFORM_TYPE_USERS)

    # def __eq__(self, ckanRecord):
    #     """Compares a ckan record with this record.  ckan record must be a dict
        
    #     :param ckanRecord: [description]
    #     :type ckanRecord: [type]
    #     """
    #     validateTypeIsComparable(self, ckanRecord)
    #     comparableData_self = self.getComparableStruct()
    #     comparableData_other = ckanRecord.getComparableStruct()
    #     # now use deepdiff to compare those two structs
    #     LOGGER.debug(f"comparableData_self: {comparableData_self}")
    #     LOGGER.debug(f"comparableData_other: {comparableData_other}")


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
        for record in self:
            if  uniqueValueToRetrieve == record.getUniqueIdentifier():
                retVal = record
                break
        return retVal

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
        return __next__()

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








