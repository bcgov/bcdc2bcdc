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

import logging
import constants
import CKANTransform

LOGGER = logging.getLogger(__name__)

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

    def validateTypeIsComparable(self, ckanDataSet):
        """[summary]
        
        :param ckanDataSet: [description]
        :type ckanDataSet: [type]
        :raises IncompatibleTypesException: [description]
        """
        otherDatatype = type(otherDatatype)
        if hasattr(ckanDataSet, dataType):
            otherDatatype = ckanDataSet.dataType
        if otherDatatype != self.dataType:
            msg = 'You are attempting to compare this object of type ' + \
                f'{self.dataType} with an object of type, {otherDatatype}'
            raise IncompatibleTypesException(msg)
    # This should be moved to CKANRecord
    def getComparableStruct(self, struct, flds2Include=None):
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
        if flds2Include is None:
            flds2Include = self.userPopulatedFields
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
        if isinstance(flds2Include, dict):
            newStruct = {}
            for key in flds2Include:
                LOGGER.debug(f'flds2Include: {flds2Include}')
                LOGGER.debug(f'key: {key}')
                LOGGER.debug(f'struct: {struct}')
                LOGGER.debug(f'struct[key]: {struct[key]}')
                newStruct[key] = self.getComparableStruct(struct[key], flds2Include[key])
            return newStruct
        if isinstance(flds2Include, bool):
            return struct

    def __eq__(self, ckanDataSet):
        # TODO: rework this, should be used to compare a collection
        self.validateTypeIsComparable(ckanDataSet)
        # so the two types are the same, now get the user generated fields from 
        # the transformation object
        transformStruct = self.transConf.__getUserPopulatedProperties(self.dataType)
        LOGGER.debug(f"transStruct: {transformStruct}")
    
class CKANRecord:

    def __init__(self, jsonData, dataType):
        self.jsonData = jsonData
        self.dataType = dataType

    def __eq__(self, ckanRecord):
        """Compares a ckan record with this record.  ckan record must be a dict
        
        :param ckanRecord: [description]
        :type ckanRecord: [type]
        """


class CKANUsersData(CKANData):
    
    def __init__(self, jsonData):
        CKANData.__init__(self, jsonData, constants.TRANSFORM_TYPE_USERS)

    
class IncompatibleTypesException(Exception):
    def __init__(self, message):
        LOGGER.debug(f"error message: {message}")
        self.message = message








