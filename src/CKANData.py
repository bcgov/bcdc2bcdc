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

class CKANData:

    def __init__(self, jsonData, dataType):
        self.jsonData = jsonData
        self.dataType = dataType
        self.transConf = CKANTransform.TransformationConfig()
        self.userPopulatedFields = self.transConf.getUserPopulatedProperties(self.dataType)

    def validateTypeIsComparable(self, ckanDataSet):
        otherDatatype = type(otherDatatype)
        if hasattr(ckanDataSet, dataType):
            otherDatatype = ckanDataSet.dataType
        if otherDatatype != self.dataType:
            msg = 'You are attempting to compare this object of type ' + \
                f'{self.dataType} with an object of type, {otherDatatype}'
            raise IncompatibleTypesException(msg)

    def getComparableStruct(self, struct, flds2Include=None):
        """Returns a new data structure with fields that should not be included
        in the comparison removed.
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
        self.validateTypeIsComparable(ckanDataSet)
        # so the two types are the same, now get the user generated fields from 
        # the transformation object
        transformStruct = self.transConf.__getUserPopulatedProperties(self.dataType)
        LOGGER.debug(f"transStruct: {transformStruct}")
    
class CKANUsersData(CKANData):
    
    def __init__(self, jsonData):
        CKANData.__init__(self, jsonData, constants.TRANSFORM_TYPE_USERS)

    
class IncompatibleTypesException(Exception):
    def __init__(self, message):
        LOGGER.debug(f"error message: {message}")
        self.message = message








