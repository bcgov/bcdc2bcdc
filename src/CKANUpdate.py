"""
Functionality that can:
    * identify two CKANData.CKANDataSet ( or subclasses of these ) and identify 
      differences between the two
    * Using the CKAN module, update one or the other.
"""

# pylint: disable=logging-format-interpolation

import abc
import logging
import operator

import constants
import CKAN
import CKANTransform

LOGGER = logging.getLogger(__name__)


class CKANUpdate_abc(abc.ABC):
    """
    """
    def __init__(self, ckanWrapper=None):
        self.CKANWrap = ckanWrapper
        if ckanWrapper is None:
            self.CKANWrap = CKAN.CKANWrapper()

    @abc.abstractmethod
    def update(self, deltaObj):
        pass

    @abc.abstractmethod
    def doAdds(self, addStruct):
        pass

    @abc.abstractmethod
    def doDeletes(self, delStruct):
        pass

    @abc.abstractmethod
    def doUpdates(self, updtStruct):
        pass

class UpdateMixin:
    """mixin method that can be re-used by all classes that are inheriting 
    from the abstract method.
    """
    def update(self, deltaObj):
        adds = deltaObj.getAddData()
        dels = deltaObj.getDeleteData()
        updts = deltaObj.getUpdateData()

        dels = self.removeIgnored(dels)
        updts = self.removeIgnored(updts)

        self.doAdds(adds)
        self.doDeletes(dels)
        self.doUpdates(updts)
        LOGGER.info("USER UPDATE COMPLETE")

    def removeIgnored(self, inputData):
        """Receives an input data structure, either list or dict.  If list its the 
        list of unique identifiers that should be excluded from any update.  If 
        inputData is a dict then the keys that for the dict are the unique
        identifiers.

        Method iterates through all the values in the input data determines if 
        they are defined as a record that should be ignored.  If so they are 
        removed from the update list / dict 
        
        :param inputData: The input data needs to be filtered based on the contents
            of the ignore list
        :type inputData: dict/list
        :raises TypeError: raised if the inputData is of a type that is not list
            or dict.
        :return: same structure that was provided as an argument with the values
            that intersect with the ignore list removed.
        :rtype: dict / list
        """
        if isinstance(inputData, list):
            filteredData = []
            for inputVal in inputData:
                if inputVal not in self.ignoreList:
                    filteredData.append(inputVal)
                else:
                    LOGGER.debug(f"found filter value, {inputVal} removing from" + 
                        "update list")
        elif isinstance(inputData, dict):
            filteredData = {}
            for inputVal in inputData:
                LOGGER.debug(f"input value: {inputVal}")
                if inputVal not in self.ignoreList:
                    filteredData[inputVal] = inputData[inputVal]
                else:
                    LOGGER.debug(f"found filter value, {inputVal} removing from" + 
                        "update list")
        else:
            msg = f"received type  {type(inputData)}.  Must be a list or dict"
            raise TypeError(msg)
        return filteredData

class CKANUserUpdate(UpdateMixin, CKANUpdate_abc):

    def __init__(self, ckanWrapper=None):
        CKANUpdate_abc.__init__(self, ckanWrapper)
        self.dataType = constants.TRANSFORM_TYPE_USERS
        self.CKANTranformConfig = CKANTransform.TransformationConfig()
        self.ignoreList = self.CKANTranformConfig.getIgnoreList(self.dataType)

    # def update(self, deltaObj):
    #     adds = deltaObj.getAddData()
    #     self.doAdds(adds)
    #     dels = deltaObj.getDeleteData()
    #     self.doDeletes()
    #     updts = deltaObj.getUpdateData()
    #     self.doUpdates(updts)
    #     LOGGER.info("USER UPDATE COMPLETE")

    def doAdds(self, addStruct):
        LOGGER.info(f"{len(addStruct)} to be added to destination instance")
        sortedList = sorted(addStruct, key=operator.itemgetter('name'))
        for addData in sortedList:
            LOGGER.debug(f"adding dataset: {addData['name']}")
            #self.CKANWrap.addUser(addData)

    def doDeletes(self, delStruct):
        #TODO: Thinking again deletes are likely something we do not want to do 
        #      for some accounts.  Example demo accounts set up for testing.
        LOGGER.info(f"{len(delStruct)} to be deleted to destination instance")
        delStruct.sort()

        for deleteUser in delStruct:
            LOGGER.info(f"removing the user: {deleteUser} from the destination")
            # self.CKANWrap.deleteUser(deleteUser)

    def doUpdates(self, updates):
        updateNames = list(updates.keys())
        updateNames.sort()
        for updt in updateNames:
            LOGGER.info(f"updating the user : {updt}")
            self.CKANWrap.updateUser(updates[updt])
        LOGGER.debug("updates complete")

# for subsequent types will define their own updates, or think about making a 
# mapping between different types and equivalent methods in CKAN.ckanWrapper 
# class 
