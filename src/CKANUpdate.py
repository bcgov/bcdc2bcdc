"""
Functionality that can:
    * identify two CKANData.CKANDataSet ( or subclasses of these ) and identify 
      differences between the two
    * Using the CKAN module, update one or the other.
"""

# pylint: disable=logging-format-interpolation, logging-not-lazy

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
    """implements the abstract base class CKANUpdate_abc to allow user data to 
    be updated easily
    
    :param UpdateMixin:  This mixin allows the the base class update 
        method to be glued to the implementing classes update methods.
    :type UpdateMixin: class
    :param CKANUpdate_abc: defined the interface that this class needs to 
        implement
    :type CKANUpdate_abc: abstract base class
    """
    
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
        """List of user data to be added to a ckan instance
        
        :param addStruct: list of user struct
        :type addStruct: list
        """
        LOGGER.info(f"{len(addStruct)} to be added to destination instance")
        sortedList = sorted(addStruct, key=operator.itemgetter('name'))
        for addData in sortedList:
            LOGGER.debug(f"adding dataset: {addData['name']}")
            #self.CKANWrap.addUser(addData)

    def doDeletes(self, delStruct):
        """list of usernames or ids to delete
        
        :param delStruct: list of user names or ids to delete
        :type delStruct: list
        """
        #TODO: Thinking again deletes are likely something we do not want to do 
        #      for some accounts.  Example demo accounts set up for testing.
        LOGGER.info(f"{len(delStruct)} to be deleted to destination instance")
        delStruct.sort()

        for deleteUser in delStruct:
            LOGGER.info(f"removing the user: {deleteUser} from the destination")
            # self.CKANWrap.deleteUser(deleteUser)

    def doUpdates(self, updtStruct):
        """Gets a list of user data that is used to updated a CKAN instance
        with.
        
        :param updtStruct: list of user data to be used to update users
        :type updtStruct: list
        """
        updateNames = list(updtStruct.keys())
        updateNames.sort()
        for updt in updateNames:
            LOGGER.info(f"updating the user : {updt}")
            self.CKANWrap.updateUser(updtStruct[updt])
        LOGGER.debug("updates complete")

# for subsequent types will define their own updates, or think about making a 
# mapping between different types and equivalent methods in CKAN.ckanWrapper 
# class 


class CKANGroupUpdate(UpdateMixin, CKANUpdate_abc):

    def __init__(self, ckanWrapper=None):
        """Gets a list of updates
        
        :param ckanWrapper: [description], defaults to None
        :type ckanWrapper: [type], optional
        """
        CKANUpdate_abc.__init__(self, ckanWrapper)
        self.dataType = constants.TRANSFORM_TYPE_GROUPS
        self.CKANTranformConfig = CKANTransform.TransformationConfig()
        self.ignoreList = self.CKANTranformConfig.getIgnoreList(self.dataType)

    def doAdds(self, addStruct):
        """gets a list of group structs that describe groups that are to be added
        to a CKAN instance
        
        :param addStruct: list of group data structs
        :type addStruct: list
        """
        LOGGER.info(f"{len(addStruct)} to be added to destination instance")
        sortedList = sorted(addStruct, key=operator.itemgetter('name'))
        for addData in sortedList:
            LOGGER.debug(f"adding group: {addData['name']}")
            # todo, this is working but am commenting out
            #self.CKANWrap.addGroup(addData)

    def doDeletes(self, delStruct):
        """Gets a list of group names or ids that are to be deleted
        
        :param delStruct: list of group names or ids that are to be deleted
        :type delStruct: list
        """
        LOGGER.error("still need to implement this {delStruct}")
        LOGGER.info(f"number of groups: {len(delStruct)} to be deleted to " + \
                    "destination instance")
        delStruct.sort()

        for deleteGroup in delStruct:
            LOGGER.info(f"removing the user: {deleteGroup} from the destination")
            # self.CKANWrap.deleteGroup(deleteGroup)


    def doUpdates(self, updtStruct):
        """Gets a list of group data that needs to be updated
        
        :param updates: list of dictionaries with the data to be used to update a group
        :type updates: list of dict
        """
        LOGGER.error("still need to implement this {updtStruct}")
        updateNames = list(updtStruct.keys())
        updateNames.sort()
        for updt in updateNames:
            LOGGER.info(f"updating the group : {updt}")
            #self.CKANWrap.updateGroup(updates[updt])
        LOGGER.debug("updates complete")

class CKANOrganizationUpdate(UpdateMixin, CKANUpdate_abc):

    def __init__(self, ckanWrapper=None):
        CKANUpdate_abc.__init__(self, ckanWrapper)
        self.dataType = constants.TRANSFORM_TYPE_ORGS
        self.CKANTransformConfig = CKANTransform.TransformationConfig()
        self.ignoreList = self.CKANTransformConfig.getIgnoreList(self.dataType)

    def doAdds(self, addStruct):
        LOGGER.debug(f"adds: {addStruct}")
        LOGGER.info(f"{len(addStruct)}: number of orgs to be added to destination instance")
        sortedList = sorted(addStruct, key=operator.itemgetter('name'))
        for addData in sortedList:
            LOGGER.debug(f"adding organization: {addData['name']}")
            # todo, this is working but am commenting out
            #self.CKANWrap.addOrganization(addData)



    def doDeletes(self, delStruct):
        LOGGER.debug(f"deletes: {delStruct}")

    def doUpdates(self, updtStruct):
        LOGGER.debug(f"deletes: {updtStruct}")
