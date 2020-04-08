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
import os

LOGGER = logging.getLogger(__name__)

class CKANUpdate_abc(abc.ABC):
    """
    abstract base class used to define the interface for CKANUpdate objects.

    Each different object type in ckan should implement its own version of this
    class.
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
        LOGGER.info("UPDATE COMPLETE")

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
                #LOGGER.debug(f"input value: {inputVal}")
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
        """List of user data to be added to a ckan instance,

        :param addStruct: list of user struct
        :type addStruct: list
        """
        LOGGER.info(f"{len(addStruct)} to be added to destination instance")
        sortedList = sorted(addStruct, key=operator.itemgetter('name'))
        for addData in sortedList:
            LOGGER.debug(f"adding dataset: {addData['name']}")
            LOGGER.debug(f"dataset data: {addData}")

            # user add specific.. generate and populate a password field
            addData['password'] = os.environ[constants.CKAN_ONETIME_PASSWORD]
            self.CKANWrap.addUser(addData)

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
            self.CKANWrap.deleteUser(deleteUser)

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
            # updtStruct is comming from a delta obj
            # delta obj is used to keep track of:
            #   - adds
            #   - deletes
            #   - updates
            if updtStruct[updt]['email'] is not None:

                self.CKANWrap.updateUser(updtStruct[updt])
            else:
                LOGGER.info(f"skipping this record as email is null: {updtStruct}")

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
            self.CKANWrap.addGroup(addData)

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
            LOGGER.info(f"removing the group: {deleteGroup} from the destination")
            self.CKANWrap.deleteGroup(deleteGroup)

    def doUpdates(self, updtStruct):
        """Gets a list of group data that needs to be updated

        :param updates: list of dictionaries with the data to be used to update a group
        :type updates: list of dict
        """
        #LOGGER.error(f"still need to implement this {updtStruct}")
        updateNames = list(updtStruct.keys())
        updateNames.sort()
        for updt in updateNames:
            LOGGER.info(f"updating the group : {updtStruct[updt]}")
            self.CKANWrap.updateGroup(updtStruct[updt])
        LOGGER.debug("updates complete")

class CKANOrganizationUpdate(UpdateMixin, CKANUpdate_abc):
    '''
    implements the interface defined by CKANUpdate_abc, the actual update
    method comes from the mixin.

    Used to provide a uniform interface that is used by the script to update
    the orgs from one ckan instance to another.
    '''

    def __init__(self, ckanWrapper=None):
        CKANUpdate_abc.__init__(self, ckanWrapper)
        self.dataType = constants.TRANSFORM_TYPE_ORGS
        self.CKANTransformConfig = CKANTransform.TransformationConfig()
        self.ignoreList = self.CKANTransformConfig.getIgnoreList(self.dataType)

    def doAdds(self, addStruct):
        """adds the orgs described in the param addStruct

        :param addStruct: dictionary where the key is the name of the org to be added
            and the value is the struct that can be passed directly to the ckan api
            to add this org
        :type addStruct: dict
        """
        #LOGGER.debug(f"adds: {addStruct}")
        LOGGER.info(f"{len(addStruct)}: number of orgs to be added to destination instance")
        sortedList = sorted(addStruct, key=operator.itemgetter('name'))
        for addData in sortedList:
            LOGGER.debug(f"adding organization: {addData['name']}")
            # todo, this is working but am commenting out
            self.CKANWrap.addOrganization(addData)

    def doDeletes(self, delStruct):
        """does deletes of all the orgs described in the delStruct

        :param delStruct: a list of org names that should be deleted
        :type delStruct: str
        """
        LOGGER.debug(f"number of deletes: {len(delStruct)}")
        for org2Del in delStruct:
            LOGGER.debug(f"    deleting the org: {org2Del}")
            self.CKANWrap.deleteOrganization(org2Del)

    def doUpdates(self, updtStruct):
        """Does the org updates

        :param updtStruct: dictionary where the key is the name of the org and the
            value is a dict that can be passed to the CKAN api to update the org
        :type updtStruct: dict
        """
        LOGGER.debug(f"number of updates: {len(updtStruct)}")
        for updateName in updtStruct:
            LOGGER.debug(f"updating the org: {updateName}")
            self.CKANWrap.updateOrganization(updtStruct[updateName])

class CKANPackagesUpdate(UpdateMixin, CKANUpdate_abc):
    '''
    implements the interface defined by CKANUpdate_abc, the actual update
    method comes from the mixin.

    Used to provide a uniform interface that is used by the script to update
    the packages from one ckan instance to another.
    '''

    def __init__(self, ckanWrapper=None):
        CKANUpdate_abc.__init__(self, ckanWrapper)
        self.dataType = constants.TRANSFORM_TYPE_PACKAGES
        self.CKANTransformConfig = CKANTransform.TransformationConfig()
        self.ignoreList = self.CKANTransformConfig.getIgnoreList(self.dataType)

    def doAdds(self, addStruct):
        """adds the packages described in the param addStruct

        :param addStruct: dictionary where the key is the name of the org to be added
            and the value is the struct that can be passed directly to the ckan api
            to add this org
        :type addStruct: dict
        """
        #LOGGER.debug(f"adds: {addStruct}")
        LOGGER.info(f"{len(addStruct)}: number of packages to be added to destination instance")
        sortedList = sorted(addStruct, key=operator.itemgetter('name'))
        for addData in sortedList:
            LOGGER.debug(f"adding package: {addData['name']}")
            # todo, this is working but am commenting out
            self.CKANWrap.addPackage(addData)

    def doDeletes(self, delStruct):
        """does deletes of all the orgs described in the delStruct

        :param delStruct: a list of org names that should be deleted
        :type delStruct: str
        """
        LOGGER.debug(f"number of packages deletes: {len(delStruct)}")
        for pkg2Del in delStruct:
            LOGGER.debug(f"    deleting the package: {pkg2Del}")
            self.CKANWrap.deletePackage(pkg2Del)
    def doUpdates(self, updtStruct):
        """Does the package updates

        :param updtStruct: dictionary where the key is the name of the package
            and the value is a dict that can be passed to the CKAN api to update
            the org
        :type updtStruct: dict
        """
        LOGGER.debug(f"number of updates: {len(updtStruct)}")
        for updateName in updtStruct:
            LOGGER.debug(f"updating the package: {updateName}")

    def update(self, deltaObj):
        # TODO: delete this method and use mixin once complete
        dels = deltaObj.getDeleteData()
        adds = deltaObj.getAddData()

        #updts = deltaObj.getUpdateData()

        #dels = self.removeIgnored(dels)
        #updts = self.removeIgnored(updts)
        LOGGER.debug(f"deltaObj: {deltaObj}")
        self.doDeletes(dels)
        self.doAdds(adds)
        #self.doUpdates(updts)
        LOGGER.info("UPDATE COMPLETE")
