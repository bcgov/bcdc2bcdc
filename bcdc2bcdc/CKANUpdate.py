"""
Functionality that can:
    * identify two CKANData.CKANDataSet ( or subclasses of these ) and identify
      differences between the two
    * Using the CKAN module, update one or the other.

IDEA:
Looking at each of the different update classes below... All could
be deleted.  updates all are identical, the only difference is the
CKANWrapper method that gets called.  Could create a method map
like {del: delMethod, updt: updateMethod, ... }, this could even be
inferred from each record.dataType

"""

# pylint: disable=logging-format-interpolation, logging-not-lazy

import abc
import json
import logging
import os

import bcdc2bcdc.CKAN as CKAN
import bcdc2bcdc.CKANData as CKANData
import bcdc2bcdc.CKANTransform as CKANTransform
import bcdc2bcdc.constants as constants

LOGGER = logging.getLogger(__name__)


class CKANUpdateAbstract(abc.ABC):
    """
    abstract base class used to define the interface for CKANUpdate objects.

    Each different object type in ckan should implement its own version of this
    class.
    """

    def __init__(self, dataCache, ckanWrapper=None):
        self.CKANWrap = ckanWrapper
        self.dataCache = dataCache
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

        # These are all lists of dicts, each dict is the info
        # that will be sent directly to the api
        adds = deltaObj.getAddData()  # list of dicts
        dels = deltaObj.getDeleteData()  # list of names
        updts = deltaObj.getUpdateData()  # list of dicts

        # this should have been calculated earlier?
        dels = self.removeIgnored(dels)
        updts = self.removeIgnored(updts)
        adds = self.removeIgnored(adds)

        self.doAdds(adds)
        self.doDeletes(dels)
        self.doUpdates(updts)
        LOGGER.info("UPDATE COMPLETE")

    def removeIgnored(self, inputRecordCollection):
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
        # receives a record collection, iterate over it
        recordCollection = None
        for ckanRecord in inputRecordCollection:
            recordUniqueId = ckanRecord.getUniqueIdentifier()
            if recordUniqueId not in self.ignoreList:
                if recordCollection is None:
                    recordCollection = CKANData.CKANRecordCollection(
                        inputRecordCollection.dataType
                    )
                recordCollection.addRecord(ckanRecord)
        retVal = inputRecordCollection
        if recordCollection is not None:
            retVal = recordCollection
        return retVal

class CKANUserUpdate(UpdateMixin, CKANUpdateAbstract):
    """implements the abstract base class CKANUpdate_abc to allow user data to
    be updated easily

    :param UpdateMixin:  This mixin allows the the base class update
        method to be glued to the implementing classes update methods.
    :type UpdateMixin: class
    :param CKANUpdate_abc: defined the interface that this class needs to
        implement
    :type CKANUpdate_abc: abstract base class
    """

    def __init__(self, dataCache, ckanWrapper=None):
        CKANUpdateAbstract.__init__(self, dataCache, ckanWrapper)
        self.dataType = constants.TRANSFORM_TYPE_USERS
        self.CKANTranformConfig = CKANTransform.TransformationConfig()
        self.ignoreList = self.CKANTranformConfig.getIgnoreList(self.dataType)

    def doAdds(self, addCollection):
        """List of user data to be added to a ckan instance,

        :param addCollection: a CKANRecordCollection object which contains the
            CKAN records that need to be added
        :type addStruct: CKANData.CKANRecordCollection
        """
        LOGGER.info(f"{len(addCollection)} to be added to destination instance")
        uniqueIds = addCollection.getUniqueIdentifiers()
        uniqueIds.sort()
        for addName in uniqueIds:
            LOGGER.debug(f"adding user: {addName}")
            addRecord = addCollection.getRecordByUniqueId(addName)

            self.doAdd(addRecord)

    def doAdd(self, addRecord, addStruct=None, retryCnt=None):
        maxRetries = 5
        try:
            if not addStruct:
                addStruct = addRecord.getComparableStructUsedForAddUpdate(
                    self.dataCache, constants.UPDATE_TYPES.ADD
                )
            # TODO:For consistency sake should move the password insertion to
            #      a custom transformer for ADD / UPDATE operations
            addStruct["password"] = os.environ[constants.CKAN_ONETIME_PASSWORD]
            self.CKANWrap.addUser(addStruct)
        except CKAN.CKANUserNameUnAvailable:
            # check that we haven't exceeded the maximum number of retries
            # get the record unique identifier
            uniqueId = addRecord.getUniqueIdentifier()

            # check the retry count
            if retryCnt is None:
                retryCnt = 1
            else:
                retryCnt += 1
            if retryCnt > maxRetries:
                LOGGER.error("cannot generate a unique identifier for the user")
                raise

            # calculate a new unique id
            if uniqueId[-1].isdigit():
                nextNum = int(uniqueId[-1]) + 1
                uniqueId = f"{uniqueId[0:-1]}{nextNum}"
            else:
                uniqueId = f"{uniqueId}{1}"
            addStruct["name"] = uniqueId
            LOGGER.warning(
                f"encountered name conflict, creating a new "
                + f"user with the name: {uniqueId}"
            )
            self.doAdd(addRecord, addStruct, retryCnt)

    def doDeletes(self, delCollection):
        """list of usernames or ids to delete

        :param delStruct: list of user names or ids to delete
        :type delStruct: list
        """
        # TODO: Thinking again deletes are likely something we do not want to do
        #      for some accounts.  Example demo accounts set up for testing.
        LOGGER.info(f"{len(delCollection)} to be deleted to destination instance")
        uniqueIds = delCollection.getUniqueIdentifiers()
        uniqueIds.sort()

        for deleteUser in uniqueIds:
            LOGGER.info(f"removing the user: {deleteUser} from the destination")
            self.CKANWrap.deleteUser(deleteUser)

    def doUpdates(self, updtCollection):
        """Gets a list of user data that is used to updated a CKAN instance
        with.

        :param updtStruct: list of user data to be used to update users
        :type updtStruct: list
        """
        LOGGER.info(f'{len(updtCollection)} to be deleted to destination instance')
        uniqueIds = updtCollection.getUniqueIdentifiers()
        uniqueIds.sort()

        for updateName in uniqueIds:
            LOGGER.info(f"updating the user : {updateName}")
            updtRecord = updtCollection.getRecordByUniqueId(updateName)
            updtStruct = updtRecord.getComparableStructUsedForAddUpdate(
                self.dataCache, constants.UPDATE_TYPES.UPDATE
            )

            # updtStruct is comming from a delta obj
            # delta obj is used to keep track of:
            #   - adds
            #   - deletes
            #   - updates

            # TODO: This logic should be moved to a custom transformer
            if updtStruct["email"] is not None:
                self.CKANWrap.updateUser(updtStruct)
            else:
                LOGGER.info(f"skipping this record as email is null: {updateName}")
        LOGGER.debug("updates complete")


class CKANGroupUpdate(UpdateMixin, CKANUpdateAbstract):
    def __init__(self, dataCache, ckanWrapper=None):
        """Gets a list of updates

        :param ckanWrapper: [description], defaults to None
        :type ckanWrapper: [type], optional
        """
        CKANUpdateAbstract.__init__(self, dataCache, ckanWrapper)
        self.dataType = constants.TRANSFORM_TYPE_GROUPS
        self.CKANTranformConfig = CKANTransform.TransformationConfig()
        self.ignoreList = self.CKANTranformConfig.getIgnoreList(self.dataType)

    def doAdds(self, addCollection):
        """gets a list of group structs that describe groups that are to be added
        to a CKAN instance

        :param addCollection: a CKANRecordCollection object which contains the
            CKAN records that need to be added
        :type addCollection: CKANData.CKANRecordCollection
        """
        LOGGER.info(f"{len(addCollection)} groups to be added to destination instance")
        uniqueIds = addCollection.getUniqueIdentifiers()
        uniqueIds.sort()
        for addName in uniqueIds:
            LOGGER.debug(f"adding group: {addName}")

            addRecord = addCollection.getRecordByUniqueId(addName)
            addStruct = addRecord.getComparableStructUsedForAddUpdate(
                self.dataCache, constants.UPDATE_TYPES.ADD
            )
            self.CKANWrap.addGroup(addStruct)

    def doDeletes(self, delCollection):
        """performs deletes of all the groups contained in the delCollection

        :param delCollection: a CKANRecordCollection object which contains the CKAN
            records that need to be deleted
        :type delCollection: CKANData.CKANRecordCollection
        """
        LOGGER.info(
            f"number of groups: {len(delCollection)} to be deleted to "
            + "destination instance"
        )
        uniqueIds = delCollection.getUniqueIdentifiers()
        uniqueIds.sort()
        for deleteGroupName in uniqueIds:
            LOGGER.info(f"removing the group: {deleteGroupName} from the destination")
            self.CKANWrap.deleteGroup(deleteGroupName)

    def doUpdates(self, updtCollection):
        """Gets a list of group data that needs to be updated

        :param updates: list of dictionaries with the data to be used to update a group
        :type updates: list of dict
        """
        # LOGGER.error(f"still need to implement this {updtStruct}")
        LOGGER.debug(f"number of group updates: {len(updtCollection)}")
        uniqueIds = updtCollection.getUniqueIdentifiers()
        uniqueIds.sort()
        for updateName in uniqueIds:
            LOGGER.info(f"updating the group : {updateName}")
            updtRecord = updtCollection.getRecordByUniqueId(updateName)
            updtStruct = updtRecord.getComparableStructUsedForAddUpdate(
                self.dataCache, constants.UPDATE_TYPES.UPDATE
            )
            self.CKANWrap.updateGroup(updtStruct)
        LOGGER.debug("updates complete")


class CKANOrganizationUpdate(UpdateMixin, CKANUpdateAbstract):
    """
    implements the interface defined by CKANUpdate_abc, the actual update
    method comes from the mixin.

    Used to provide a uniform interface that is used by the script to update
    the orgs from one ckan instance to another.
    """

    def __init__(self, dataCache, ckanWrapper=None):
        CKANUpdateAbstract.__init__(self, dataCache, ckanWrapper)
        self.dataType = constants.TRANSFORM_TYPE_ORGS
        self.CKANTransformConfig = CKANTransform.TransformationConfig()
        self.ignoreList = self.CKANTransformConfig.getIgnoreList(self.dataType)

    def doAdds(self, addCollection):
        """adds the orgs described in the param addStruct

        :param addCollection: a CKANRecordCollection object which contains the
            CKAN records that need to be added
        :type addCollection: CKANData.CKANRecordCollection
        """
        # LOGGER.debug(f"adds: {addStruct}")
        LOGGER.info(
            f"{len(addCollection)}: number of orgs to be added to destination instance"
        )
        uniqueIds = addCollection.getUniqueIdentifiers()
        uniqueIds.sort()
        for addName in uniqueIds:
            LOGGER.debug(f"adding organization: {addName}")
            addRecord = addCollection.getRecordByUniqueId(addName)
            addStruct = addRecord.getComparableStructUsedForAddUpdate(
                self.dataCache, constants.UPDATE_TYPES.ADD
            )
            # todo, this is working but am commenting out
            self.CKANWrap.addOrganization(addStruct)

    def doDeletes(self, delCollection):
        """performs deletes of all the orgs contained in the delCollection

        :param delStruct: a CKANRecordCollection object which contains the CKAN
            records that need to be deleted
        :type delStruct: CKANData.CKANRecordCollection
        """
        LOGGER.debug(f"number of deletes: {len(delCollection)}")
        uniqueIds = delCollection.getUniqueIdentifiers()
        uniqueIds.sort()
        for org2Del in uniqueIds:
            LOGGER.debug(f"    deleting the org: {org2Del}")
            self.CKANWrap.deleteOrganization(org2Del)

    def doUpdates(self, updtCollection):
        """Performs the org updates

        :param updtStruct: a CKANRecordCollection object which contains the CKAN
            records that need to be deleted
        :type updtStruct: CKANData.CKANRecordCollection
        """
        LOGGER.debug(f"number of updates: {len(updtCollection)}")
        uniqueIds = updtCollection.getUniqueIdentifiers()
        uniqueIds.sort()
        for updateName in uniqueIds:
            updtRecord = updtCollection.getRecordByUniqueId(updateName)
            updtStruct = updtRecord.getComparableStructUsedForAddUpdate(
                self.dataCache, constants.UPDATE_TYPES.UPDATE
            )

            LOGGER.debug(f"updating the org: {updateName}")

            self.CKANWrap.updateOrganization(updtStruct)


class CKANPackagesUpdate(UpdateMixin, CKANUpdateAbstract):
    """
    implements the interface defined by CKANUpdate_abc, the actual update
    method comes from the mixin.

    Used to provide a uniform interface that is used by the script to update
    the packages from one ckan instance to another.
    """

    def __init__(self, dataCache, ckanWrapper=None):
        CKANUpdateAbstract.__init__(self, dataCache, ckanWrapper)
        self.dataType = constants.TRANSFORM_TYPE_PACKAGES
        self.CKANTransformConfig = CKANTransform.TransformationConfig()
        self.ignoreList = self.CKANTransformConfig.getIgnoreList(self.dataType)

    def doAdds(self, addCollection):
        """adds the packages described in the param addStruct

        :param addStruct: dictionary where the key is the name of the org to be added
            and the value is the struct that can be passed directly to the ckan api
            to add this org
        :type addStruct: dict
        """
        LOGGER.info(
            f"{len(addCollection)}: number of packages to be added to destination instance"
        )
        uniqueIds = addCollection.getUniqueIdentifiers()
        uniqueIds.sort()
        for addDataSetName in uniqueIds:
            addDataRecord = addCollection.getRecordByUniqueId(addDataSetName)
            addStruct = addDataRecord.getComparableStructUsedForAddUpdate(
                self.dataCache, constants.UPDATE_TYPES.ADD
            )
            if constants.isDataDebug():
                with open("add_package.json", "w") as fh:
                    json.dump(addStruct, fh)
                    LOGGER.debug("wrote data to: add_package.json")
            LOGGER.info(f"adding package: {addDataSetName}")
            jsonStr = json.dumps(addStruct)
            LOGGER.debug(f"pkg Struct: {jsonStr[0:100]} ...")
            # TODO: uncomment
            self.CKANWrap.addPackage(addStruct)

    def doDeletes(self, delCollection):
        """does deletes of all the orgs described in the delStruct

        :param delStruct: a list of org names that should be deleted
        :type delStruct: str
        """
        LOGGER.info(f"number of packages deletes: {len(delCollection)}")
        uniqueIds = delCollection.getUniqueIdentifiers()
        uniqueIds.sort()
        for pkg2Del in uniqueIds:
            LOGGER.info(f"deleting the package: {pkg2Del}")
            self.CKANWrap.deletePackage(pkg2Del)

    def doUpdates(self, updtCollection):
        """Does the package updates

        :param updtStruct: dictionary where the key is the name of the package
            and the value is a dict that can be passed to the CKAN api to update
            the org
        :type updtStruct: dict
        """
        LOGGER.debug(f"number of updates: {len(updtCollection)}")
        uniqueIds = updtCollection.getUniqueIdentifiers()
        uniqueIds.sort()
        for updateName in uniqueIds:
            updtDataRecord = updtCollection.getRecordByUniqueId(updateName)
            updtStruct = updtDataRecord.getComparableStructUsedForAddUpdate(
                self.dataCache, constants.UPDATE_TYPES.UPDATE
            )

            if constants.isDataDebug():
                tmpCacheFileName = "updt_package.json"
                with open(tmpCacheFileName, "w") as fh:
                    json.dump(updtStruct, fh)
                    LOGGER.debug(f"wrote updt data for {updateName} to: {tmpCacheFileName}")

            LOGGER.info(f"updating the package: {updateName}")
            self.CKANWrap.updatePackage(updtStruct)
            # was originally going to catch this and fix, but realized that
            # it is more likely a problem due to a lack of migration on the
            # cat instance, if need to do that catch
            #  CKAN.MoreInfoNeedsDeStringify
