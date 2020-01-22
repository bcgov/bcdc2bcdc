import CKAN
import CKANData
import CKANUpdate
import constants
import logging
import logging.config
import os
import posixpath

LOGGER = logging.getLogger(__name__)


class RunUpdate:
    def __init__(self):
        self.prodWrapper = CKAN.CKANWrapper(
            os.environ[constants.CKAN_URL_SRC], os.environ[constants.CKAN_APIKEY_SRC]
        )
        self.testWrapper = CKAN.CKANWrapper(
            os.environ[constants.CKAN_URL_DEST], os.environ[constants.CKAN_APIKEY_DEST]
        )

    def updateUsers(self):
        # get the raw json data from the api
        userDataProd = self.prodWrapper.getUsers(includeData=True)
        userDataTest = self.testWrapper.getUsers(includeData=True)

        # wrap the data with CKANDataset class
        prodUserCKANDataSet = CKANData.CKANUsersDataSet(userDataProd)
        testUserCKANDataSet = CKANData.CKANUsersDataSet(userDataTest)

        # use CKANDataset functionality to determine if differences
        if prodUserCKANDataSet != testUserCKANDataSet:
            # perform the update
            LOGGER.info("found differences between users defined in prod and test")
            deltaObj = prodUserCKANDataSet.getDelta(testUserCKANDataSet)
            updater = CKANUpdate.CKANUserUpdate(self.testWrapper)
            updater.update(deltaObj)

    def updateGroups(self):
        groupDataProd = self.prodWrapper.getGroups(includeData=True)
        groupDataTest = self.testWrapper.getGroups(includeData=True)
        #LOGGER.debug(f"Groupdata is: {groupDataProd}")

        prodGroupCKANDataSet = CKANData.CKANGroupDataSet(groupDataProd)
        testGroupCKANDataSet = CKANData.CKANGroupDataSet(groupDataTest)

        if prodGroupCKANDataSet != testGroupCKANDataSet:
            LOGGER.info('found differences between group data in src an dest')
            deltaObj = prodGroupCKANDataSet.getDelta(testGroupCKANDataSet)
            LOGGER.info(f"Delta obj for groups: {deltaObj}")
            # TODO: create update methods

    def updateOrganizations(self):
        #orgDataProd = self.prodWrapper.getOrganizations(includeData=True)
        #LOGGER.debug(f"first orgDataProd record: {orgDataProd[0]}")
        orgDataTest = self.testWrapper.getOrganizations(includeData=True)
        LOGGER.debug(f"first orgDataTest record: {orgDataTest[0]}")

        """

        prodOrgCKANDataSet = CKANData.CKANOrganizationDataSet(orgDataProd)
        testOrgCKANDataSet = CKANData.CKANOrganizationDataSet(orgDataTest)
        if prodOrgCKANDataSet != testOrgCKANDataSet:
            LOGGER.info("found differences between orgs defined in prod and test")
            deltaObj = prodOrgCKANDataSet.getDelta(testOrgCKANDataSet)
            LOGGER.info(f"delta object: {deltaObj}")
            #updater = CKANUpdate.CKANOrganizationUpdate(self.testWrapper)
            #updater.update(deltaObj)

        """
        pass

    def updatePackages(self):
        # TODO: need to complete this method... ... left incomplete while work on
        #       org compare and update instead.  NEEDS TO BE COMPLETED
        prodPkgList = self.prodWrapper.getPackageNames()
        testPkgList = self.testWrapper.getPackageNames()






if __name__ == "__main__":

    # ----- LOGGING SETUP -----
    appDir = os.path.dirname(__file__)
    # LOG config file
    logConfigFile = os.path.join(
        appDir,
        "..",
        constants.TRANSFORM_CONFIG_DIR,
        constants.LOGGING_CONFIG_FILE_NAME,
    )
    logConfigFile = os.path.abspath(logConfigFile)
    # output log file for roller if implemented
    logOutputsDir = os.path.join(appDir, "..", constants.LOGGING_OUTPUT_DIR)
    logOutputsDir = os.path.normpath(logOutputsDir)
    if not os.path.exists(logOutputsDir):
        os.mkdir(logOutputsDir)

    logOutputsFilePath = os.path.join(logOutputsDir, constants.LOGGING_OUTPUT_FILE_NAME)
    logOutputsFilePath = logOutputsFilePath.replace(os.path.sep, posixpath.sep)
    print(f"log config file: {logConfigFile}")
    logging.config.fileConfig(
        logConfigFile, 
        defaults={"logfilename": logOutputsFilePath}
    )
    LOGGER = logging.getLogger('main')
    LOGGER.debug(f'__name__ is {__name__}')

    # ----- RUN SCRIPT -----
    updater = RunUpdate()
    # This is complete, commented out while work on group
    #updater.updateUsers()
    #updater.updateGroups()
    updater.updateOrganizations()