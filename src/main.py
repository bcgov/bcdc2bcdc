import CKAN
import CKANData
import CKANUpdate
import DataCache
import constants
import logging
import logging.config
import os
import posixpath
import CacheFiles

# pylint: disable=logging-format-interpolation

# set scope for the logger
LOGGER = None


class RunUpdate:
    def __init__(self):
        params = CKAN.CKANParams()
        self.srcCKANWrapper = params.getSrcWrapper()
        self.destCKANWrapper = params.getDestWrapper()
        self.dataCache = DataCache.DataCache()
        self.cachedFilesPaths = CacheFiles.CKANCacheFiles()

    def updateUsers(self, useCache=False):

        getUsersMap = {
            False: {
                "src": self.srcCKANWrapper.getUsers,
                "dest": self.destCKANWrapper.getUsers
            },
            True: {
                "src": self.srcCKANWrapper.getUsers_cached,
                "dest": self.destCKANWrapper.getUsers_cached
                }
        }

        srcCacheFile = self.cachedFilesPaths.getSrcUserJsonPath()
        destCacheFile = self.cachedFilesPaths.getDestUserJsonPath()

        # get the raw json data from the api
        #userDataSrc = self.srcCKANWrapper.getUsers(includeData=True)
        #userDataDest = self.destCKANWrapper.getUsers(includeData=True)
        userDataSrc = getUsersMap[useCache]['src'](cacheFileName=srcCacheFile, includeData=True)
        userDataDest = getUsersMap[useCache]['dest'](cacheFileName=destCacheFile, includeData=True)


        # wrap the data with CKANDataset class
        # cache is populated when delta obj is requested!
        srcUserCKANDataSet = CKANData.CKANUsersDataSet(userDataSrc, self.dataCache)
        destUserCKANDataSet = CKANData.CKANUsersDataSet(userDataDest, self.dataCache)

        # use CKANDataset functionality to determine if differences
        if srcUserCKANDataSet != destUserCKANDataSet:
            # perform the update
            LOGGER.info("found differences between users defined in prod and test")

            deltaObj = srcUserCKANDataSet.getDelta(destUserCKANDataSet)
            LOGGER.info(f"Delta obj for groups: {deltaObj}")
            updater = CKANUpdate.CKANUserUpdate(ckanWrapper=self.destCKANWrapper)
            updater.update(deltaObj)

    def updateGroups(self, useCache=False):
        """Based on descriptions of SRC / DEST CKAN instances in environment
        variables performes the update, reading from SRC, writing to DEST.
        """
        getGroupsMap = {
            False: {
                "src": self.srcCKANWrapper.getGroups,
                "dest": self.destCKANWrapper.getGroups
            },
            True: {
                "src": self.srcCKANWrapper.getGroups_cached,
                "dest": self.destCKANWrapper.getGroups_cached
                }
        }
        srcCacheFile = self.cachedFilesPaths.getSrcGroupJsonPath()
        destCacheFile = self.cachedFilesPaths.getDestGroupJsonPath()

        groupDataSrc = getGroupsMap[useCache]['src'](cacheFileName=srcCacheFile, includeData=True)
        groupDataDest = getGroupsMap[useCache]['dest'](cacheFileName=destCacheFile, includeData=True)

        #groupDataSrc = self.srcCKANWrapper.getGroups(includeData=True)
        #groupDataDest = self.destCKANWrapper.getGroups(includeData=True)
        # LOGGER.debug(f"Groupdata is: {groupDataProd}")

        srcGroupCKANDataSet = CKANData.CKANGroupDataSet(groupDataSrc, self.dataCache)
        destGroupCKANDataSet = CKANData.CKANGroupDataSet(groupDataDest, self.dataCache)

        if srcGroupCKANDataSet != destGroupCKANDataSet:
            LOGGER.info("found differences between group data in src an dest")
            deltaObj = srcGroupCKANDataSet.getDelta(destGroupCKANDataSet)
            LOGGER.info(f"Delta obj for groups: {deltaObj}")
            updater = CKANUpdate.CKANGroupUpdate(ckanWrapper=self.destCKANWrapper)
            updater.update(deltaObj)
        else:
            LOGGER.info("no differences found for groups between src and dest")

    def updateOrganizations(self, useCache=False):
        getOrgsMap = {
            False: {
                "src": self.srcCKANWrapper.getOrganizations,
                "dest": self.destCKANWrapper.getOrganizations
            },
            True: {
                "src": self.srcCKANWrapper.getOrganizations_cached,
                "dest": self.destCKANWrapper.getOrganizations_cached
                }
        }
        srcCacheFile = self.cachedFilesPaths.getSrcOrganizationsJsonPath()
        destCacheFile = self.cachedFilesPaths.getDestOrganizationsJsonPath()

        orgDataSrc = getOrgsMap[useCache]['src'](cacheFileName=srcCacheFile, includeData=True)
        orgDataDest = getOrgsMap[useCache]['dest'](cacheFileName=destCacheFile, includeData=True)

        #orgDataSrc = self.srcCKANWrapper.getOrganizations(includeData=True)
        #LOGGER.debug(f"first orgDataProd record: {orgDataSrc[0]}")
        #orgDataDest = self.destCKANWrapper.getOrganizations(includeData=True)
        #LOGGER.debug(f"first orgDataTest record: {orgDataDest[0]}")

        srcOrgCKANDataSet = CKANData.CKANOrganizationDataSet(orgDataSrc, self.dataCache)
        destOrgCKANDataSet = CKANData.CKANOrganizationDataSet(
            orgDataDest, self.dataCache
        )

        if srcOrgCKANDataSet != destOrgCKANDataSet:
            LOGGER.info("found differences between group data in src an dest")
            deltaObj = srcOrgCKANDataSet.getDelta(destOrgCKANDataSet)
            LOGGER.info(f"Delta obj for orgs: {deltaObj}")
            updater = CKANUpdate.CKANOrganizationUpdate(
                ckanWrapper=self.destCKANWrapper
            )
            updater.update(deltaObj)

    def updatePackages(self, useCache=False):
        getPackagesMap = {
            False: {
                "src": self.srcCKANWrapper.getPackagesAndData,
                "dest": self.destCKANWrapper.getPackagesAndData
            },
            True: {
                "src": self.srcCKANWrapper.getPackagesAndData_cached,
                "dest": self.destCKANWrapper.getPackagesAndData_cached
                }
        }
        srcCacheFile = self.cachedFilesPaths.getSrcPackagesJsonPath()
        destCacheFile = self.cachedFilesPaths.getDestPackagesJsonPath()

        srcPkgList = getPackagesMap[useCache]['src'](cacheFileName=srcCacheFile)
        destPkgList = getPackagesMap[useCache]['dest'](cacheFileName=destCacheFile)

        # TODO: need to complete this method... ... left incomplete while work on
        #       org compare and update instead.  NEEDS TO BE COMPLETED

        # TODO: once debug is complete remove the canned part
        #srcPkgList = self.srcCKANWrapper.getPackagesAndData()
        #destPkgList = self.destCKANWrapper.getPackagesAndData()
        # srcPkgList = self.srcCKANWrapper.getPackagesAndData_cached(constants.CACHE_SRC_PKGS_FILE)
        # destPkgList = self.destCKANWrapper.getPackagesAndData_cached(constants.CACHE_DEST_PKGS_FILE)

        srcPkgDataSet = CKANData.CKANPackageDataSet(srcPkgList, self.dataCache)
        destPkgDataSet = CKANData.CKANPackageDataSet(destPkgList, self.dataCache)

        self.dataCache.addData(srcPkgDataSet, constants.DATA_SOURCE.SRC)
        self.dataCache.addData(destPkgDataSet, constants.DATA_SOURCE.DEST)

        if srcPkgDataSet != destPkgDataSet:
            LOGGER.debug("packages are not the same")

            deltaObj = srcPkgDataSet.getDelta(destPkgDataSet)
            LOGGER.info(f"Delta obj for orgs: {deltaObj}")
            updater = CKANUpdate.CKANPackagesUpdate(ckanWrapper=self.destCKANWrapper)
            updater.update(deltaObj)


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

    # output log file for roller if implemented... not implemented atm
    logOutputsDir = os.path.join(appDir, "..", constants.LOGGING_OUTPUT_DIR)
    logOutputsDir = os.path.normpath(logOutputsDir)
    if not os.path.exists(logOutputsDir):
        os.mkdir(logOutputsDir)

    logOutputsFilePath = os.path.join(logOutputsDir, constants.LOGGING_OUTPUT_FILE_NAME)
    logOutputsFilePath = logOutputsFilePath.replace(os.path.sep, posixpath.sep)
    print(f"log config file: {logConfigFile}")
    logging.config.fileConfig(
        logConfigFile, defaults={"logfilename": logOutputsFilePath}
    )
    LOGGER = logging.getLogger("main")
    LOGGER.debug(f"__name__ is {__name__}")

    # ----- RUN SCRIPT -----
    updater = RunUpdate()
    # This is complete, commented out while work on group
    updater.updateUsers(useCache=True)
    updater.updateGroups(useCache=True)
    updater.updateOrganizations(useCache=True)
    updater.updatePackages(useCache=True)
