"""[summary]
"""
# pylint: disable=logging-format-interpolation, wrong-import-position

import logging
import logging.config
import os
import posixpath
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

import bcdc2bcdc.CacheFiles as CacheFiles
import bcdc2bcdc.CKAN as CKAN
import bcdc2bcdc.CKANData as CKANData
import bcdc2bcdc.CKANScheming as CKANScheming
import bcdc2bcdc.CKANUpdate as CKANUpdate
import bcdc2bcdc.constants as constants
import bcdc2bcdc.DataCache as DataCache

# set scope for the logger
LOGGER = None


class RunUpdate:
    def __init__(self):
        params = CKAN.CKANParams()
        self.srcCKANWrapper = params.getSrcWrapper()
        self.destCKANWrapper = params.getDestWrapper()

        # verify that destination is not prod
        self.destCKANWrapper.checkUrl()
        self.dataCache = DataCache.DataCache()
        self.cachedFilesPaths = CacheFiles.CKANCacheFiles()

        # create the directory for detailed data dumps
        self.cachedFilesPaths.getCreateDataDumpDir()

    def updateUsers(self, useCache=False):

        getUsersMap = {
            False: {
                "src": self.srcCKANWrapper.getUsers,
                "dest": self.destCKANWrapper.getUsers,
            },
            True: {
                "src": self.srcCKANWrapper.getUsers_cached,
                "dest": self.destCKANWrapper.getUsers_cached,
            },
        }

        srcCacheFile = self.cachedFilesPaths.getSrcUserJsonPath()
        destCacheFile = self.cachedFilesPaths.getDestUserJsonPath()

        # get the raw json data from the api
        # userDataSrc = self.srcCKANWrapper.getUsers(includeData=True)
        # userDataDest = self.destCKANWrapper.getUsers(includeData=True)
        userDataSrc = getUsersMap[useCache]["src"](
            cacheFileName=srcCacheFile, includeData=True
        )
        userDataDest = getUsersMap[useCache]["dest"](
            cacheFileName=destCacheFile, includeData=True
        )

        # wrap the data with CKANDataset class
        # cache is populated when delta obj is requested!
        srcUserCKANDataSet = CKANData.CKANUsersDataSet(
            userDataSrc, self.dataCache, constants.DATA_SOURCE.SRC
        )
        destUserCKANDataSet = CKANData.CKANUsersDataSet(
            userDataDest, self.dataCache, constants.DATA_SOURCE.DEST
        )

        # specific to users, implemented a method to augment the ignore list
        # for duplicate users
        dupEmails = srcUserCKANDataSet.getDuplicateEmailAddresses()

        self.dataCache.addData(srcUserCKANDataSet, constants.DATA_SOURCE.SRC)
        self.dataCache.addData(destUserCKANDataSet, constants.DATA_SOURCE.DEST)

        # use CKANDataset functionality to determine if differences
        if srcUserCKANDataSet != destUserCKANDataSet:
            # perform the update
            LOGGER.info("found differences between users defined in prod and test")

            deltaObj = srcUserCKANDataSet.getDelta(destUserCKANDataSet)
            LOGGER.info(f"Delta obj for groups: {deltaObj}")
            updater = CKANUpdate.CKANUserUpdate(
                self.dataCache, ckanWrapper=self.destCKANWrapper
            )
            updater.update(deltaObj)

    def updateGroups(self, useCache=False):
        """Based on descriptions of SRC / DEST CKAN instances in environment
        variables performes the update, reading from SRC, writing to DEST.
        """
        getGroupsMap = {
            False: {
                "src": self.srcCKANWrapper.getGroups,
                "dest": self.destCKANWrapper.getGroups,
            },
            True: {
                "src": self.srcCKANWrapper.getGroups_cached,
                "dest": self.destCKANWrapper.getGroups_cached,
            },
        }
        srcCacheFile = self.cachedFilesPaths.getSrcGroupJsonPath()
        destCacheFile = self.cachedFilesPaths.getDestGroupJsonPath()

        groupDataSrc = getGroupsMap[useCache]["src"](
            cacheFileName=srcCacheFile, includeData=True
        )
        groupDataDest = getGroupsMap[useCache]["dest"](
            cacheFileName=destCacheFile, includeData=True
        )

        # groupDataSrc = self.srcCKANWrapper.getGroups(includeData=True)
        # groupDataDest = self.destCKANWrapper.getGroups(includeData=True)
        # LOGGER.debug(f"Groupdata is: {groupDataProd}")

        srcGroupCKANDataSet = CKANData.CKANGroupDataSet(
            groupDataSrc, self.dataCache, constants.DATA_SOURCE.SRC
        )
        destGroupCKANDataSet = CKANData.CKANGroupDataSet(
            groupDataDest, self.dataCache, constants.DATA_SOURCE.DEST
        )

        self.dataCache.addData(srcGroupCKANDataSet, constants.DATA_SOURCE.SRC)
        self.dataCache.addData(destGroupCKANDataSet, constants.DATA_SOURCE.DEST)

        if srcGroupCKANDataSet != destGroupCKANDataSet:
            LOGGER.info("found differences between group data in src an dest")
            deltaObj = srcGroupCKANDataSet.getDelta(destGroupCKANDataSet)
            LOGGER.info(f"Delta obj for groups: {deltaObj}")
            updater = CKANUpdate.CKANGroupUpdate(
                self.dataCache, ckanWrapper=self.destCKANWrapper
            )
            updater.update(deltaObj)
        else:
            LOGGER.info("no differences found for groups between src and dest")

    def updateOrganizations(self, useCache=False):
        getOrgsMap = {
            False: {
                "src": self.srcCKANWrapper.getOrganizations,
                "dest": self.destCKANWrapper.getOrganizations,
            },
            True: {
                "src": self.srcCKANWrapper.getOrganizations_cached,
                "dest": self.destCKANWrapper.getOrganizations_cached,
            },
        }
        srcCacheFile = self.cachedFilesPaths.getSrcOrganizationsJsonPath()
        destCacheFile = self.cachedFilesPaths.getDestOrganizationsJsonPath()

        orgDataSrc = getOrgsMap[useCache]["src"](
            cacheFileName=srcCacheFile, includeData=True
        )
        orgDataDest = getOrgsMap[useCache]["dest"](
            cacheFileName=destCacheFile, includeData=True
        )

        # orgDataSrc = self.srcCKANWrapper.getOrganizations(includeData=True)
        # LOGGER.debug(f"first orgDataProd record: {orgDataSrc[0]}")
        # orgDataDest = self.destCKANWrapper.getOrganizations(includeData=True)
        # LOGGER.debug(f"first orgDataTest record: {orgDataDest[0]}")

        srcOrgCKANDataSet = CKANData.CKANOrganizationDataSet(
            orgDataSrc, self.dataCache, constants.DATA_SOURCE.SRC
        )
        destOrgCKANDataSet = CKANData.CKANOrganizationDataSet(
            orgDataDest, self.dataCache, constants.DATA_SOURCE.DEST
        )

        self.dataCache.addData(srcOrgCKANDataSet, constants.DATA_SOURCE.SRC)
        self.dataCache.addData(destOrgCKANDataSet, constants.DATA_SOURCE.DEST)

        if srcOrgCKANDataSet != destOrgCKANDataSet:
            LOGGER.info("found differences between group data in src an dest")
            deltaObj = srcOrgCKANDataSet.getDelta(destOrgCKANDataSet)
            LOGGER.info(f"Delta obj for orgs: {deltaObj}")
            updater = CKANUpdate.CKANOrganizationUpdate(
                dataCache=self.dataCache, ckanWrapper=self.destCKANWrapper
            )
            updater.update(deltaObj)

    def updatePackages(self, useCache=False):
        """ updates packages based on

        :param useCache: [description], defaults to False
        :type useCache: bool, optional
        """
        getPackagesMap = {
            False: {
                "src": self.srcCKANWrapper.getPackagesAndData,
                "dest": self.destCKANWrapper.getPackagesAndData,
            },
            True: {
                "src": self.srcCKANWrapper.getPackagesAndData_cached,
                "dest": self.destCKANWrapper.getPackagesAndData_cached,
            },
        }

        srcCacheFile = self.cachedFilesPaths.getSrcPackagesJsonPath()
        destCacheFile = self.cachedFilesPaths.getDestPackagesJsonPath()

        srcPkgList = getPackagesMap[useCache]["src"](cacheFileName=srcCacheFile)
        destPkgList = getPackagesMap[useCache]["dest"](cacheFileName=destCacheFile)

        # TODO: once debug is complete remove the canned part
        # srcPkgList = self.srcCKANWrapper.getPackagesAndData()
        # destPkgList = self.destCKANWrapper.getPackagesAndData()
        # srcPkgList = self.srcCKANWrapper.getPackagesAndData_cached(constants.CACHE_SRC_PKGS_FILE)
        # destPkgList = self.destCKANWrapper.getPackagesAndData_cached(constants.CACHE_DEST_PKGS_FILE)

        srcPkgDataSet = CKANData.CKANPackageDataSet(
            srcPkgList, self.dataCache, constants.DATA_SOURCE.SRC
        )
        destPkgDataSet = CKANData.CKANPackageDataSet(
            destPkgList, self.dataCache, constants.DATA_SOURCE.DEST
        )

        self.dataCache.addData(srcPkgDataSet, constants.DATA_SOURCE.SRC)
        self.dataCache.addData(destPkgDataSet, constants.DATA_SOURCE.DEST)

        if srcPkgDataSet != destPkgDataSet:

            LOGGER.debug("packages are not the same")

            deltaObj = srcPkgDataSet.getDelta(destPkgDataSet)
            LOGGER.info(f"Delta obj for packages: {deltaObj}")
            updater = CKANUpdate.CKANPackagesUpdate(
                self.dataCache, ckanWrapper=self.destCKANWrapper
            )
            updater.update(deltaObj)

    def refreshSchemingDefs(self):
        """Every time the update runs it will download the scheming definitions.
        These are used later when transforming the packages for update.
        """
        # check for scheming file, and delete it, then create a ckanschemoing
        # object, cache it in the datacache
        cacheFiles = CacheFiles.CKANCacheFiles()
        schemingCacheFile = cacheFiles.getSchemingCacheFilePath()
        if os.path.exists(schemingCacheFile):
            LOGGER.info(f"deleting the scheming cache file: {schemingCacheFile}")
            os.remove(schemingCacheFile)

        scheming = CKANScheming.Scheming()
        self.dataCache.setScheming(scheming)


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

    # Adding a new "verbose" log level, to use: logger.debugv
    # DEBUG_LEVELV_NUM = 9
    # logging.addLevelName(DEBUG_LEVELV_NUM, "DEBUGV")
    # def debugv(self, message, *args, **kws):
    #     self._log(DEBUG_LEVELV_NUM, message, args, **kws)

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
    updater.refreshSchemingDefs()
    # This is complete, commented out while work on group
    # not running user update for now

    updater.updateUsers(useCache=True)
    # updater.updateGroups(useCache=True)
    updater.updateOrganizations(useCache=True)
    updater.updatePackages(useCache=True)
