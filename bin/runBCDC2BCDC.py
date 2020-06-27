#!/usr/bin/env python3
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

        argList = {
            True: {
                "src": {"cacheFileName":  self.cachedFilesPaths.getSrcUserJsonPath(),
                        "includeData": True},
                "dest": {"cacheFileName": self.cachedFilesPaths.getSrcUserJsonPath(),
                        "includeData": True}
            },
            False: {
                "src": {"cacheFileName":  None,
                        "includeData": True},
                "dest": {"cacheFileName": None,
                        "includeData": True}
            }
        }
        # TODO: Need to figure out how to call the getusers with or
        #       without cache and then deal with the other methods
        userDataSrc = self.srcCKANWrapper.getUsers(**argList[useCache]['src'])
        userDataDest = self.destCKANWrapper.getUsers(**argList[useCache]['dest'])

        # cache will be populated when delta obj is requested
        srcUserCKANDataSet = CKANData.CKANUsersDataSet(
            userDataSrc, self.dataCache, constants.DATA_SOURCE.SRC
        )
        destUserCKANDataSet = CKANData.CKANUsersDataSet(
            userDataDest, self.dataCache, constants.DATA_SOURCE.DEST
        )

        # specific to users, implemented a method to augment the ignore list
        # for duplicate users
        dupEmails = srcUserCKANDataSet.getDuplicateEmailAddresses()
        LOGGER.debug(f"found the following duplicate emails: {dupEmails}")

        self.dataCache.addData(srcUserCKANDataSet, constants.DATA_SOURCE.SRC)
        self.dataCache.addData(destUserCKANDataSet, constants.DATA_SOURCE.DEST)

        # use CKANDataset functionality to determine if differences
        # perform the update
        LOGGER.info("calculating deltas between src / dest for users...")

        deltaObj = srcUserCKANDataSet.getDelta(destUserCKANDataSet)
        LOGGER.info(f"Delta obj for users: {deltaObj}")
        updater = CKANUpdate.CKANUserUpdate(
            self.dataCache, ckanWrapper=self.destCKANWrapper
        )
        updater.update(deltaObj)

    def updateGroups(self, useCache=False):
        """Based on descriptions of SRC / DEST CKAN instances in environment
        variables performes the update, reading from SRC, writing to DEST.
        """
        argList = {
            True: {
                "src": {"cacheFileName":  self.cachedFilesPaths.getSrcGroupJsonPath(),
                        "includeData": True},
                "dest": {"cacheFileName": self.cachedFilesPaths.getDestGroupJsonPath(),
                        "includeData": True}
            },
            False: {
                "src": {"cacheFileName":  None,
                        "includeData": True},
                "dest": {"cacheFileName": None,
                        "includeData": True}
            }
        }
        groupDataSrc = self.srcCKANWrapper.getGroups(**argList[useCache]['src'])
        groupDataDest = self.destCKANWrapper.getGroups(**argList[useCache]['dest'])

        srcGroupCKANDataSet = CKANData.CKANGroupDataSet(
            groupDataSrc, self.dataCache, constants.DATA_SOURCE.SRC
        )
        destGroupCKANDataSet = CKANData.CKANGroupDataSet(
            groupDataDest, self.dataCache, constants.DATA_SOURCE.DEST
        )

        self.dataCache.addData(srcGroupCKANDataSet, constants.DATA_SOURCE.SRC)
        self.dataCache.addData(destGroupCKANDataSet, constants.DATA_SOURCE.DEST)

        LOGGER.info("calculating deltas between src / dest for groups...")
        deltaObj = srcGroupCKANDataSet.getDelta(destGroupCKANDataSet)
        LOGGER.info(f"Delta obj for groups: {deltaObj}")
        updater = CKANUpdate.CKANGroupUpdate(
            self.dataCache, ckanWrapper=self.destCKANWrapper
        )
        updater.update(deltaObj)
        #else:
        #    LOGGER.info("no differences found for groups between src and dest")

    def updateOrganizations(self, useCache=False):
        argList = {
            True: {
                "src": {"cacheFileName":  self.cachedFilesPaths.getSrcOrganizationsJsonPath(),
                        "includeData": True},
                "dest": {"cacheFileName": self.cachedFilesPaths.getDestOrganizationsJsonPath(),
                        "includeData": True}
            },
            False: {
                "src": {"cacheFileName":  None,
                        "includeData": True},
                "dest": {"cacheFileName": None,
                        "includeData": True}
            }
        }
        orgDataSrc = self.srcCKANWrapper.getOrganizations(**argList[useCache]['src'])
        orgDataDest = self.destCKANWrapper.getOrganizations(**argList[useCache]['dest'])

        srcOrgCKANDataSet = CKANData.CKANOrganizationDataSet(
            orgDataSrc, self.dataCache, constants.DATA_SOURCE.SRC
        )
        destOrgCKANDataSet = CKANData.CKANOrganizationDataSet(
            orgDataDest, self.dataCache, constants.DATA_SOURCE.DEST
        )

        self.dataCache.addData(srcOrgCKANDataSet, constants.DATA_SOURCE.SRC)
        self.dataCache.addData(destOrgCKANDataSet, constants.DATA_SOURCE.DEST)

        LOGGER.info("calculating deltas between src / dest for organizations...")
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
        argList = {
            True: {
                "src": {"cacheFileName":  self.cachedFilesPaths.getSrcPackagesJsonPath()},
                "dest": {"cacheFileName": self.cachedFilesPaths.getDestPackagesJsonPath()}
            },
            False: {
                "src": {"cacheFileName":  None},
                "dest": {"cacheFileName": None}
            }
        }
        srcPkgList = self.srcCKANWrapper.getPackagesAndData(**argList[useCache]['src'])
        destPkgList = self.destCKANWrapper.getPackagesAndData(**argList[useCache]['dest'])

        srcPkgDataSet = CKANData.CKANPackageDataSet(
            srcPkgList, self.dataCache, constants.DATA_SOURCE.SRC
        )
        destPkgDataSet = CKANData.CKANPackageDataSet(
            destPkgList, self.dataCache, constants.DATA_SOURCE.DEST
        )

        self.dataCache.addData(srcPkgDataSet, constants.DATA_SOURCE.SRC)
        self.dataCache.addData(destPkgDataSet, constants.DATA_SOURCE.DEST)

        LOGGER.debug("calculating deltas between src / dest for packages...")

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
        # check for scheming file, and delete it, then create a ckan schemeing
        # object, cache it in the datacache
        cacheFiles = CacheFiles.CKANCacheFiles()
        schemingCacheFile = cacheFiles.getSchemingCacheFilePath()
        if os.path.exists(schemingCacheFile):
            LOGGER.info(f"deleting the scheming cache file: {schemingCacheFile}")
            os.remove(schemingCacheFile)

        scheming = CKANScheming.Scheming()
        self.dataCache.setScheming(scheming)

    def checkForRequiredEnvironmentVariables(self):
        """Checks to make sure that required environment variables have been
        populated
        """
        for envVarName in constants.REQUIRED_ENV_VARS:
            if envVarName not in os.environ:
                msg = (
                    f"Script requires the environment variable {envVarName} "
                    "to be populated"
                )
                raise NameError(msg)


if __name__ == "__main__":

    # LOGGING SETUP
    # -----------------------------------------------------------------------
    appDir = os.path.dirname(__file__)

    filePathUtils = CacheFiles.CKANCacheFiles()
    logConfigFile = filePathUtils.getLogConfigFileFullPath()
    print(f"log config file: {logConfigFile}")

    logOutputsFilePath = ''

    logging.config.fileConfig(
        logConfigFile
        #, defaults={"logfilename": logOutputsFilePath}
    )

    # Code here will add a file log file handler if the environment variable
    # LOGGING_OUTPUT_DIR_ENV_VAR is defined with a path to an output
    # log file name, if not then logging will just go to console
    if constants.LOGGING_OUTPUT_FILE_ENV_VAR in os.environ:
        print("config logger")
        logOutputsFilePath = os.environ[constants.LOGGING_OUTPUT_FILE_ENV_VAR]
        fh = logging.FileHandler(logOutputsFilePath)
        rootLogger = logging.getLogger()

        loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
        formatterSet = False
        for logger in loggers:
            if 'bcdc2bcdc' in logger.name:
                if not formatterSet:
                    formatter = logger.handlers[0].formatter
                    fh.setFormatter(formatter)
                    formatterSet = True
                    rootLogger.addHandler(fh)
                logger.addHandler(fh)
        rootLogger.info("new messages")

        LOGGER = logging.getLogger("main")
        LOGGER.addHandler(fh)

    LOGGER = logging.getLogger("main")
    LOGGER.debug(f"__name__ is {__name__}")

    #  RUN SCRIPT
    # -----------------------------------------------------------------------
    updater = RunUpdate()
    updater.checkForRequiredEnvironmentVariables()
    updater.refreshSchemingDefs()

    useCache = False
    # This is complete, commented out while work on group
    # not running user update for now
    if constants.isDataDebug():
        useCache=True
    updater.updateUsers(useCache=useCache)
    updater.updateGroups(useCache=useCache)
    updater.updateOrganizations(useCache=useCache)
    updater.updatePackages(useCache=useCache)
