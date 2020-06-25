
import CKANData
import constants
import logging
import CKANTransform
import json
import os

LOGGER = logging.getLogger(__name__)

class CheckForIgnores:

    def __init__(self, ckanStruct):
        self.ckanStruct = ckanStruct

    def hasIgnoreUsers(self):
        userIgnoreList = []
        transConf = CKANTransform.getTransformationConfig()
        transUserConf = transConf[constants.TRANSFORM_TYPE_USERS]
        if constants.TRANSFORM_TYPE_USERS in transUserConf:
            userIgnoreList = transUserConf[constants.TRANSFORM_TYPE_USERS]

        retVal = False
        for user in userIgnoreList:
            if user['name'] in userIgnoreList:
                retVal = True
                logging.warning("found the ignore user: %s", user['name'])
                break
        return retVal

class CKAN_Test_Paths:
    """Putting logic used by figures into this helper so that it can be easily
    retrieved by fixtures with different scopes.  Eliminates the issues that
    come up when trying to combine fixtures with different scopes.
    """
    def __init__(self):

        # datadir is part of the repo... where dummy data gets located that is used
        # by tests
        self.datadirName = 'data'
        # junk not part of the repo... a temporary holding place for cached data.
        # is in the .gitignore and should NEVER be part of the repo.
        self.junkdirName = 'junk'

    def getDataDirFullPath(self):
        """Gets the data dir used for tests

        :return: data dir used for tests
        :rtype: str, path
        """
        dataDir = os.path.join(os.path.dirname(__file__), '..', self.datadirName)
        dataDir = os.path.abspath(dataDir)
        return dataDir

    def getJunkDirFullPath(self):
        """gets the junk dir used for tests.. junk dir is where data can be
        cached.. not part of repo.  Should be in the .gitignore.

        :return: test junk dir
        :rtype: str
        """
        junkDir = os.path.join(os.path.dirname(__file__), '..', self.junkdirName)
        dataDir = os.path.abspath(junkDir)
        return dataDir

    def getTransformConfigDir(self):
        """The config dir used for the transformation configuration.

        :yield: config dir used for config files for the transformation
        :rtype: str, path
        """
        datadir = os.path.join(
            os.path.basename(__file__), "..", constants.TRANSFORM_CONFIG_DIR
        )
        datadir = os.path.abspath(datadir)
        return datadir

    def getProdUsersCacheJsonFile(self):
        """returns the full path to the the file used to cache the
        prod users

        :return: path to json with prod users
        :rtype: str, path
        """
        junkDir = self.getJunkDirFullPath()
        prodUserFile = os.path.join(junkDir, constants.CACHE_PROD_USERS_FILE)
        return prodUserFile

    def getDestPackagesCacheJsonFile(self):
        """Gets the cached version of the destination packaged data.

        :return: the full path to where a cached version of destination data can
            be found.  Not live or necessarily relective of what is contained
            in the actual ckan instance.  Used to speed up testing so as retrieval
            of pkg data can be time consuming
        :rtype: str, path
        """
        junkDir = self.getJunkDirFullPath()
        destPkgFile = os.path.join(junkDir, constants.CACHE_DEST_PKGS_FILE)
        return destPkgFile

    def getSrcPackagesCacheJsonFile(self):
        """Gets the cached version of the source packaged data.

        :return: the full path to where a cached version of source data can
            be found.  Not live or necessarily relective of what is contained
            in the actual ckan instance.  Used to speed up testing so as retrieval
            of pkg data can be time consuming
        :rtype: str, path
        """
        junkDir = self.getJunkDirFullPath()
        srcPkgFile = os.path.join(junkDir, constants.CACHE_SRC_PKGS_FILE)
        return srcPkgFile

    def getTestUsersCacheJsonFile(self):
        """returns the full path to the the file used to cache the
        prod users

        :return: path to json with prod users
        :rtype: str, path
        """
        junkDir = self.getJunkDirFullPath()
        prodUserFile = os.path.join(junkDir, constants.CACHE_TEST_USERS_FILE)
        return prodUserFile

    def getTestUsersDataFilePath(self):
        """returns the dummy / test data path that is used for various types of user
        testing

        :return: the path to where the dummy / test user data is located
        :rtype: str, path
        """
        TestDataDir = self.getDataDirFullPath()
        return  os.path.join(TestDataDir, constants.TEST_USER_DATA_FILE)

    def getTransformConfigFile(self):
        """retrieves the full path to the transformation config file

        :return: The transformation config file path
        :rtype: str, path
        """
        transDir = self.getTransformConfigDir()
        transFile = os.path.join(transDir, constants.TRANSFORM_CONFIG_FILE_NAME)
        LOGGER.debug(f"getting the trans config file: {transFile}")
        return transFile

    def getTestOrgsCacheJsonFile(self):
        """returns the full path to the the file used to cache the
        prod users

        :return: path to json with prod users
        :rtype: str, path
        """
        junkDir = self.getJunkDirFullPath()
        testOrgFile = os.path.join(junkDir, constants.CACHE_TEST_ORG_FILE)
        return testOrgFile

    def getProdOrgsCacheJsonFile(self):
        """returns the full path to the the file used to cache the
        prod users

        :return: path to json with prod users
        :rtype: str, path
        """
        junkDir = self.getJunkDirFullPath()
        prodOrgFile = os.path.join(junkDir, constants.CACHE_PROD_ORG_FILE)
        return prodOrgFile

class CKAN_Test_Data:
    """wrapper that helps retrieve information from
    """

    def __init__(self):
        self.dataPathHelper = CKAN_Test_Paths()

    def getTestUserData(self):
        dummyDataPath = self.dataPathHelper.getTestUsersDataFilePath()
        with open(dummyDataPath) as f:
            dummyUserData = json.load(f)
        return dummyUserData
