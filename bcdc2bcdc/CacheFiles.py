"""
Used to generate paths to various cache files that are used to
speed up development, by using cached JSON data instead of retrieving
from API.
"""
import logging
import os
import sys

import bcdc2bcdc.constants as constants

LOGGER = logging.getLogger(__name__)

# pylint: disable=logging-format-interpolation

class CKANCacheFiles:
    """
    Common methods to retrieve various cache file absolute paths
    """

    def __init__(self, dataDir=None):
        self.dir = dataDir
        if not self.dir:
            self.dir = self.getJunkDirPath()
        if not os.path.exists(self.dir):
            LOGGER.info(f"creating the temp dir: {self.dir}")
            os.mkdir(self.dir)

    def getLogConfigFileFullPath(self):
        """Calculates the path to the log config file relative to the
        path of this module

        :return: log configuration file for BCDC2BCDC
        :rtype: str, path
        """
        logConfFilePath = os.path.join(os.path.dirname(__file__), '..',
                     constants.TRANSFORM_CONFIG_DIR,
                     constants.LOGGING_CONFIG_FILE_NAME)
        logConfFilePath = os.path.abspath(logConfFilePath)
        return logConfFilePath

    def getJunkDirPath(self):
        """calculates the path to a Junk dir where the temporary cached
        versions of various objects will be kept.

        :return: the path to the junk directory
        :rtype: str (path)
        """
        curDir = os.path.dirname(__file__)
        cacheDirRelative = os.path.join(curDir, "..", constants.CACHE_TMP_DIR)
        cacheDir = os.path.normpath(cacheDirRelative)
        LOGGER.debug(f"cache dir: {cacheDir}")
        return cacheDir

    def getCreateDataDumpDir(self):
        """Searches for a sub directory in the temp data directory that does
        not exist.  Increments a count onto the end of the directory name until
        it finds a directory that doesn't exist, creates it, and returns that
        name

        :return: [description]
        :rtype: [type]
        """
        cnt = 1
        while True:
            dirName = f'details_{cnt}'
            cacheDirRelative = os.path.join(self.dir, dirName)
            cacheDir = os.path.normpath(cacheDirRelative)
            if not os.path.exists(cacheDir):
                LOGGER.debug(f"cache dir: {cacheDir}")
                os.mkdir(cacheDir)
                break
            cnt += 1
        return cacheDir

    def getDebugDataDumpDir(self):
        """Similar to getCreateDataDumpDir() however this method returns the
        name of the data directory that was most recently created.  Example
        if there are the following directories:
            * details_1
            * details_2
            * details_3

        This method will return the full path to details_3 as it has the highest
        number

        :return: full path to the most recently create debug dir
        :rtype: str (path)
        """
        cnt = 1

        while True:
            dirName = f'details_{cnt}'
            cacheDirRelative = os.path.join(self.dir, dirName)
            cacheDir = os.path.normpath(cacheDirRelative)
            if not os.path.exists(cacheDir):
                if cnt != 1:
                    dirName = f'details_{cnt-1}'
                    cacheDirRelative = os.path.join(self.dir, dirName)
                    cacheDir = os.path.normpath(cacheDirRelative)
                LOGGER.debug(f"current cache dir: {cacheDir}")
                break
            cnt += 1
        return cacheDir

    def getDebugDataPath(self, pkgName, origin=None, keyword=None):
        cnt = 1
        resDir = self.getDebugDataDumpDir()
        if not os.path.exists(resDir):
            os.mkdir(resDir)
        while True:
            fileName = f'{pkgName}'
            if keyword:
                fileName = f'{fileName}_{keyword}'
            if origin:
                fileName = f'{fileName}_{origin}'
            fileName = f'{fileName}_{cnt}.json'
            resPath = os.path.join(resDir, fileName)
            if not os.path.exists(resPath):
                break
            else:
                cnt += 1
        return resPath

    def getResourceFilePath(self, pkgName, origin):
        """using the input package name appends _res_{cnt} to the end
        of the path and adds a .json suffix.

        If the cnt exists then it increments until it finds a path that
        does not exist

        :param pkgName: [description]
        :type pkgName: [type]
        """
        tmpPath = self.getDebugDataPath(pkgName, origin=origin, keyword='RES')
        return tmpPath

    def getDataTypeFilePath(self, name, dataType, origin=None):
        """[summary]

        :param name: [description]
        :type name: [type]
        :param dataType: [description]
        :type dataType: [type]
        :param origin: [description], defaults to None
        :type origin: [type], optional
        """
        tmpPath = self.getDebugDataPath(name, keyword=dataType.upper())
        return tmpPath

    def getSrcUserJsonPath(self):
        """Gets the cache file for the source instance 'user' cache file

        :return: 'users' cache file absolute path
        :rtype: str (path)
        """
        return os.path.join(self.dir, constants.CACHE_SRC_USERS_FILE)

    def getDestUserJsonPath(self):
        """The Destination 'user' cache file.  This is the cache file where
        the user data retrieved from the destination instance is cached

        :return: 'users' from destination cache file path
        :rtype: str (path)
        """
        return os.path.join(self.dir, constants.CACHE_DEST_USERS_FILE)

    def getDestGroupJsonPath(self):
        """The Destination 'groups' cache file. Cache file where group data
        retrieved from the destination CKAN instance is cached.

        :return: cache file that will be used for 'group' data from destination
        :rtype: str (path)
        """
        return os.path.join(self.dir, constants.CACHE_DEST_GROUPS_FILE)

    def getSrcGroupJsonPath(self):
        """The source 'groups' cache file. Cache file where group data
        retrieved from the source CKAN instance is cached.

        :return: cache file that will be used for 'group' data from source
        :rtype: str
        """
        return os.path.join(self.dir, constants.CACHE_SRC_GROUPS_FILE)

    def getSrcOrganizationsJsonPath(self):
        """The source 'organizations' cache file. Cache file where organizations
        data retrieved from the source CKAN instance is cached.

        :return: cache file that will be used for 'organization' data from source
        :rtype: str
        """
        return os.path.join(self.dir, constants.CACHE_SRC_ORG_FILE)

    def getDestOrganizationsJsonPath(self):
        """The destination 'organizations' cache file. Cache file where organizations
        data retrieved from the destination CKAN instance is cached.

        :return: cache file that will be used for 'organization' data from destination
        :rtype: str
        """
        return os.path.join(self.dir, constants.CACHE_DEST_ORG_FILE)

    def getSrcPackagesJsonPath(self):
        """The source 'packages' cache file. Cache file where packages
        data retrieved from the source CKAN instance is cached.

        :return: cache file that will be used for 'packages' data from source
        :rtype: str
        """
        return os.path.join(self.dir, constants.CACHE_SRC_PKGS_FILE)

    def getDestPackagesJsonPath(self):
        """The destination 'packages' cache file. Cache file where packages
        data retrieved from the destination CKAN instance is cached.

        :return: cache file that will be used for 'packages' data from destination
        :rtype: str
        """
        return os.path.join(self.dir, constants.CACHE_DEST_PKGS_FILE)

    def getSchemingCacheFilePath(self):
        """The destination 'packages' cache file. Cache file where packages
        data retrieved from the destination CKAN instance is cached.

        :return: cache file that will be used for 'packages' data from destination
        :rtype: str
        """
        return os.path.join(self.dir, constants.CACHE_SCHEMING_FILE)
