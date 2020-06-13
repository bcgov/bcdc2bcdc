"""
Used to generate paths to various cache files that are used to
speed up development, by using cached JSON data instead of retrieving
from API.
"""
import logging
import os

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

    def getDebugDataDumpDir(self):
        retDir = os.path.join(self.dir, 'details')
        retDir = os.path.normpath(retDir)
        return retDir

    def getDebugDataPath(self, pkgName, origin, keyword):
        cnt = 1
        resDir = self.getDebugDataDumpDir()
        if not os.path.exists(resDir):
            os.mkdir(resDir)
        while True:
            resPath = os.path.join(resDir, f'{pkgName}_{keyword}_{origin}_{cnt}.json')
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
        tmpPath = self.getDebugDataPath(pkgName, origin, 'RES')
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
