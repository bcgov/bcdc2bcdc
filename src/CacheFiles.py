"""
Used to generate paths to various cache files that are used to
speed up development, by using cached JSON data instead of retrieving
from API.
"""
import logging
import os

import constants

LOGGER = logging.getLogger(__name__)


class CKANCacheFiles:

    def __init__(self, dataDir=None):
        self.dir = dataDir
        if not self.dir:
            self.dir = self.getJunkDirPath()
        if not os.path.exists(self.dir):
            LOGGER.info(f"creating the temp dir: {self.dir}")
            os.mkdir(self.dir)

    def getJunkDirPath(self):
        """calculates the path to a Junk dir where the temporary cached
        versions of various objects will be kept

        :return: [description]
        :rtype: [type]
        """

        curDir = os.path.dirname(__file__)
        cacheDirRelative = os.path.join(curDir, '..', constants.CACHE_TMP_DIR)
        cacheDir = os.path.normpath(cacheDirRelative)
        LOGGER.debug(f"cache dir: {cacheDir}")

        return cacheDir

    def getSrcUserJsonPath(self):
        return os.path.join(self.dir, constants.CACHE_SRC_USERS_FILE)

    def getDestUserJsonPath(self):
        return os.path.join(self.dir, constants.CACHE_DEST_USERS_FILE)

    def getDestGroupJsonPath(self):
        return os.path.join(self.dir, constants.CACHE_DEST_GROUPS_FILE)

    def getSrcGroupJsonPath(self):
        return os.path.join(self.dir, constants.CACHE_SRC_GROUPS_FILE)

    def getSrcOrganizationsJsonPath(self):
        return os.path.join(self.dir, constants.CACHE_SRC_ORG_FILE)

    def getDestOrganizationsJsonPath(self):
        return os.path.join(self.dir, constants.CACHE_DEST_ORG_FILE)

    def getSrcPackagesJsonPath(self):
        return os.path.join(self.dir, constants.CACHE_SRC_PKGS_FILE)

    def getDestPackagesJsonPath(self):
        return os.path.join(self.dir, constants.CACHE_DEST_PKGS_FILE)
