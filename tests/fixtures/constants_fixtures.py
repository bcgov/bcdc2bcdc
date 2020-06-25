"""
Defined constants that are used by the tests
"""

import pytest
import logging
import os.path
import tests.helpers.CKANDataHelpers as CKANDataHelpers

LOGGER = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def TestDataDir():
    #dataDir = os.path.join(os.path.dirname(__file__), '..', 'data')
    #dataDir = os.path.abspath(dataDir)
    helper = CKANDataHelpers.CKAN_Test_Paths()
    yield helper.getDataDirFullPath()

@pytest.fixture(scope="session")
def TestJunkDir():
    helper = CKANDataHelpers.CKAN_Test_Paths()
    datadir = helper.getJunkDirFullPath()
    yield datadir

@pytest.fixture(scope="session")
def TestProdUserCacheJsonfile():
    """The cached version of the user data.

    :param TestJunkDir: junk directory where caches are located
    :type TestJunkDir: str
    """
    helper = CKANDataHelpers.CKAN_Test_Paths()
    prodJson = helper.getProdUsersCacheJsonFile()
    yield prodJson

@pytest.fixture(scope="session")
def TestTestUserCacheJsonfile():
    """The cached version of the user data.

    :param TestJunkDir: junk directory where caches are located
    :type TestJunkDir: str
    """
    helper = CKANDataHelpers.CKAN_Test_Paths()
    testJson = helper.getTestUsersCacheJsonFile()
    yield testJson

@pytest.fixture(scope="session")
def TestDestPackageCacheJsonfile():
    helper = CKANDataHelpers.CKAN_Test_Paths()
    testJson = helper.getDestPackagesCacheJsonFile()
    yield testJson

@pytest.fixture(scope="session")
def TestSrcPackageCacheJsonfile():
    helper = CKANDataHelpers.CKAN_Test_Paths()
    testJson = helper.getSrcPackagesCacheJsonFile()
    yield testJson

@pytest.fixture(scope="session")
def TestUserJsonFile():
    helper = CKANDataHelpers.CKAN_Test_Paths()
    testUserDataFilePath = helper.getTestUsersDataFilePath()
    LOGGER.debug("testUserDataFilePath: %s", testUserDataFilePath)
    yield testUserDataFilePath

@pytest.fixture(scope="session")
def TestProdOrgCacheJsonFile():
    helper = CKANDataHelpers.CKAN_Test_Paths()
    prodOrgCacheFilePath = helper.getProdOrgsCacheJsonFile()
    yield prodOrgCacheFilePath

@pytest.fixture(scope="session")
def TestTestOrgCacheJsonFile():
    helper = CKANDataHelpers.CKAN_Test_Paths()
    testOrgCacheFilePath = helper.getTestOrgsCacheJsonFile
    yield testOrgCacheFilePath
