"""
Defined constants that are used by the tests
"""

import pytest
import logging
import os.path

LOGGER = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def TestDataDir():
    dataDir = os.path.join(os.path.dirname(__file__), '..', 'data')
    dataDir = os.path.abspath(dataDir)
    yield dataDir

@pytest.fixture(scope="session")
def TestJunkDir():
    dataDir = os.path.join(os.path.dirname(__file__), '..', 'junk')
    dataDir = os.path.abspath(dataDir)
    yield dataDir


@pytest.fixture(scope="session")
def TestUserJsonFile(TestDataDir):
    yield os.path.join(TestDataDir, 'users_src.json')

@pytest.fixture(scope="session")
def TestProdOrgCacheJsonFile(TestJunkDir):
    orgCacheFile = os.path.join(TestJunkDir, 'prod_org.json')
    yield orgCacheFile

@pytest.fixture(scope="session")
def TestTestOrgCacheJsonFile(TestJunkDir):
    orgCacheFile = os.path.join(TestJunkDir, 'test_org.json')
    yield orgCacheFile



