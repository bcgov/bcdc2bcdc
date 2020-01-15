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
    return dataDir

@pytest.fixture(scope="session")
def TestUserJsonFile(TestDataDir):
    return os.path.join(TestDataDir, 'users_src.json')

