import os
import CKANTransform
import constants
import pytest
import logging
import tests.helpers.CKANDataHelpers as CKANDataHelpers

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def TransformConfigDir():
    pathHelper = CKANDataHelpers.CKAN_Test_Paths()
    transConfDir = pathHelper.getTransformConfigDir()
    yield transConfDir

@pytest.fixture(scope="session")
def TransformFile():
    pathHelper = CKANDataHelpers.CKAN_Test_Paths()
    transConfDir = pathHelper.getTransformConfigDir()
    yield transConfDir

    transFile = os.path.join(DataDir, constants.TRANSFORM_CONFIG_FILE_NAME)
    yield transFile

@pytest.fixture(scope="session")
def TransformationConfig(TransformFile):
    pathHelper = CKANDataHelpers.CKAN_Test_Paths()
    transConfFile = pathHelper.getTransformConfigFile()
    yield transConfFile

