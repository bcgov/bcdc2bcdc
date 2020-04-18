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
    LOGGER.debug(f"transConfDir: {transConfDir}")
    yield transConfDir

@pytest.fixture(scope="session")
def TransformFile():
    pathHelper = CKANDataHelpers.CKAN_Test_Paths()
    transConfDir = pathHelper.getTransformConfigDir()
    LOGGER.debug(f"transConfDir: {transConfDir}")
    #yield transConfDir

    transFile = os.path.join(transConfDir, constants.TRANSFORM_CONFIG_FILE_NAME)
    yield transFile

@pytest.fixture(scope="session")
def TransformationConfigFile(TransformFile):
    pathHelper = CKANDataHelpers.CKAN_Test_Paths()
    transConfFile = pathHelper.getTransformConfigFile()
    yield transConfFile

@pytest.fixture(scope="session")
def CKANTransform_Fixture(TransformationConfigFile):
    transConf = CKANTransform.TransformationConfig(TransformationConfigFile)
    yield transConf

@pytest.fixture(scope="session")
def AutoGenFieldList_v1(CKANTransform_Fixture):
    flds2Add = transConf.getFieldsToIncludeOnAdd()
    yield flds2Add

