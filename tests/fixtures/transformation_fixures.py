import os
import CKANTransform
import constants
import pytest
import logging

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def DataDir():
    datadir = os.path.join(
        os.path.basename(__file__), "..", constants.TRANSFORM_CONFIG_DIR
    )
    datadir = os.path.abspath(datadir)
    yield datadir


@pytest.fixture(scope="session")
def TransformFile(DataDir):
    transFile = os.path.join(DataDir, constants.TRANSFORM_CONFIG_FILE_NAME)
    yield transFile


@pytest.fixture(scope="session")
def TransformationConfig(TransformFile):
    LOGGER.debug(f"TransformFile: {TransformFile}")
    trans = CKANTransform.TransformationConfig(TransformFile)
    yield trans

