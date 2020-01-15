"""[summary]

:return: [description]
:rtype: [type]
"""

import os
import constants
import pytest
import json
import logging
import CKANData

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def CKANData_User_Data_Raw(TestUserJsonFile):
    """returns a user dataset
    """
    with open(TestUserJsonFile) as json_file:
        CkanUserData = json.load(json_file)
    yield CkanUserData

@pytest.fixture(scope="session")
def CKANData_User_Data(CKANData_User_Data_Raw):
    ckanUserData = CKANData.CKANData(CKANData_User_Data_Raw, constants.TRANSFORM_TYPE_USERS)
    yield ckanUserData
