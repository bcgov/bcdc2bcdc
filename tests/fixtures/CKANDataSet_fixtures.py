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
def CKANData_User_Data_Set(CKANData_User_Data_Raw):
    ckanUserDataSet = CKANData.CKANUsersDataSet(CKANData_User_Data_Raw)
    yield ckanUserDataSet

@pytest.fixture(scope="session")
def CKANData_User_Data_Record(CKANData_User_Data_Set):
    ckanUserRecord = CKANData_User_Data_Set.next()
    LOGGER.debug(f"ckanUserRecord:{ckanUserRecord}")
    #ckanUserDataSet = CKANData.CKANUsersDataSet(CKANData_User_Data_Raw, constants.TRANSFORM_TYPE_USERS)
    #yield ckanUserDataSet
    yield ckanUserRecord



# TODO: need a test that verifies the data iterator works when no configuration is 
# found
