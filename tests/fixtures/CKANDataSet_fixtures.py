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

# @pytest.fixture(scope="session")
# def CKAN_Prod_data


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

@pytest.fixture(scope="session")
def CKAN_Cached_Prod_User_Data(TestProdUserCacheJsonfile, CKANWrapperProd):
    """Checks to see if a cache file exists in the junk directory.  If it does 
    load the data from there otherwise will make an api call, cache the data for
    next time and then return the org data

    This method returns the prod data
    """
    if not os.path.exists(TestProdUserCacheJsonfile):
        userDataProd = CKANWrapperProd.getUsers(includeData=True)
        with open(TestProdUserCacheJsonfile, 'w') as outfile:
            json.dump(userDataProd, outfile)
    else:
        with open(TestProdUserCacheJsonfile) as json_file:
            userDataProd = json.load(json_file)
    yield userDataProd

@pytest.fixture(scope="session")
def CKAN_Cached_Test_User_Data(TestTestUserCacheJsonfile, CKANWrapperTest):
    """Checks to see if a cache file exists in the junk directory.  If it does 
    load the data from there otherwise will make an api call, cache the data for
    next time and then return the org data

    This method returns the prod data
    """
    if not os.path.exists(TestTestUserCacheJsonfile):
        userDataTest = CKANWrapperTest.getUsers(includeData=True)
        with open(TestTestUserCacheJsonfile, 'w') as outfile:
            json.dump(userDataTest, outfile)
    else:
        with open(TestTestUserCacheJsonfile) as json_file:
            userDataTest = json.load(json_file)
    yield userDataTest

@pytest.fixture(scope="session")
def CKAN_Cached_Test_User_Data_Set(CKAN_Cached_Test_User_Data):
    ds = CKANData.CKANUsersDataSet(CKAN_Cached_Test_User_Data)
    yield ds

@pytest.fixture(scope="session")
def CKAN_Cached_Prod_User_Data_Set(CKAN_Cached_Prod_User_Data):
    ds = CKANData.CKANUsersDataSet(CKAN_Cached_Prod_User_Data)
    yield ds

@pytest.fixture(scope="session")
def CKAN_Cached_Prod_Org_Data(TestProdOrgCacheJsonFile, CKANWrapperProd):
    """Checks to see if a cache file exists in the junk directory.  If it does 
    load the data from there otherwise will make an api call, cache the data for
    next time and then return the org data

    This method returns the prod data
    """
    #CKANWrapperProd
    if not os.path.exists(TestProdOrgCacheJsonFile):
        orgDataProd = CKANWrapperProd.getOrganizations(includeData=True)
        with open(TestProdOrgCacheJsonFile, 'w') as outfile:
            json.dump(orgDataProd, outfile)
    else:
        with open(TestProdOrgCacheJsonFile) as json_file:
            orgDataProd = json.load(json_file)
    yield orgDataProd

@pytest.fixture(scope="session")
def CKAN_Cached_Test_Org_Data(TestTestOrgCacheJsonFile, CKANWrapperTest):
    """Checks to see if a cache file exists in the junk directory.  If it does 
    load the data from there otherwise will make an api call, cache the data for
    next time and then return the org data

    This method returns the prod data
    """
    if not os.path.exists(TestTestOrgCacheJsonFile):
        orgDataTest = CKANWrapperTest.getOrganizations(includeData=True)
        with open(TestTestOrgCacheJsonFile, 'w') as outfile:
            json.dump(orgDataTest, outfile)
    else:
        with open(TestTestOrgCacheJsonFile) as json_file:
            orgDataTest = json.load(json_file)
    yield orgDataTest

@pytest.fixture(scope="session")
def CKAN_Cached_Test_Org_Data_Set(CKAN_Cached_Test_Org_Data):
    ds = CKANData.CKANOrganizationDataSet(CKAN_Cached_Test_Org_Data)
    yield ds

@pytest.fixture(scope="session")
def CKAN_Cached_Test_Org_Record(CKAN_Cached_Test_Org_Data_Set):
    rec = CKAN_Cached_Test_Org_Data_Set.next()
    yield rec


