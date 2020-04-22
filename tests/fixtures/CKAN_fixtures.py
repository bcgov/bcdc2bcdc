import json
import logging
import os.path
import time

import pytest

import CKAN
import constants
import tests.helpers.CKANDataHelpers as CKANDataHelpers

LOGGER = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def CKAN_Dest_fixture(CKANParamsTest):
    return CKAN.CKANWrapper(CKANParamsTest['ckanUrl'], CKANParamsTest['ckanAPIKey'])

@pytest.fixture(scope="session")
def CKAN_Src_fixture(CKANParamsProd):
    return CKAN.CKANWrapper(CKANParamsProd['ckanUrl'], CKANParamsProd['ckanAPIKey'])

@pytest.fixture(scope="function")
def CKANDeleteTestUser(CKANParamsTest):
    """makes sure at  the start of every test that uses this fixture the test
    user state is set to 'deleted'.

    Method will ensure the state is 'deleted' before and after yield statement.
    """
    ckan = CKAN.CKANWrapper(CKANParamsTest['ckanUrl'], CKANParamsTest['ckanAPIKey'])

    dataHelper = CKANDataHelpers.CKAN_Test_Data()
    dummyUserData = dataHelper.getTestUserData()

    dummyUser = dummyUserData[constants.TEST_USER_DATA_POSITION]
    if ckan.userExists(dummyUser['name']) and not ckan.userIsDeleted(dummyUser['name']):
        ckan.deleteUser(dummyUser['name'])
        LOGGER.info("deleted the user: %s", dummyUser)
    yield
    if ckan.userExists(dummyUser['name']) and not ckan.userIsDeleted(dummyUser['name']):
        time.sleep(4)
        ckan.deleteUser(dummyUser['name'])
        LOGGER.info("deleted the user: %s", dummyUser)
    return

@pytest.fixture(scope="function")
def CKANAddTestUser(CKANParamsTest):
    """Some methods need the demo user to exist.  This fixture makes sure the
    demo user exists, and that its state is set to 'active'

                "state": "active"

    After yield will revert the state to "deleted"
    """
    ckan = CKAN.CKANWrapper(CKANParamsTest['ckanUrl'], CKANParamsTest['ckanAPIKey'])

    dataHelper = CKANDataHelpers.CKAN_Test_Data()
    dummyUserData = dataHelper.getTestUserData()

    dummyUser = dummyUserData[constants.TEST_USER_DATA_POSITION]
    LOGGER.debug("dummyUSer: %s", dummyUser)
    # make sure the user exists
    if not ckan.userExists(dummyUser['name']):
        retVal = ckan.addUser(dummyUserData)

    # make sure the state is active
    user = ckan.getUser(dummyUser['name'])
    LOGGER.debug("user: %s", user)
    if dummyUser['state'] != 'active':
        dummyUser['state'] = 'active'
        ckan.updateUser(dummyUser)
    yield

    # now delete the user.
    ckan.deleteUser(dummyUser['name'])
    return

def getOrgData(CKANWrapper):

    pathHelper = CKANDataHelpers.CKAN_Test_Paths()
    orgCacheFile = pathHelper.getTestOrgsCacheJsonFile()
    if os.path.exists(orgCacheFile):
        with open(orgCacheFile) as fh:
            orgs = json.load(fh)
    else:
        orgs = CKANWrapper.getOrganizations(includeData=True)
        with open(orgCacheFile, 'w') as fh:
            json.dump(orgs, fh)
    return orgs

@pytest.fixture(scope="session")
def CKAN_Src_OrganizationsCached(CKAN_Src_fixture):
    orgs = getOrgData(CKAN_Src_fixture)
    yield orgs

@pytest.fixture(scope="session")
def CKAN_Dest_OrganizationsCached(CKAN_Dest_fixture):
    orgs = getOrgData(CKAN_Dest_fixture)
    yield orgs


