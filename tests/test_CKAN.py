"""tests the actual api calls to CKAN.

"""

import CKAN
import logging
import pprint
import os
import json

LOGGER = logging.getLogger(__name__)

'''
---------------------------------------------------------
Because you cannot actually delete a user it is impossible to test
adding a user.
---------------------------------------------------------
def test_addUser(CKANData_Test_User_Data_Raw, CKAN_Dest_fixture):
    """Tests ability to add new users to the CKAN instance through the
    API.

    Uses a bunch of test data.  Fixtures should clean this up. after the test
    through additional api calls.

    :param CKANData_User_Data_Raw: [description]
    :type CKANData_User_Data_Raw: [type]
    :param CKAN_Dest_fixture: [description]
    :type CKAN_Dest_fixture: [type]
    """
    LOGGER.debug("user data: %s", pprint.pformat(CKANData_Test_User_Data_Raw))
    # because users cannot be deleted, there is no real way to test adding
    # a user.  The logic used so far has worked.
    retVal = CKAN_Dest_fixture.addUser(CKANData_Test_User_Data_Raw)
    LOGGER.debug("return value from user add: %s", retVal)
'''
def test_getUser(CKAN_Dest_fixture):
    """Tests the ability to retrieve a user
    """
    userList = CKAN_Dest_fixture.getUsers()
    #LOGGER.debug(f"userList: {userList}")
    for cnt in range(0, 5):
        user = CKAN_Dest_fixture.getUser(userList[cnt])
        LOGGER.debug(f"user obj: {user}")
        LOGGER.debug(f"user obj: {userList[cnt]}")
        assert userList[cnt] == user['name']
        LOGGER.debug(f"users: {user}")

    for cnt in range(5, 10):
        user = CKAN_Dest_fixture.getUser({'name' : userList[cnt]})
        LOGGER.debug(f"user obj: {user}")
        LOGGER.debug(f"user obj: {userList[cnt]}")
        assert userList[cnt] == user['name']
        LOGGER.debug(f"users: {user}")



# def test_updateUser(CKAN_Dest_fixture, CKANData_Test_User_Data_Raw,
#                     CKANAddTestUser, CKANDeleteTestUser):
#     """ Will test an update of a field associated with a user.

#     fixture: CKANAddTestUser
#       - makes sure the user exists and calls delete after yield

#     The fixture CKANDeleteTestUser will:
#       - ensure the user exists
#       - also calls delete after yield

#     """
#     LOGGER.info("user data: %s", CKANData_Test_User_Data_Raw)
#     updateValue = CKAN_Dest_fixture.updateUser(CKANData_Test_User_Data_Raw)
#     LOGGER.info("updateValue: %s", updateValue)

# def test_userExists(CKAN_Dest_fixture, CKANData_Test_User_Data_Raw, CKANDeleteTestUser):
#     LOGGER.debug("CKANData_Test_User_Data_Raw: %s", CKANData_Test_User_Data_Raw)
#     assert not CKAN_Dest_fixture.userExists("does not exist user")
#     assert CKAN_Dest_fixture.userExists(CKANData_Test_User_Data_Raw['name'])

def test_getPackagesWithData(CKAN_Dest_fixture, CKAN_Src_fixture):
    curDir = os.path.dirname(__file__)
    destJsonDir = os.path.join(curDir, '..', 'junk')
    destJsonDir = os.path.normpath(destJsonDir)

    if not os.path.exists(destJsonDir):
        msg = f'the directory; {destJsonDir} doesn\'t exist'
        raise IOError(msg)
    destJsonPath = os.path.join(destJsonDir, 'dest_pkgs.json')
    srcJsonPath = os.path.join(destJsonDir, 'src_pkgs.json')


    # destpkgs = CKAN_Dest_fixture.getPackagesAndData()

    # with open(destJsonPath, 'w') as outfile:
    #     json.dump(destpkgs, outfile)

    srcPkgs = CKAN_Src_fixture.getPackagesAndData()
    with open(srcJsonPath, 'w') as outfile:
        json.dump(srcPkgs, outfile)

def test_getOrganization(CKAN_Dest_fixture):

    orgNames = CKAN_Dest_fixture.getOrganizationNames()
    cnt = 0
    for orgName in orgNames:
        if cnt > 10 or cnt > len(orgNames):
            break
        org = CKAN_Dest_fixture.getOrganization({'id': orgName})
        assert org['name'] == orgName
        cnt += 1

def test_getPackage(CKAN_Dest_fixture):
    packages = CKAN_Dest_fixture.getSinglePagePackageNames(offset=0, pageSize=25)
    LOGGER.debug(f"packages: {len(packages)} {packages}")

    for pckgName in packages:
        LOGGER.debug(f"pkgName: {pckgName}")
        pkg = CKAN_Dest_fixture.getPackage({'id': pckgName})
        assert pkg['name'] == pckgName
