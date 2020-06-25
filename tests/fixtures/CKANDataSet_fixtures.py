"""[summary]

:return: [description]
:rtype: [type]
"""

import json
import logging
import os
import pickle
import dill
import random
import sys

import pytest

import CKANData
import constants
import DataCache
import tests.helpers.CKANDataHelpers as CKANDataHelpers

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def CKANData_User_Data_Raw():
    """returns a user dataset
    """
    ckanDataHelper = CKANDataHelpers.CKAN_Test_Data()
    ckanTestUserData = ckanDataHelper.getTestUserData()
    yield ckanTestUserData

@pytest.fixture(scope="session")
def CKANData_Test_User_Data_Raw(CKANData_User_Data_Raw):
    UserData = CKANData_User_Data_Raw[constants.TEST_USER_DATA_POSITION]
    UserData['password'] = 'dummy'
    del UserData['id']
    del UserData['number_of_edits']
    del UserData['email_hash']
    del UserData['created']
    del UserData['apikey']
    LOGGER.debug("user: %s", UserData)
    yield UserData

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
def CKAN_Cached_Src_Package_Data(TestSrcPackageCacheJsonfile, CKAN_Src_fixture):
    if not os.path.exists(TestSrcPackageCacheJsonfile):
        pkgDataSrc = CKANWrapperSrc.getPackagesAndData()
        with open(TestSrcPackageCacheJsonfile, 'w') as outfile:
            json.dump(pkgDataSrc, outfile)
    else:
        with open(TestSrcPackageCacheJsonfile) as json_file:
            pkgDataSrc = json.load(json_file)
    yield pkgDataSrc

@pytest.fixture(scope="session")
def CKAN_Cached_Dest_Package_Data(TestDestPackageCacheJsonfile, CKAN_Dest_fixture):
    if not os.path.exists(TestDestPackageCacheJsonfile):
        pkgDataDest = CKANWrapperDest.getPackagesAndData()
        with open(TestDestPackageCacheJsonfile, 'w') as outfile:
            json.dump(pkgDataDest, outfile)
    else:
        with open(TestDestPackageCacheJsonfile) as json_file:
            pkgDataDest = json.load(json_file)
    yield pkgDataDest

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

@pytest.fixture(scope="session")
def DestPkgCacheJsonFilePath(scope="session"):
    pathHelper = CKANDataHelpers.CKAN_Test_Paths()
    DESTPackageFilePath = pathHelper.getDestPackagesCacheJsonFile()
    LOGGER.info(f"src package cached file: {DESTPackageFilePath}")
    yield DESTPackageFilePath

@pytest.fixture(scope="session")
def SrcPkgCacheJsonFilePath(scope="session"):
    pathHelper = CKANDataHelpers.CKAN_Test_Paths()
    SrcPackageFilePath = pathHelper.getSrcPackagesCacheJsonFile()
    LOGGER.info(f"src package cached file: {SrcPackageFilePath}")
    yield SrcPackageFilePath

@pytest.fixture(scope="session")
def CKAN_Cached_Dest_Pkg_Data(DestPkgCacheJsonFilePath, CKANWrapperTest):

    pkgData = CKANWrapperTest.getPackagesAndData_cached(DestPkgCacheJsonFilePath)
    yield pkgData

@pytest.fixture(scope="session")
def CKAN_Cached_Dest_Package_Add_Dataset(CKAN_Cached_Dest_Pkg_Data):
    # have the data now wrap with a dataset
    #with open('junk_dest.json', 'w') as fh:
    #    json.dump(CKAN_Cached_Dest_Pkg_Data, fh)
    cache = DataCache.DataCache()
    ds = CKANData.CKANPackageDataSet(CKAN_Cached_Dest_Pkg_Data, cache)
    yield ds

@pytest.fixture(scope="session")
def CKAN_Cached_Src_Pkg_Data(SrcPkgCacheJsonFilePath, CKANWrapperProd):
    pkgData = CKANWrapperProd.getPackagesAndData_cached(SrcPkgCacheJsonFilePath)
    yield pkgData

@pytest.fixture(scope="session")
def CKAN_Cached_Src_Package_Add_Dataset(CKAN_Cached_Src_Pkg_Data):
    # have the data now wrap with a dataset
    LOGGER.debug("loading data...")
    #with open('junk_src.json', 'w') as fh:
    #    json.dump(CKAN_Cached_Src_Pkg_Data, fh)
    cache = DataCache.DataCache()
    ds = CKANData.CKANPackageDataSet(CKAN_Cached_Src_Pkg_Data, cache)
    LOGGER.debug("loading complete!")
    yield ds

@pytest.fixture(scope="session")
def CKAN_Cached_Pkg_DeltaObj_cached(CKAN_Cached_Dest_Package_Add_Dataset, CKAN_Cached_Src_Package_Add_Dataset):
    # creates a dummy add dataset
    # pkgDelta = CKANData.CKANDataSetDeltas(CKAN_Cached_Src_Package_Add_Dataset,
    #                                       CKAN_Cached_Dest_Package_Add_Dataset)
    cachedPickleFile = 'junk_pickle.p'
    if os.path.exists(cachedPickleFile):
        LOGGER.debug("loading cached data from pickle file")
        deltaObj = pickle.load( open(cachedPickleFile, "rb"))
        # overwrite the transformation config with a fresh set of data as it
        # doesn't take very long to load this data.
        deltaObj.transConf = CKAN_Cached_Dest_Package_Add_Dataset.transConf
    else:
        LOGGER.debug("creating a new cached dataset")

        destDataSet = CKAN_Cached_Dest_Package_Add_Dataset
        srcDataSet = CKAN_Cached_Src_Package_Add_Dataset

        deltaObj = CKANData.CKANDataSetDeltas(srcDataSet, destDataSet)

        dstUniqueIds = set(destDataSet.getUniqueIdentifiers())
        srcUniqueids = set(srcDataSet.getUniqueIdentifiers())

        addList = srcDataSet.getAddList(dstUniqueIds, srcUniqueids)
        deltaObj.setAddDatasets(addList)
        # with open(cachedPickleFile, "wb") as fh:
        #     #pickle.dump( deltaObj, fh)
        #     dill.dump( deltaObj, fh)
        LOGGER.debug('load is complete')

    #delta = srcDataSet.getDelta(destDataSet)
    LOGGER.debug('data loaded')

    # dstUniqueIds = set(destDataSet.getUniqueIdentifiers())
    # srcUniqueids = set(srcDataSet.getUniqueIdentifiers())

    # addList = srcDataSet.getAddList(dstUniqueIds, srcUniqueids)
    # pkgDelta.setAddDataset(addList)
    yield deltaObj

