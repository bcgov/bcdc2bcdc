"""used to verify methods in ckanCompare

"""

import logging
import pytest
import constants
import CKANTransform
import CKANData
import pprint
import copy


# pylint: disable=logging-format-interpolation

LOGGER = logging.getLogger(__name__)
PP = pprint.PrettyPrinter(indent=4)

def test_UserData(CKANData_User_Data, CKANData_User_Data_Raw):
    compData = CKANData_User_Data.getComparableStruct(CKANData_User_Data_Raw)
    LOGGER.debug(f"compData: {compData}")

def test_UserData_Record(CKANData_User_Data_Record):
    compData = CKANData_User_Data_Record.getComparableStruct()
    LOGGER.debug(f"compData: {compData}")

def test_Unique_Field(CKANData_User_Data_Record):
    uniqueIdValue = CKANData_User_Data_Record.getUniqueIdentifier()
    LOGGER.debug(f"uniqueIdValue: {uniqueIdValue}")
    assert uniqueIdValue is not None

def test_Unique_Field_Dataset(CKANData_User_Data_Set):
    uniqueList = CKANData_User_Data_Set.getUniqueIdentifiers()
    LOGGER.debug(f"uniqueList: {uniqueList}")
    assert isinstance(uniqueList, list)
    uniqueListEnforced = list(set(uniqueList))
    assert len(uniqueListEnforced) == len(uniqueList)
    assert len(CKANData_User_Data_Set) == len(uniqueList)

def test_UserData_Dataset_eq_ne(CKANData_User_Data_Raw):
    ckanUserDataSet1 = CKANData.CKANUsersDataSet(CKANData_User_Data_Raw)
    ckanUserDataSet2 = CKANData.CKANUsersDataSet(CKANData_User_Data_Raw)
    isEqual = (ckanUserDataSet2 == ckanUserDataSet1)
    LOGGER.debug(f"isEqual: {isEqual}")
    assert isEqual

    # # remove one of the records 
    CKANData_User_Data_Raw_less_one = copy.deepcopy(CKANData_User_Data_Raw)
    CKANData_User_Data_Raw_less_one =  CKANData_User_Data_Raw_less_one[1:]
    ckanUserDataSet_ne = CKANData.CKANUsersDataSet(CKANData_User_Data_Raw_less_one)
    assert ckanUserDataSet_ne != ckanUserDataSet1
    assert ckanUserDataSet_ne != ckanUserDataSet2

    # change the name in one of the records
    CKANData_User_Data_Raw2 = copy.deepcopy(CKANData_User_Data_Raw)
    CKANData_User_Data_Raw2[0]['name'] = 'billbarillco99'

    LOGGER.debug(f"CKANData_User_Data_Raw2: {CKANData_User_Data_Raw2}")
    LOGGER.debug(f"CKANData_User_Data_Raw: {CKANData_User_Data_Raw}")

    # changing one of the unique identifier fields
    ckanUserDataSet_diffRec = CKANData.CKANUsersDataSet(CKANData_User_Data_Raw2)
    assert ckanUserDataSet_diffRec != ckanUserDataSet1
    assert ckanUserDataSet_diffRec != ckanUserDataSet2
    assert ckanUserDataSet_ne != ckanUserDataSet_diffRec

    # changing one of the user populated values
    CKANData_User_Data_Raw2 = copy.deepcopy(CKANData_User_Data_Raw)
    CKANData_User_Data_Raw2[0]['fullname'] = 'Commander Picard'
    ckanUserDataSet_diffRec = CKANData.CKANUsersDataSet(CKANData_User_Data_Raw2)
    assert ckanUserDataSet_diffRec != ckanUserDataSet1


def test_OrgData_Dataset(CKAN_Cached_Prod_Org_Data, CKAN_Cached_Test_Org_Data):
    """tests retrieval of data
    """
    # make sure something got returned
    assert CKAN_Cached_Prod_Org_Data is not None
    assert CKAN_Cached_Test_Org_Data is not None

    # double check the type of the return data
    LOGGER.debug(f"first record CKAN_Cached_Prod_Org_Data: {CKAN_Cached_Prod_Org_Data[0]}")
    assert isinstance(CKAN_Cached_Prod_Org_Data, list)
    LOGGER.debug(f"first record CKAN_Cached_Test_Org_Data: {CKAN_Cached_Test_Org_Data[0]}")
    assert isinstance(CKAN_Cached_Test_Org_Data, list)

    # just a sanity check here.. should be around 200+ orgs 
    assert len(CKAN_Cached_Prod_Org_Data) > 100
    assert len(CKAN_Cached_Test_Org_Data) > 100

def test_OrgDataDelta(CKAN_Cached_Prod_Org_Data, CKAN_Cached_Test_Org_Data):
    prodOrgCKANDataSet = CKANData.CKANOrganizationDataSet(CKAN_Cached_Prod_Org_Data)
    testOrgCKANDataSet = CKANData.CKANOrganizationDataSet(CKAN_Cached_Test_Org_Data)

    deltaObj = prodOrgCKANDataSet.getDelta(testOrgCKANDataSet)
    LOGGER.info(f"delta obj: {deltaObj}")


def test_OrgRecord(CKAN_Cached_Test_Org_Record):
    LOGGER.debug("testing removal of members of embedded data types ")
    # monkey patching...
    CKAN_Cached_Test_Org_Record.transConf.transConf['users']['ignore_list'].append('bkelsey')
    CKAN_Cached_Test_Org_Record.transConf.transConf['users']['ignore_list'].append('dkelsey')
    comparable = CKAN_Cached_Test_Org_Record.getComparableStruct()
    dataCell = CKANData.DataCell(comparable)
    dataCell = CKAN_Cached_Test_Org_Record.removeEmbeddedIgnores(dataCell)
    LOGGER.debug(f"final modified struct: {dataCell.struct}")
