"""used to verify methods in ckanCompare

"""

import logging
import pytest
import constants
import CKANTransform
import CKANData
import pprint
import copy
import tests.helpers.CKANDataHelpers


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

def test_user_diffs(CKAN_Cached_Prod_User_Data_Set, CKAN_Cached_Test_User_Data_Set, TransformationConfig):
    """Gets User Dataset objects for TEST and PROD.

    :param CKAN_Cached_Test_Org_Data: [description]
    :type CKAN_Cached_Test_Org_Data: [type]
    :param CKAN_Cached_Prod_Org_Data: [description]
    :type CKAN_Cached_Prod_Org_Data: [type]
    """
    delta = CKAN_Cached_Prod_User_Data_Set.getDelta(CKAN_Cached_Test_User_Data_Set)

    ignoreList = TransformationConfig.getIgnoreList(CKAN_Cached_Prod_User_Data_Set.dataType)
    LOGGER.info("total users in PROD: %s", len(CKAN_Cached_Prod_User_Data_Set.jsonData))
    LOGGER.info("total users in TEST: %s", len(CKAN_Cached_Test_User_Data_Set.jsonData))
    LOGGER.info("delta between prod / test users: %s", delta)

    deleteNames = delta.getDeleteData()
    LOGGER.info("DELETES: %s", deleteNames)

    addNames = [i['name'] for i in delta.getAddData()]
    LOGGER.info("ADDS: %s", addNames)
    updateNames = [i for i in delta.getUpdateData().keys()]
    LOGGER.info("UPDATES: %s", updateNames)

    # make sure none of the adds are in the ignore list
    for addName in addNames:
        assert addName not in ignoreList

    for deleteName in deleteNames:
        assert deleteName not in ignoreList

    for updateName in updateNames:
        assert updateName not in ignoreList

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
    '''
    test the getDelta method that identifies differences between two
    datasets
    '''
    prodOrgCKANDataSet = CKANData.CKANOrganizationDataSet(CKAN_Cached_Prod_Org_Data)
    testOrgCKANDataSet = CKANData.CKANOrganizationDataSet(CKAN_Cached_Test_Org_Data)

    deltaObj = prodOrgCKANDataSet.getDelta(testOrgCKANDataSet)
    diffs = []
    # print the updates:
    for updtId in deltaObj.updates:
        LOGGER.debug("update: %s", updtId)
        srcRec = prodOrgCKANDataSet.getRecordByUniqueId(updtId)
        destRec = testOrgCKANDataSet.getRecordByUniqueId(updtId)
        diff = srcRec.getDiff(destRec)
        diffs.append(diff)
        LOGGER.debug("update: %s", updtId)
        LOGGER.debug("diff: %s", pprint.pformat(diffs))

    LOGGER.info(f"delta obj: {deltaObj}")

def test_OrgDataRecordDelta(CKAN_Cached_Prod_Org_Data, CKAN_Cached_Test_Org_Data):
    srcOrgCKANDataSet = CKANData.CKANOrganizationDataSet(CKAN_Cached_Prod_Org_Data)
    destOrgCKANDataSet = CKANData.CKANOrganizationDataSet(CKAN_Cached_Test_Org_Data)

    #dstUniqueIds = set(destOrgCKANDataSet.getUniqueIdentifiers())
    #srcUniqueIds = set(srcOrgCKANDataSet.getUniqueIdentifiers())

    # check the first record in the dataset
    srcRecord = srcOrgCKANDataSet.next()
    srcRecordId = srcRecord.getUniqueIdentifier()
    destRecord = destOrgCKANDataSet.getRecordByUniqueId(srcRecordId)

    if srcRecord == destRecord:
        LOGGER.debug("records are equal")
    else:
        LOGGER.debug("not equal")

def test_OrgRecord_removeEmbedded(CKAN_Cached_Test_Org_Record, TransformationConfig):
    """ CKAN can have embedded data structures.  Example a org can have
    users embedded in it.  The ETL script wants to ignores some users.
    removal of embedded structs will detect embedded structs by their
    properties and remove them from datasets that are being compared.


    :param CKAN_Cached_Test_Org_Record: a ckan record that can be used to test
        removal of embedded data
    :type CKAN_Cached_Test_Org_Record: CKANData.CKANRecord
    """
    LOGGER.debug("testing removal of members of embedded data types ")
    # monkey patching...
    CKAN_Cached_Test_Org_Record.transConf.transConf['users']['ignore_list'].append('bkelsey')
    CKAN_Cached_Test_Org_Record.transConf.transConf['users']['ignore_list'].append('dkelsey')
    comparable = CKAN_Cached_Test_Org_Record.getComparableStruct()
    dataCell = CKANData.DataCell(comparable)
    dataCell = CKAN_Cached_Test_Org_Record.removeEmbeddedIgnores(dataCell)
    LOGGER.debug(f"final modified struct: {dataCell.struct}")

    # now dataCell.struct should contain a different data structure where
    # the embedded data that should be ignored has been removed.
    usersIgnore = TransformationConfig.getIgnoreList(constants.TRANSFORM_TYPE_USERS)
    for ignoreUser in usersIgnore:
        for userObj in dataCell.struct[constants.TRANSFORM_TYPE_USERS]:
            assert userObj['name'] != ignoreUser

def test_Org_Dataset_EmbedScrub(CKAN_Cached_Test_Org_Data_Set):
    """Will use a cached data set, and iterate over each record verifying that
    the ignore data does not exist after it has been removed
    """
    for CKANOrgRecord in CKAN_Cached_Test_Org_Data_Set:
        LOGGER.debug(f"Org record: {CKANOrgRecord}")
        compStruct = CKANOrgRecord.getComparableStruct()
        dataCell = CKANData.DataCell(compStruct)
        dataCellNoIgnores = CKANOrgRecord.removeEmbeddedIgnores(dataCell)
        # now use the helper to make sure that all embeds have been removed.
        ignoreChecker = tests.helpers.CKANDataHelpers.CheckForIgnores(dataCellNoIgnores.struct)
        hasIgnores = ignoreChecker.hasIgnoreUsers()
        LOGGER.debug(f"HAS IGNORES: {hasIgnores}")
        if hasIgnores:
            LOGGER.error(f"ignores not removed: {dataCellNoIgnores.struct}")
        assert not hasIgnores

def test_Package_DataSet(CKAN_Cached_Src_Package_Data, CKAN_Cached_Dest_Package_Data):
    """used as a verification that the cached data retrieval is working

    :param CKAN_Cached_Src_Package_Data: a struct containing the data from the source
        ckan instance
    :type CKAN_Cached_Src_Package_Data: list
    :param CKAN_Cached_Dest_Package_Data: a list of dicts representing the package
        data that comes from the  destination ckan instance
    :type CKAN_Cached_Dest_Package_Data: list of dicts
    """
    LOGGER.debug(f"first record SRC: {CKAN_Cached_Src_Package_Data[0]['name']}")
    LOGGER.debug(f"first record DEST: {CKAN_Cached_Dest_Package_Data[0]['name']}")

def test_Package_Delta(CKAN_Cached_Src_Package_Data, CKAN_Cached_Dest_Package_Data):
    #LOGGER.debug(f"{CKAN_Cached_Src_Package_Data[0]}")
    #LOGGER.debug(f"{CKAN_Cached_Dest_Package_Data[0]}")

    srcPkgCKANDataSet = CKANData.CKANPackageDataSet(CKAN_Cached_Src_Package_Data)
    destPkgCKANDataSet = CKANData.CKANPackageDataSet(CKAN_Cached_Dest_Package_Data)

    srcRecord = srcPkgCKANDataSet.next()
    srcRecordId = srcRecord.getUniqueIdentifier()
    # keep iterating over the source record until one is found that
    # exists in the destination
    destRecord = destPkgCKANDataSet.getRecordByUniqueId(srcRecordId)
    while destRecord is None:
        LOGGER.debug("getting anther record...")
        srcRecord = srcPkgCKANDataSet.next()
        srcRecordId = srcRecord.getUniqueIdentifier()
        LOGGER.debug(f"src record id: {srcRecordId}")
        destRecord = destPkgCKANDataSet.getRecordByUniqueId(srcRecordId)

    LOGGER.debug(f'srcRecord: {srcRecord}')
    LOGGER.debug(f'destRecord: {destRecord}')
    LOGGER.debug("have source and dest record")
    if srcRecord == destRecord:
        LOGGER.debug("records are equal")
    else:
        LOGGER.debug("not equal")

def test_addAutoGenFields(CKAN_Cached_Pkg_DeltaObj_cached):
    # CKAN_Cached_Dest_Pkg_AddData: contains an add dataset, can now use to
    #    debug the add fields stuff.
    # destDataSet = CKAN_Cached_Dest_Package_Add_Dataset
    # srcDataSet = CKAN_Cached_Src_Package_Add_Dataset

    # dstUniqueIds = set(destDataSet.getUniqueIdentifiers())
    # srcUniqueids = set(srcDataSet.getUniqueIdentifiers())

    # addList = srcDataSet.getAddList(dstUniqueIds, srcUniqueids)


    adds = CKAN_Cached_Pkg_DeltaObj_cached.getAddData()
    pass