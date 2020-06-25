import logging
import pprint
import os
import json
import CKAN
import constants

LOGGER = logging.getLogger(__name__)

def test(first_fixture):
    LOGGER.debug(f'first fixture: {first_fixture}')

def test_OrgLookup(DataCache_fixture, CKAN_Src_fixture, CKAN_Dest_fixture, CKANTransform_Fixture):
    # get some orgs
    orgNames = CKAN_Src_fixture.getOrganizationNames()
    assert len(orgNames) > 1

    orgName = orgNames[0]
    query = {constants.CKAN_SHOW_IDENTIFIER: orgName}

    orgDataSrc = CKAN_Src_fixture.getOrganization(query)
    orgDataDest = CKAN_Dest_fixture.getOrganization(query)

    orgFieldMaps = CKANTransform_Fixture.getFieldMappings(constants.TRANSFORM_TYPE_ORGS)

    for orgFieldMap in orgFieldMaps:
        userFieldName = orgFieldMap[constants.FIELD_MAPPING_USER_FIELD]
        autoFieldName = orgFieldMap[constants.FIELD_MAPPING_AUTOGEN_FIELD]
        break

    destAutoID = DataCache_fixture.src2DestRemap(autoFieldName,
                                    constants.TRANSFORM_TYPE_ORGS,
                                    orgDataSrc[autoFieldName])

    assert destAutoID == orgDataDest[autoFieldName]

def test_OrgLookupAllData(DataCacheWithOrgData, CKAN_Dest_OrganizationsCached, CKAN_Src_OrganizationsCached):
    transConf = DataCacheWithOrgData.transConf
    orgFieldMaps = transConf.getFieldMappings(constants.TRANSFORM_TYPE_ORGS)
    for orgFieldMap in orgFieldMaps:
        userFieldName = orgFieldMap[constants.FIELD_MAPPING_USER_FIELD]
        autoFieldName = orgFieldMap[constants.FIELD_MAPPING_AUTOGEN_FIELD]
        break

    for srcOrg in CKAN_Src_OrganizationsCached:
        destAutoGenUniId = DataCacheWithOrgData.src2DestRemap(autoFieldName,
                                           constants.TRANSFORM_TYPE_ORGS,
                                           srcOrg[autoFieldName])
        for destOrgData in CKAN_Dest_OrganizationsCached:
            if srcOrg[userFieldName] == destOrgData[userFieldName]:
                destAutoGenUniIdManual = destOrgData[autoFieldName]
                break
        assert destAutoGenUniId == destAutoGenUniIdManual







