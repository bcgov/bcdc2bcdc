import logging
import pytest
import constants
import DataCache

LOGGER = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def first_fixture():
    yield 'testtesttest'

@pytest.fixture(scope="session")
def DataCache_fixture():
    """creates a simple datafixture without any data in it.

    :yield: [description]
    :rtype: [type]
    """
    dc = DataCache.DataCache()
    yield dc

@pytest.fixture(scope="session")
def DataCacheWithOrgData(DataCache_fixture, CKAN_Src_OrganizationsCached, CKAN_Dest_OrganizationsCached):
    #     def addRawData(self, rawData, dataType, dataOrigin):

    DataCache_fixture.addRawData(CKAN_Src_OrganizationsCached,
                                 constants.TRANSFORM_TYPE_ORGS,
                                 constants.DATA_SOURCE.SRC)

    DataCache_fixture.addRawData(CKAN_Dest_OrganizationsCached,
                                 constants.TRANSFORM_TYPE_ORGS,
                                 constants.DATA_SOURCE.DEST)
    yield DataCache_fixture





