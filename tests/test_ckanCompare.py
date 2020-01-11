"""used to verify methods in ckanCompare

"""

import logging
import pytest

# pylint: disable=logging-format-interpolation

LOGGER = logging.getLogger(__name__)

def test_package_list_Test(CKANWrapperTest):
    pkgNamesTest = CKANWrapperTest.getPackageNames()
    
    LOGGER.debug(f"pkgNamesTest count: {len(pkgNamesTest)}")

    assert isinstance(pkgNamesTest, list)
    assert len(pkgNamesTest) > 1

def test_package_list_Prod(CKANWrapperProd):
    pkgNamesProd = CKANWrapperProd.getPackageNames()
    
    LOGGER.debug(f"pkgNamesProd count: {len(pkgNamesProd)}")

    assert isinstance(pkgNamesProd, list)
    assert len(pkgNamesProd) > 1

def test_getOrganizationNames_Test(CKANWrapperTest):
    orgNames = CKANWrapperTest.getOrganizationNames()
    LOGGER.debug(f"orgNames test count: {len(orgNames)}")
    assert len(orgNames) > 10
    assert isinstance(orgNames, list)

def test_getOrganizationNames_Prod(CKANWrapperProd):
    orgNames = CKANWrapperProd.getOrganizationNames()
    LOGGER.debug(f"orgNames prod count: {len(orgNames)}")
    assert len(orgNames) > 10
    assert isinstance(orgNames, list)

def test_getUsers_Test(CKANWrapperTest):
    userNames = CKANWrapperTest.getUsers(includeData=True)
    LOGGER.debug(f"userNames: {userNames}")
    LOGGER.debug(f"usernames test count: {len(userNames)}")
    assert len(userNames) > 10
    assert isinstance(userNames, list)
