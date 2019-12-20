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
