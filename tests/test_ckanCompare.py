"""used to verify methods in ckanCompare

"""

import logging
import pytest

LOGGER = logging.getLogger(__name__)


def test_package_list(CKANWrapper):
    pkgNames = CKANWrapper.getPackageNames()
    LOGGER.debug(f"pkgNames: {pkgNames}")
    assert isinstance(pkgNames, list)
    assert len(pkgNames) > 1
    