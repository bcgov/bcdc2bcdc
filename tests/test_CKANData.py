"""used to verify methods in ckanCompare

"""

import logging
import pytest
import constants
import CKANTransform
import pprint


# pylint: disable=logging-format-interpolation

LOGGER = logging.getLogger(__name__)
PP = pprint.PrettyPrinter(indent=4)

def test_UserData(CKANData_User_Data, CKANData_User_Data_Raw):
    compData = CKANData_User_Data.getComparableStruct(CKANData_User_Data_Raw)
    LOGGER.debug(f"compData: {compData}")


    