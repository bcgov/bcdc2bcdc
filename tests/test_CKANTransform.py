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

def test_validateType(Transform):
    validTypes = constants.VALID_TRANSFORM_TYPES
    # assert valid types do not raise exception
    for validType in validTypes:
        CKANTransform.validateType(validType)
    inValidTypes = [i + '_INVALID' for i in validTypes]
    LOGGER.debug(f"invalidTypes: {inValidTypes}")

    # assert invalid types raise exception
    with pytest.raises(CKANTransform.InValidTransformationTypeError):
        for inValidType in inValidTypes:
            CKANTransform.validateType(inValidType)

def test_transformConfig(TransformationConfig):
    userType = constants.TRANSFORM_TYPE_USERS
    orgType = constants.TRANSFORM_TYPE_ORGS
    config_user_auto = TransformationConfig.getAutoPopulatedProperties(userType)
    config_user_user = TransformationConfig.getUserPopulatedProperties(userType)
    spacer = "_" * 100
    LOGGER.debug(f"{spacer}")
    LOGGER.debug(f"config_auto: {config_user_auto}")
    LOGGER.debug(f"config_user: {config_user_user}")

    config_org_user = TransformationConfig.getUserPopulatedProperties(orgType)
    LOGGER.debug(f"config_org_user: {config_org_user}")

