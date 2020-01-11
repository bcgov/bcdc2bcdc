"""used to verify methods in ckanCompare

"""

import logging
import pytest
import constants
import CKANTransform


# pylint: disable=logging-format-interpolation

LOGGER = logging.getLogger(__name__)


def test_validateType(Transform):
    validTypes = constants.VALID_TRANSFORM_TYPES
    # assert valid types do not raise exception
    for validType in validTypes:
        Transform.validateType(validType)
    inValidTypes = [i + '_INVALID' for i in validTypes]
    LOGGER.debug(f"invalidTypes: {inValidTypes}")

    # assert invalid types raise exception
    with pytest.raises(CKANTransform.InValidTransformationTypeError):
        for inValidType in inValidTypes:
            Transform.validateType(inValidType)

def test_transform(Transform, transform_user_data):
    Transform.transform
