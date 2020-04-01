import os
import CKAN
import pytest
import constants
import ckanapi


@pytest.fixture(scope="session")
def CKANParamsProd():
    obj = {}
    obj["ckanUrl"] = os.environ[constants.CKAN_URL_SRC]
    obj["ckanAPIKey"] = os.environ[constants.CKAN_APIKEY_SRC]
    yield obj


@pytest.fixture(scope="session")
def CKANParamsTest():
    obj = {}
    obj["ckanUrl"] = os.environ[constants.CKAN_URL_DEST]
    obj["ckanAPIKey"] = os.environ[constants.CKAN_APIKEY_DEST]
    yield obj


@pytest.fixture(scope="session")
def remoteAPITest(CKANParamsTest):
    """
    :return: a remote ckan object with super admin privledges that has been
             authenticated with an api key
    :rtype: ckanapi.RemoteCKAN
    """
    rmt_api = ckanapi.RemoteCKAN(
        CKANParamsTest["ckanUrl"], CKANParamsTest["ckanAPIKey"]
    )
    yield rmt_api


@pytest.fixture(scope="session")
def remoteAPIProd(CKANParamsProd):
    """
    :return: a remote ckan object with super admin privledges that has been
             authenticated with an api key
    :rtype: ckanapi.RemoteCKAN
    """
    rmt_api = ckanapi.RemoteCKAN(
        CKANParamsProd["ckanUrl"], CKANParamsProd["ckanAPIKey"]
    )
    yield rmt_api


@pytest.fixture(scope="session")
def CKANWrapperTest(CKANParamsTest):
    yield CKAN.CKANWrapper(
        url=CKANParamsTest["ckanUrl"], apiKey=CKANParamsTest["ckanAPIKey"]
    )

def CKANWrapperDest(CKANWrapperTest):
    """just returning TEST as slowly try to move to SRC / DEST naming

    :param CKANWrapperTest: the CKAN test data wrapper object
    :type CKANWrapperTest: CKAN.CKANWrapper
    :yield: a ckan api wrapper object that is configured for the Destination
        ckan instance
    :rtype: CKAN.CKANWrapper
    """
    yield CKANWrapperTest

def CKANWrapperSrc(CKANWrapperProd):
    """just returning PROD CKAN api wrapper,
    as slowly try to move to SRC / DEST naming

    :param CKANWrapperTest: the CKAN test data wrapper object
    :type CKANWrapperTest: CKAN.CKANWrapper
    :yield: a ckan api wrapper object that is configured for the Destination
        ckan instance
    :rtype: CKAN.CKANWrapper
    """
    yield CKANWrapperProd


@pytest.fixture(scope="session")
def CKANWrapperProd(CKANParamsProd):
    yield CKAN.CKANWrapper(
        url=CKANParamsProd["ckanUrl"], apiKey=CKANParamsProd["ckanAPIKey"]
    )
