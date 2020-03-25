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


@pytest.fixture(scope="session")
def CKANWrapperProd(CKANParamsProd):
    yield CKAN.CKANWrapper(
        url=CKANParamsProd["ckanUrl"], apiKey=CKANParamsProd["ckanAPIKey"]
    )
