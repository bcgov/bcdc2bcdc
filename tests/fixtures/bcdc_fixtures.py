
import os

@pytest.fixture(scope="session")
def remoteAPI():
    '''
    :return: a remote ckan object with super admin privs that has been
             authenticated with an api key
    :rtype: ckanapi.RemoteCKAN
    '''
    ckanUrl = os.environ['CKAN_URL']
    ckanAPIKey = os.environ['CKAN_API_KEY']

    rmt_api = ckanapi.RemoteCKAN(ckan_url, ckanAPIKey)
    yield rmt_api
