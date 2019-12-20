


import CKANCompare
import constants
import os

class RunUpdate:

    def __init__(self):
        self.prodWrapper = CKANCompare.CKANWrapper(
            os.environ[constants.CKAN_URL_PROD], 
            os.environ[constants.CKAN_APIKEY_PROD])
        self.testWrapper = CKANCompare.CKANWrapper(
            os.environ[constants.CKAN_URL_TEST], 
            os.environ[constants.CKAN_APIKEY_TEST])

    def compare():
        prodPkgList = self.prodWrapper.getPackageNames()
        testPkgList = self.testWrapper.getPackageNames()
        


if __name__ == '__main__':
    updater = RunUpdate()
