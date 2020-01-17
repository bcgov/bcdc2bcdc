


import CKANCompare
import CKANData
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

    def comparePackages(self):

        # TODO: need to complete this method... ... left incomplete while work on 
        #       org compare and update instead.
        prodPkgList = self.prodWrapper.getPackageNames()
        testPkgList = self.testWrapper.getPackageNames()

    def compareUsers(self):
        userDataProd = self.prodWrapper.getUsers(includeData=True)
        userDataTest = self.testPkgList.getUsers(includeData=True)

        prodUserCKANDataSet = CKANData.CKANUsersDataSet(userListProd)
        testUserCKANDataSet = CKANData.CKANUsersDataSet(userListTest)

        


        


if __name__ == '__main__':
    updater = RunUpdate()
