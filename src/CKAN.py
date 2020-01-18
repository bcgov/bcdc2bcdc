"""
simple access to various CKAN methods:

Connection api keys are specified in environment variables defined in:
constants.py
"""


import logging
import os
import pprint

import ckanapi
import requests
import constants

# pylint: disable=logging-format-interpolation

LOGGER = logging.getLogger(__name__)


class CKANWrapper:
    def __init__(self, url=None, apiKey=None):

        if url is None:
            url = os.environ[constants.CKAN_URL_DEST]
        if apiKey is None:
            apiKey = os.environ[constants.CKAN_APIKEY_DEST]

        if not apiKey or not url:
            msg = (
                "Need to either provide ckan url and api key as args "
                + "to this constructor or define them in env vars: "
                + f"{constants.CKAN_URL_DEST} and {constants.CKAN_APIKEY_DEST}"
            )
            raise ValueError(msg)
        self.remoteapi = ckanapi.RemoteCKAN(url, apikey=apiKey)
        self.CKANHeader = {"X-CKAN-API-KEY": apiKey}
        self.CKANUrl = url
        if self.CKANUrl[len(self.CKANUrl) - 1] != "/":
            self.CKANUrl = self.CKANUrl + "/"

        # debug helper
        self.pp = pprint.PrettyPrinter(indent=4)

    def __packageListPaging(self):
        """
        package_list call to prod doesn't properly page.  This is a requests
        based replacement to the same end point.


        """
        packageListEndPoint = "api/3/action/package_list"
        package_list_call = f"{self.CKANUrl}{packageListEndPoint}"

        packageList = []
        params = {"limit": 500, "offset": 0}
        package_list_cnt = 0
        while True:
            LOGGER.debug("offset: %s", params["offset"])
            resp = requests.get(
                package_list_call, headers=self.CKANHeader, params=params
            )
            LOGGER.debug("status: %s", resp.status_code)
            pkg_list = resp.json()
            package_list_cnt = package_list_cnt + len(pkg_list["result"])
            LOGGER.debug(
                "package cnt: %s %s", package_list_cnt, len(pkg_list["result"])
            )
            packageList.extend(pkg_list["result"])
            if len(pkg_list["result"]) < params["limit"]:
                LOGGER.debug("end of pages, breaking out")
                break
            params["offset"] = params["limit"] + params["offset"]
        return packageList

    def getPackageNames(self):
        """Gets a list of package names from the API works through 
        multiple 500 element pages until the call returns empty
        list
        
        :return: a list of package names
        :rtype: list
        """
        packageList = []
        elemCnt = 500
        pageCnt = 0
        LOGGER.info("Getting package names:")

        # CKAN that is currently running in prod will not return a blank
        # list when a page beyond the data is requested, instead it will
        # return the previously page again and again.
        #
        # caching the last page in this directory to detect this condition.
        prevPage = None
        success = True
        while True:

            offset = pageCnt * elemCnt
            LOGGER.info(f"    - page: {pageCnt} {offset} {elemCnt}")

            params = {"limit": elemCnt, "offset": offset}
            # pageData = self.remoteapi.action.package_list(limit=elemCnt,
            #                                              offset=offset)
            pageData = self.remoteapi.action.package_list(context=params)

            if not isinstance(pageData, list):
                LOGGER.debug(f"page data is: {pageData}")
            if packageList:
                LOGGER.debug(f"last element: {packageList[len(packageList) - 1]}")
            if pageData:
                LOGGER.debug(f"first add element: {pageData[0]}")
            LOGGER.debug(f"pageData len: {len(pageData)}")

            # newer ckan will return a blank page when beyond the dataframe
            # older ckan returns the same page again and again.
            if (prevPage) and prevPage == pageData:
                success = False
                break
            if not pageData:
                break

            packageList.extend(pageData)
            pageCnt += 1
            prevPage = pageData

        if not success:
            packageList = self.__packageListPaging()

        return packageList

    def getOrganizationNames(self):
        """retrieves a list of the organizations from CKAN and 
        returns them.
        
        :return: [description]
        :rtype: [type]
        """
        orgList = self.remoteapi.action.organization_list()
        return orgList

    def getUsers(self, includeData=False):
        """gets a list of users in the ckan instance
        
        :param includeData: when set to true returns the full user objects 
            otherwise will only return a list of user names, defaults to False
        :type includeData: bool, optional
        :return: a list of usernames or userdata
        :rtype: list
        """
        params = {"all_fields": includeData}
        users = self.remoteapi.action.user_list(data_dict=params)
        return users
        