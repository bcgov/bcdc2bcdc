"""
simple access to various CKAN methods:

Connection api keys are specified in environment variables defined in:
constants.py
"""

import json
import logging
import os
import pprint
import time
import sys

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
            # resp = requests.get(
            #     package_list_call, headers=self.CKANHeader, params=params
            # )
            resp = self.__getWithRetries(package_list_call, params)
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

    def __getWithRetries(self, endpoint, payload, retries=0):
        maxRetries = 5
        waitTime = 3
        LOGGER.debug(f'end point: {endpoint}')
        resp = requests.get(
            endpoint, headers=self.CKANHeader, params=payload
        )
        LOGGER.debug(f"status_code: {resp.status_code}")
        if resp.status_code != 200:
            LOGGER.warning(f"package_get received non 200 status code: {resp.status_code}")
            if retries < maxRetries:
                retries += 1
                time.sleep(waitTime)
                resp = self.__getWithRetries(endpoint, payload, retries)
            else:
                msg = f'made {retries} attempts to retrieve package data, getting ' + \
                    f'status code: {resp.status_code}.  Unable to complete request'
                raise CKANPackagesGetError(msg)
        return resp

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

    def getPackagesAndData_cached(self, cacheFileName):
        """Used for debugging, re-uses a cached version of the package data
        instead of retrieving it from the api.
        """
        dataDir = constants.getCachedDir()
        cachedPackagesFileName = os.path.join(dataDir, cacheFileName)
        if not os.path.exists(cachedPackagesFileName):
            pkgs = self.getPackagesAndData()
            with open(cachedPackagesFileName, 'w') as fh:
                json.dump(pkgs, fh)
        else:
            with open(cachedPackagesFileName) as fh:
                pkgs = json.load(fh)
        return pkgs

    def getPackagesAndData(self):
        LOGGER.debug(f"url: {self.CKANUrl}")
        packageList = []
        elemCnt = 500
        #elemCnt = 2

        pageCnt = 0
        LOGGER.info("Getting packages with data:")

        packageSearchEndPoint = "api/3/action/package_search"
        package_list_call = f"{self.CKANUrl}{packageSearchEndPoint}"
        LOGGER.debug(f'package_list_call: {package_list_call}')

        packageList = []
        params = {"rows": elemCnt, "start": 1}
        package_list_cnt = 0

        while True:

            offset = pageCnt * elemCnt
            LOGGER.info(f"    - page: {pageCnt} {offset} {elemCnt}")

            params = {"rows": elemCnt, "start": offset}
            resp = self.__getWithRetries(package_list_call, params)
            data = resp.json()
            # also avail is resp['success']
            #LOGGER.debug(f"resp: {data['result']}")
            LOGGER.debug(f"data type: {type(data)}, {len(data['result'])}")
            packageList.extend(data['result']['results'])
            LOGGER.debug(f"number of packages retrieved: {len(packageList)}")


            # import json
            # with open("/mnt/c/Kevin/proj/bcdc_2_bcdc/junk/pkgs_demo.json", 'w') as fh:
            #     json.dump(data, fh)
            # raise
            if len(data["result"]['results']) < params["rows"]:
                LOGGER.debug("end of pages, breaking out")
                break


            pageCnt += 1

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
        LOGGER.debug("getting users")
        params = {"all_fields": includeData}
        try:
            users = self.remoteapi.action.user_list(data_dict=params)
        except requests.exceptions.ConnectionError:
            # try manually using the api end point
            LOGGER.warning('caught error with ckanapi module call, trying directly')
            userListendPoint = 'api/3/action/user_list'
            userListUrl = f"{self.CKANUrl}{userListendPoint}"
            LOGGER.debug(f"userlist url: {userListUrl}")

            resp = requests.get(userListUrl, headers=self.CKANHeader)
            LOGGER.debug(f'status_code: {resp.status_code}')
            userResp = resp.json()
            users = userResp['result']
        LOGGER.info(f"retrieved {len(users)} users")
        return users

    def addUser(self, userData):
        """makes api call to ckan to create a new user

        :param userData: data used to create the user
        :type userData: dict
        """
        #TODO: hasn't been tested... Waiting for proper access to prod.
        LOGGER.debug(f"creating a new user with the data: {userData}")
        retVal = self.remoteapi.action.user_create(**userData)
        LOGGER.debug(f"User Created: {retVal}")
        return retVal

    def updateUser(self, userData):
        """receives a dictionary that it can use to update the data.

        :param userData: a dictionary with the data to use to update an existing
            ckan user
        :type userData: dict
        """

        # wants the id to be the user or the name
        if 'id' not in userData and 'name' in userData:
            userData['id'] = userData['name']
            del userData['name']
        LOGGER.debug(f"trying to update a user using the data: {userData}")
        # data_dict=userData
        retVal = self.remoteapi.action.user_update(**userData)
        LOGGER.debug(f"User Updated: {retVal}")

    def getUser(self, userId):
        LOGGER.debug(f"Getting the information associated with user: {userId}")
        userData = {"id": userId}
        retVal = self.remoteapi.action.user_show(**userData)
        return retVal

    def userExists(self, userId):
        """identify if a specific user exists in a CKAN instance

        :param userId: name or id of the user who's existence is to be tested
        :type userId: str
        :return: boolean indicating if the user exists
        :rtype: bool
        """
        exists = True
        userData = {"id": userId}
        try:
            exists = self.remoteapi.action.user_show(**userData)
        except ckanapi.errors.NotFound:
            exists = False
        return exists

    def userIsDeleted(self, userId):
        retVal = False
        try:
            user = self.getUser(userId)
            if user['state'] == 'deleted':
                retVal = True
        except ckanapi.errors.NotFound:
            LOGGER.info("user %s was not found", userId)
        return retVal

    def deleteUser(self, userId):
        """Deletes a user

        :param userId: either the user 'id' or 'name'
        :type userId: str
        """
        LOGGER.debug(f"trying to delete the user: {userId}")
        userParams = {'id': userId}
        retVal = self.remoteapi.action.user_delete(**userParams)
        LOGGER.debug(f"User Deleted: {retVal}")

    def getGroups(self, includeData=False):
        """Retrieves groups from ckan api

        :param includeData: if set to True will return all the properties of
            groups, otherwise will return only the names
        :type includeData: bool, optional
        :return: list of groups
        :rtype: list (struct)
        """
        groupConfig = {}
        if includeData:
            groupConfig = {
                'order_by': 'name',
                'all_fields': True,
                'include_extras': True,
                'include_tags': True,
                'include_groups': True,
                'include_users': True
            }
        LOGGER.debug(f"groupconfig is {groupConfig}")
        retVal = self.remoteapi.action.group_list(**groupConfig)
        return retVal

    def addGroup(self, groupData):
        """makes an api call to CKAN to create the group described in groupData

        :param groupData: [description]
        :type groupData: [type]
        """
        LOGGER.debug(f'groupData: {groupData}')
        LOGGER.debug(f"creating a new Group with the data: {groupData}")
        retVal = self.remoteapi.action.group_create(**groupData)
        LOGGER.debug(f"Group Created: {retVal}")
        return retVal

    def deleteGroup(self, groupIdentifier=None):
        """Deletes the groups that matches the provided identifying information.
        groupIdentifier can be either the group id or name

        :param groupIdentifier: The unique identifier for the group that
            is to be deleted.  Either 'name' or 'id'
        :type groupIdentifier: str
        """
        LOGGER.info(f"trying to delete the group: {groupIdentifier}")
        orgParams = {'id': groupIdentifier}
        retVal = self.remoteapi.action.group_delete(**orgParams)
        LOGGER.debug("group delete return val: %s", retVal)

    def updateGroup(self, groupData):
        """receives a dictionary that it can use to update the Group data.

        :param groupData: a dictionary with the data to use to update an existing
            ckan user
        :type groupData: dict
        """
        # wants the id to be the group name
        if 'id' not in groupData and 'name' in groupData:
            #groupData['id'] = groupData['name']
            #del groupData['name']
            pass
        LOGGER.debug(f"trying to update a group using the data: {groupData}")
        retVal = self.remoteapi.action.group_update(**groupData)
        LOGGER.debug(f"Group Updated: {retVal}")

    def getOrganizations(self, includeData=False):
        """Gets organizations, if include data is false then will only
        get the names, otherwise will return all the data for the orgs

        :param includeData: [description], defaults to False
        :type includeData: bool, optional
        """
        # TODO: call as it is seems to crash with 502 error. breaking it up into
        #       a paged call
        orgConfig = {}
        organizations = []
        pageSize = 100
        currentPosition = 0
        pageCnt = 1
        if includeData:
            orgConfig = {
                'order_by': 'name',
                'all_fields': True,
                'include_extras': True,
                'include_tags': True,
                'include_groups': True,
                'include_users': True,
                'limit': pageSize,
                'offset': currentPosition
            }
        while True:
            LOGGER.debug(f"OrgConfig is {orgConfig}")
            LOGGER.debug(f"pagecount is {pageCnt}")

            retVal = self.remoteapi.action.organization_list(**orgConfig)
            LOGGER.debug(f"records returned: {len(retVal)}")
            organizations.extend(retVal)

            if not retVal or len(retVal) < pageSize:
                break
            currentPosition = currentPosition + pageSize
            orgConfig['offset'] = currentPosition
            pageCnt += 1
        return organizations

    def deleteOrganization(self, organizationIdentifier=None):
        """Deletes the organization that matches the provided identifying information.
        organizationIdentifier can be either the organization id or name

        :param organizationIdentifier: The unique identifier for the organization that
            is to be deleted.  Either 'name' or 'id'
        :type organizationIdentifier: str
        """
        LOGGER.info(f"trying to delete the organization: {organizationIdentifier}")
        orgParams = {'id': organizationIdentifier}
        retVal = self.remoteapi.action.organization_delete(**orgParams)
        LOGGER.debug("org delete return val: %s", retVal)

    def addOrganization(self, organizationData):
        """creates a new organization

        :param organizationData: creates a new organization
        :type organizationData: struct
        """
        LOGGER.debug(f"creating a new organization with the data: {organizationData}")
        retVal = self.remoteapi.action.organization_create(**organizationData)
        LOGGER.debug(f"Organization Created: {retVal}")

    def updateOrganization(self, organizationData):
        """receives a dictionary that it can use to update the organizations data.

        :param organizationData: a dictionary with the data to use to update an existing
            ckan organization
        :type organizationData: dict
        """
        LOGGER.debug(f"trying to update a organization using the data: {organizationData}")
        retVal = self.remoteapi.action.organization_update(**organizationData)
        LOGGER.debug(f"Organization Updated: {retVal}")



# ----------------- EXCEPTIONS
class CKANPackagesGetError(Exception):
    """CKAN instances seem to randomely go offline when in the middle of paging
    through packages.  Logic implemented to wait and try again.  When the wait
    and try again logic fails this error is raised.
    """
    def __init__(self, message):
        LOGGER.error(f"error message: {message}")
        self.message = message
