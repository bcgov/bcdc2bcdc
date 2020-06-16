"""
simple access to various CKAN methods:

Connection api keys are specified in environment variables defined in:
constants.py
"""

import concurrent.futures
import itertools
import json
import logging
import os
import pprint
import time
import urllib.parse
from ast import literal_eval

import ckanapi
import requests

import bcdc2bcdc.CacheFiles as CacheFiles
import bcdc2bcdc.constants as constants

# pylint: disable=logging-format-interpolation

LOGGER = logging.getLogger(__name__)

# TODO: remove the requirement of the ckanapi module.  Doesn't always calculate
#       the correct url.  Just use requests, gives more control anyway


class CKANParams:
    def __init__(self):
        if constants.CKAN_URL_SRC not in os.environ:
            msg = "The environment variable: CKAN_URL_SRC is not defined"
            raise ValueError(msg)
        self.srcUrl = os.environ[constants.CKAN_URL_SRC]

        if constants.CKAN_APIKEY_SRC not in os.environ:
            msg = "The environment variable: CKAN_APIKEY_SRC is not defined"
            raise ValueError(msg)
        self.srcAPIKey = os.environ[constants.CKAN_APIKEY_SRC]

        if constants.CKAN_URL_DEST not in os.environ:
            msg = "The environment variable: CKAN_URL_DEST is not defined"
            raise ValueError(msg)
        self.destUrl = os.environ[constants.CKAN_URL_DEST]

        if constants.CKAN_APIKEY_DEST not in os.environ:
            msg = "The environment variable: CKAN_APIKEY_DEST is not defined"
            raise ValueError(msg)
        self.destAPIKey = os.environ[constants.CKAN_APIKEY_DEST]

    def getSrcWrapper(self):
        srcCKANWrapper = CKANWrapper(self.srcUrl, self.srcAPIKey)
        return srcCKANWrapper

    def getDestWrapper(self):
        destCKANWrapper = CKANWrapper(self.destUrl, self.destAPIKey)
        return destCKANWrapper


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
        self.cacheFilePaths = CacheFiles.CKANCacheFiles()

        # most of the requests use the ckanapi.RemoteCKAN howe ver there are some
        # requests that require features of requests module.  For this reason
        # a session object is created here to help with those requests.
        self.rsession = requests.Session()
        self.rsession.headers.update(self.CKANHeader)

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
        LOGGER.debug(f"end point: {endpoint}")
        resp = requests.get(endpoint, headers=self.CKANHeader, params=payload)
        LOGGER.debug(f"status_code: {resp.status_code}")
        if resp.status_code != 200:
            LOGGER.warning(
                f"package_get received non 200 status code: {resp.status_code}"
            )
            if retries < maxRetries:
                retries += 1
                time.sleep(waitTime)
                resp = self.__getWithRetries(endpoint, payload, retries)
            else:
                msg = (
                    f"made {retries} attempts to retrieve package data, getting "
                    + f"status code: {resp.status_code}.  Unable to complete request"
                )
                raise CKANPackagesGetError(msg)
        return resp

    def __isResponseSuccess(self, resp):
        """as gradually remove the ckanapi module dependency, need to evalute
        the response of the various rest requests.

        :param resp: a requests resp object that is evaluated, returns true if
                     its deemed to have been successful, false if its not
        :type resp: [type]
        :return: indicates if the reponse is deemed to have been succesful
        :rtype: bool
        """
        retVal = False
        if resp:
            LOGGER.debug(f"status_code: {resp.status_code}")
            if resp.status_code >= 200 and resp.status_code < 300:
                respStruct = resp.json()
                if ("success" in respStruct) and respStruct["success"]:
                    retVal = True
        return retVal

    def getSinglePagePackageNames(self, offset=0, pageSize=500):
        params = {"limit": pageSize, "offset": offset}
        LOGGER.debug(f"params: {params}")
        # pageData = self.remoteapi.action.package_list(limit=elemCnt,
        #                                              offset=offset)
        try:
            orgList = self.remoteapi.action.package_list(**params)
        except ckanapi.errors.CKANAPIError:
            endPoint = "api/3/action/package_list"
            apiUrl = f"{self.CKANUrl}{endPoint}"
            LOGGER.debug(f"url end point: {apiUrl}")
            resp = requests.get(apiUrl, headers=self.CKANHeader, json=params)
            LOGGER.debug(f"response status code: {resp.status_code}")
            if self.__isResponseSuccess(resp):
                respJson = resp.json()
                orgList = respJson["result"]
            else:
                raise InvalidRequestError(respJson)
        return orgList

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
            # offset=0, pageSize
            pageData = self.getSinglePagePackageNames(offset=offset, pageSize=elemCnt)
            # params = {"limit": elemCnt, "offset": offset}
            # pageData = self.remoteapi.action.package_list(limit=elemCnt,
            #                                              offset=offset)
            # pageData = self.remoteapi.action.package_list(context=params)

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
        if not os.path.exists(cacheFileName):
            pkgs = self.getPackagesAndData()
            with open(cacheFileName, "w") as fh:
                json.dump(pkgs, fh)
        else:
            with open(cacheFileName) as fh:
                pkgs = json.load(fh)
        return pkgs

    def getPackagesAndData(self):
        """ Makes a bunch of different calls.  Initially calls package_list and
        then iterates through each object package name retrieving the data for it
        using package_show api calls.

        :return: a list of pkgs where each pkg is a python struct describing a
            dataset in ckan.
        :rtype: list of pkgs
        """
        pkgs = []
        pkgList = self.getPackageNames()
        LOGGER.debug(f"got {len(pkgList)} pkg names")
        asyncWrapper = CKANAsyncWrapper(self.CKANUrl, header=self.CKANHeader)
        pkgs = asyncWrapper.getPackages(pkgList)
        return pkgs

    def getPackagesAndData_solr(self):
        """Collects a complete list of packages from ckan.  Uses package_search
        end point.  package_search hits cached state of CKAN managed by SOLR thus
        cannot be relied upon to return the latest set of data.

        If latest greatest data is important then use 'getPackagesAndData' method
        that retrieves the data through individual package_show calls.  Takes a
        longer period of time though.

        :return: [description]
        :rtype: [type]
        """
        LOGGER.debug(f"url: {self.CKANUrl}")
        packageList = []
        elemCnt = 500
        # elemCnt = 2

        pageCnt = 0
        LOGGER.info("Getting packages with data:")

        packageSearchEndPoint = "api/3/action/package_search"
        package_list_call = f"{self.CKANUrl}{packageSearchEndPoint}"
        LOGGER.debug(f"package_list_call: {package_list_call}")

        packageList = []
        params = {"rows": elemCnt, "start": 1}
        # package_list_cnt = 0

        while True:

            offset = pageCnt * elemCnt
            LOGGER.info(f"    - page: {pageCnt} {offset} {elemCnt}")

            params = {"rows": elemCnt, "start": offset}
            resp = self.__getWithRetries(package_list_call, params)
            data = resp.json()
            # also avail is resp['success']
            # LOGGER.debug(f"resp: {data['result']}")
            LOGGER.debug(f"data type: {type(data)}, {len(data['result'])}")
            packageList.extend(data["result"]["results"])
            LOGGER.debug(f"number of packages retrieved: {len(packageList)}")

            # import json
            # with open("/mnt/c/Kevin/proj/bcdc_2_bcdc/junk/pkgs_demo.json", 'w') as fh:
            #     json.dump(data, fh)
            # raise
            if len(data["result"]["results"]) < params["rows"]:
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
        try:
            orgList = self.remoteapi.action.organization_list()
        except ckanapi.errors.CKANAPIError:
            endPoint = "api/3/action/organization_list"
            apiUrl = f"{self.CKANUrl}{endPoint}"
            LOGGER.debug(f"url end point: {apiUrl}")
            resp = requests.get(apiUrl, headers=self.CKANHeader)
            LOGGER.debug(f"response status code: {resp.status_code}")
            if self.__isResponseSuccess(resp):
                respJson = resp.json()
                orgList = respJson["result"]
            else:
                raise InvalidRequestError(respJson)
        return orgList

    def getUsers_cached(self, cacheFileName, includeData=False):
        if not os.path.exists(cacheFileName):
            users = self.getUsers(includeData)
            with open(cacheFileName, "w") as fh:
                json.dump(users, fh)
        else:
            with open(cacheFileName) as fh:
                users = json.load(fh)
        return users

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
        users = []
        try:
            users = self.remoteapi.action.user_list(**params)
        except (requests.exceptions.ConnectionError, ckanapi.errors.CKANAPIError):
            # try manually using the api end point
            LOGGER.warning("caught error with ckanapi module call, trying directly")
            userListendPoint = "api/3/action/user_list"
            userListUrl = f"{self.CKANUrl}{userListendPoint}"
            LOGGER.debug(f"userlist url: {userListUrl}")

            resp = requests.get(userListUrl, headers=self.CKANHeader)
            if self.__isResponseSuccess(resp):
                userJson = resp.json()
                users = userJson["result"]
            else:
                raise InvalidRequestError(resp)
        LOGGER.info(f"retrieved {len(users)} users")
        return users

    def checkUrl(self):
        """This method has been added to all the methods that perform an
        update operation. Its another level of protection that has been
        added in order to ensure that any updates are not run against the
        prod instance of CKAN
        """
        doNotWriteEnvVarName = constants.CKAN_DO_NOT_WRITE_URL
        if doNotWriteEnvVarName in os.environ:
            doNotWriteInstance = os.environ[doNotWriteEnvVarName]
            LOGGER.debug(f"Do not write instance: {doNotWriteInstance}")
            prodHostFromEnvVar = urllib.parse.urlparse(doNotWriteInstance)
            hostInDestUrl = urllib.parse.urlparse(self.CKANUrl)
            LOGGER.debug(
                f"host in DestURL: {hostInDestUrl.netloc} vs prod host: "
                f"{prodHostFromEnvVar.netloc}"
            )
            if prodHostFromEnvVar == hostInDestUrl:
                msg = (
                    f"Attempting to perform an operation that writes to an "
                    "instance that has specifically been defined as read only: "
                    f"{constants.CKAN_DO_NOT_WRITE_URL}"
                )
                raise DoNotWriteToHostError(msg)
            LOGGER.debug(f"safe destination instance: {self.CKANUrl}")
        else:
            msg = (
                f"The environment variable: {constants.CKAN_DO_NOT_WRITE_URL} "
                "has not been defined.  Define and re-run"
            )
            raise ValueError(msg)

    def addUser(self, userData):
        """makes api call to ckan to create a new user

        :param userData: data used to create the user
        :type userData: dict
        """
        self.checkUrl()
        retVal = None
        # TODO: hasn't been tested... Waiting for proper access to prod.
        LOGGER.debug(f"creating a new user with the data: {userData}")
        try:
            LOGGER.warning("actual user add api call commented out")
            retVal = self.remoteapi.action.user_create(**userData)
        except ckanapi.errors.CKANAPIError:
            # try as a requests call
            userCreateEndPoint = "api/3/action/user_create"
            userCreateURL = f"{self.CKANUrl}{userCreateEndPoint}"
            LOGGER.debug(f"url end point: {userCreateURL}")
            resp = requests.post(userCreateURL, headers=self.CKANHeader, json=userData)
            if self.__isResponseSuccess(resp):
                respJson = resp.json()
                retVal = respJson["result"]
            else:
                raise InvalidRequestError(resp)
            LOGGER.debug(f"response status code: {resp.status_code}")
        except ckanapi.errors.ValidationError:
            # usually because the user already exists but is in an deleted
            # state.  To resolve retrieve the user, update the user defs
            # with defs from this description, swap the deleted tag to
            # false
            # userData['state'] = 'active'
            # LOGGER.debug(f"userdata: {userData}")
            # userShow = {"id": userData['name']}
            # retVal = self.remoteapi.action.user_show(**userShow)
            # LOGGER.debug(f"retVal: {retVal}")
            userData["id"] = userData["name"]
            LOGGER.warning("actual user add api call commented out")
            retVal = self.remoteapi.action.user_update(**userData)
        LOGGER.debug(f"User Created: {retVal}")
        return retVal

    def updateUser(self, userData):
        """receives a dictionary that it can use to update the data.

        :param userData: a dictionary with the data to use to update an existing
            ckan user
        :type userData: dict
        """
        self.checkUrl()
        retVal = None
        # wants the id to be the user or the name
        if "id" not in userData and "name" in userData:
            userData["id"] = userData["name"]
            del userData["name"]
        LOGGER.debug(f"trying to update a user using the data: {userData}")
        LOGGER.warning("actual api commented out")
        try:
            retVal = self.remoteapi.action.user_update(**userData)
        except ckanapi.errors.CKANAPIError:
            # redo request using requests.
            userUpdtEndPoint = "api/3/action/user_update"
            userUpdtURL = f"{self.CKANUrl}{userUpdtEndPoint}"
            LOGGER.debug(f"url end point: {userUpdtURL}")
            resp = requests.post(userUpdtURL, headers=self.CKANHeader, json=userData)
            if self.__isResponseSuccess(resp):
                respJson = resp.json()
                retVal = respJson["result"]
            else:
                raise InvalidRequestError(resp)
            LOGGER.debug(f"response status code: {resp.status_code}")
        LOGGER.debug(f"User Updated: {retVal}")

    def getUser(self, userId):
        LOGGER.debug(f"Getting the information associated with user: {userId}")
        if isinstance(userId, str):
            userData = {"id": userId}
        elif isinstance(userId, dict):
            userData = userId
            if "name" in userData:
                msg = (
                    f"unable to process the dictionary {userData}, individual "
                    + "queries for users must use the parameter 'id' instead "
                    + "and not 'name', Going to swap the param name for id."
                )
                userData["id"] = userData["name"]
                del userData["name"]
        else:
            msg = (
                f'parameter "userId" provided: {userId} which has a type '
                + f"of {type(userId)} which is an invalid type.  Valid types "
                + "include: (str, dict)"
            )
            raise ValueError(msg)
        try:
            retVal = self.remoteapi.action.user_show(**userData)
        except ckanapi.errors.CKANAPIError:
            endPoint = "api/3/action/user_show"
            apiUrl = f"{self.CKANUrl}{endPoint}"
            LOGGER.debug(f"url end point: {apiUrl}")
            resp = requests.get(apiUrl, headers=self.CKANHeader, json=userData)
            if self.__isResponseSuccess(resp):
                respJson = resp.json()
                retVal = respJson["result"]
            else:
                raise InvalidRequestError(resp)
            LOGGER.debug(f"response status code: {resp.status_code}")
        return retVal

    def getOrganization(self, query):
        retVal = self.remoteapi.action.organization_show(**query)
        return retVal

    def getGroup(self, query):
        retVal = self.remoteapi.action.group_show(**query)
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
            if user["state"] == "deleted":
                retVal = True
        except ckanapi.errors.NotFound:
            LOGGER.info("user %s was not found", userId)
        return retVal

    def deleteUser(self, userId):
        """Deletes a user

        :param userId: either the user 'id' or 'name'
        :type userId: str
        """
        self.checkUrl()
        retVal = None
        LOGGER.debug(f"trying to delete the user: {userId}")
        userParams = {"id": userId}  # noqa
        LOGGER.warning("actual user delete api call commented out")
        # TODO: uncomment when craig available to verify that ignore configs are working
        try:
            retVal = self.remoteapi.action.user_delete(**userParams)
        except ckanapi.errors.CKANAPIError:
            endPoint = "api/3/action/user_delete"
            apiUrl = f"{self.CKANUrl}{endPoint}"
            LOGGER.debug(f"url end point: {apiUrl}")
            resp = requests.post(apiUrl, headers=self.CKANHeader, json=userParams)
            if self.__isResponseSuccess(resp):
                respJson = resp.json()
                retVal = respJson["result"]
            else:
                raise InvalidRequestError(resp)
        LOGGER.debug(f"User Deleted: {retVal}")

    def getGroups(self, includeData=False):
        """Retrieves groups from ckan api

        :param includeData: if set to True will return all the properties of
            groups, otherwise will return only the names
        :type includeData: bool, optional
        :return: list of groups
        :rtype: list (struct)
        """
        groupConfig = {"order_by": "name"}
        if includeData:
            groupConfig = {
                "order_by": "name",
                "all_fields": True,
                "include_extras": True,
                "include_tags": True,
                "include_groups": True,
                "include_users": True,
            }
        LOGGER.debug(f"groupconfig is {groupConfig}")
        try:
            retVal = self.remoteapi.action.group_list(**groupConfig)
        except ckanapi.errors.CKANAPIError:
            endPoint = "api/3/action/group_list"
            apiUrl = f"{self.CKANUrl}{endPoint}"
            LOGGER.debug(f"url end point: {apiUrl}")
            resp = requests.get(apiUrl, headers=self.CKANHeader, params=groupConfig)
            if self.__isResponseSuccess(resp):
                respJson = resp.json()
                retVal = respJson["result"]
            else:
                raise InvalidRequestError(resp)
        return retVal

    def getGroups_cached(self, cacheFileName, includeData=False):
        if not os.path.exists(cacheFileName):
            groups = self.getGroups(includeData)
            with open(cacheFileName, "w") as fh:
                json.dump(groups, fh)
        else:
            with open(cacheFileName) as fh:
                groups = json.load(fh)
        return groups

    def addGroup(self, groupData):
        """makes an api call to CKAN to create the group described in groupData

        :param groupData: [description]
        :type groupData: [type]
        """
        self.checkUrl()
        retVal = None
        retValStr = None
        LOGGER.debug(f"groupData: {groupData}")
        LOGGER.debug(f"creating a new Group with the data: {groupData}")

        if "id" not in groupData and "name" in groupData:
            groupData["id"] = groupData["name"]
        try:
            retVal = self.remoteapi.action.group_create(**groupData)
        except ckanapi.errors.CKANAPIError:
            endPoint = "api/3/action/group_create"
            apiUrl = f"{self.CKANUrl}{endPoint}"
            LOGGER.debug(f"url end point: {apiUrl}")
            resp = requests.post(apiUrl, headers=self.CKANHeader, json=groupData)
            if self.__isResponseSuccess(resp):
                respJson = resp.json()
                retVal = respJson["result"]
            else:
                raise InvalidRequestError(resp)

        except ckanapi.errors.ValidationError:
            # when this happens its likely because the package already exists
            # but is in a deleted state, (state='deleted')
            # going to try to update the data instead
            retVal = self.remoteapi.action.group_update(**groupData)
        if retVal:
            retValStr = json.dumps(retVal)
        LOGGER.debug(f"Group Created: {retValStr[0:100]} ...")
        return retVal

    def deleteGroup(self, groupIdentifier=None):
        """Deletes the groups that matches the provided identifying information.
        groupIdentifier can be either the group id or name

        :param groupIdentifier: The unique identifier for the group that
            is to be deleted.  Either 'name' or 'id'
        :type groupIdentifier: str
        """
        self.checkUrl()
        LOGGER.info(f"trying to delete the group: {groupIdentifier}")
        orgParams = {"id": groupIdentifier}
        try:
            retVal = self.remoteapi.action.group_delete(**orgParams)
        except ckanapi.errors.CKANAPIError:
            endPoint = "api/3/action/group_delete"
            apiUrl = f"{self.CKANUrl}{endPoint}"
            LOGGER.debug(f"url end point: {apiUrl}")
            resp = requests.post(apiUrl, headers=self.CKANHeader, json=orgParams)
            if self.__isResponseSuccess(resp):
                respJson = resp.json()
                retVal = respJson["result"]
            else:
                raise InvalidRequestError(resp)
        LOGGER.debug("group delete return val: %s", retVal)

    def updateGroup(self, groupData):
        """receives a dictionary that it can use to update the Group data.

        :param groupData: a dictionary with the data to use to update an existing
            ckan user
        :type groupData: dict
        """
        self.checkUrl()
        # wants the id to be the group name
        if "id" not in groupData and "name" in groupData:
            # groupData['id'] = groupData['name']
            # del groupData['name']
            pass
        LOGGER.debug(f"trying to update a group using the data: {groupData}")
        with open("updt_group.json", "w") as groupFileHandle:
            json.dump(groupData, groupFileHandle)
        try:
            retVal = self.remoteapi.action.group_update(**groupData)
        except ckanapi.errors.CKANAPIError:
            endPoint = "api/3/action/group_update"
            apiUrl = f"{self.CKANUrl}{endPoint}"
            LOGGER.debug(f"url end point: {apiUrl}")
            resp = requests.post(apiUrl, headers=self.CKANHeader, json=groupData)
            if self.__isResponseSuccess(resp):
                respJson = resp.json()
                retVal = respJson["result"]
            else:
                raise InvalidRequestError(resp)
        retValStr = json.dumps(retVal)
        LOGGER.debug(f"Group Updated: {retValStr[0:100]} ...")

    def updatePackage(self, packageData, retry=False):
        """[summary]

        Changed this method to use requests because it was freezing up during
        runs.  Origional code can be seen here:

        https://github.com/bcgov/bcdc2bcdc/blob/31f9bcd09b619268c8de8b7b37455cb666b485c7/src/CKAN.py#L583

        This link includes logic that was put in place for the cati update, which
        detects problems with the more_info field and corrects them.  This is now
        missing from this impleemntation.  If required again link above

        :param packageData: [description]
        :type packageData: [type]
        :param retry: [description], defaults to False
        :type retry: bool, optional
        """
        self.checkUrl()
        packageJsonStr = json.dumps(packageData)
        LOGGER.debug(
            f"trying to update a package using the data: {packageJsonStr[0:100]} ..."
        )

        # send the data as a request.post
        requestTimeOut = 120
        packageUpdateEndPoint = "api/3/action/package_update"
        packageUpdateCall = f"{self.CKANUrl}{packageUpdateEndPoint}"

        # resp = requests.post(packageUpdateCall, json=packageData, headers=self.CKANHeader, timeout=requestTimeOut)
        try:
            resp = self.rsession.post(
                packageUpdateCall,
                json=packageData,
                headers=self.CKANHeader,
                timeout=requestTimeOut,
            )
            LOGGER.info(f"package_update status_code: {resp.status_code}")
            responseStruct = resp.json()
            retValStr = json.dumps(responseStruct)
            LOGGER.debug(f"Package Updated: {retValStr[0:125]} ...")
            if resp.status_code < 200 or resp.status_code >= 300:
                raise InvalidRequestError(retValStr)
        except requests.exceptions.ReadTimeout:
            if retry:
                LOGGER.error("have already tried resending this requiest")
                raise
            else:
                LOGGER.warning("got a timeout on this request.. trying again!")
                self.updatePackage(packageData, retry=True)

    def getOrganizations(self, includeData=False, attempts=0, currentPosition=None):
        """Gets organizations, if include data is false then will only
        get the names, otherwise will return all the data for the orgs

        :param includeData: [description], defaults to False
        :type includeData: bool, optional
        :return: a list of organization dictionaries
        :rtype: list
        """
        # TODO: call as it is seems to crash with 502 error. breaking it up into
        #       a paged call
        orgConfig = {}
        organizations = []
        pageSize = 70
        if not currentPosition:
            currentPosition = 0
        pageCnt = 1
        maxRetries = 5

        if includeData:
            orgConfig = {
                "order_by": "name",
                "all_fields": True,
                "include_extras": True,
                "include_tags": True,
                "include_groups": True,
                "include_users": True,
                "limit": pageSize,
                "offset": currentPosition,
            }
        while True:
            LOGGER.debug(f"OrgConfig is {orgConfig}")
            LOGGER.debug(f"pagecount is {pageCnt}")
            try:
                # retVal = self.remoteapi.action.organization_list(**orgConfig)
                endPoint = "api/3/action/organization_list"
                apiUrl = f"{self.CKANUrl}{endPoint}"
                LOGGER.debug(f"url end point: {apiUrl}")
                resp = requests.post(apiUrl, headers=self.CKANHeader, json=orgConfig)
                if self.__isResponseSuccess(resp):
                    respJson = resp.json()
                    retVal = respJson["result"]
                else:
                    raise InvalidRequestError(resp)

            except ckanapi.errors.CKANAPIError as err:
                LOGGER.error(err)
                # catch 504 errors raised by ckanapi, otherwise re-raise
                if (
                    ((attempts < maxRetries) and hasattr(err, "extra_msg"))
                    and len(literal_eval(err.extra_msg)) >= 2
                ) and literal_eval(err.extra_msg)[1] == 504:
                    attempts += 1
                    LOGGER.warning(
                        f"organization_list: status: 504, retry {attempts} of {maxRetries} "
                    )
                    retVal = self.getOrganizations(
                        includeData=includeData,
                        attempts=attempts,
                        currentPosition=currentPosition,
                    )
                    # if gets here then succeeded, reset attempts
                    attempts = 0

                else:
                    raise

            LOGGER.debug(f"records returned: {len(retVal)}")
            organizations.extend(retVal)

            if not retVal or len(retVal) < pageSize:
                break
            currentPosition = currentPosition + pageSize
            orgConfig["offset"] = currentPosition
            pageCnt += 1
        return organizations

    def getOrganizations_cached(self, cacheFileName, includeData=False):
        if not os.path.exists(cacheFileName):
            orgs = self.getOrganizations(includeData)
            with open(cacheFileName, "w") as fh:
                json.dump(orgs, fh)
        else:
            with open(cacheFileName) as fh:
                orgs = json.load(fh)
        return orgs

    def deleteOrganization(self, organizationIdentifier=None):
        """Deletes the organization that matches the provided identifying information.
        organizationIdentifier can be either the organization id or name

        :param organizationIdentifier: The unique identifier for the organization that
            is to be deleted.  Either 'name' or 'id'
        :type organizationIdentifier: str
        """
        self.checkUrl()
        LOGGER.info(f"trying to delete the organization: {organizationIdentifier}")
        orgParams = {"id": organizationIdentifier}
        try:
            retVal = self.remoteapi.action.organization_delete(**orgParams)
        except ckanapi.errors.CKANAPIError:
            endPoint = "api/3/action/organization_delete"
            apiUrl = f"{self.CKANUrl}{endPoint}"
            LOGGER.debug(f"url end point: {apiUrl}")
            resp = requests.post(apiUrl, headers=self.CKANHeader, json=orgParams)
            if self.__isResponseSuccess(resp):
                respJson = resp.json()
                retVal = respJson["result"]
            else:
                raise InvalidRequestError(resp)
        LOGGER.debug("org delete return val: %s", retVal)

    def addOrganization(self, organizationData):
        """creates a new organization

        :param organizationData: creates a new organization
        :type organizationData: struct
        """
        self.checkUrl()
        LOGGER.debug(f"creating a new organization with the data: {organizationData}")
        try:
            retVal = self.remoteapi.action.organization_create(**organizationData)
        except ckanapi.errors.CKANAPIError:
            endPoint = "api/3/action/organization_create"
            apiUrl = f"{self.CKANUrl}{endPoint}"
            LOGGER.debug(f"url end point: {apiUrl}")
            resp = requests.post(apiUrl, headers=self.CKANHeader, json=organizationData)
            if self.__isResponseSuccess(resp):
                respJson = resp.json()
                retVal = respJson["result"]
            else:
                raise InvalidRequestError(resp)
        except ckanapi.errors.ValidationError:
            LOGGER.warning(
                f"org {organizationData['name']}, must already exist in deleted state... updating instead"
            )
            organizationData["id"] = organizationData["name"]
            retVal = self.remoteapi.action.organization_update(**organizationData)
        LOGGER.debug(f"Organization Created: {retVal}")

    def updateOrganization(self, organizationData):
        """receives a dictionary that it can use to update the organizations data.

        :param organizationData: a dictionary with the data to use to update an existing
            ckan organization
        :type organizationData: dict
        """
        self.checkUrl()
        # LOGGER.debug(
        #    f"trying to update a organization using the data: {organizationData}"
        # )
        LOGGER.debug(f"updating org: {organizationData['name']}")
        try:
            retVal = self.remoteapi.action.organization_update(**organizationData)
        except ckanapi.errors.CKANAPIError:
            endPoint = "api/3/action/organization_update"
            apiUrl = f"{self.CKANUrl}{endPoint}"
            LOGGER.debug(f"url end point: {apiUrl}")
            resp = requests.post(apiUrl, headers=self.CKANHeader, json=organizationData)
            if self.__isResponseSuccess(resp):
                respJson = resp.json()
                retVal = respJson["result"]
            else:
                raise InvalidRequestError(resp)

        retValJson = json.dumps(retVal)
        LOGGER.debug(f"Organization Updated: {retValJson[0:100]} ...")

    def addPackage(self, packageData, retries=0):
        self.checkUrl()
        try:
            LOGGER.debug("adding the package data")
            retVal = self.remoteapi.action.package_create(**packageData)
            LOGGER.debug(f"name from retVal: {retVal['name']}, {retVal['id']}")
        except ckanapi.errors.CKANAPIError:
            endPoint = "api/3/action/package_create"
            apiUrl = f"{self.CKANUrl}{endPoint}"
            LOGGER.debug(f"url end point: {apiUrl}")
            resp = requests.post(apiUrl, headers=self.CKANHeader, json=packageData)
            if self.__isResponseSuccess(resp):
                respJson = resp.json()
                retVal = respJson["result"]
            else:
                raise InvalidRequestError(resp)
        except requests.exceptions.ConnectionError:
            LOGGER.error("Error when adding package...", exc_info=True)
            LOGGER.warning("skipping this package")
            time.sleep(2)
            # if retries >= 5:
            #     raise
            # LOGGER.warning(f'connection error raised on instance: {self.CKANUrl} retry {retries} of 5')
            # LOGGER.info(f'pause-ing for {retries * 2}')
            # time.sleep(retries * 2)
            # retries += 1

            # self.addPackage(packageData, retries)

    def deletePackage(self, deletePckg):
        """deleting the package: deletePckg

        :param deletePckg: name or id of the package that is to be deleted
        :type deletePckg: str
        """
        self.checkUrl()
        packageParams = {"id": deletePckg}
        LOGGER.debug(f"trying to delete the package: {deletePckg}")
        try:
            retVal = self.remoteapi.action.package_delete(**packageParams)
        except ckanapi.errors.CKANAPIError:
            endPoint = "api/3/action/package_delete"
            apiUrl = f"{self.CKANUrl}{endPoint}"
            LOGGER.debug(f"url end point: {apiUrl}")
            resp = requests.post(apiUrl, headers=self.CKANHeader, json=packageParams)
            if self.__isResponseSuccess(resp):
                respJson = resp.json()
                retVal = respJson["result"]
            else:
                raise InvalidRequestError(resp)

        LOGGER.debug(f"Package Deleted: {retVal}")

    def getPackage(self, query):
        retVal = self.remoteapi.action.package_show(**query)
        return retVal

    def getScheming(self):
        """hits the scheming api retrieving the scheming definitions

        :return: [description]
        :rtype: [type]
        """
        LOGGER.debug(f"retriving the scheming definitions...")
        # {{DOMAIN}}api/3/action/scheming_dataset_schema_show?type=bcdc_dataset
        endPoint = "api/3/action/scheming_dataset_schema_show"
        params = {"type": "bcdc_dataset"}
        apiUrl = f"{self.CKANUrl}{endPoint}"
        LOGGER.debug(f"url end point: {apiUrl}")
        resp = requests.post(apiUrl, headers=self.CKANHeader, params=params)
        if self.__isResponseSuccess(resp):
            respJson = resp.json()
            retVal = respJson["result"]
        else:
            raise InvalidRequestError(resp)
        LOGGER.debug(f"Retrieved scheming defs: ")
        return retVal


class CKANAsyncWrapper:
    """
    trying to implement this pattern
    https://alexwlchan.net/2019/10/adventures-with-concurrent-futures/
    """

    def __init__(self, baseUrl, apiKey=None, header=None):
        self.baseUrl = baseUrl.strip()
        self.header = header
        if apiKey:
            if not self.header:
                self.header = {}
            self.header["X-CKAN-API-KEY"] = apiKey

        self.packageShowEndPoint = "/api/3/action/package_show?id="

        self.packages = []

        self.TASK_BUNDLE_SIZE = 20
        self.MAX_CONCURRENT_TASKS = 10

        self.requestSession = None

        # used to track how many times an api called has been retried, and the
        # maximum number of times the api call will be retried
        self.currentRetry = 0
        self.maxRetries = 5

    def packageRequestTask(self, url, retries=0):
        """retrieves the data associated with the url.

        :param url: url to call using get to get an individual ckan package
        :type url: str
        :return: the ckan package that was requested
        :rtype: dict
        """
        # LOGGER.debug(f"url: {url}")
        maxRetries = 5
        retVal = None
        try:
            resp = self.requestSession.get(url, headers=self.header)
            if resp.status_code != 200:
                LOGGER.debug(f"status code: {resp.status_code}, {url}")
            packageData = resp.json()
            retVal = packageData["result"]
        except requests.exceptions.ConnectionError:
            if retries > maxRetries:
                raise
            retries += 1
            LOGGER.warning(f"non 200 status, trying again... retries: {retries} of 5")
            time.sleep(5)
            retVal = self.packageRequestTask(url, retries)
        return retVal

    def getPackages(self, packageNameList):
        """Retrieves the list of packages described in the arg: packageNameList
        Packages are retrieve asynchronously

        :param packageNameList: list of packages that need to be retrieved
        :type packageNameList: list of str
        :return: list of dicts where each dict describes one of the packages in
            the 'packageNameList' list.
        :rtype: list of packages
        """
        self.requestSession = requests.Session()
        self.spoolRequests(packageNameList)
        return self.packages

    def spoolRequests(
        self, packageList, missingPackages=None
    ):  # pylint: disable=too-many-locals
        """where the async calls are created.  Gets the list of package names
        and spools up a number of requests, monitors the requests, retrieves the
        data from the requests and stuffs them into self.packages.

        :param packageList: list of package names to retrieve
        :type packageList: list
        :param missingPackages: This method will be called again if the verification
            determines that the number of requested packages is not equal to the
            packages that have been retrieved.  In that event the missing packages
            go into this parameter.
        :type missingPackages: list of package names that failed, optional
        """
        # trim training / from baseUrl
        if self.baseUrl[-1] == "/":
            self.baseUrl = self.baseUrl[0:-1]
        pkgShowUrl = f"{self.baseUrl}{self.packageShowEndPoint}"
        completed = 0

        pkgs2Get = packageList
        if missingPackages:
            pkgs2Get = missingPackages

        packageIterator = iter(pkgs2Get)

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.MAX_CONCURRENT_TASKS
        ) as executor:
            # Schedule the first N futures.  We don't want to schedule them all
            # at once, to avoid consuming excessive amounts of memory.  Also
            # allows finer grained monitoring of tasks
            futures = {}
            cnt = 0
            for pkgName in itertools.islice(packageIterator, self.TASK_BUNDLE_SIZE):
                curUrl = f"{pkgShowUrl}{pkgName}"
                fut = executor.submit(self.packageRequestTask, curUrl)
                futures[fut] = curUrl
                cnt += 1
            LOGGER.debug(f"stack size: {cnt}")
            # using futures dict as a stack of tasks
            while futures:
                # Wait for the next future to complete.
                done, _ = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )
                completed += len(done)
                if not completed % 200:
                    LOGGER.debug(
                        f"total completed: {completed} of {len(pkgs2Get)} (pkgs in loop: {len(done)})"
                    )
                    # LOGGER.debug(f" num done in this loop: {len(done)}")
                for fut in done:
                    original_task = futures.pop(fut)
                    # can add error catching and re-add to executor here
                    data = fut.result()
                    self.packages.append(data)
                # Schedule the next set of futures.  We don't want more than N futures
                # in the pool at a time, to keep memory consumption down.
                for pkgName in itertools.islice(packageIterator, len(done)):
                    # LOGGER.debug(f"adding: {pkgName} to the queue")
                    # adding the package name to the url, as a param
                    curUrl = f"{pkgShowUrl}{pkgName}"
                    fut = executor.submit(self.packageRequestTask, curUrl)
                    futures[fut] = curUrl
        # verify everthing we asked for has been returned
        self.verify(packageList)
        LOGGER.debug(f"number of packages fetched: {len(self.packages)}")

    def verify(self, packageList):
        """verifies that all the requested packages have actually been
        returned, and populated into self.packages struct.

        :param packageList: list of requested package names
        :type packageList: list
        """
        LOGGER.info(f"num requested packages: {len(packageList)}")
        LOGGER.info(f"packages returned: {len(self.packages)}")
        if len(packageList) > len(self.packages):
            if self.currentRetry < self.maxRetries:
                self.currentRetry += 1
                numMissingPkgs = len(packageList) - len(self.packages)
                LOGGER.warning(f"missing: {numMissingPkgs} packages")

                # iterate through self.pkgData and populate with just names
                pkgNamesInReturnData = [pkg["name"] for pkg in self.packages]
                missingPkgNames = [
                    pkgName
                    for pkgName in packageList
                    if pkgName not in pkgNamesInReturnData
                ]

                LOGGER.debug(f"missingPkgNames: {missingPkgNames}")
                LOGGER.info(f"re-requesting {len(missingPkgNames)} missing packages...")
                self.pkgFutures = []
                self.spoolRequests(packageList, missingPkgNames)
            else:
                msg = (
                    f"after {self.maxRetries} attempts to retrieve all the "
                    "packages has failed, raising this exception, have retrieved"
                    f"{len(self.packages)} of {len(packageList)} requested "
                    "packages"
                )
                raise AsyncPackagesGetError(msg)


# ----------------- EXCEPTIONS
class CKANPackagesGetError(Exception):
    """CKAN instances seem to randomely go offline when in the middle of paging
    through packages.  Logic implemented to wait and try again.  When the wait
    and try again logic fails this error is raised.
    """

    def __init__(self, message):
        LOGGER.error(message)
        self.message = message


class DoNotWriteToHostError(Exception):
    """This error is raised when the module detects that you are attempting to
    write to a host that has explicitly been marked as a read only host.
    """

    def __init__(self, message):
        LOGGER.error(message)
        self.message = message


class AsyncPackagesGetError(Exception):
    def __init__(self, message):
        LOGGER.error(message)
        self.message = message


class InvalidRequestError(Exception):
    def __init__(self, message):
        if isinstance(message, requests.Response):
            message = message.json()
        LOGGER.error(message)
        self.message = message


if __name__ == "__main__":

    LOGGER = logging.getLogger()
    LOGGER.setLevel(logging.DEBUG)
    hndlr = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s"
    )
    hndlr.setFormatter(formatter)
    LOGGER.addHandler(hndlr)
    LOGGER.debug("test")

    lg = logging.getLogger("urllib3.connectionpool")
    lg.setLevel(logging.INFO)
    #     pkgList = ["aircraft-emissions-2000-5-km-grid",
    #         "airfields-trim-enhanced-base-map-ebm",
    #         "airphoto-centroids",
    #         "airphoto-flightlines",
    #         "air-photo-system-air-photo-polygons",
    #         "air-photo-system-air-photo-polygons-spatial-view",
    #         "air-photo-system-centre-points"]

    #     pkgListPath = 'temp/pkgNames.json'
    #     with open(pkgListPath) as fh:
    #         pkgList = json.load(fh)
    #     LOGGER.debug(f"numpkgs: {len(pkgList)}")

    destUrl = os.environ[constants.CKAN_URL_DEST]
    destAPIKey = os.environ[constants.CKAN_APIKEY_DEST]

    # asyncWrap = CKANAsyncWrapper(destUrl, destAPIKey)
    # pkgs = asyncWrap.getPackages(pkgList)
    # LOGGER.debug(f"packages returned {len(pkgs)}")

    wrapper = CKANWrapper()
    testDataPath = "updt_package_test.json"
    with open(testDataPath) as fh:
        pkgStruct = json.load(fh)

    wrapper.updatePackage(pkgStruct)
