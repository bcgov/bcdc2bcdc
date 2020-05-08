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
import re
import sys
from ast import literal_eval
import concurrent.futures
import requests_futures.sessions
import time


import ckanapi
import requests
import urllib.parse

import constants
import CacheFiles

# pylint: disable=logging-format-interpolation

LOGGER = logging.getLogger(__name__)


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

    def getSinglePagePackageNames(self, offset=0, pageSize=500):
        params = {"limit": pageSize, "offset": offset}
        LOGGER.debug(f"params: {params}")
        # pageData = self.remoteapi.action.package_list(limit=elemCnt,
        #                                              offset=offset)
        pageData = self.remoteapi.action.package_list(**params)
        return pageData

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
            params = {"limit": elemCnt, "offset": offset}
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
        for pkgName in pkgList:
            pass


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
        orgList = self.remoteapi.action.organization_list()
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
        try:
            users = self.remoteapi.action.user_list(**params)
        except requests.exceptions.ConnectionError:
            # try manually using the api end point
            LOGGER.warning("caught error with ckanapi module call, trying directly")
            userListendPoint = "api/3/action/user_list"
            userListUrl = f"{self.CKANUrl}{userListendPoint}"
            LOGGER.debug(f"userlist url: {userListUrl}")

            resp = requests.get(userListUrl, headers=self.CKANHeader)
            LOGGER.debug(f"status_code: {resp.status_code}")
            userResp = resp.json()
            users = userResp["result"]
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
                f'host in DestURL: {hostInDestUrl.netloc} vs prod host: '
                f'{prodHostFromEnvVar.netloc}')
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
                f'The environment variable: {constants.CKAN_DO_NOT_WRITE_URL} '
                'has not been defined.  Define and re-run'
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
            #retVal = self.remoteapi.action.user_create(**userData)
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
            #retVal = self.remoteapi.action.user_update(**userData)
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
        #retVal = self.remoteapi.action.user_update(**userData)
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
        retVal = self.remoteapi.action.user_show(**userData)
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
        userParams = {"id": userId}
        LOGGER.warning("actual user delete api call commented out")
        #retVal = self.remoteapi.action.user_delete(**userParams)
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
                "order_by": "name",
                "all_fields": True,
                "include_extras": True,
                "include_tags": True,
                "include_groups": True,
                "include_users": True,
            }
        LOGGER.debug(f"groupconfig is {groupConfig}")
        retVal = self.remoteapi.action.group_list(**groupConfig)
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

        if 'id' not in groupData and 'name' in groupData:
            groupData['id'] = groupData['name']
        try:
            retVal = self.remoteapi.action.group_create(**groupData)
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
        retVal = self.remoteapi.action.group_delete(**orgParams)
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
        retVal = self.remoteapi.action.group_update(**groupData)
        retValStr = json.dumps(retVal)
        LOGGER.debug(f"Group Updated: {retValStr[0:100]} ...")

    def updatePackage(self, packageData, retry=False):

        self.checkUrl()
        packageJsonStr = json.dumps(packageData)
        LOGGER.debug(f"trying to update a package using the data: {packageJsonStr[0:100]} ...")
        try:
            retVal = self.remoteapi.action.package_update(**packageData)
            retValStr = json.dumps(retVal)
            LOGGER.debug(f"Package Updated: {retValStr[0:125]} ...")
        except ckanapi.errors.CKANAPIError as e:
            LOGGER.debug("CAUGHT CKANAPIError EXCEPTION")
            if retry:
                raise
            errMatchPatern = (
                '^\s*Only\s+lists\s+of\s+dicts\s+can\s+be\s+placed\s+agains'
                't\s+subschema\s+\(\'more_info\'\,\)\,\s+not\s+\<type\s+\'u'
                'nicode\'\>\s*$'
            )
            # catching stringified issue
            args = e.args
            #print(f'args: {args}, {type(args)}, {len(args)}')
            raiseErr = True
            if len(args):
                errMsgs = literal_eval(args[0])
                ckanErrMsgs = json.loads(errMsgs[2])
                ckanErrMsg = ckanErrMsgs['error']['message']
                if re.match(errMatchPatern, ckanErrMsg):
                    # caught the issue of more_info being expected as a non
                    # stringified obj
                    packageData['more_info'] = json.loads(packageData['more_info'])
                    raiseErr = False
                    self.updatePackage(packageData, retry=True)
            if raiseErr:
                raise

        except ckanapi.errors.ValidationError as e:
            LOGGER.debug("CAUGHT ValidationError EXCEPTION")
            if retry:
                raise
            # Discovered that there is inconsistency in how ckan handles the
            # more_info field.  Sometimes it wants it as a stringified object
            # other times it wants it as a regular json object.
            # Here we are trying to catch the exception, determine if it is
            # associated with the more_info field, and if it is then stringify
            # the contents of that field and send the request back to the api.
            # if this is not a more_info related error, then just re-raise
            # the exception
            args = e.args
            # example of what the e.args looks like if its a more_info related
            # exception:
            #
            # ({'__type': 'Validation Error', '__junk': ["The input field [('more_info', 0, 'link'), ('more_info', 0, 'delete')] was not expected."]},)
            if ((len(args)) and "__junk" in e.args[0]) and len(e.args[0]['__junk']) and \
                    re.match('^The\s+input\s+field\s+\[\(\'more_info\'.+was\s+not\s+expected\.$', e.args[0]['__junk'][0]):
                msg = (
                    'The update object contains a non stringified more_info '
                    'field, and for some unknown reason it wants it as a '
                    'stringified field for this update situation.... going '
                    'to stringify the more_info property and resubmit the '
                    'request'
                )
                LOGGER.warning(msg)
                LOGGER.info("modifying the more_info field so it stringified")
                packageData['more_info'] = json.dumps(packageData['more_info'])
                LOGGER.debug(f"stringified version: {packageData['more_info']}")
                LOGGER.debug("resubmit request")
                self.updatePackage(packageData, retry=True)
                LOGGER.info("SUCCESS")
            else:
                LOGGER.error("Failed after attempt to catch exception", exc_info=True)
                raise

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
                retVal = self.remoteapi.action.organization_list(**orgConfig)
            except ckanapi.errors.CKANAPIError as err:
                LOGGER.error(err)
                # catch 504 errors raised by ckanapi, otherwise re-raise
                if ((((attempts < maxRetries) and
                        hasattr(err, 'extra_msg')) and
                        len(literal_eval(err.extra_msg)) >= 2) and
                            literal_eval(err.extra_msg)[1] == 504):
                    attempts += 1
                    LOGGER.warning(f"organization_list: status: 504, retry {attempts} of {maxRetries} ")
                    retVal = self.getOrganizations(includeData=includeData,
                                                   attempts=attempts,
                                                   currentPosition=currentPosition)
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
        retVal = self.remoteapi.action.organization_delete(**orgParams)
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
        retVal = self.remoteapi.action.organization_update(**organizationData)
        retValJson = json.dumps(retVal)
        LOGGER.debug(f"Organization Updated: {retValJson[0:100]} ...")

    def addPackage(self, packageData):
        self.checkUrl()

        LOGGER.debug("adding the package data")
        retVal = self.remoteapi.action.package_create(**packageData)
        LOGGER.debug(f"name from retVal: {retVal['name']}, {retVal['id']}")

    def deletePackage(self, deletePckg):
        """deleting the package: deletePckg

        :param deletePckg: name or id of the package that is to be deleted
        :type deletePckg: str
        """
        self.checkUrl()
        packageParams = {"id": deletePckg}
        LOGGER.debug(f"trying to delete the package: {deletePckg}")
        retVal = self.remoteapi.action.package_delete(**packageParams)
        LOGGER.debug(f"Package Deleted: {retVal}")

    def getResources(self):
        """makes call to the api and returns all the resources
        """
        # TODO: need to either define this or modify how resources are retrieved in the DataCache.CacheLoader.loadResources
        return []

    def getPackage(self, query):
        retVal = self.remoteapi.action.package_show(**query)
        return retVal

    def getResource(self, query):
        # TODO: code this when get to resources
        pass


class CKANAsyncWrapper:
    def __init__(self, baseUrl, header=None):
        self.header = header
        self.url = baseUrl
        self.session = None

        self.maxRetries = 5
        self.max_workers = 20
        self.currentRetry = 0

        self.pkgFutures = []
        self.pkgData = []

        self.retries = {}

    def getDataWrapper(self, session, url, params=None, headers=None, retries=0):
        if retries < self.maxRetries:
            retries += 1
            kwargs = {}
            if params:
                kwargs['params'] = params
            if headers:
                kwargs['headers'] = headers

            pkgShowRequest = session.get(url, **kwargs)
            #session.hooks['response'] = self.getPackageDataHook
            self.pkgFutures.append(pkgShowRequest)

    def getPackageData(self, packageList):
        # reloading the package data, start by ensuring the package data list
        # is empty.

        #session = FuturesSession()
        #session.hooks['response'] = response_hook
        self.pkgFutures = []
        self.pkgData = []

        self.asyncPackageShowRequest(packageList)
        return self.pkgData

    def asyncPackageShowRequest(self, packageList, missingPkgs=None, exceptionsList=None):
        LOGGER.debug(f"packageList length: {len(packageList)}")
        packageShow = 'api/3/action/package_show'
        pkgShowUrl = f"{self.url}/{packageShow}"

        pkgIterList = packageList
        if missingPkgs:
            pkgIterList = missingPkgs

        with requests_futures.sessions.FuturesSession(max_workers=self.max_workers) as session:
            if not exceptionsList:
                for pkgName in pkgIterList:
                    pkgShowParams = {'id': pkgName}
                    self.getDataWrapper(session, pkgShowUrl, params=pkgShowParams)
            else:
                LOGGER.debug("re-running failed requests")
                for failedRequest in exceptionsList:
                    self.getDataWrapper(session, failedRequest.url, headers=failedRequest.headers)

        # stack is now loaded, now wait for responses...
        reRunList = []
        cnter = 0
        for future in concurrent.futures.as_completed(self.pkgFutures):
            futureExcept = future.exception()
            if futureExcept:
                # catching exceptions where the request never completed, adding
                # to a list that will be re-run in another async call
                LOGGER.warning(f"future: {futureExcept}")
                reRunList.append(futureExcept.request)
            else:
                resp = future.result()
                self.getPackageDataHook(resp)
            cnter += 1

        if reRunList:

            # dont' re-run if max retries is exceeded
            if self.currentRetry < self.maxRetries:
                self.currentRetry + = 1

                self.pkgFutures = []
                LOGGER.info(f"re-run list length: {len(reRunList)}")
                self.asyncPackageShowRequest(packageList, exceptionsList=reRunList)
            else:
                msg = (
                    f'after {self.maxRetries} attempts to retrieve all the '
                    'packages has failed, raising this exception, have retrieved'
                    f'{len(self.pkgData)} of {len(packageList)} requested '
                    'packages'
                )
                raise AsyncPackagesGetError(msg)

        self.verify(packageList)
        return self.pkgData

    def getPackageSync(self, url, headers):
        resp = requests.get(url, headers=headers)
        data = resp.json()
        self.pkgData.append(data['result'])

    def verify(self, packageList):
        """Checks that all the requested package names have been populated into
        self.pkgData, if they have not it constructs a list of the missing
        packages and sends it back to the async request method.

        This will keep on looping back until the max retries is exceeded.

        :param packageList: [description]
        :type packageList: [type]
        """
        LOGGER.info(f"num requested packages: {len(packageList)}")
        LOGGER.info(f"packages returned: {len(self.pkgData)}")
        if len(packageList) > len(self.pkgData):
            if self.currentRetry < self.maxRetries:
                self.currentRetry + = 1
                numMissingPkgs = len(packageList) - len(self.pkgData)
                LOGGER.warning(f'missing: {numMissingPkgs} packages')

                # iterate through self.pkgData and populate with just names
                pkgNamesInReturnData = [pkg['name'] for pkg in self.pkgData]
                missingPkgNames = [pkgName for pkgName in packageList if pkgName not in pkgNamesInReturnData]

                LOGGER.debug(f"missingPkgNames: {missingPkgNames}")
                LOGGER.info(f"re-requesting {len(missingPkgNames)} missing packages...")
                self.pkgFutures = []
                self.asyncPackageShowRequest(packageList, missingPkgNames)
            else:
                msg = (
                    f'after {self.maxRetries} attempts to retrieve all the '
                    'packages has failed, raising this exception, have retrieved'
                    f'{len(self.pkgData)} of {len(packageList)} requested '
                    'packages'
                )
                raise AsyncPackagesGetError(msg)

    def getPackageDataHook(self, resp, *args, **kwargs):
        # if the response status_code is not 200 then
        # reload to the stack.
        # also extract the data and load to pkgData
        #LOGGER.debug(f"args: {args}")
        #LOGGER.debug(f"kwargs: {kwargs}")
        #LOGGER.debug(f"resp: {resp}, {resp.url}")

        if not (resp.status_code >= 200 and resp.status_code < 300):
            # unsuccessful request, try again by adding to the stack
            if not os.path.exists('fail.json'):
                fh = open('fail.json', 'w')
                fh.close()

            LOGGER.debug(f"resp status_code: {resp.status_code}")
            if (resp.url in self.retries) and \
                    self.retries[resp.url] > self.maxRetries:
                msg = f'unable to retrieve: {resp.url}'
                raise requests.exceptions.HTTPError(msg)

            if resp.url not in self.retries:
                self.retries[resp.url] = 0
            self.retries[resp.url] += 1
        else:
            # get the data from the request and add it to the collection
            # of data
            pkgDataRaw = resp.json()
            prevLen = len(self.pkgData)
            LOGGER.debug(f"have data for: {pkgDataRaw['result']['name']}, {resp.status_code}, {prevLen}")
            self.pkgData.append(pkgDataRaw['result'])
            LOGGER.debug(f"current length: {len(self.pkgData)}, prev: {prevLen}")
            if pkgDataRaw['result'] not in self.pkgData:
                LOGGER.debug(f"DID NOT GET WRITTEN: {pkgDataRaw['result']['name']}")

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
