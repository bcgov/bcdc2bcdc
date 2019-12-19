"""



"""

import ckanapi
import os
import pprint

class CKANWrapper:

    def __init__(self, url=None, apiKey=None):

        if url is None:
            url = os.environ['CKAN_URL']
        if apiKey is None:
            apiKey = os.environ['CKAN_API_KEY']
        
        if not apiKey or not url:
            msg = "Need to either provide ckan url and api key as args " + \
                  "to this constructor or define them in env vars: CKAN_URL" + \
                  ", CKAN_API_KEY"
            raise ValueError(msg)
        self.remoteapi = ckanapi.RemoteCKAN(url, apikey=apiKey)
        
        # debug helper
        self.pp = pprint.PrettyPrinter(indent=4)

    def getPackageNames(self):
        """Gets a list of package names from the API
        
        :return: a list of package names
        :rtype: list
        """
        packageList = self.remoteapi.action.package_list()
        return packageList

        