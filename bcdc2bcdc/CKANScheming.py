"""simple interface to help retrive information from the scheming json that describes CKAN object rules.
"""
import json
import os.path

import bcdc2bcdc.CacheFiles as CacheFiles
import bcdc2bcdc.CKAN as CKAN



class Scheming:
    """constructor, looks for the cached scheming file and reads it or if it
    doesn't exist makes an api call, and dumps the results to the extension.
    """

    def __init__(self):
        cacheFiles = CacheFiles.CKANCacheFiles()
        schemingCacheFile = cacheFiles.getSchemingCacheFilePath()
        if os.path.exists(schemingCacheFile):
            # load from cache file if its there
            with open(schemingCacheFile, "r") as fh:
                self.struct = json.load(fh)
        else:
            # otherwise make api call and then create the cache file
            ckanWrap = CKAN.CKANWrapper()
            self.struct = ckanWrap.getScheming()
            with open(schemingCacheFile, "w") as fh:
                json.dump(self.struct, fh)

    def getResourceDomain(self, fieldname):
        """Gets the domains if they are defined for the provided
        fieldname/property type of a resource

        :param fieldname: The name of the field who's domain is to be returned
        :type fieldname: str
        :return: a list of allowable values for the provided field
        :rtype: list
        """
        return self.getDomain(fieldname, "resource_fields")

    def getDatasetDomain(self, fieldname):
        """Gets the domains if they are defined for the provided
        fieldname/property type of a dataset

        :param fieldname: [description]
        :type fieldname: [type]
        :return: [description]
        :rtype: [type]
        """
        return self.getDomain(fieldname, "dataset_fields")

    def getDomain(self, fieldname, objType):
        """ gets the domains for the provided fieldname / property for the
        given object type, object type can be be either dataset_fields, or

        :param fieldname: [description]
        :type fieldname: [type]
        :param objType: [description]
        :type objType: [type]
        :return: [description]
        :rtype: [type]
        """
        retVal = None
        resultStruct = self.struct[objType]
        for fldDef in resultStruct:
            if fldDef["field_name"].lower() == fieldname.lower():
                if "choices" in fldDef:
                    retVal = []
                    for choice in fldDef["choices"]:
                        retVal.append(choice["value"])
        return retVal

