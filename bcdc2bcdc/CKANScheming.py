"""simple interface to scheming.

"""
import json
import os.path

import bcdc2bcdc.CacheFiles as CacheFiles
import bcdc2bcdc.CKAN as CKAN


class Scheming:
    def __init__(self):
        cacheFiles = CacheFiles.CKANCacheFiles()
        schemingCacheFile = cacheFiles.getSchemingCacheFilePath()
        if os.path.exists(schemingCacheFile):
            # load from cache file if its there
            with open(schemingCacheFile, 'r') as fh:
                self.struct = json.load(fh)
        else:
            # otherwise make api call and then create the cache file
            ckanWrap = CKAN.CKANWrapper()
            self.struct = ckanWrap.getScheming()
            with open(schemingCacheFile, 'w') as fh:
                self.struct = json.dump(self.struct, fh)

    def getDomain(self, fieldname):
        retVal = None
        for fldDef in self.struct:
            if fldDef['resource_fields'].lower() == fieldname.lower():
                if 'choices' in fldDef:
                    retVal = []
                    for choice in fldDef['choices']:
                        retVal.append(choice['value'])
        return retVal



