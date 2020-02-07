
import CKANData
import constants
import logging
import CKANTransform

class CheckForIgnores:

    def __init__(self, ckanStruct):
        self.ckanStruct = ckanStruct

    def hasIgnoreUsers(self):
        userIgnoreList = []
        transConf = CKANTransform.getTransformationConfig()
        transUserConf = transConf[constants.TRANSFORM_TYPE_USERS]
        if constants.TRANSFORM_TYPE_USERS in transUserConf:
            userIgnoreList = transUserConf[constants.TRANSFORM_TYPE_USERS]

        retVal = False
        for user in userIgnoreList:
            if user['name'] in userIgnoreList:
                retVal = True
                logging.warning("found the ignore user: %s", user['name'])
                break
        return retVal
