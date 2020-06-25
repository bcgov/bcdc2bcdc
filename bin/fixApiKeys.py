"""
Simple script to run that will cycle through all the users after a database
dump and reset the api keys.
"""
import sys
import os
import logging


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.DEBUG)
hndlr = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
hndlr.setFormatter(formatter)
LOGGER.addHandler(hndlr)
LOGGER.debug("test")


LOGGER.setLevel(logging.INFO)


LOGGER.debug('ADDING PATH')
newPath = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(newPath)


import bcdc2bcdc.CKAN


url = os.environ['CKAN_URL']
LOGGER.debug(f"using the url: {url}")
apiKey = os.environ['CKAN_API_KEY']

wrapper = bcdc2bcdc.CKAN.CKANWrapper(url=url, apiKey=apiKey)

ignoreList = ['kjnether', 'admin', 'bsharrat@idir', 'crigdon@idir',
             'mdwilkie', 'idir-bsharrat1', 'stchapma@idir', 'yusotoza',
             'ajbenter']
ignoreList = ['kjnether', 'admin']


users = wrapper.getUsers(includeData=True)

# run special stuff
#ignoreList = []
#users = [{'name':'kjnether', 'apikey': 'silly'}]
#LOGGER.debug(f'users: {len(users)}')
#print(f'few users: {users[0:5]}')

cnt = 0
for user in users:
    userName = user['name']

    if userName not in ignoreList:
        LOGGER.info(f"{userName} apikey before: {user['apikey']}")
        try:
            apiKeyReturn = wrapper.updateUserAPIKey(userName)
            LOGGER.info(f'new api key: {apiKeyReturn["apikey"]}')
            #updatedUser = wrapper.getUser(userName)
        except bcdc2bcdc.CKAN.CKANFailedAPIRequest:
            LOGGER.debug(f'can\'t update {userName}')
        #LOGGER.debug(f"{userName} apikey after: {updatedUser['apikey']}")
        cnt += 1
