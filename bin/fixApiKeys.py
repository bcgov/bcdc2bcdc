"""
Simple script to run that will cycle through all the users after a database
dump and reset the api keys.
"""
import sys
import os
print('ADDING PATH')
newPath = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(newPath)


import bcdc2bcdc.CKAN


url = os.environ['CKAN_URL']
print(f"using the url: {url}")
apiKey = os.environ['CKAN_API_KEY']

wrapper = bcdc2bcdc.CKAN.CKANWrapper(url=url, apiKey=apiKey)

ignoreList = ['kjnether', 'admin', 'bsharrat@idir', 'crigdon@idir',
             'mdwilkie', 'idir-bsharrat1', 'stchapma@idir', 'yusotoza',
             'ajbenter']

users = wrapper.getUsers(includeData=True)
print(f'users: {len(users)}')
#print(f'few users: {users[0:5]}')

cnt = 0
for user in users:
    userName = user['name']
    if userName not in ignoreList:
        print(f"{userName} apikey before: {user['apikey']}")
        try:
            wrapper.updateUserAPIKey(userName)
            updatedUser = wrapper.getUser(userName)
        except bcdc2bcdc.CKAN.CKANFailedAPIRequest:
            print(f'can\'t update {userName}')
        print(f"{userName} apikey after: {updatedUser['apikey']}")

        cnt += 1

