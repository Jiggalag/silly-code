import json
import os

file = os.getcwd() + '/123'
with open(file, 'r') as f:
    os.path.exists(file)
    text = f.readline()


a = json.loads(text)
print(a)
orders = a.get('byCampaign')
remoteids = []
for item in orders :
    remoteids.append(item.get('campaign').get('remoteId'))
remoteids.sort()
print(','.join(remoteids))