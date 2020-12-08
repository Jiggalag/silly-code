import json

import requests

url = 'http://inv-prd-ppt-01.inventale.com:8085/pdb/query/v4/facts?query=%s'
headers = {'Content-Type': 'application/json'}

result = requests.get(url, headers=headers)

prc = json.loads(result.text)
ks = dict()
for item in prc:
    ks.update({item.get('name'): item.get('value')})
print('stop')
