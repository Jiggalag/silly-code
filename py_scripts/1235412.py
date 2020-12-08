import json

with open('/home/polter/data1.json', 'r') as f1:
    d1 = json.loads(f1.read())
with open('/home/polter/data2.json', 'r') as f2:
    d2 = json.loads(f2.read())

print('ok')
