import json

file = '/home/polter/Desktop/pitt-result'

with open(file, 'r') as file:
    result = file.readlines()

jarray = list()
for item in result:
    jarray.append(json.dumps(item))
print('k')
