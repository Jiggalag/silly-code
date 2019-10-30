import json

with open('/home/polter/wee', 'r') as file:
    without = json.loads(file.read())

with open('/home/polter/ee-aeterna', 'r') as file:
    ee = json.loads(file.read())

a = without.get('AdUnit/Date').get('10011181')
b = ee.get('AdUnit/Date').get('10011181')

for item in a:
    if a.get(item) == b.get(item):
        print('Forecast for {} equals'.format(item))
    else:
        print('Forecast for {} differs'.format(item))


# c = without.get('Date/AdUnit').get('2019-08-02').get('10011181')
# d = ee.get('Date/AdUnit').get('2019-08-02').get('10011181')

for item in ee.get('Date/AdUnit').get('2019-08-02'):
    if item not in without.get('Date/AdUnit').get('2019-08-02'):
        print('error')
    else:
        if ee.get('Date/AdUnit').get('2019-08-02').get(item) != without.get('Date/AdUnit').get('2019-08-02').get(item):
            print(f"Item {item}, ee {ee.get('Date/AdUnit').get('2019-08-02').get(item)}', without {without.get('Date/AdUnit').get('2019-08-02').get(item)}")




if without.get('AdUnit/Date').get('10011181') == ee.get('AdUnit/Date').get('10011181'):
    print('10011181 is OK')

print('kik')
