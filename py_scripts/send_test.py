import json
import sys

from py_scripts.helpers.ifmsApiHelper import IFMSApiHelper
from py_scripts.helpers.logging_helper import Logger

server = 'inv-dev-02.inventale.com'
# server = 'pitt-us.inventale.com'
# server = 'localhost'
user = 'pavel.kiselev'
# user = 'analyst'
password = '6561bf7aacf5e58c6e03d6badcf13831'
# password = '4c029545591e3f76e2858dcb19c31808'
# server = 'pubmatic.inventale.com'
# user = 'api-pubmatic'
# password = 'ede8f9991bfae90868d0d53081a6342f'
# user = 'qa-ivi'
# password = '2c821c5131319f3b9cfa83885d552637'
# password = 'c64e8262333065b2f35cd52742bd5cfb'
context = 'ifms5'
client = 'pitt'
request = 'frc.json'
logger = Logger('DEBUG')

api_point = IFMSApiHelper(server, user, password, context, logger)
#
# cookie = api_point.change_scope(client, scope)
#
# result = api_point.check_available_inventory(request, cookie).text
#
# try:
#     json_result = json.loads(result)
# except json.JSONDecodeError:
#     sys.exit(1)

results = list()

# for scope in ['default', 'extrapolation']:
cookie = api_point.change_scope(client, 156535)

base = {
    "startDate": "2019-10-29T05:00:01.000Z",
    "endDate": "2019-11-16T04:59:59.000Z",
    "dimensions": [
        "Date",
        "Date/AdUnit",
        "AdUnit/Date",
        "AdUnit"
    ],
    "pubMatic": {
        "priority": 1
    }
}

raw_result = api_point.check_available_inventory(base, cookie, account_id=156535)

try:
    json_result = json.loads(raw_result.text)
    with open('/home/polter/test', 'w') as file:
        file.write(json.dumps(json_result, indent=4))
        print('ok')
except json.JSONDecodeError:
    sys.exit(1)


def get_size(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size


def wrwr(filename, forecast):
    with open(f'/home/polter/{filename}', 'w') as targetfile:
        targetfile.write(json.dumps(forecast))


def aggregate_all_deep(collection):
    res = dict()
    for item in collection.values():
        for i in item.values():
            for key in order:
                if key not in res.keys():
                    value = i.get(key)
                    res.update({key: value})
                else:
                    value = i.get(key) + res.get(key)
                    res.update({key: value})
    return res


def aggregate_all(collection):
    res = dict()
    for item in collection.keys():
        for key in order:
            if key not in res.keys():
                value = collection.get(item).get(key)
                res.update({key: value})
            else:
                value = collection.get(item).get(key) + res.get(key)
                res.update({key: value})
    return res


def compare(one, two):
    for key in order:
        if one.get(key) != two.get(key):
            print(f'First = {one.get(key)}, second = {two.get(key)}')


order = [
    'availableUniques',
    'bookedImpressions',
    'expectedDelivery',
    'matchedImpressions',
    'matchedUniques',
    'sharedImpressions'
]

# wrwr('757n801', results[0])

d = dict()
f = dict()

size1 = get_size(results[0])

for name in order:
    with open('/home/jiggalag/results/{}.tsv'.format(name), 'w') as file:
        for num in range(len(results[0].get('byDate'))):
            date = results[0].get('byDate')[num].get('to')
            default = results[0].get('byDate')[num].get(name)
            extrapolator = results[1].get('byDate')[num].get(name)
            file.write('{},{},{}\n'.format(date, default, extrapolator))


print('OK')