import json
import sys

from py_scripts.helpers.ifmsApiHelper import IFMSApiHelper
from py_scripts.helpers.logging_helper import Logger

server = 'eu-dev-01.inventale.com'
user = 'pavel.kiselev'
password = '6561bf7aacf5e58c6e03d6badcf13831'
# server = 'pubmatic.inventale.com'
# user = 'api-pubmatic'
# password = 'ede8f9991bfae90868d0d53081a6342f'
# user = 'qa-ivi'
# password = '2c821c5131319f3b9cfa83885d552637'
# password = 'c64e8262333065b2f35cd52742bd5cfb'
context = 'ifms2'
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

cases = [
    [{"is": [{"key": "section", "value": "Insights"}]}],
    [{"isNot": [{"key": "section", "value": "Insights"}]}],
    [{"isGreaterThan": [{"key": "section", "value": "Insights"}]}],
    [{"isLesserThan": [{"key": "section", "value": "Insights"}]}],
    [{"isNotGreaterThan": [{"key": "section", "value": "Insights"}]}],
    [{"isNotLesserThan": [{"key": "section", "value": "Insights"}]}],
    [{"isBetween": [{"key": "section", "value": "Insights"}]}],
    [{"contains": [{"key": "section", "value": "Insights"}]}],
    [{"doesNotContain": [{"key": "section", "value": "Insights"}]}],
    [{"startsWith": [{"key": "section", "value": "Insights"}]}],
    [{"endsWith": [{"key": "section", "value": "Insights"}]}]
]

# for scope in ['default', 'extrapolation']:
cookie = api_point.change_scope(client, 156315)

for case in cases:
    print(f'Process {case}')
    base = {
        "startDate": "2019-04-29T05:00:00.000Z",
        "endDate": "2019-05-01T04:59:59.000Z",
        "dimensions": [
            "Date",
            "Date/AdUnit",
            "AdUnit/Date",
            "AdUnit",
            "Region",
            "City",
            "LineItem",
            "Country",
            "Order",
            "AdSize",
            "AdFormat",
            "DMA",
            "BrowserLanguage",
            "DeviceType",
            "Device",
            "OS",
            "Browser",
            "CompetingLineItem",
            "DeviceCapability",
            "CustomKey"
        ],
        "pubMatic": {
            "priority": 16,
            "accountId": "156315",
            "customKey": case
        }
    }

    raw_result  = api_point.check_available_inventory(base, cookie, account_id=156315)

    try:
        json_result = json.loads(raw_result.text)
        results.append(json_result)
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


def wrwr(name, forecast):
    with open(f'/home/polter/{name}', 'w') as f:
        f.write(json.dumps(forecast))

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

first = '{"Native": {"matchedImpressions": 3307953, "bookedImpressions": 578754, "expectedDelivery": 2892945, "sharedImpressions": 163746, "matchedUniques": 181269, "availableUniques": 181269, "criteria": {"type": "AD_FORMAT", "remoteId": "Native", "name": "Native", "path": "stub-none", "parentRemoteId": "stub-none"}}, "Banner/Rich Media": {"matchedImpressions": 3442560, "bookedImpressions": 3399, "expectedDelivery": 3442560, "sharedImpressions": 3399, "matchedUniques": 531927, "availableUniques": 531927, "criteria": {"type": "AD_FORMAT", "remoteId": "Banner/Rich Media", "name": "Banner/Rich Media", "path": "stub-none", "parentRemoteId": "stub-none"}}}'
ff = json.loads(first)

raw_time = json_result.get('dbgInfo').get('requestTime')

for name in order:
    with open('/home/jiggalag/results/{}.tsv'.format(name), 'w') as file:
        for num in range(len(results[0].get('byDate'))):
            date = results[0].get('byDate')[num].get('to')
            default = results[0].get('byDate')[num].get(name)
            extrapolator = results[1].get('byDate')[num].get(name)
            file.write('{},{},{}\n'.format(date, default, extrapolator))


print('OK')
