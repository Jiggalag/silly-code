import json
import logging
import sys

from helpers.ifmsApiHelper import IFMSApiHelper

server = 'eu-dev-01.inventale.com'
user = 'pavel.kiselev'
password = '6561bf7aacf5e58c6e03d6badcf13831'
context = 'ifms2'
client = 'pitt'
account_id = '156315'
logger = logging.getLogger("check_dsp_report")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

api_point = IFMSApiHelper(server, user, password, context, logger)

results = list()

cases = [
    [{"is": [{"key": "postId", "value": "7"}]}],
    [{"isNot": [{"key": "postId", "value": "7"}]}],
    [{"isGreaterThan": [{"key": "postId", "value": "7"}]}],
    [{"isLesserThan": [{"key": "postId", "value": "7"}]}],
    [{"isNotGreaterThan": [{"key": "postId", "value": "7"}]}],
    [{"isNotLesserThan": [{"key": "postId", "value": "7"}]}],
    [{"isBetween": [{"key": "postId", "value": "7-10"}]}],
    [{"contains": [{"key": "postId", "value": "7"}]}],
    [{"doesNotContain": [{"key": "postId", "value": "7"}]}],
    [{"startsWith": [{"key": "postId", "value": "7"}]}],
    [{"endsWith": [{"key": "postId", "value": "7"}]}]
]

cookie = api_point.change_scope(client, account_id)


def compare_dicts(dict1, dict2):
    answer = True
    for key in dict1.keys():
        if key in ['dbgInfo', 'queryId']:
            continue
        if dict1.get(key) != dict2.get(key):
            print(key)
            answer = False
    return answer


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
            "accountId": account_id,
            "customKey": case
        }
    }

    raw_result = api_point.check_available_inventory(base, cookie, account_id=account_id)

    try:
        json_result = json.loads(raw_result.text)
        results.append(json_result)
    except json.JSONDecodeError:
        sys.exit(1)

order = [
    'availableUniques',
    'bookedImpressions',
    'expectedDelivery',
    'matchedImpressions',
    'matchedUniques',
    'sharedImpressions'
]

if not compare_dicts(results[0], results[4]):
    logger.error('Seems IsNotGreaterThan operator broken!')
if not compare_dicts(results[0], results[5]):
    logger.error('Seems IsNotLesserThan operator broken!')
if not compare_dicts(results[0], results[6]):
    logger.error('Seems isBetween operator broken!')
if not compare_dicts(results[0], results[7]):
    logger.error('Seems Contains operator broken!')
if not compare_dicts(results[0], results[9]):
    logger.error('Seems StartsWith operator broken!')
if not compare_dicts(results[0], results[10]):
    logger.error('Seems EndsWith operator broken!')
if not compare_dicts(results[1], results[2]):
    logger.error('Seems IsGreaterThan operator broken!')
if not compare_dicts(results[1], results[3]):
    logger.error('Seems IsLesserThan operator broken!')
if not compare_dicts(results[1], results[8]):
    logger.error('Seems DoesNotContain operator broken!')

for name in order:
    with open('/home/jiggalag/results/{}.tsv'.format(name), 'w') as file:
        for num in range(len(results[0].get('byDate'))):
            date = results[0].get('byDate')[num].get('to')
            default = results[0].get('byDate')[num].get(name)
            extrapolator = results[1].get('byDate')[num].get(name)
            file.write('{},{},{}\n'.format(date, default, extrapolator))

print('OK')
