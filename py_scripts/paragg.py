import json

from py_scripts.helpers.ifmsApiHelper import IFMSApiHelper
from py_scripts.helpers.ifms_api_v2 import IFMSApiV2
from py_scripts.helpers.logging_helper import Logger

# server = 'ifms3.inventale.com'
server = 'inv-dev-02.inventale.com'
# server = 'pitt-us.inventale.com'
user = 'pavel.kiselev'
# user = 'analyst'
# password = '2c821c5131319f3b9cfa83885d552637'
# password = '4c029545591e3f76e2858dcb19c31808'  # from analyst
password = '6561bf7aacf5e58c6e03d6badcf13831'  # from PoKuTe713
# password = 'c64e8262333065b2f35cd52742bd5cfb'
context = 'ifms1'
client = 'rick'
logger = Logger('DEBUG')

forecast_values = [
    'matchedImpressions',
    'availableUniques',
    'matchedUniques',
    'sharedImpressions',
    'expectedDelivery',
    'bookedImpressions'
]


def prepare_dimension_result(forecast_dimension, value):
    result = dict()
    for item in forecast_dimension:
        try:
            remoteid = item.get('criteria').get('remoteId')
        except AttributeError as e:
            remoteid = item.get('campaign').get('remoteId')
        forecast = item.get(value)
        result.update({remoteid: forecast})
    return result


dimensions = [
    "summary",
    "site",
    "page",
    "geo",
    "keyvalue",
    "campaign",
    "order",
    "date"
]

results = dict()
for dimension in dimensions:
    request = {
        "startDate": "2020-07-30T00:00:00.000Z",
        "endDate": "2020-08-30T00:00:00.000Z",
        "dimensions": [dimension],
        "priority": 90
    }
    request2 = {
        "startDate": "2020-07-30T00:00:00.000Z",
        "endDate": "2020-08-30T00:00:00.000Z",
        "dimensions": ["age", "gender", "income", "interest", "section", "subSection",
                       "browser", "deviceType", "deviceWidth", "mobileOperator", "os"
                       ],
        "priority": 90
    }
    tmp = dict()
    for scope in ['default', 'paragg']:
        api_point2 = IFMSApiV2(server, user, password, context, 'rick', scope, logger)
        api_point = IFMSApiHelper(server, user, password, context, logger)
        print(f'Forecast for scope {scope}')
        cookie = api_point.change_scope(client, scope)
        # result = api_point.get_query(cookie, 'a0306ab0-7bb7-11e9-8e25-7d73cdcbd8fa')
        result = json.loads(api_point.check_available_inventory(request, cookie).text).get('dbgInfo').get('requestTime')
        # result = json.loads(api_point.check_available_inventory(request, cookie).text)
        result2 = json.loads(api_point2.check_available_inventory(request).text).get('dbgInfo').get('requestTime')
        # result2 = json.loads(api_point2.check_available_inventory(request).text)
        # try:
        #     json_result = json.loads(result)
        #     # with open('/home/polter/wee2', 'w') as file:
        #     #     file.write(json.dumps(json_result, indent=4))
        #     results.append(json_result)
        # except json.JSONDecodeError:
        #     sys.exit(1)
        tmp.update({scope: [result, result2]})
    results.update({dimension: tmp})
print(json.dumps(results, indent=4))
print('stop')
