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

apis = {
    'irving': IFMSApiHelper('ifms-test.inventale.com', 'analyst', '4c029545591e3f76e2858dcb19c31808', 'ifms', logger),
    'rick': IFMSApiHelper('ifms3-test.inventale.com', 'analyst', '4c029545591e3f76e2858dcb19c31808', 'ifms', logger)
}

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

for name in order:
    with open('/home/jiggalag/results/{}.tsv'.format(name), 'w') as file:
        for num in range(len(results[0].get('byDate'))):
            date = results[0].get('byDate')[num].get('to')
            default = results[0].get('byDate')[num].get(name)
            extrapolator = results[1].get('byDate')[num].get(name)
            file.write('{},{},{}\n'.format(date, default, extrapolator))

print('OK')
