import json
import sys
from datetime import datetime, timedelta

from py_scripts.helpers.ifmsApiHelper import IFMSApiHelper
from py_scripts.helpers.logging_helper import Logger

server1 = 'ifms3.inventale.com'
server2 = 'ifms3-t3.inventale.com'
user1 = 'analyst'
user2 = 'analyst'
password1 = '4c029545591e3f76e2858dcb19c31808'
password2 = '4c029545591e3f76e2858dcb19c31808'
context1 = 'ifms'
context2 = 'ifms'
scope1 = 'default'
scope2 = 'default_t3'


server = 'inv-dev-02.inventale.com'
user = 'pavel.kiselev'
password = '6561bf7aacf5e58c6e03d6badcf13831'
context = 'ifms'
client = 'rick'
logger = Logger('WARNING')

api_point_aws = IFMSApiHelper(server1, user1, password1, context1, logger)
api_point_t3 = IFMSApiHelper(server2, user2, password2, context2, logger)

time = str(datetime.now()).replace(' ', '').replace(':', '')
results = list()

for i in range(1,59):
    print(f'Forecast for {i} days')
    base = {
        "startDate": "{}T05:00:01.000Z".format(datetime.today().date()),
        "endDate": "{}T04:59:59.000Z".format(datetime.today().date() + timedelta(days=i)),
        # "geoTargeting": {"includeOther": ["1305"]},
        # "keyvalueTargeting":[{
        #     "keyname": "device_width",
        #     "includeValues": [],
        #     "excludeValues": [],
        # }],
        "dimensions": [# "page",
                       # "campaign",
                       # "order",
                       # "country",
                       # "state",
                       # "city",
                       "keyvalue"
                       # "browser",
                       # "os",
                       # "event",
                       # "eventByDate",
                       # "trafficAllocation",
                       # "summary"
                       ],
        "priority": 90
    }

    cookie = api_point_aws.change_scope(client, scope1)
    raw_result = api_point_aws.check_available_inventory(base, cookie)
    times = list()
    try:
        json_result = json.loads(raw_result.text)
        print(f"Forecast time for scope {scope1} is {json_result.get('dbgInfo').get('requestTime')}")
    except json.JSONDecodeError:
        sys.exit(1)

    cookie = api_point_t3.change_scope(client, scope2)
    raw_result = api_point_t3.check_available_inventory(base, cookie)

    try:
        json_result = json.loads(raw_result.text)
        print(f"Forecast time for scope {scope2} is {json_result.get('dbgInfo').get('requestTime')}")
    except json.JSONDecodeError:
        sys.exit(1)
