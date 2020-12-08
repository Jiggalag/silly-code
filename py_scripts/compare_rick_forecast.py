import json
import sys

from py_scripts.helpers.ifmsApiHelper import IFMSApiHelper
from py_scripts.helpers.logging_helper import Logger

server = 'dev01.inventale.com'
user = 'pavel.kiselev'
password = '6561bf7aacf5e58c6e03d6badcf13831'
scopes = ['dropped_adfoxdata', 'stopped_adfox']
context = 'ifms'
client = 'rick'
request = 'frc.json'
logger = Logger('INFO')

results = list()

for scope in scopes:
    api_point = IFMSApiHelper(server, user, password, context, logger)
    cookie = api_point.change_scope(client, scope)
    result = api_point.check_available_inventory(request, cookie).text
    print(request)
    print(cookie)

    try:
        json_result = json.loads(result)
        results.append(json_result)
    except json.JSONDecodeError:
        sys.exit(1)

for item in results[0].keys():
   print('Test {}'.format(item))
   a = results[0].get(item)
   b = results[1].get(item)
   print('Len of 0 result is {}'.format(a))
   print('Len of 1 result is {}'.format(b))
   print('Step finished...')

print('OK')
