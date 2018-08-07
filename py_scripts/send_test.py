from py_scripts.helpers.ifmsApiHelper import IFMSApiHelper
from py_scripts.helpers.logging_helper import Logger
import json

server = 'dev01.inventale.com'
# server = 'ifms.inventale.com'
user = 'pavel.kiselev'
password = '6561bf7aacf5e58c6e03d6badcf13831'
# password = '2c821c5131319f3b9cfa83885d552637'
context = 'ifms'
client = 'irving'
scope = 'default'
request = 'frc.json'
logger = Logger('INFO')

api_point = IFMSApiHelper(server, user, password, context, logger)

values = [
    'availableUniques',
    'bookedImpressions',
    'expectedDelivery',
    'matchedImpressions',
    'matchedUniques',
    'sharedImpressions'
]

results = list()

for scope in ['default', 'extrapolation']:
    cookie = api_point.change_scope(client, scope)
    result = api_point.check_available_inventory(request, cookie).text
    results.append(json.loads(result))

for value in values:
    with open('/home/polter/results/{}.tsv'.format(value), 'w') as f:
        for n in range(0, len(results[0].get('byDate')) - 1):
            date = results[0].get('byDate')[n].get('from')
            first = results[0].get('byDate')[n].get(value)
            second = results[1].get('byDate')[n].get(value)
            f.write('{},{},{}\n'.format(date, first, second))


print('OK')

json_result = json.loads(result)
print(json_result.get('dbgInfo'))
print(json_result.get('summary'))
frc = json_result.get('bySite')
