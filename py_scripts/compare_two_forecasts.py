import json
import sys
from py_scripts.helpers.ifmsApiHelper import IFMSApiHelper
from py_scripts.helpers.logging_helper import Logger

server = 'eu-dev-01.inventale.com'
user = 'pavel.kiselev'
password = '6561bf7aacf5e58c6e03d6badcf13831'
context = 'ifms'
client = 'irving'
request = 'frc.json'
logger = Logger('DEBUG')

api_point = IFMSApiHelper(server, user, password, context, logger)
results = list()


for scope in ['def_pub', 'default']:
    print(f'Process scope {scope}')
    cookie = api_point.change_scope(client, scope)

    result = api_point.check_available_inventory(request, cookie).text

    try:
        json_result = json.loads(result)
        results.append(json_result)
    except json.JSONDecodeError:
        sys.exit(1)


def wrwr(name, forecast):
    with open(f'/home/polter/{name}', 'w') as f:
        f.write(json.dumps(forecast))

order = [
    'availableUniques',
    'bookedImpressions',
    'expectedDelivery',
    'matchedImpressions',
    'matchedUniques',
    'sharedImpressions'
]

wrwr('757n801', results[0])

d = dict()
f = dict()

for key in results[0].keys():
    if results[0].get(key) != results[1].get(key):
        print(f'Oops on key {key}')



for name in order:
    with open('/home/jiggalag/results/{}.tsv'.format(name), 'w') as file:
        for num in range(len(results[0].get('byDate'))):
            date = results[0].get('byDate')[num].get('to')
            default = results[0].get('byDate')[num].get(name)
            extrapolator = results[1].get('byDate')[num].get(name)
            file.write('{},{},{}\n'.format(date, default, extrapolator))

print('OK')
