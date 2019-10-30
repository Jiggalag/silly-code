import json
import sys
import argparse

from py_scripts.helpers.ifmsApiHelper import IFMSApiHelper
from py_scripts.helpers.logging_helper import Logger

server = 'eu-dev-01.inventale.com'
user = 'pavel.kiselev'
password = '6561bf7aacf5e58c6e03d6badcf13831'
# user = 'qa-ivi'
# password = '2c821c5131319f3b9cfa83885d552637'
# password = 'c64e8262333065b2f35cd52742bd5cfb'
context = 'ifms5'
client = 'pitt'
request = 'frc.json'
logger = Logger('DEBUG')

api_point = IFMSApiHelper(server, user, password, context, logger)

results = list()

for scope in ['default', 'api']:
    print(f'Process scope {scope}')
    cookie = api_point.change_scope(client, scope)

    result = api_point.check_available_inventory(request, cookie).text

    try:
        json_result = json.loads(result)
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

size1 = get_size(results[0])
size2 = get_size(results[1])


for name in order:
    with open('/home/jiggalag/results/{}.tsv'.format(name), 'w') as file:
        for num in range(len(results[0].get('byDate'))):
            date = results[0].get('byDate')[num].get('to')
            default = results[0].get('byDate')[num].get(name)
            extrapolator = results[1].get('byDate')[num].get(name)
            file.write('{},{},{}\n'.format(date, default, extrapolator))

print('OK')
