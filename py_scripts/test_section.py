import json
import os.path

from py_scripts.helpers.ifmsApiHelper import IFMSApiHelper
from py_scripts.helpers.logging_helper import Logger

server = 'dev01.inventale.com'
user = 'pavel.kiselev'
password = '6561bf7aacf5e58c6e03d6badcf13831'
context = 'ifms'
client = 'rick'
scope = 'default'
request = 'frc.json'
logger = Logger('INFO')

api_point = IFMSApiHelper(server, user, password, context, logger)

cookie = api_point.change_scope(client, scope)

result = api_point.check_available_inventory(request, cookie).text
json_result = json.loads(result)
print(json_result.get('dbgInfo'))
print(json_result.get('summary'))
kv = json_result.get('byKeyvalue')



def create_section_map():
    section_map = dict()


directory = '/tmp/send_test/'
# os.mkdir(directory)
file_name = '{}result{}'.format(directory, len(os.listdir(directory)))
with open(file_name, 'w') as file:
    file.write(result)

order = {
    'availableUniques': 0,
    'bookedImpressions': 0,
    'expectedDelivery': 0,
    'matchedImpressions': 0,
    'matchedUniques': 0,
    'sharedImpressions': 0
}

remoteids = list()

for item in kv:
    if 'Section' in item.get('keyname').get('path'):
        target_kv = item.get('byValue')
        for section in target_kv:
            remoteids.append(section.get('criteria').get('remoteId'))
            if target_kv[0].get('criteria').get('remoteId') == 'UNKNOWN':
                print('Stop!')
            for name in order.keys():
                new_value = order.get(name) + section.get(name)

summary = json_result.get('summary')

for name in order.keys():
    print('For key {} diff equals {}'.format(name, summary.get(name) - order.get(name)))


print("File {} successfully saved...".format(file_name))
