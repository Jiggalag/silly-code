import argparse
import os.path
from py_scripts.helpers.ifmsApiHelper import IFMSApiHelper
from py_scripts.helpers.logging_helper import Logger
import json

parser = argparse.ArgumentParser(description='Utility intended to comparing databases')
parser.add_argument('password', type=str, help='Password hash for user')
parser.add_argument('--server', type=str, default='dev01.inventale.com',
                    help='Host, where script sends requests (default: dev01.inventale.com)')


args = parser.parse_args()

server = args.server
user = 'pavel.kiselev'
password = args.password
context = 'ifms'
client = 'rick'
scope = 'default'
request = 'frc.json'
logger = Logger('INFO')

api_point = IFMSApiHelper(server, user, password, context, logger)

cookie = api_point.change_scope(client, scope)

result = api_point.check_available_inventory(request, cookie).text
json_result = json.loads(result)
logger.info(str(json_result.get('dbgInfo')))
logger.info(str(json_result.get('summary')))
kv = json_result.get('byKeyvalue')
kv_list = [
    'Section',
    'Subsection',
    'Income',
    'Interest',
    'Age',
    'Gender'
]

dimension_list = [
    'byPage',
    'byGeo',
    'bySite'
]


def init_dict():
    result_dict = dict()
    order = [
        'availableUniques',
        'bookedImpressions',
        'expectedDelivery',
        'matchedImpressions',
        'matchedUniques',
        'sharedImpressions'
    ]
    for item in order:
        result_dict.update({item: 0})
    return result_dict


def aggregate_forecast(section):
    order = init_dict()
    for item in section:
        # remoteids.append(section.get('criteria').get('remoteId'))
        if item.get('criteria').get('remoteId') == 'UNKNOWN':
            for u in order.keys():
                logger.info('Statistics for UNKNOWN: {} is {}'.format(u, item.get(u)))
        for key in order.keys():
            new_value = order.get(key) + item.get(key)
            order.update({key: new_value})
    return order


def aggregate_by_date(data):
    order = init_dict()
    for item in data:
        for key in order.keys():
            new_value = order.get(key) + item.get(key)
            order.update({key: new_value})
    return order


def aggregate_for_campaigns(data):
    order = init_dict()
    for item in data:
        for key in order.keys():
            # remoteids.append(data.get('campaign').get('remoteId'))
            new_value = order.get(key) + item.get(key)
            order.update({key: new_value})
    return order


def aggregate_kv_forecast(kv_section, kv_name):
    unknown_list = [
        'UNKNOWN',
        'section!?',
        'subsection!?',
        'age!?',
        'income!?',
        'gender!?',
        'interest!?'
    ]
    order = init_dict()
    for item in kv_section:
        if kv_name in item.get('keyname').get('path'):
            target_kv = item.get('byValue')
            for section in target_kv:
                # remoteids.append(section.get('criteria').get('remoteId'))
                for unknown in unknown_list:
                    if section.get('criteria').get('remoteId') == unknown:
                        for u in order.keys():
                            logger.info('Statistics for UNKNOWN: {} is {}'.format(u, section .get(u)))
                for key in order.keys():
                    new_value = order.get(key) + section.get(key)
                    order.update({key: new_value})
    return order


def print_results(summary, aggregated):
    for name in aggregated.keys():
        if summary.get(name) > aggregated.get(name):
            logger.info(('For key {} diff value from summary section '.format(name) +
                         'more than aggregated value on {}'.format(summary.get(name) - aggregated.get(name))))
        if summary.get(name) < aggregated.get(name):
            logger.info(('For key {} diff value from summary section '.format(name) +
                         'less than aggregated value on {}'.format(aggregated.get(name) - summary.get(name))))
        if summary.get(name) == aggregated.get(name):
            logger.info('For key {} value from summary section value equals aggregated value'.format(name))


directory = '/tmp/send_test/'
if not os.path.exists(directory):
    os.mkdir(directory)
file_name = '{}result{}'.format(directory, len(os.listdir(directory)))
with open(file_name, 'w') as file:
    file.write(result)

summary_dict = json_result.get('summary')

for kv_type in kv_list:
    logger.info('Now we check {} dimension'.format(kv_type))
    aggregated_dict = aggregate_kv_forecast(kv, kv_type)
    print_results(summary_dict, aggregated_dict)
    print('\n')

for dim_type in dimension_list:
    logger.info('Now we check {} dimension'.format(dim_type))
    aggregated_dict = aggregate_forecast(json_result.get(dim_type))
    print_results(summary_dict, aggregated_dict)
    print('\n')

logger.info('Now we check {} dimension'.format('byDate'))
aggregated_dict = aggregate_by_date(json_result.get('byDate'))
print_results(summary_dict, aggregated_dict)
print('\n')

logger.info('Now we check {} dimension'.format('byCampaign'))
aggregated_dict = aggregate_for_campaigns(json_result.get('byCampaign'))
print_results(summary_dict, aggregated_dict)
print('\n')

logger.info('Now we check {} dimension'.format('byOrder'))
aggregated_dict = aggregate_for_campaigns(json_result.get('byOrder'))
print_results(summary_dict, aggregated_dict)
print('\n')

logger.info("File {} successfully saved...".format(file_name))
