import sys
from py_scripts.helpers.ifmsApiHelper import IFMSApiHelper
from py_scripts.helpers.logging_helper import Logger
import json

server = 'dev01.inventale.com'
# server = 'ifms3.inventale.com'
user = 'pavel.kiselev'
# user = 'qa-rick'
password = '6561bf7aacf5e58c6e03d6badcf13831'
# password = 'c64e8262333065b2f35cd52742bd5cfb'
context = 'ifms'
client = 'marvin'
scope = 'default'
request = 'frc.json'
logger = Logger('DEBUG')

sql_params = {
    'host': 'samaradb03.maxifier.com',
    'user': 'itest',
    'password': 'ohk9aeVahpiz1wi',
    'db': 'marvin'
}

api_point = IFMSApiHelper(server, user, password, context, logger)
#
# cookie = api_point.change_scope(client, scope)
#
# result = api_point.check_available_inventory(request, cookie).text
#
# try:
#     json_result = json.loads(result)
# except json.JSONDecodeError:
#     sys.exit(1)

results = list()

# for scope in ['default', 'extrapolation']:
for scope in ['default', 'mrv_dspbranch']:
# for scope in ['default', 'mrv_extrapolator']:
    cookie = api_point.change_scope(client, scope)

    result = api_point.check_available_inventory(request, cookie).text

    try:
        json_result = json.loads(result)
        results.append(json_result)
    except json.JSONDecodeError:
        sys.exit(1)


    # campaignids = list()
    # for item in json_result.get('byCampaign'):
    #     campaignids.append(item.get('campaign').get('remoteId'))
    #
    # pageids = list()
    # for item in json_result.get('byPage'):
    #     pageids.append(item.get('criteria').get('remoteId'))
    #
    # orderids = list()
    # for item in json_result.get('byOrder'):
    #     id = item.get('campaign').get('remoteId')
    #     orderids.append(id)
    #     if id in ('90871', '91082', '92745', '93273', '93357', '93634'):
    #         print(item)
    #
    # siteids = list()
    # for item in json_result.get('bySite'):
    #     siteids.append(item.get('criteria').get('remoteId'))
    #
    # table_name = 'campaign'
    # query = 'SELECT * FROM {} WHERE remoteid IN ({});'.format(table_name, campaignids)
    # query1 = 'SELECT * FROM insertionorder WHERE remoteid in ({});'.format(orderids)
    # query2 = 'SELECT * FROM page WHERE remoteid in ({});'.format(pageids)
    # query3 = 'SELECT * FROM site WHERE remoteid in ({});'.format(siteids)
    #
    # print(json_result.get('dbgInfo'))
    # print(json_result.get('summary'))
    # frc = json_result.get('bySite')

order = [
    'availableUniques',
    'bookedImpressions',
    'expectedDelivery',
    'matchedImpressions',
    'matchedUniques',
    'sharedImpressions'
]

for name in order:
    with open('/home/jiggalag/results/{}.tsv'.format(name), 'w') as file:
        for num in range(len(results[0].get('byDate'))):
            date = results[0].get('byDate')[num].get('to')
            default = results[0].get('byDate')[num].get(name)
            extrapolator = results[1].get('byDate')[num].get(name)
            file.write('{},{},{}\n'.format(date, default, extrapolator))

print('OK')
