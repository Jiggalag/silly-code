import json
import sys

from py_scripts.helpers.ifmsApiHelper import IFMSApiHelper
from py_scripts.helpers.logging_helper import Logger

# server = 'dev01.inventale.com'
server = 'inv-dev-02.inventale.com'
# server = 'pitt-us.inventale.com'
user = 'pavel.kiselev'
# user = 'analyst'
# password = '2c821c5131319f3b9cfa83885d552637'
# password = '4c029545591e3f76e2858dcb19c31808'
password = '6561bf7aacf5e58c6e03d6badcf13831'  # from PoKuTe713
# password = 'c64e8262333065b2f35cd52742bd5cfb'
context = 'ifms1'
client = 'rick'
# scope = '79505'
request = 'frc.json'
logger = Logger('DEBUG')

# request = {([('startDate', '2018-09-19T21:00:00.000Z'), ('endDate', '2018-09-20T21:00:00.000Z'), ('priority', 10),
#              ('dimensions', '["summary"]'), ('pacing', 'ASAP'), ('weight', 100)])}

request = open('frc.json', 'r').read()

# api_point = IFMSApiV2(server, user, password, context, 'pitt', '132109', logger)
api_point = IFMSApiHelper(server, user, password, context, logger)

results = list()

for scope in ['default', 'improve']:
    print(f'Forecast for scope {scope}')
    cookie = api_point.change_scope(client, scope)
    # result = api_point.get_query(cookie, 'a0306ab0-7bb7-11e9-8e25-7d73cdcbd8fa')
    result = api_point.check_available_inventory('frc.json', cookie).text
    try:
        json_result = json.loads(result)
        # with open('/home/polter/wee2', 'w') as file:
        #     file.write(json.dumps(json_result, indent=4))
        results.append(json_result)
    except json.JSONDecodeError:
        sys.exit(1)

campaigns = list()
for result in results:
    tmp_campaigns = set()
    for item in result['byCampaign']:
        tmp_campaigns.add(item.get('campaign').get('remoteId'))
    campaigns.append(tmp_campaigns)

c1 = dict()
c2 = dict()
for item in results[0]['byCampaign']:
    id = item['campaign']['remoteId']
    imps = item['matchedImpressions']
    c1.update({id: imps})

for item in results[1]['byCampaign']:
    id = item['campaign']['remoteId']
    imps = item['matchedImpressions']
    c2.update({id: imps})

common = set(c1.keys()).intersection(set(c2.keys()))
for item in common:
    sub = abs(c1.get(item) - c2.get(item))
    perc = sub / c1.get(item) * 100
    if perc > 20:
        print(f'remoteid {item}: default - {c1.get(item)}, improve - {c2.get(item)}')

print('stop')

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

campaign1 = results[0].get('byCampaign')
campaign2 = results[1].get('byCampaign')
remoteid1 = set()
remoteid2 = set()

for campaign in campaign1:
    id = campaign.get('campaign').get('remoteId')
    remoteid1.add(id)

for campaign in campaign2:
    id = campaign.get('campaign').get('remoteId')
    remoteid2.add(id)

rrr = remoteid2 - remoteid1

for campaign in campaign2:
    remoteid = campaign.get('campaign').get('remoteId')
    matchedImpressions = campaign.get('matchedImpressions')
    if remoteid in rrr:
        print(f'remoteId {remoteid}: matchedImpressions {matchedImpressions}')

with open('/home/polter/forecast/default0', 'w') as file:
    file.write(json.dumps(results[0], indent=4))

with open('/home/polter/forecast/improve0', 'w') as file:
    file.write(json.dumps(results[1], indent=4))

for name in order:
    with open('/home/jiggalag/results/{}.tsv'.format(name), 'w') as file:
        for num in range(len(results[0].get('byDate'))):
            date = results[0].get('byDate')[num].get('to')
            default = results[0].get('byDate')[num].get(name)
            extrapolator = results[1].get('byDate')[num].get(name)
            file.write('{},{},{}\n'.format(date, default, extrapolator))

print('OK')
