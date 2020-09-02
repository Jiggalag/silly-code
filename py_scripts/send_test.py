import json
import sys

from py_scripts.helpers.ifms_api_v2 import IFMSApiV2
from py_scripts.helpers.logging_helper import Logger

# server = 'ifms3.inventale.com'
server = 'inv-dev-02.inventale.com'
# server = 'pitt-us.inventale.com'
user = 'pavel.kiselev'
# user = 'analyst'
# password = '2c821c5131319f3b9cfa83885d552637'
# password = '4c029545591e3f76e2858dcb19c31808'  # from analyst
password = '6561bf7aacf5e58c6e03d6badcf13831'  # from PoKuTe713
# password = 'c64e8262333065b2f35cd52742bd5cfb'
context = 'ifms1'
client = 'rick'
# scope = '79505'
request = 'frc.json'
logger = Logger('DEBUG')

forecast_values = [
    'matchedImpressions',
    'availableUniques',
    'matchedUniques',
    'sharedImpressions',
    'expectedDelivery',
    'bookedImpressions'
]


def prepare_dimension_result(forecast_dimension, value):
    result = dict()
    for item in forecast_dimension:
        try:
            remoteid = item.get('criteria').get('remoteId')
        except AttributeError as e:
            remoteid = item.get('campaign').get('remoteId')
        forecast = item.get(value)
        result.update({remoteid: forecast})
    return result


# request = {([('startDate', '2018-09-19T21:00:00.000Z'), ('endDate', '2018-09-20T21:00:00.000Z'), ('priority', 10),
#              ('dimensions', '["summary"]'), ('pacing', 'ASAP'), ('weight', 100)])}

request = open('frc.json', 'r').read()

api_point2 = IFMSApiV2(server, user, password, context, 'rick', 'default', logger)
# api_point = IFMSApiHelper(server, user, password, context, logger)

results = list()

for scope in ['paragg']:
    print(f'Forecast for scope {scope}')
    # cookie = api_point.change_scope(client, scope)
    # result = api_point.get_query(cookie, 'a0306ab0-7bb7-11e9-8e25-7d73cdcbd8fa')
    # result = json.loads(api_point.check_available_inventory('frc.json', cookie).text)
    result2 = json.loads(api_point2.check_available_inventory('frc.json').text)
    try:
        json_result = json.loads(result2)
        # with open('/home/polter/wee2', 'w') as file:
        #     file.write(json.dumps(json_result, indent=4))
        results.append(json_result)
    except json.JSONDecodeError:
        sys.exit(1)

ks = [
    'byDate',
    'byPage',
    'bySite',
    'bySection',
    'byBrowser',
    'byBrowserV',
    'byOS',
    'byCity',
    'byState',
    'byCountry',
    'byGeo',
    'byTrafficAllocation',
    'byPagePosition',
    'byStatePlatform',
    'byCityPlatform',
    'byGenrePlatform',
    'byDatePage',
    'byCampaign',
    'byOrder'
]

print(f"first time is {results[0].get('dbgInfo').get('requestTime')}")
print(f"second time is {results[1].get('dbgInfo').get('requestTime')}")

for fv in forecast_values:
    # print(f'Check value {fv}')
    for c in range(0, 10):
        kv0 = prepare_dimension_result(results[0].get('byKeyvalue')[c].get('byValue'), fv)
        kv1 = prepare_dimension_result(results[1].get('byKeyvalue')[c].get('byValue'), fv)
        if kv0 != kv1:
            print(f'There is differences on kv {c} for value {fv}')
    for k in ks:
        # print(f'Now we process key {k}')
        t0 = prepare_dimension_result(results[0].get(k), fv)
        t1 = prepare_dimension_result(results[1].get(k), fv)
        if t0 != t1:
            print(f'There is differences for key {k} on {fv}')
    for k in ['byCampaign', 'byOrder']:
        # print(f'Now we process key {k}')
        t0 = prepare_dimension_result(results[0].get(k), fv)
        t1 = prepare_dimension_result(results[1].get(k), fv)
        if t0 != t1:
            print(f'There is differences for key {k} on {fv}')
print('stop')

for key in results[0].keys():
    if results[0].get(key) != results[1].get(key):
        if key in ['queryId']:
            print(f'Skipped {key}')
            continue
        print(f'There is differences in key {key}')
        for item1, item2 in zip(results[0].get(key), results[1].get(key)):
            if item1 != item2:
                for fv in forecast_values:
                    if item1.get(fv) != item2.get(fv):
                        # print(f'item1: {item1.get("criteria")}, item2 {item2.get("criteria")}, value1 {item1.get(fv)}, value2 {item2.get(fv)}, key {fv}')
                        if item1.get('criteria').get("remoteId") == item2.get('criteria').get("remoteId"):
                            print(
                                f'remoteid {item1.get("criteria").get("remoteId")} key {fv}, value1 {item1.get(fv)}, value2 {item2.get(fv)}')

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

with open('/home/polter/forecast/default7', 'w') as file:
    file.write(json.dumps(results[0], indent=4))

with open('/home/polter/forecast/improve7', 'w') as file:
    file.write(json.dumps(results[1], indent=4))

for name in order:
    with open('/home/jiggalag/results/{}.tsv'.format(name), 'w') as file:
        for num in range(len(results[0].get('byDate'))):
            date = results[0].get('byDate')[num].get('to')
            default = results[0].get('byDate')[num].get(name)
            extrapolator = results[1].get('byDate')[num].get(name)
            file.write('{},{},{}\n'.format(date, default, extrapolator))

print('OK')
