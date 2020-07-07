import json
import sys

import pymysql

from py_scripts.helpers.ifmsApiHelper import IFMSApiHelper
from py_scripts.helpers.logging_helper import Logger

host = 'eu-db-01.inventale.com'
user = 'liquibase'
password = 'thiequ0WiW6UaNgelilu'
db = 'rick_cpopro'

server = 'inv-dev-02.inventale.com'
ifmsuser = 'pavel.kiselev'
ifmspassword = '6561bf7aacf5e58c6e03d6badcf13831'  # from PoKuTe713
context = 'ifms1'
client = 'rick'
request = 'frc.json'
logger = Logger('DEBUG')


def get_connection(host, user, password, db):
    try:
        connection = pymysql.connect(host=host,
                                     user=user,
                                     password=password,
                                     db=db,
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.DictCursor)
        return connection
    except pymysql.err.OperationalError as e:
        print(e)
        return None


request = open('frc.json', 'r').read()

# api_point = IFMSApiV2(server, user, password, context, 'pitt', '132109', logger)
api_point = IFMSApiHelper(server, ifmsuser, ifmspassword, context, logger)

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

remoteId = 78979879
startdate = "2020-07-01"
enddate = "2020-07-05"
commonquery = (f"SELECT cmp.remoteId, sum(r.impressions) FROM rickcreativesiteplacereport r " +
               f"JOIN rickCreative crv ON crv.id = r.rickCreativeId " +
               f"JOIN rickcampaign cmp ON cmp.id = crv.rickCampaign_id " +
               f"WHERE r.dt BETWEEN '{startdate}' AND '{enddate}' " +
               f"group by cmp.remoteId;")

sql = get_connection(host, user, password, db)

with sql.cursor() as cursor:
    cursor.execute(commonquery)
    commonresult = cursor.fetchall()

resultids = set()

for item in commonresult:
    resultids.add(item.get('remoteId'))

query = (f"SELECT cmp.remoteId, sum(r.impressions) FROM rickcreativesiteplacereport r " +
         f"JOIN rickCreative crv ON crv.id = r.rickCreativeId " +
         f"JOIN rickcampaign cmp ON cmp.id = crv.rickCampaign_id " +
         f"WHERE cmp.remoteId in ({', '.join(campaigns[0])}) and r.dt BETWEEN '{startdate}' AND '{enddate}' " +
         f"group by cmp.remoteId;")
print(query)
with sql.cursor() as cursor:
    cursor.execute(query)
    result = cursor.fetchall()
print('stop')
