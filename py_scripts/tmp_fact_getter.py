import json

import pymysql

host = 'eu-db-01.inventale.com'
user = 'liquibase'
password = 'thiequ0WiW6UaNgelilu'
db = 'rick_cpopro'


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


def get_forecast_dict(filename, value_type):
    with open(filename, 'r') as file:
        campaign_list = json.loads(file.read()).get('byCampaign')
        campaign_forecast = dict()
        for item in campaign_list:
            remoteid = item.get('campaign').get('remoteId')
            forecast = item.get(value_type)
            campaign_forecast.update({remoteid: forecast})
    return campaign_forecast


for count in [4]:
    print(f'Count {count}')
    default_file = f'/home/polter/forecast/default{count}'
    improve_file = f'/home/polter/forecast/improve{count}'
    value_type = 'matchedImpressions'

    default_forecast = get_forecast_dict(default_file, value_type)
    improve_forecast = get_forecast_dict(improve_file, value_type)

    startdate = "2020-07-13"
    enddate = "2020-07-16"

    sql = get_connection(host, user, password, db)
    query = (f"SELECT cmp.remoteId, sum(r.impressions) FROM rickcreativesiteplacereport r " +
             f"JOIN rickCreative crv ON crv.id = r.rickCreativeId " +
             f"JOIN rickcampaign cmp ON cmp.id = crv.rickCampaign_id " +
             f"WHERE r.dt BETWEEN '{startdate}' AND '{enddate}' " +
             f"group by cmp.remoteId;")
    testquery = 'describe rickcampaign;'
    with sql.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    fact = dict()
    for item in result:
        remoteid = item.get('remoteId')
        fact_value = item.get('sum(r.impressions)')
        fact.update({remoteid: fact_value})
    improve_fact = set(improve_forecast.keys()).intersection(set(fact.keys()))
    default_fact = set(default_forecast.keys()).intersection(set(fact.keys()))
    ucamp = default_fact.union(improve_fact)
    default_fail = 0
    improve_fail = 0
    for key in ucamp:
        default_f = default_forecast.get(key, None)
        improve_f = improve_forecast.get(key, None)
        f = fact.get(key, None)
        if default_f is not None and improve_f is not None and fact is not None:
            default_delta = abs(default_f - f) / default_f * 100
            improve_delta = abs(improve_f - f) / improve_f * 100
            # if improve_delta > 20 or default_delta > 20:
            #     print(f'Campaign {key}, default_forecast {default_f}, improve_forecast {improve_f}, fact {f}, default_delta {default_delta:.2f}, improve_delta {improve_delta:.2f}')
            if improve_delta > default_delta:
                improve_fail += 1
                print(
                    f'Campaign {key}, default_forecast {default_f}, improve_forecast {improve_f}, fact {f}, default_delta {default_delta:.2f}, improve_delta {improve_delta:.2f}')
            elif default_delta > improve_delta:
                default_fail += 1
                print(
                    f'Campaign {key}, default_forecast {default_f}, improve_forecast {improve_f}, fact {f}, default_delta {default_delta:.2f}, improve_delta {improve_delta:.2f}')
    print(f'Default fail {default_fail}, improve_fail {improve_fail}')
    # for key in improve_fact:
    #     print(f'Improve {key} forecast: {improve_forecast.get(key)}, fact {fact.get(key)}')
    # for key in default_fact:
    #     print(f'Default {key} forecast: {default_forecast.get(key)}, fact {fact.get(key)}')
print('stop')
