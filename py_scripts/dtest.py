import datetime

import pymongo

connection_dict = {
    'host': 'eu-smr-mng-01.maxifier.com',
    'port': 27017,
    'db': 'ifms_rick_default-perm'
}

collection = 'ifms.ForecastProgressEvent'


def mongo_connect(connection_dict):
    connection = pymongo.MongoClient(connection_dict.get('host'), int(connection_dict.get('port')))
    base = connection[connection_dict.get('db')]
    return base


def get_start_end_dates():
    s_date = datetime.date.today() - datetime.timedelta(days=2)
    s_date_time = datetime.datetime.fromordinal(s_date.toordinal())
    e_date = datetime.date.today() - datetime.timedelta(days=1)
    e_date_time = datetime.datetime.fromordinal(e_date.toordinal())
    return s_date_time, e_date_time


conn = mongo_connect(connection_dict)
progress = conn[collection]
start_date, end_date = get_start_end_dates()

find_query = {"state": "COMPLETED", "$and": [{"submitted": {"$gte": start_date}},
                                             {"submitted": {"$lte": end_date}}]}

result = progress.find(find_query)

for i in result:
    print(i)
