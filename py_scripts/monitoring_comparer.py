import csv
with open('/home/polter/Downloads/monitoring_2020-09-30.csv', 'r') as monitoring:
    monitoring_reader = csv.reader(monitoring, delimiter=',', quotechar='"')
    monitoring_list = list()
    monitoring_dict = dict()
    for row in monitoring_reader:
        monitoring_list.append(row)
        try:
            remoteid = int(row[2])
            monitoring_dict.update({remoteid: row[15]})
        except:
            pass

with open('/home/polter/Downloads/forecast-campaign-improve.csv', 'r') as forecast:
    forecast_reader = csv.reader(forecast, delimiter=',', quotechar='"')
    forecast_list = list()
    forecast_dict = dict()
    for row in forecast_reader:
        forecast_list.append(row)
        try:
            remoteid = int(row[0])
            forecast_dict.update({remoteid: row[11]})
        except:
            pass
common_ids = set(monitoring_dict.keys()).intersection(set(forecast_dict.keys()))
monitoring_uniqs = set(monitoring_dict.keys()) - set(forecast_dict.keys())
for row in monitoring_list[3:]:
    remoteid = int(row[2])
    status = row[3]
    yesterday = row[5]
    delivery = row[7]
    goal = row[18]
    if remoteid in monitoring_uniqs:
        if delivery < goal:
            print(f'{status} flight {remoteid} not in forecast. Yesterday delivered: {yesterday}, today delivered: {delivery}, goal: {goal}')

for id in common_ids:
    monitoring_value = monitoring_dict.get(id)
    forecast_value = forecast_dict.get(id)
    if monitoring_value != forecast_value:
        print(f'Difference for flight {id}. Monitoring: {monitoring_value}, forecast: {forecast_value}')
print('stop')
