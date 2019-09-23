import json
import time

with open('/home/polter/123', 'r') as file:
    content = file.read().split('\n')
    print('stop')

prc_bssid = content[5:519]

print('stop')

datalist = list()

for bssid in prc_bssid:
    mold = {
          "bssid": "{}".format(bssid),
          "centerFreq0": 2437,
          "centerFreq1": 0,
          "channelWidth": 1,
          "frequency": 2427,
          "isConnected": False,
          "lastSeen": int(time.time() * 1000),
          "level": -57,
          "operatorFriendlyName": "",
          "ssid": "test1",
          "venueName": "",
          "idType": "AAID",
          "timestamp": 1565241485302
    }
    datalist.append(mold)
with open('/home/polter/bssid_result', 'w') as file:
    file.write(json.dumps(datalist, indent=4))
print('stop')
