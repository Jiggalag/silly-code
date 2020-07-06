import argparse
import json
import time
from os.path import expanduser

import requests
import rstr

parser = argparse.ArgumentParser(description='Intended to generating file with incorrect wifi bssids')
parser.add_argument('amount', type=int, help='amount of incorrect bssids')
args = parser.parse_args()
amount = args.amount

good_bssids = set()
counter = 0

while len(good_bssids) < amount:
    counter += 1
    bssid = rstr.xeger(r'(?:[A-Fa-f0-9]{2}[:]){5}(?:[A-Fa-f0-9]{2})').lower()
    url = 'https://api.mylnikov.org/wifi?v=1.1&bssid={}'.format(bssid)
    response = requests.get(url)
    if response.status_code != 200:
        response = requests.get(url)
        if response.status_code != 200:
            response = requests.get(url)
    try:
        result = json.loads(response.text)
    except json.decoder.JSONDecodeError as e:
        print(e.args[0])
        continue
    code = result.get('result')
    if code == 200:
        print('{}'.format(bssid))
        try:
            good_bssids.update({bssid})
        except:
            print('[ERROR] bssid {} already added to set'.format(bssid))
            continue
    print('Processed {} bssids, found {} good bssids...'.format(counter, len(good_bssids)))

datalist = list()

for bssid in good_bssids:
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
filepath = '{}/good_bssid_result'.format(expanduser('~'))
with open(filepath, 'w') as file:
    file.write(json.dumps(datalist, indent=4))
    print('Request successfully saved to {}'.format(filepath))
