import json
import rstr
import requests

while True:
    bssid = rstr.xeger(r'(?:[A-Fa-f0-9]{2}[:]){5}(?:[A-Fa-f0-9]{2})').lower()
    # print('Now we process bssid {}...'.format(bssid))
    url = 'https://api.mylnikov.org/wifi?v=1.1&bssid={}'.format(bssid)
    response = requests.get(url)
    result = json.loads(response.text)
    code = result.get('result')
    if code == 404:
        # print('Found bad bssid: {}'.format(bssid))
        print('{}'.format(bssid))
