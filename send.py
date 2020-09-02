import argparse
import datetime
import json
import time
from random import randrange

import requests

from py_scripts.helpers.ifms_api_v2 import IFMSApiV2
from py_scripts.helpers.logging_helper import Logger

server = 'ifms3.inventale.com'
user = 'analyst'
password = '4c029545591e3f76e2858dcb19c31808'
context = 'ifms'
client = 'rick'

scope = 'default'
scopes = [
    '1269',
    '132109',
    '156315',
    '156535',
    '79505'
]

parser = argparse.ArgumentParser(description='Check that forecast will be calculated for archived campaigns')
parser.add_argument('scope', help='List of clients, ehisch should be tested')
args = parser.parse_args()


# scope = args.scope
# scope = 'default'


def api_authenticate(server, user, password):
    login_url = 'https://{}/login.action'.format(server)
    data = {'loginActionForm.login': user, 'loginActionForm.password': password}
    try:
        login_response = requests.post(url=login_url, data=data, allow_redirects=False)
    except requests.exceptions.ConnectionError:
        login_response = requests.post(url=login_url, data=data, allow_redirects=False)
    return login_response.cookies


def change_scope(server, user, password, client, scope):
    cookies = api_authenticate(server, user, password)
    url = "https://{}/changeClient.action?clientScope={}.{}".format(server, client, scope)
    headers = {
        'Content-Type': 'application/json',
        'Host': server,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Upgrade-Insecure-Requests': '1'
    }
    try:
        requests.get(url, headers=headers, cookies=cookies)
    except requests.exceptions.ConnectionError:
        requests.get(url, headers=headers, cookies=cookies)
    cookies = api_authenticate(server, user, password)
    return cookies


def check_available_inventory(json_file, cookies, account_id=None, wait_timeout=600, ping_timeout=1):
    if account_id is None:
        available_forecast_url = 'https://{}/{}/api/v1/checkAvailableInventory'.format(server, context)
    else:
        available_forecast_url = 'https://{}/{}/api/v1/checkAvailableInventory?accountId={}'.format(server,
                                                                                                    context,
                                                                                                    account_id)
    try:
        loaded_json = json.load(open(json_file))
        name = json_file
    except TypeError:
        if 'JsonQuery' in str(type(json_file)):
            loaded_json = json_file.json_data
            name = json_file.queryname
        else:
            loaded_json = json_file
            name = 'forecast_query'
    headers = {'Content-Type': 'application/json'}
    response_text = None
    stop_time = datetime.datetime.now() + datetime.timedelta(seconds=wait_timeout)
    while response_text is None:
        if datetime.datetime.now() > stop_time:
            print('Forecast takes more {} seconds, skipped...'.format(wait_timeout))
            return None
        counter = 0
        response = requests.post(available_forecast_url, json=loaded_json, cookies=cookies, headers=headers)
        if response.status_code == 401:
            print("You unauthorized. Exit...")
            return None
        elif response.status_code == 502:
            while counter < 5:
                print("Check host, you've got 502 bad gateway error")
                response = requests.post(available_forecast_url, json=loaded_json, cookies=cookies, headers=headers)
                counter += 1
                if response.status_code != 502:
                    break
                time.sleep(3)
            print("Bad gateway, we waste all 5 attempts.")
            return None
        elif response.status_code != 200:
            if 'Unexpected' in response.text:
                print(("Check json {}. ".format(name) +
                       "Probably json have syntax problem. {}".format(response.text)))
                return None
            elif 'possible' in response.text:
                print(("Check json {}. ".format(name) +
                       "Probably json have date format problem. {}".format(response.text)))
                return None
            elif 'BEGIN_ARRAY' in response.text:
                print(("Check json {}. ".format(name) +
                       "Probably json have data structure problem. {}".format(response.text)))
                return None
            elif 'Pages not found:' in response.text:
                print("Check json {}. Probably you set incorrect page id. {}".format(name,
                                                                                     response.text))
                return None
            elif 'Last simulation cache version has changed' in response.text:
                print("{}\nProbably, future samples are generating now".format(response.text))
                return None
            elif 'recover forecast' in response.text:
                print(response.text)
                return None
            elif 'not found:' in response.text:
                print("{}\nProbably, some entities in json cannot be found".format(response.text))
                return None
            elif 'only one product in query supported' in response.text:
                print(response.text)
                return response
            elif 'Following parameters are not supported:' in response.text:
                print(response.text)
                return None
            else:
                return response
        elif response.status_code == 200:
            if 'matched' in response.text:
                return response
            else:
                # print(response.text)
                progress_percent = json.loads(response.text)
                # print(("Forecasting {}. ".format(name) +
                #                   "Progress percent is {}".format(progress_percent.get('progressPercent'))))
                time.sleep(ping_timeout)
        else:
            print('Error raised during forecasting. Backend message:\n{}'.format(response.text))


cookie = change_scope(server, user, password, client, scope)
request = {
    "startDate": "2020-07-16T21:00:00.000Z",
    "endDate": "2021-09-19T20:59:{}.000Z".format(randrange(60)),
    "dimensions": [
        'site',
        'page',
        'geo',
        'keyvalue',
        'campaign',
        'order',
        'date'
    ],
    "priority": 90
}

while True:
    request = {
        "startDate": "2020-10-18T21:00:00.000Z",
        "endDate": "2021-10-06T20:59:59.000Z",
        # "endDate": "2020-06-21T20:{0:0>2}:{0:0>2}.{0:0>3}Z".format(randrange(60), format(randrange(60)), randrange(500)),
        "dimensions": [
            "Date",
            "summary"
        ],
        # "frequencyTargetings": [
        #     {"scope": "DAILY",
        #     "timePeriod": 20,
        #      "impsLimit": 3}
        # ],
        "priority": 20
    }
    # print(request.get('endDate'))
    apiv2 = IFMSApiV2(server, user, password, context, client, scope, Logger('DEBUG'))
    result = json.loads(apiv2.check_available_inventory(request).text)
    requesttime = result.get('dbgInfo').get('requestTime')
    rpcrequesttime = result.get('dbgInfo').get('rpcRequestTime')
    dbrequesttime = result.get('dbgInfo').get('dbRequestTime')
    print(f'requestTime is {requesttime}, rpcRequestTime is {rpcrequesttime}, dbRequestTime {dbrequesttime}')
    # print('Forecast for {} successfully calculated in {}...'.format(scope, result.get('d')))
