import datetime
import json
import time

import requests


class IFMSApiHelper:
    def __init__(self, server, user, password, context, logger):
        self.logger = logger
        self.server = server
        self.user = user
        self.password = password
        self.context = context

    def api_authenticate(self):
        login_url = 'https://{}/login.action'.format(self.server)
        data = {'loginActionForm.login': self.user, 'loginActionForm.password': self.password}
        login_response = requests.post(url=login_url, data=data, allow_redirects=False)
        return login_response.cookies

    def change_scope(self, client, scope):
        cookies = self.api_authenticate()
        url = "https://{}/changeClient.action?clientScope={}.{}".format(self.server, client, scope)
        headers = {
            'Content-Type': 'application/json',
            'Host': self.server,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Upgrade-Insecure-Requests': '1'
        }
        requests.get(url, headers=headers, cookies=cookies)
        cookies = self.api_authenticate()
        return cookies

    def check_available_inventory(self, json_file, cookies, account_id=None, wait_timeout=600, ping_timeout=1):
        if account_id is None:
            available_forecast_url = 'https://{}/{}/api/v1/checkAvailableInventory'.format(self.server, self.context)
        else:
            available_forecast_url = 'https://{}/{}/api/v1/checkAvailableInventory?accountId={}'.format(self.server,
                                                                                                        self.context,
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
                self.logger.error('Forecast takes more {} seconds, skipped...'.format(wait_timeout))
                return None
            counter = 0
            response = requests.post(available_forecast_url, json=loaded_json, cookies=cookies, headers=headers)
            if response.status_code == 401:
                self.logger.critical("You unauthorized. Exit...")
                return None
            elif response.status_code == 502:
                while counter < 5:
                    self.logger.warn("Check host, you've got 502 bad gateway error")
                    response = requests.post(available_forecast_url, json=loaded_json, cookies=cookies, headers=headers)
                    counter += 1
                    if response.status_code != 502:
                        break
                    time.sleep(3)
                self.logger.error("Bad gateway, we waste all 5 attempts.")
                return None
            elif response.status_code != 200:
                if 'Unexpected' in response.text:
                    self.logger.error(("Check json {}. ".format(name) +
                                       "Probably json have syntax problem. {}".format(response.text)))
                    return None
                elif 'possible' in response.text:
                    self.logger.error(("Check json {}. ".format(name) +
                                       "Probably json have date format problem. {}".format(response.text)))
                    return None
                elif 'BEGIN_ARRAY' in response.text:
                    self.logger.error(("Check json {}. ".format(name) +
                                       "Probably json have data structure problem. {}".format(response.text)))
                    return None
                elif 'Pages not found:' in response.text:
                    self.logger.error("Check json {}. Probably you set incorrect page id. {}".format(name,
                                                                                                     response.text))
                    return None
                elif 'Last simulation cache version has changed' in response.text:
                    self.logger.error("{}\nProbably, future samples are generating now".format(response.text))
                    return None
                elif 'recover forecast' in response.text:
                    self.logger.debug(response.text)
                    return None
                elif 'not found:' in response.text:
                    self.logger.error("{}\nProbably, some entities in json cannot be found".format(response.text))
                    return None
                elif 'only one product in query supported' in response.text:
                    self.logger.debug(response.text)
                    return response
                elif 'Following parameters are not supported:' in response.text:
                    self.logger.debug(response.text)
                    return None
                else:
                    return response
            elif response.status_code == 200:
                if 'matched' in response.text:
                    return response
                else:
                    self.logger.debug(response.text)
                    progress_percent = json.loads(response.text)
                    self.logger.info(("Forecasting {}. ".format(name) +
                                      "Progress percent is {}".format(progress_percent.get('progressPercent'))))
                    time.sleep(ping_timeout)
            else:
                self.logger.error('Error raised during forecasting. Backend message:\n{}'.format(response.text))

    def get_products(self, cookies):
        get_products_url = 'https://{}/{}/api/v1/getProducts'.format(self.server, self.context)
        headers = {'Content-Type': 'application/json'}
        response = requests.post(get_products_url, cookies=cookies, headers=headers)
        if response.status_code != 200:
            self.logger.error(response.text)
        else:
            return json.loads(response.text)

    def get_query(self, cookies, iid):
        url = 'https://{}/{}/api/v1/forecast/{}'.format(self.server, self.context, iid)
        headers = {'Content-Type': 'application/json'}
        response = requests.get(url, cookies=cookies, headers=headers)
        print('kek')
