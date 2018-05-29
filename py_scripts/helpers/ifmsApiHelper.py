import json
import requests
import time


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
        try:
            login_response = requests.post(url=login_url, data=data, allow_redirects=False)
        except requests.exceptions.ConnectionError:
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
        try:
            requests.get(url, headers=headers, cookies=cookies)
        except requests.exceptions.ConnectionError:
            requests.get(url, headers=headers, cookies=cookies)
        cookies = self.api_authenticate()
        return cookies

    def check_available_inventory(self, json_query, cookies, timeout=1):
        available_forecast_url = 'https://{}/{}/api/v1/checkAvailableInventory'.format(self.server, self.context)
        try:
            json_file = json.loads(open(json_query, 'r').read())
        except TypeError:
            json_file = json_query
        headers = {'Content-Type': 'application/json'}
        response_text = None
        while response_text is None:
            counter = 0
            try:
                response = requests.post(available_forecast_url, json=json_file, cookies=cookies, headers=headers)
            except requests.exceptions.ConnectionError:
                response = requests.post(available_forecast_url, json=json_file, cookies=cookies, headers=headers)
            if response.status_code == 502:
                while counter < 5:
                    self.logger.warn("Check host, you've got 502 bad gateway error")
                    response = requests.post(available_forecast_url, json=json_file, cookies=cookies, headers=headers)
                    counter += 1
                    if response.status_code != 502:
                        break
                    time.sleep(3)
                self.logger.error("Bad gateway, we waste all 5 attempts.")
                return response
            elif response.status_code == 200:
                if 'QUEUED' in response.text:
                    self.logger.debug(response.text)
                    progress_percent = json.loads(response.text)
                    self.logger.info(("Forecasting {}. ".format(json_file) +
                                     "Progress percent is {}".format(progress_percent.get('progressPercent'))))
                    time.sleep(timeout)
                elif 'IN_PROGRESS' in response.text:
                    self.logger.debug("Progress percents is {}".format(json.loads(response.text).get('progressPercent')))
                if 'matched' in response.text:
                    return response
            else:
                self.logger.error('Error raised during forecasting. Backend message:\n{}'.format(response.text))
                return response

    def get_products(self, cookies):
        get_products_url = 'https://{}/{}/api/v1/getProducts'.format(self.server, self.context)
        headers = {'Content-Type': 'application/json'}
        response = requests.post(get_products_url, cookies=cookies, headers=headers)
        if response.status_code != 200:
            self.logger.error(response.text)
        else:
            return json.loads(response.text)
