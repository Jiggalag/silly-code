import json
import time

import requests


class IFMSApiV2:
    def __init__(self, server, user, password, context, client, scope, logger):
        self.logger = logger
        self.server = server
        self.user = user
        self.password = password
        self.context = context
        self.client = client
        self.scope = scope
        self.account_id = scope
        self.params = {'accountId': self.account_id}
        self.base_url = 'https://{}/{}/api/v2'.format(self.server, self.context)
        self.cookies = self._change_scope()

    def _api_authenticate(self):
        login_url = 'https://{}/login.action'.format(self.server)
        data = {'loginActionForm.login': self.user, 'loginActionForm.password': self.password}
        login_response = requests.post(url=login_url, data=data, allow_redirects=False)
        return login_response.cookies

    def _change_scope(self):
        cookies = self._api_authenticate()
        url = "https://{}/changeClient.action?clientScope={}.{}".format(self.server, self.client, self.scope)
        headers = {
            'Content-Type': 'application/json',
            'Host': self.server,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Upgrade-Insecure-Requests': '1'
        }
        requests.get(url, headers=headers, cookies=cookies)
        cookies = self._api_authenticate()
        return cookies

    def get_forecast(self, json_file):
        available_forecast_url = self.base_url + '/query'
        try:
            loaded_json = json.load(open(json_file))
        except TypeError:
            if 'JsonQuery' in str(type(json_file)):
                loaded_json = json_file.json_data
            else:
                loaded_json = json_file
        headers = {'Content-Type': 'application/json'}
        response = requests.post(available_forecast_url, json=loaded_json, cookies=self.cookies, headers=headers,
                                 params=self.params)
        self.logger.info(response.text)
        if 'errorMessage' in response.text:
            self.logger.error(json.loads(response.text).get('errorMessage'))
            return
        if 'Location' in response.headers:
            return 'https://{}'.format(self.server) + response.headers['Location']
        else:
            return response

    def get_syncronous_forecast(self, json_file):
        available_forecast_url = self.base_url + '/forecast'
        try:
            loaded_json = json.load(open(json_file))
        except TypeError:
            if 'JsonQuery' in str(type(json_file)):
                loaded_json = json_file.json_data
            else:
                loaded_json = json_file
        headers = {'Content-Type': 'application/json'}
        return requests.get(available_forecast_url, json=loaded_json, cookies=self.cookies, headers=headers,
                            params=self.params)

    def get_status(self, status_url):
        result = requests.get(status_url, cookies=self.cookies)
        return result

    def get_result(self, query_id):
        result_url = self.base_url + '/query/{}'.format(query_id)
        result_url = requests.get(result_url, params=self.params, cookies=self.cookies)
        return result_url

    def cancel_forecast(self, query_id):
        cancel_url = self.base_url + '/query/{}'.format(query_id)
        response = requests.delete(cancel_url, params=self.params, cookies=self.cookies)
        return response

    def get_forecast_by_dimension(self, query_id, dimension):
        forecast_by_dimension_url = self.base_url + '/query/{}/{}'.format(query_id, dimension)
        response = requests.get(forecast_by_dimension_url, params=self.params, cookies=self.cookies)
        return response

    def get_dimension_value(self, query_id, dimension, value):
        forecast_by_dimension_url = self.base_url + '/query/{}/{}/{}'.format(query_id, dimension, value)
        response = requests.get(forecast_by_dimension_url, params=self.params, cookies=self.cookies)
        return response

    def check_available_inventory(self, json_file):
        monitoring = self.get_forecast(json_file)
        if type(monitoring) is str:
            while True:  # TODO: fix potentially endless cycle
                result = self.get_status(monitoring)
                try:
                    state = json.loads(result.text).get('state')
                    if state in ['IN_PROGRESS', 'QUEUED']:
                        time.sleep(10)
                    else:
                        return result
                except json.decoder.JSONDecodeError as e:
                    self.logger.error(e.args[0])
                    self.logger.info(result.text)
        else:
            return monitoring
