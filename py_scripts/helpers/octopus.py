import json

import requests
from helpers.ifmsApiHelper import IFMSApiHelper


class OctopusApi:

    def __init__(self, host, context, user, password, logger):
        self.host = host
        self.context = context
        self.user = user
        self.password = password
        self.logger = logger
        self.ifms_point = IFMSApiHelper(self.host, self.user, self.password, self.context, self.logger)
        self.cookies = self.ifms_point.change_scope('pitt', '79505')
        self.headers = {
            'Content-Type': 'application/json'
        }
        self.url = 'https://{}/{}/api/v2'.format(self.host, self.context)
        # self.url = 'http://195.201.81.215:10000/pubmatic'

    def list_adjustments(self):
        raw_result = requests.get(url=self.url + '/adjustment', headers=self.headers, cookies=self.cookies)
        if raw_result.text:
            try:
                result = json.loads(raw_result.text)
                return result
            except ValueError as e:
                print(e)
                return list()
        else:
            return list()

    def validate_data(self, data):
        required = [
            'name',
            'accountId',
            'adjustmentType',
            'startDate',
            'endDate',
            'timeZone',
            'status',
            'adjustmentValue',
            'targeting'
        ]
        keys = list(data.keys())
        fail_list = list()
        for key in required:
            if key not in keys:
                fail_list.append(key)
                self.logger.error('There is no {} field in data'.format(key))
        return fail_list

    def create_adjustment(self, data):
        result = requests.post(url=self.url + '/adjustment', headers=self.headers, json=data, cookies=self.cookies)
        if result.status_code != 200:
            self.logger.error(result.text)
        else:
            if 'malformed' in result.text:
                self.logger.error(result.text)
            else:
                self.logger.info('Successfully created!')

    def get_adjustment_by_id(self, adj_id):
        raw_result = requests.get(url=self.url + '/adjustment/{}'.format(adj_id), headers=self.headers,
                                  cookies=self.cookies)
        if raw_result.status_code == 404:
            self.logger.error(raw_result.text)
        elif raw_result.status_code != 200:
            self.logger.error(raw_result.text)
        else:
            if raw_result.text:
                try:
                    result = json.loads(raw_result.text)
                    return result
                except ValueError as e:
                    print(e)
                    return list()
            else:
                return list()

    def update_adjustment(self, adj_id, data):
        result = requests.put(url=self.url + '/adjustment/{}'.format(adj_id), headers=self.headers, json=data,
                              cookies=self.cookies)
        if result.status_code == 404:
            self.logger.error('Adjustment with id {} not found'.format(adj_id))
            self.logger.error(result.text)
        elif result.status_code == 200:
            self.logger.info('Successfully updated!')
        else:
            self.logger.error(result.text)

    def patch_adjustment(self, adj_id, data):
        result = requests.patch(url=self.url + '/adjustment/{}'.format(adj_id),
                                headers=self.headers, json=data, cookies=self.cookies)
        if result.status_code == 404:
            self.logger.error('Adjustment with id {} not found'.format(adj_id))
            self.logger.error(result.text)
        elif result.status_code == 200:
            self.logger.info('Successfully updated!')
        else:
            self.logger.error(result.text)

    def delete_adjustment(self, adj_id):
        result = requests.delete(url=self.url + '/adjustment/{}'.format(adj_id), headers=self.headers,
                                 cookies=self.cookies)
        if result.status_code == 404:
            self.logger.error('Adjustment with id {} not found'.format(adj_id))
            self.logger.error(result.text)
        elif result.status_code == 200:
            self.logger.info('Successfully deleted!')
        else:
            self.logger.error(result.text)

    def get_adjustment_ids(self):
        adjustment_list = self.list_adjustments()
        id_list = list()
        for item in adjustment_list:
            id_list.append(item.get('id'))
        return id_list

    def clear_all(self):
        adjustment_ids = self.get_adjustment_ids()
        for adj_id in adjustment_ids:
            self.delete_adjustment(adj_id)
