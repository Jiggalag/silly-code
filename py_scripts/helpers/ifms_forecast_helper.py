import json
from py_scripts.helpers.ifmsApiHelper import IFMSApiHelper
from py_scripts.helpers.logging_helper import Logger


class IFMSForecast:
    def __init__(self, server, user, password, context, client, scope, query):
        logger = Logger('DEBUG')
        api_point = IFMSApiHelper(server, user, password, context, logger)
        self.value_list = [
            'availableUniques',
            'bookedImpressions',
            'expectedDelivery',
            'matchedImpressions',
            'matchedUniques',
            'sharedImpressions'
        ]
        self.cookies = api_point.change_scope(client, scope)
        self.raw_forecast = json.loads(api_point.check_available_inventory(query, self.cookies).text)
        self.query_id = self.raw_forecast.get('dbgInfo').get('_id')
        self.request_time = self.raw_forecast.get('dbgInfo').get('requestTime')
        self.summary = self.raw_forecast.get('summary')
        self.kvs = self.parse_keyvalues()
        self.interest = self.kvs.get('Interest')
        self.gender = self.kvs.get('Gender')
        self.age = self.kvs.get('Age')
        self.income = self.kvs.get('Income')
        self.section = self.kvs.get('Section')
        self.subsection = self.kvs.get('Subsection')
        self.geo = self.get_forecast_by_entity_type('byGeo')
        self.campaign = self.by_campaign('byCampaign')
        self.order = self.by_campaign('byOrder')
        self.site = self.get_forecast_by_entity_type('bySite')
        self.bydate = self.by_date()
        self.page = self.get_forecast_by_entity_type('byPage')

    # TODO: probably it's misusable
    def __init_order(self):
        order_dict = dict()
        for item in self.value_list:
            order_dict.update({item: 0})
        return order_dict

    def __get_forecast_values(self, entity):
        value_dict = dict()
        for value in self.value_list:
            value_dict.update({value: entity.get(value)})
        return value_dict

    def by_campaign(self, attribute):
        result_dict = dict()
        for item in self.raw_forecast.get(attribute):
            remoteid = item.get('campaign').get('remoteId')
            result_dict.update({remoteid: self.__get_forecast_values(item)})
        return result_dict

    def by_date(self):
        date_dict = dict()
        for item in self.raw_forecast.get('byDate'):
            date = item.get('from')
            date_dict.update({date: self.__get_forecast_values(item)})
        return date_dict

    def hierarchy_campaign(self):
        pass

    def hierarchy_site(self):
        pass

    def get_forecast_by_entity_type(self, attribute):
        result_dict = dict()
        for item in self.raw_forecast.get(attribute):
            remoteid = item.get('criteria').get('remoteId')
            result_dict.update({remoteid: self.__get_forecast_values(item)})
        return result_dict

    def get_values(self, pack):
        value_dict = dict()
        for item in pack:
            remoteid = item.get('criteria').get('remoteId')
            value_dict.update({remoteid: self.__get_forecast_values(item)})
        return value_dict

    def parse_keyvalues(self):
        kvs = dict()
        for entity in self.raw_forecast.get('byKeyvalue'):
            kv = entity.get('keyname').get('path')[0]
            kvs.update({kv: self.get_values(entity.get('byValue'))})
        return kvs
