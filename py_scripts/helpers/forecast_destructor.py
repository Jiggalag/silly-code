import json


class Forecast:
    def __init__(self, forecast_string):
        self.json_forecast = json.loads(forecast_string)
        self.request_time = self.json_forecast.get('dbgInfo').get('requestTime')
        self.order = [
            'availableUniques',
            'bookedImpressions',
            'expectedDelivery',
            'matchedImpressions',
            'matchedUniques',
            'sharedImpressions'
        ]
        self.top_order = self.get_top_entity('Order')
        self.top_campaign = self.get_top_entity('LineItem')
        self.top_adunit = self.get_top_entity('AdUnit')
        self.top_country = self.get_top_entity('Country')
        self.top_region = self.get_top_entity('Region')
        self.top_city = self.get_top_entity('City')
        self.top_dma = self.get_top_entity('DMA')
        self.top_adsize = self.get_top_entity('AdSize')
        self.top_adformat = "Banner/Rich Media"
        self.top_device_capability = self.get_top_entity('DeviceCapability')
        if self.json_forecast.get('CustomKey') is not None:
            self.top_kv = self.get_top_custom_keyvalue()
        else:
            self.top_kv = None
        self.order_list = self.get_id_list('Order')
        self.campaign_list = self.get_id_list('LineItem')
        self.adunit_list = self.get_id_list('AdUnit')
        self.country_list = self.get_id_list('Country')
        self.region_list = self.get_id_list('Region')
        self.city_list = self.get_id_list('City')
        self.dma_list = self.get_id_list('DMA')
        self.adsize_list = self.get_id_list('AdSize')
        self.adformat_list = ['Native', 'Video']

    def get_top_entity(self, dimension):
        result = {
            '0': 0
        }
        target_dimension = self.json_forecast.get(dimension)
        for item in target_dimension:
            if target_dimension.get(item).get('matchedImpressions') > list(result.values())[0]:
                result = {item: target_dimension.get(item).get('matchedImpressions')}
        return list(result.keys())[0]

    def get_top_custom_keyvalue(self):
        final_list = list()
        keynames = self.json_forecast.get('CustomKey')
        for name in keynames:
            keyname_result = self.json_forecast.get('CustomKey').get(name)
            max_value = 0
            current_keyname = ''
            for target in keyname_result:
                if keyname_result.get(target).get('matchedImpressions') > max_value:
                    max_value = keyname_result.get(target).get('matchedImpressions')
                    current_keyname = target
            final_list.append({name: current_keyname})
        return final_list

    def get_id_list(self, dimension):
        return list(self.json_forecast.get(dimension).keys())
