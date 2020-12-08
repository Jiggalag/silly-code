import random

from natsort import natsorted


class Account:
    def __init__(self, account_id, sql_point):
        self.account_id = account_id
        self.sql_point = sql_point
        self.entity_query = {
            "adsize": "SELECT DISTINCT adsize.remoteid FROM adsize " +
                      "INNER JOIN page_adsize on page_adsize.adsizes_id = adsize.id " +
                      "INNER JOIN page on page_adsize.page_id = page.id " +
                      "WHERE page.remoteid IN " +
                      "(SELECT remoteid FROM page WHERE adsDeleted IS null AND isDeleted = 0 AND siteId IN " +
                      "(SELECT id FROM site WHERE remoteId LIKE '%{}%'))".format(self.account_id) +
                      "AND adsize.adsdeleted is null;",
            "adformat": "SELECT DISTINCT adformats FROM page WHERE adsDeleted IS null AND isDeleted=0 AND siteId IN " +
                        "(SELECT id FROM site WHERE remoteId LIKE '%{}%');".format(self.account_id),
            "page": "SELECT remoteid FROM page WHERE adsDeleted IS null AND isDeleted = 0 AND siteId IN " +
                    "(SELECT id FROM site WHERE remoteId LIKE '%{}%');".format(self.account_id)
        }
        self.adunit_list = self.get_entity_list('page')
        self.adunit = random.choice(self.adunit_list)
        self.country_list = self.get_entity_list('country')
        self.country = random.choice(self.country_list)
        self.region_list = self.get_entity_list('state')
        self.region = random.choice(self.region_list)
        self.city_list = self.get_entity_list('city')
        self.city = random.choice(self.city_list)
        self.dma_list = self.get_entity_list('dma')
        self.dma = random.choice(self.dma_list)
        self.adsize_list = self.get_entity_list('adsize')
        self.adsize = random.choice(self.adsize_list)
        self.adformat_list = self.get_entity_list('adformat')
        self.adformat = random.choice(self.adformat_list)
        self.browser_language_list = self.get_entity_list('browserlanguage')
        self.browser_language = random.choice(self.browser_language_list)
        self.device_type_list = self.get_entity_list('mobiledevicecategory')
        self.device_type = random.choice(self.device_type_list)
        self.device_list = self.get_entity_list('mobilemanufacturer')
        self.device = random.choice(self.device_list)
        self.device_capability_list = self.get_entity_list('devicecapability')
        self.device_capability = random.choice(self.device_capability_list)
        self.os_list = self.get_entity_list('os')
        self.os = random.choice(self.os_list)
        self.browser_list = self.get_entity_list('browser')
        self.browser = random.choice(self.browser_list)
        self.kv_list = self.create_kv_list()
        self.freeform_kv_list = self.create_kv_list(freeform=True)
        self.numeric_kvs = self.get_kn_with_numeric_kv()

    def get_entity_list(self, entity_type):
        default_query = "SELECT remoteid FROM {} WHERE adsDeleted IS null AND isDeleted = 0;".format(entity_type)
        return self.sql_point.select(self.entity_query.get(entity_type, default_query))

    def create_kv_list(self, freeform=False):
        get_key_query = ("SELECT name FROM keyname WHERE adsDeleted IS null AND archived IS null " +
                         "AND isDeleted = 0 AND isFreeform={};".format(int(freeform)))
        keys = self.sql_point.select(get_key_query)
        result = dict()
        for key in keys:
            tmp_kv_query = ("SELECT name FROM keyvalue WHERE keyname_id IN " +
                            "(SELECT id FROM keyname WHERE name = '{}')".format(key))
            tmp_kv = self.sql_point.select(tmp_kv_query)
            if freeform:
                tmp_kv.append("RandomValue")
            if tmp_kv:
                result.update({key: tmp_kv})
            else:
                result.update({key: ["FreeformAddedByTest"]})
        return result

    def get_kn_with_numeric_kv(self):
        query = ("SELECT keyname.name, keyvalue.name FROM keyvalue " +
                 "INNER JOIN keyname ON keyname_id = keyname.id " +
                 "WHERE keyvalue.name REGEXP '^-?[0-9]+$';")
        result = self.sql_point.select_rf(query)
        final_dict = dict()
        for item in result:
            key = item.get('name')
            value = item.get('keyvalue.name')
            if key not in final_dict.keys():
                final_dict.update({key: [value]})
            else:
                new_value = final_dict.get(key)
                new_value.append(value)
                final_dict.update({key: new_value})
        for key in final_dict:
            final_dict.update({key: natsorted(final_dict.get(key))})
        return final_dict
