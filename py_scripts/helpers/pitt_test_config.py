import json
import logging
import random
import sys
from datetime import datetime
from datetime import timedelta

import pytz
from helpers.dbHelper import DbConnector
from helpers.forecast_destructor import Forecast
from helpers.ifms_apiv2 import IFMSApiV2
from helpers.pitt_account import Account


class PittTestConfig:
    def __init__(self, server, login, password, account_id, context):
        self.account_id = account_id
        self.logger = logging.getLogger("pitt_test_config")
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.info('Now we process data for account {}'.format(self.account_id))
        self.server = server
        self.user = login
        self.password = password
        self.context = context
        self.client = 'pitt'
        self.scope = account_id
        self.sql_params = {
            'host': 'eu-db-01.inventale.com',
            'user': 'itest',
            'password': 'ohk9aeVahpiz1wi',
            'db': 'pitt_cpopro_{}'.format(account_id)
        }
        self.sql_point = DbConnector(self.sql_params, self.logger)
        self.acc = Account(self.account_id, self.sql_point)
        # self.timezone = 'Pacific/Easter' # TODO: clarify this
        self.timezone = 'Europe/Moscow'
        self.now_date = datetime.now(pytz.timezone(self.timezone)).replace(hour=0, minute=0, second=0, microsecond=0)
        self.start_date = self.format_date(self.now_date + timedelta(days=1))
        self.api_point = IFMSApiV2(self.server, self.user, self.password, self.context, self.client, self.scope,
                                   self.logger)
        self.priorities = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        self.order = [
            'availableUniques',
            'bookedImpressions',
            'expectedDelivery',
            'matchedImpressions',
            'matchedUniques',
            'sharedImpressions'
        ]
        self.pubmatic_part = {
            "priority": random.choice(self.priorities),
            "inventory": [],
            "country": [],
            "region": [],
            "city": [],
            "adFormat": [],
            "adSize": [],
            "dma": [],
            "frequencyCaps": [],
            "browser": [],
            "browserLanguage": [],
            "deviceType": [],
            "device": [],
            "os": [],
            "customKey": []
        }
        self.base_request = {
            "startDate": self.start_date,
            "endDate": self.get_end_date(2),
            "dimensions": [],
            "pubMatic": self.pubmatic_part
        }
        self.adunit = self.acc.adunit
        self.country = self.acc.country
        self.region = self.acc.region
        self.city = self.acc.city
        self.dma = self.acc.dma
        self.adsize = self.acc.adsize
        self.adformat = self.acc.adformat
        self.browser_language = self.acc.browser_language
        self.device_type = self.acc.device_type
        self.device = self.acc.device
        self.device_capability = self.acc.device_capability
        self.os = self.acc.os
        self.browser = self.acc.browser
        self.adunit_list = self.acc.adunit_list
        self.country_list = self.acc.country_list
        self.region_list = self.acc.region_list
        self.city_list = self.acc.city_list
        self.dma_list = self.acc.dma_list
        self.adsize_list = self.acc.adsize_list
        self.adformat_list = self.acc.adformat_list
        self.browser_language_list = self.acc.browser_language_list
        self.device_type_list = self.acc.device_type_list
        self.device_list = self.acc.device_list
        self.os_list = self.acc.os_list
        self.browser_list = self.acc.browser_list
        self.kv_list = self.acc.kv_list
        self.freeform_kv_list = self.acc.freeform_kv_list
        self.numeric_kvs = self.acc.numeric_kvs
        self.device_capability_list = self.acc.device_capability_list
        self.incorrect_id = str(sys.maxsize)
        self.incorrect_adformat = "Incorrect AdFormat"
        self.incorrect_kv = "Incorrect KV"
        self.incorrect_kn = "Incorrect KN"
        self.dimensions = [
            'Date',
            'Date/AdUnit',
            'AdUnit/Date',
            'AdUnit',
            'Region',
            'City',
            'LineItem',
            'Country',
            'Order',
            'AdSize',
            'AdFormat',
            'DMA',
            'BrowserLanguage',
            'DeviceType',
            'Device',
            'OS',
            'Browser',
            'CompetingLineItem',
            'DeviceCapability'
        ]
        if self.kv_list:
            self.dimensions.append('CustomKey')
        self.base_request.update({"pubMatic": {"priority": 2}, "dimensions": self.dimensions})
        raw_forecast = self.api_point.check_available_inventory(self.base_request)
        if raw_forecast is not None:
            try:
                self.frc = Forecast(raw_forecast.text)
            except json.decoder.JSONDecodeError as e:
                self.frc = None
                print(e.args[0])
                sys.exit(1)
        else:
            self.logger.error('Service forecast is None!')
            sys.exit(1)
        self.cases = [
            {"priority": 1},
            {"priority": 1,
             "inventory": {"include": [self.adunit]}},
            {"priority": 1,
             "country": {"include": [self.country]}},
            {"priority": 1,
             "region": {"include": [self.region]}},
            {"priority": 1,
             "city": {"include": [self.city]}},
            {"priority": 1,
             "dma": {"include": [self.dma]}},
            {"priority": 1,
             "adSize": {"include": [self.adsize]}},
            {"priority": 1,
             "adFormat": {"include": [self.adformat]}},
            {"priority": 1,
             "deviceCapability": {"include": [self.device_capability]}},
            {"priority": 1,
             "inventory": {"exclude": [self.adunit]}},
            {"priority": 1,
             "country": {"exclude": [self.country]}},
            {"priority": 1,
             "region": {"exclude": [self.region]}},
            {"priority": 1,
             "city": {"exclude": [self.city]}},
            {"priority": 1,
             "dma": {"exclude": [self.dma]}},
            {"priority": 1,
             "adSize": {"exclude": [self.adsize]}},
            {"priority": 1,
             "adFormat": {"exclude": [self.adformat]}},
            {"priority": 1,
             "deviceCapability": {"exclude": [self.device_capability]}},
            {"priority": 1,
             "frequencyCaps": [{"scope": "MINUTE", "impsLimit": 1}]},
            {"priority": 1,
             "frequencyCaps": [{"scope": "HOURLY", "impsLimit": 1}]},
            {"priority": 1,
             "frequencyCaps": [{"scope": "DAILY", "impsLimit": 1}]},
            {"priority": 1,
             "frequencyCaps": [{"scope": "WEEKLY", "impsLimit": 1}]},
            {"priority": 1,
             "frequencyCaps": [{"scope": "MONTHLY", "impsLimit": 1}]},
            {"priority": 1,
             "frequencyCaps": [{"scope": "LIFETIME", "impsLimit": 1}]},
            {"priority": 1,
             "frequencyCaps": [{"scope": "MINUTE", "impsLimit": 1},
                               {"scope": "HOURLY", "impsLimit": 10},
                               {"scope": "DAILY", "impsLimit": 50},
                               {"scope": "WEEKLY", "impsLimit": 100},
                               {"scope": "MONTHLY", "impsLimit": 300},
                               {"scope": "LIFETIME", "impsLimit": 500}]
             }
        ]
        if self.kv_list:
            random_key = random.choice(list(self.kv_list.keys()))
            self.logger.info('Random key is {}'.format(random_key))
            try:
                random_value = random.choice(self.kv_list.get(random_key))
            except IndexError:
                random_value = ''
            self.logger.info('Random value is {}'.format(random_value))
            self.cases.extend(
                [
                    {
                        "priority": 1,
                        "customKey":
                            [{
                                "is":
                                    [{
                                        "key": random_key,
                                        "value": random_value
                                    }]
                            }]
                    },
                    {
                        "priority": 1,
                        "customKey":
                            [{
                                "isNot":
                                    [{
                                        "key": random_key,
                                        "value": random_value
                                    }]
                            }]
                    },
                    {
                        "priority": 1,
                        "customKey":
                            [{
                                "exists":
                                    [
                                        random_key
                                    ]
                            }]
                    },
                    {
                        "priority": 1,
                        "customKey":
                            [{
                                "notExists":
                                    [
                                        random_key
                                    ]
                            }]
                    }
                ])
        else:
            self.logger.warn('There is no kv for account {}'.format(account_id))
        if self.freeform_kv_list:
            freeform_random_key = random.choice(list(self.freeform_kv_list.keys()))
            self.logger.info('Random freeform key is {}'.format(freeform_random_key))
            try:
                freeform_random_value = random.choice(self.freeform_kv_list.get(freeform_random_key))
            except IndexError:
                freeform_random_value = ''
            self.logger.info('Random freeform value is {}'.format(freeform_random_value))
            self.cases.extend(
                [
                    {
                        "priority": 1,
                        "customKey":
                            [{
                                "is":
                                    [{
                                        "key": freeform_random_key,
                                        "value": freeform_random_value
                                    }]
                            }]
                    },
                    {
                        "priority": 1,
                        "customKey":
                            [{
                                "isNot":
                                    [{
                                        "key": freeform_random_key,
                                        "value": freeform_random_value
                                    }]
                            }]
                    },
                    {
                        "priority": 1,
                        "customKey":
                            [{
                                "exists":
                                    [
                                        freeform_random_key
                                    ]
                            }]
                    },
                    {
                        "priority": 1,
                        "customKey":
                            [{
                                "notExists":
                                    [
                                        freeform_random_key
                                    ]
                            }]
                    }
                ])
        else:
            self.logger.warn('There is no freeform kv for account {}'.format(account_id))
        self.negative_cases = [
            {"priority": 100},
            {"priority": 0},
            {"priority": 1,
             "inventory": {"include": [self.incorrect_id]}},
            {"priority": 1,
             "country": {"include": [self.incorrect_id]}},
            {"priority": 1,
             "region": {"include": [self.incorrect_id]}},
            {"priority": 1,
             "city": {"include": [self.incorrect_id]}},
            {"priority": 1,
             "dma": {"include": [self.incorrect_id]}},
            {"priority": 1,
             "adSize": {"include": [self.incorrect_id]}},
            {"priority": 1,
             "adFormat": {"include": [self.incorrect_adformat]}},
            {"priority": 1,
             "inventory": {"exclude": [self.incorrect_id]}},
            {"priority": 1,
             "country": {"exclude": [self.incorrect_id]}},
            {"priority": 1,
             "region": {"exclude": [self.incorrect_id]}},
            {"priority": 1,
             "city": {"exclude": [self.incorrect_id]}},
            {"priority": 1,
             "dma": {"exclude": [self.incorrect_id]}},
            {"priority": 1,
             "adSize": {"exclude": [self.incorrect_id]}},
            {"priority": 1,
             "adFormat": {"exclude": [self.incorrect_adformat]}},
            {"priority": 1,
             "frequencyCaps": [{"impsLimit": 1}]},
            {"priority": 1,
             "frequencyCaps": [{"scope": "WEEKLY"}]},
            {"priority": 1,
             "frequencyCaps": [{"scope": "LIFETIME", "impsLimit": [1]}]},
            {"priority": 1,
             "frequencyCaps": [{"scope": "QWERTY", "impsLimit": 1}]},
            {"priority": 1,
             "frequencyCaps": [{"scope": "LIFETIME", "impsLimit": "one"}]},
            {"priority": 1,
             "frequencyCaps": [{"scope": ["LIFETIME"], "impsLimit": 1}]},
            {"priority": 1,
             "frequencyCaps": {"scope": "LIFETIME", "impsLimit": 1}}
        ]
        if self.kv_list:
            self.negative_cases.extend(
                [
                    {
                        "priority": 1,
                        "customKey":
                            [{
                                "is":
                                    [{
                                        "key": self.incorrect_kn,
                                        "value": self.incorrect_kv
                                    }]
                            }]
                    },
                    {
                        "priority": 1,
                        "customKey":
                            [{
                                "isNot":
                                    [{
                                        "key": self.incorrect_kn,
                                        "value": self.incorrect_kv
                                    }]
                            }]
                    },
                    {
                        "priority": 1,
                        "customKey":
                            [{
                                "exists":
                                    [
                                        self.incorrect_kn
                                    ]
                            }]
                    },
                    {
                        "priority": 1,
                        "customKey":
                            [{
                                "notExists":
                                    [
                                        self.incorrect_kn
                                    ]
                            }]
                    }
                ])
        else:
            self.logger.warn('There is no kv for account {}'.format(account_id))
        self.good_cases = [
            {"priority": 1,
             "inventory": {"include": [self.frc.top_adunit]}},
            {"priority": 1,
             "country": {"include": [self.frc.top_country]}},
            {"priority": 1,
             "region": {"include": [self.frc.top_region]}},
            {"priority": 1,
             "city": {"include": [self.frc.top_city]}},
            {"priority": 1,
             "dma": {"include": [self.frc.top_dma]}},
            {"priority": 1,
             "adSize": {"include": [self.frc.top_adsize]}},
            {"priority": 1,
             "adFormat": {"include": [self.frc.top_adformat]}},
            {"priority": 1,
             "inventory": {"exclude": [self.frc.top_adunit]}},
            {"priority": 1,
             "country": {"exclude": [self.frc.top_country]}},
            {"priority": 1,
             "region": {"exclude": [self.frc.top_region]}},
            {"priority": 1,
             "city": {"exclude": [self.frc.top_city]}},
            {"priority": 1,
             "dma": {"exclude": [self.frc.top_dma]}},
            {"priority": 1,
             "adSize": {"exclude": [self.frc.top_adsize]}},
            {"priority": 1,
             "adFormat": {"exclude": [self.frc.top_adformat]}},
        ]
        self.compare_cases_include = [
            [
                {"priority": 1,
                 "inventory": {"include": [self.frc.top_adunit, random.choice(self.frc.adunit_list)]}},
                {"priority": 1,
                 "inventory": {"include": [self.frc.top_adunit]}},
            ],
            [
                {"priority": 1,
                 "country": {"include": [self.frc.top_country, random.choice(self.frc.country_list)]}},
                {"priority": 1,
                 "country": {"include": [self.frc.top_country]}}
            ],
            [
                {"priority": 1,
                 "region": {"include": [self.frc.top_region, random.choice(self.frc.region_list)]}},
                {"priority": 1,
                 "region": {"include": [self.frc.top_region]}},
            ],
            [
                {"priority": 1,
                 "city": {"include": [self.frc.top_city, random.choice(self.frc.city_list)]}},
                {"priority": 1,
                 "city": {"include": [self.frc.top_city]}},
            ],
            [
                {"priority": 1,
                 "dma": {"include": [self.frc.top_dma, random.choice(self.frc.dma_list)]}},
                {"priority": 1,
                 "dma": {"include": [self.frc.top_dma]}},
            ],
            [
                {"priority": 1,
                 "adSize": {"include": [self.frc.top_adsize, random.choice(self.frc.adsize_list)]}},
                {"priority": 1,
                 "adSize": {"include": [self.frc.top_adsize]}},
            ],
            [
                {"priority": 1,
                 "adFormat": {"include": [self.frc.top_adformat, random.choice(self.frc.adformat_list)]}},
                {"priority": 1,
                 "adFormat": {"include": [self.frc.top_adformat]}},
            ]
        ]
        self.compare_cases_exclude = [
            [
                {"priority": 1,
                 "inventory": {"exclude": [self.frc.top_adunit, random.choice(self.frc.adunit_list)]}},
                {"priority": 1,
                 "inventory": {"exclude": [self.frc.top_adunit]}},
            ],
            [
                {"priority": 1,
                 "country": {"exclude": [self.frc.top_country, random.choice(self.frc.country_list)]}},
                {"priority": 1,
                 "country": {"exclude": [self.frc.top_country]}}
            ],
            [
                {"priority": 1,
                 "region": {"exclude": [self.frc.top_region, random.choice(self.frc.region_list)]}},
                {"priority": 1,
                 "region": {"exclude": [self.frc.top_region]}},
            ],
            [
                {"priority": 1,
                 "city": {"exclude": [self.frc.top_city, random.choice(self.frc.city_list)]}},
                {"priority": 1,
                 "city": {"exclude": [self.frc.top_city]}},
            ],
            [
                {"priority": 1,
                 "dma": {"exclude": [self.frc.top_dma, random.choice(self.frc.dma_list)]}},
                {"priority": 1,
                 "dma": {"exclude": [self.frc.top_dma]}},
            ],
            [
                {"priority": 1,
                 "adSize": {"exclude": [self.frc.top_adsize, random.choice(self.frc.adsize_list)]}},
                {"priority": 1,
                 "adSize": {"exclude": [self.frc.top_adsize]}},
            ],
            [
                {"priority": 1,
                 "adFormat": {"exclude": [self.frc.top_adformat, random.choice(self.frc.adformat_list)]}},
                {"priority": 1,
                 "adFormat": {"exclude": [self.frc.top_adformat]}},
            ]
        ]
        self.case_adformats = [
            {
                "priority": 1,
                "adFormat": {"include": ["Native"]}
            },
            {
                "priority": 1,
                "adFormat": {"include": ["Banner/Rich Media"]}
            }
        ]
        if self.frc.top_kv is not None and self.frc.top_kv:
            random_pair = random.choice(self.frc.top_kv)
            top_kn = list(random_pair.keys())[0]
            self.logger.info('Top_kn is {}'.format(top_kn))
            top_kv = random_pair.get(top_kn)
            self.logger.info('Top_kv is {}'.format(top_kv))
            numeric_kn = self.get_numeric_kn()
            nkn = list(numeric_kn.keys())[0]
            rkv = random.choice(numeric_kn.get(nkn))
            max_kv = numeric_kn.get(nkn)[len(numeric_kn.get(nkn)) - 1]
            min_kv = numeric_kn.get(nkn)[0]
            self.kv_cases = [
                {
                    "priority": 1,
                    "customKey": [
                        {"is": [{"key": top_kn, "value": top_kv}]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"isNot": [{"key": top_kn, "value": top_kv}]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"exists": [top_kn]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"notExists": [top_kn]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"isGreaterThan": [{"key": nkn, "value": rkv}]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"isGreaterThan": [{"key": nkn, "value": min_kv}]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"isGreaterThan": [{"key": nkn, "value": max_kv}]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"isLesserThan": [{"key": nkn, "value": rkv}]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"isLesserThan": [{"key": nkn, "value": min_kv}]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"isLesserThan": [{"key": nkn, "value": max_kv}]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"isNotGreaterThan": [{"key": nkn, "value": rkv}]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"isNotGreaterThan": [{"key": nkn, "value": min_kv}]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"isNotGreaterThan": [{"key": nkn, "value": max_kv}]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"isNotLesserThan": [{"key": nkn, "value": rkv}]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"isNotLesserThan": [{"key": nkn, "value": min_kv}]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"isNotLesserThan": [{"key": nkn, "value": max_kv}]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"isBetween": [{"key": nkn, "value": '{}-{}'.format(rkv, max_kv)}]}
                    ]
                },
                # {
                #     "priority": 1,
                #     "customKey": [
                #         {"isBetween": [{"key": nkn, "value": '{}-{}'.format(min_kv, max_kv)}]}
                #     ]
                # },
                # {
                #     "priority": 1,
                #     "customKey": [
                #         {"isBetween": [{"key": nkn, "value": '{}-{}'.format(max_kv, min_kv)}]}
                #     ]
                # },
                {
                    "priority": 1,
                    "customKey": [
                        {"contains": [{"key": top_kn, "value": top_kv}]}
                    ]
                },
                {
                    "priority": 1,
                    "customKey": [
                        {"doesNotContain": [{"key": top_kn, "value": top_kv}]}
                    ]
                },
                # {
                #     "priority": 1,
                #     "customKey": [
                #         {"startsWith": [{"key": top_kn, "value": top_kv[:1]}]}  # TODO: generate amount dynamically
                #     ]
                # },
                {
                    "priority": 1,
                    "customKey": [
                        {"endsWith": [{"key": top_kn, "value": top_kv[1:]}]}  # TODO: generate amount dynamically
                    ]
                }
            ]

    def get_end_date(self, days):
        return self.format_date(self.now_date + timedelta(days=days) - timedelta(seconds=1))

    @staticmethod
    def format_date(date):
        return date.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def deep_aggregate_all(self, collection):
        res = dict()
        for item in collection.values():
            for i in item.values():
                for key in self.order:
                    if key not in res.keys():
                        value = i.get(key)
                        res.update({key: value})
                    else:
                        value = i.get(key) + res.get(key)
                        res.update({key: value})
        return res

    def get_numeric_kn(self):
        candidate = ''
        lenght = 0
        for item in self.numeric_kvs:
            new_lenght = len(self.numeric_kvs.get(item))
            if new_lenght > lenght:
                candidate = item
                lenght = new_lenght
        return {candidate: self.numeric_kvs.get(candidate)}

    def aggregate_all(self, collection):
        res = dict()
        for item in collection.keys():
            for key in self.order:
                if key not in res.keys():
                    value = collection.get(item).get(key)
                    res.update({key: value})
                else:
                    value = collection.get(item).get(key) + res.get(key)
                    res.update({key: value})
        return res

    def check_response_keys(self, dimension, response):
        existed_keys = ['summary', 'dbgInfo', 'queryId']
        if dimension not in existed_keys:
            existed_keys.append(dimension)
        failed_keys = list()
        response_keys = list(response.keys())
        for key in existed_keys:
            if key not in response_keys:
                self.logger.error('There is no dimension {} in response'.format(dimension))
                failed_keys.append(key)
            else:
                if not response.get(key):
                    self.logger.error('There is empty section {} in response'.format(dimension))
                response_keys.remove(key)
        for key in response_keys:
            if response.get(key):
                self.logger.error('Dimension {} is not empty, check it'.format(key))
                failed_keys.append(key)
        return failed_keys
