import json

from helpers.kv_operator_parser import KVCaseParser

result = [
    {
        'example': True, 'greater': False, 'additional': False  # only case in result
    },
    {
        'example': False, 'greater': True, 'additional': True  # case not in result
    },
    {
        'example': True, 'greater': True, 'additional': True  # all kv of kn in result
    },
    {
        'example': False, 'greater': False, 'additional': True  # empty or smth except case in result
    },
    {
        'example': False, 'greater': True, 'additional': True  # [4]
    },
    {
        'example': False, 'greater': True, 'additional': True  # more than case in result
    },
    {
        'example': False, 'greater': True, 'additional': True  # [6] all except case in result
    },
    {
        'example': False, 'greater': False, 'additional': True  # all except case in result
    },
    {
        'example': False, 'greater': False, 'additional': True  # [8] all lesser than case in result
    },
    {
        'example': False, 'greater': False, 'additional': True  # all lesser than case in result
    },
    {
        'example': True, 'greater': False, 'additional': True  # [10] empty
    },
    {
        'example': True, 'greater': False, 'additional': True  # all
    },
    {
        'example': True, 'greater': False, 'additional': True  # [12] case + all more than case
    },
    {
        'example': True, 'greater': True, 'additional': True  # all
    },
    {
        'example': True, 'greater': True, 'additional': True  # [14] case
    },
    {
        'example': True, 'greater': True, 'additional': True
    },
    {
        'example': True, 'greater': True, 'additional': True  # [16] # [random-max]
    },
    # {
    #     'example': False, 'greater': False, 'additional': False  # empty TODO: fix it!
    # },
    # {
    #     'example': True, 'greater': False, 'additional': False  # [18] TODO: fix it!
    # },
    {
        'example': True, 'greater': False, 'additional': False  # not contains case KN TODO: check sense
    },
    {
        'example': False, 'greater': False, 'additional': True  # [20] case
    },
    # {
    #     'example': True, 'greater': False, 'additional': True  # case  # TODO: FIX!
    # },
    {
        'example': True, 'greater': False, 'additional': False  # [22]
    }
]


class KeyvalueTest:
    @staticmethod
    def keyvalue_test(test_config):
        test_config.logger.info('!---Kv test started for account {}---!'.format(test_config.account_id))
        request = test_config.base_request.copy()
        request.update({"dimensions": test_config.dimensions})
        results = list()
        raw_results = list()
        test_results = list()
        try:
            cases = test_config.kv_cases
        except AttributeError:
            cases = None
            test_config.logger.info('There is no kv cases for account {}'.format(test_config.account_id))
            return True
        if cases is not None:
            for case in cases:
                request.update({"pubMatic": case})
                forecast_result = test_config.api_point.check_available_inventory(request)
                raw_results.append(forecast_result)
                if forecast_result.text is not None:
                    forecast = json.loads(forecast_result.text)
                    results.append(forecast)
                    idx = test_config.kv_cases.index(case)
                    customkey_section = forecast.get('CustomKey')
                    if customkey_section is not None:
                        kv_parser = KVCaseParser(case, result[idx], customkey_section, test_config.logger)
                        parsing_result = kv_parser.parse_kv_operator()
                        test_results.append(parsing_result)
                    else:
                        test_results.append(False)

            test_config.logger.debug('Test_results: {}'.format(test_results))
            if all(test_results):
                test_config.logger.info('Everything is OK')
                return True
            else:
                test_config.logger.info('KV test failed')
                return False
