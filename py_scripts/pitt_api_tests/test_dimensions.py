import json


class DimensionsTest:
    @staticmethod
    def dimensions_test(test_config):
        test_config.logger.info('!---Dimension test started for account {}---!'.format(test_config.account_id))
        request = test_config.base_request.copy()
        results = list()
        testrun = list()
        for dimension in test_config.dimensions:
            test_config.logger.info('Now we process dimension {}'.format(dimension))
            request.update({'pubMatic': {"priority": 1, "accountId": test_config.account_id}})
            request.update({'dimensions': [dimension]})
            forecast_result = test_config.api_point.check_available_inventory(request)
            if forecast_result:
                results.append(forecast_result)
                if forecast_result.status_code == 200:
                    if test_config.check_response_keys(dimension, json.loads(forecast_result.text)):
                        testrun.append(False)
                        test_config.logger.error('Testcase with dimension {} failed...'.format(dimension))
                        test_config.logger.error('{}'.format(forecast_result.text))
                    else:
                        testrun.append(True)
                else:
                    testrun.append(False)
                    test_config.logger.error('Testcase with dimension {} failed...'.format(dimension))
                    test_config.logger.error('{}'.format(forecast_result.text))
            else:
                testrun.append(False)
        if all(testrun):
            test_config.logger.info('Everything is fine')
            return True
        else:
            test_config.logger.error('Something went wrong')
            return False
