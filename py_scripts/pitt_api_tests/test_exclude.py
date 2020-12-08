import json


class ExcludeTest:
    @staticmethod
    def exclude_test(test_config):
        test_config.logger.info('!---Exclude test started---!'.format(test_config.account_id))
        request = test_config.base_request.copy()
        results = list()
        testrun = list()
        for case in test_config.compare_cases_exclude:
            test_config.logger.info('Now we process case {}'.format(case))
            tmp_results = list()
            for subcase in case:
                request.update({"pubMatic": subcase})
                forecast_result = test_config.api_point.check_available_inventory(request)
                tmp_results.append(forecast_result)
                if forecast_result:
                    if forecast_result.status_code != 200:
                        testrun.append(False)
                        test_config.logger.error('Testcase {} failed...'.format(case))
                        test_config.logger.error('{}'.format(forecast_result.text))
                else:
                    testrun.append(False)
            results.append(tmp_results)
            for item in test_config.order:
                first = json.loads(tmp_results[0].text).get('summary').get(item)
                second = json.loads(tmp_results[1].text).get('summary').get(item)
                if first > second:
                    testrun.append(False)
                    test_config.logger.error('Including element leads to decreasing value {}: '.format(item) +
                                             '1: {}, 2: {}'.format(first, second))
                    continue
            testrun.append(True)
        if all(testrun):
            test_config.logger.info('Everything is fine')
            return True
        else:
            test_config.logger.error('Something went wrong')
            return False
