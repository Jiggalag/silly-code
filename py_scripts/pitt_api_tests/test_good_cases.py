class GoodCasesTest:
    @staticmethod
    def good_cases_test(test_config):
        test_config.logger.info('!---Good cases test started for account {}---!'.format(test_config.account_id))
        request = test_config.base_request.copy()
        results = list()
        testrun = list()
        for case in test_config.good_cases:
            test_config.logger.info('Now we process case {}'.format(case))
            request.update({"pubMatic": case})
            forecast_result = test_config.api_point.check_available_inventory(request)
            results.append(forecast_result)
            if forecast_result:
                if forecast_result.status_code != 200:
                    testrun.append(False)
                    test_config.logger.error('Testcase {} failed...'.format(case))
                    test_config.logger.error('{}'.format(forecast_result.text))
                else:
                    testrun.append(True)
            else:
                testrun.append(False)
        if results:
            half_size = int(len(results) / 2)
            for i in range(half_size):
                if results[i].text == results[i + half_size].text:
                    test_config.logger.error('Error in case {}, include and exclude forecasts are equal'.format(i))
                    testrun.append(False)
        if all(testrun):
            test_config.logger.info('Everything is fine')
            return True
        else:
            test_config.logger.error('Something went wrong')
            return False
