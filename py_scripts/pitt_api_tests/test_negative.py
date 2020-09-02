class NegativeTest:
    @staticmethod
    def negative_test(test_config):
        test_config.logger.info('!---Negative test started for account {}---!'.format(test_config.account_id))
        results = list()
        testrun = list()
        for case in test_config.negative_cases:
            test_config.logger.info('Now we process case {}'.format(case))
            request = test_config.base_request.copy()
            request.update({"pubMatic": case})
            forecast_result = test_config.api_point.check_available_inventory(request)
            if forecast_result:
                results.append(forecast_result)
                if forecast_result.status_code != 500:
                    testrun.append(False)
                    test_config.logger.error('Testcase {} failed...'.format(case))
                    test_config.logger.error('{}'.format(forecast_result.text))
                else:
                    testrun.append(True)
            else:
                testrun.append(False)
        if not all(testrun):
            test_config.logger.info('Everything is fine')
            return True
        else:
            test_config.logger.error('Something went wrong')
            return False
