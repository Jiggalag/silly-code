import json


class AdformatsBugTest:
    @staticmethod
    def adformats_bug_test(test_config):
        test_config.logger.info('!---Adformats bug test started for account {}---!'.format(test_config.account_id))
        request = test_config.base_request.copy()
        request.update({"dimensions": test_config.dimensions})
        results = list()
        for case in test_config.case_adformats:
            request.update({"pubMatic": case})
            forecast_result = test_config.api_point.check_available_inventory(request)
            if forecast_result.text is not None:
                forecast = json.loads(forecast_result.text)
                results.append(forecast)
        first = results[0].get('AdFormat')
        second = results[1].get('AdFormat')
        if first == second:
            test_config.logger.error('Seems like adformat include not works!')
            return False
        else:
            test_config.logger.info('Everything is OK')
            return True
