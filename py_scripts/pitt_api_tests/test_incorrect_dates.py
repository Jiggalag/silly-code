from datetime import timedelta


class IncorrectDatesTest:
    @staticmethod
    def incorrect_dates_test(test_config):
        test_config.logger.info('!---Incorrect dates test started for account {}---!'.format(test_config.account_id))
        start_date = test_config.format_date(test_config.now_date - timedelta(10))
        request = {
            "startDate": start_date,
            "endDate": test_config.get_end_date(-9),
            "dimensions": [],
            "pubMatic": {"priority": 1}
        }
        response = test_config.api_point.check_available_inventory(request).text
        if 'errorMessage' in response:
            test_config.logger.info('Everything is fine')
            return True
        else:
            test_config.logger.error('Unexpectedly forecasted with incorrect dates')
            return False
