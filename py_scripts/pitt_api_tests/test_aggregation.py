import json


class AggregationTest:
    @staticmethod
    def aggregation_test(test_config):
        test_config.logger.info('!---Aggregation test started for account {}---!'.format(test_config.account_id))
        request = test_config.base_request.copy()
        request.update({"dimensions": test_config.dimensions})
        request.update({"pubMatic": {"priority": 1, "accountId": test_config.account_id}})
        forecast_result = test_config.api_point.check_available_inventory(test_config.base_request)
        if forecast_result.text is not None:
            forecast = json.loads(forecast_result.text)
            pagedate = test_config.deep_aggregate_all(forecast.get('AdUnit/Date'))
            datepage = test_config.deep_aggregate_all(forecast.get('Date/AdUnit'))
            date = test_config.aggregate_all(forecast.get('Date'))
            pages = test_config.aggregate_all(forecast.get('AdUnit'))
            campaigns = test_config.aggregate_all(forecast.get('LineItem'))
            orders = test_config.aggregate_all(forecast.get('Order'))
            cities = test_config.aggregate_all(forecast.get('City'))
            regions = test_config.aggregate_all(forecast.get('Region'))
            countries = test_config.aggregate_all(forecast.get('Country'))
            error_count = 0
            for item in test_config.order:
                if pagedate.get(item) != datepage.get(item):
                    test_config.logger.error("Value of {} for pagedate {}, ".format(item, pagedate.get(item)) +
                                             "for datepage {}".format(datepage.get(item)))
                    error_count += 1
                if pagedate.get(item) != date.get(item):
                    test_config.logger.error("Value of {} for pagedate {}, ".format(item, pagedate.get(item)) +
                                             "for date {}".format(date.get(item)))
                    error_count += 1
                if cities.get(item) != regions.get(item):
                    test_config.logger.error("Value of {} for city {}, ".format(item, cities.get(item)) +
                                             "for region {}".format(regions.get(item)))
                    error_count += 1
                if cities.get(item) != countries.get(item):
                    test_config.logger.error("Value of {} for city {}, ".format(item, cities.get(item)) +
                                             "for country {}".format(countries.get(item)))
                    error_count += 1
                if campaigns.get(item) != orders.get(item):
                    test_config.logger.error("Value of {} for campaigns {}, ".format(item, campaigns.get(item)) +
                                             "for orders {}".format(orders.get(item)))
                    error_count += 1
                if pages.get(item) != countries.get(item):
                    test_config.logger.error("Value of {} for pages {}, ".format(item, pages.get(item)) +
                                             "for countries {}".format(countries.get(item)))
                    error_count += 1
                if pages.get(item) != date.get(item):
                    test_config.logger.error("Value of {} for pages {}, ".format(item, pages.get(item)) +
                                             "for date {}".format(date.get(item)))
                    error_count += 1
            if error_count > 0:
                test_config.logger.error('Error count more than 0')
                return False
            else:
                test_config.logger.info('Everything is fine')
                return True
        else:
            test_config.logger.error('Forecast result is None!')
            return False
