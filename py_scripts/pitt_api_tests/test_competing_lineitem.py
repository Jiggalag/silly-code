import json


class CompetingLineitemsTest:
    @staticmethod
    def competing_lineitems_test(test_config):
        test_config.logger.info('!---Competing LI test started for account {}---!'.format(test_config.account_id))
        request = test_config.base_request.copy()
        results = list()
        forecast_result = test_config.api_point.check_available_inventory(request)
        results.append(forecast_result)
        if forecast_result.status_code != 200:
            test_config.logger.error('Competing LI test failed...')
            test_config.logger.error('{}'.format(forecast_result.text))
            return False
        else:
            result = json.loads(forecast_result.text)
            competing_li = result.get('CompetingLineItem')
            li = result.get('LineItem')
            if not competing_li:
                test_config.logger.warn('Competing LI section empty for this request...')
                return True
            if len(competing_li) > len(li):
                test_config.logger.error('Competing LI test failed...')
                test_config.logger.error('Amount of competing LIs more than total amount of LIs')
                return False
            else:
                competing_li_total = CompetingLineitemsTest.aggregate_all(competing_li, test_config.order)
                li_total = CompetingLineitemsTest.aggregate_all(li, test_config.order)
                check_passed = True
                for item in test_config.order:
                    if competing_li_total.get(item) > li_total.get(item):
                        test_config.logger.error('Competing LI test failed...')
                        test_config.logger.error('Competing LIs {} {}\n'.format(item, competing_li_total.get(item)))
                        test_config.logger.error('LIs {} {}\n'.format(item, li_total.get(item)))
                        check_passed = False
                return check_passed

    @staticmethod
    def aggregate_all(collection, order):
        result = dict()
        for item in collection.keys():
            for key in order:
                if key not in result.keys():
                    value = collection.get(item).get(key)
                    result.update({key: value})
                else:
                    value = collection.get(item).get(key) + result.get(key)
                    result.update({key: value})
        return result
