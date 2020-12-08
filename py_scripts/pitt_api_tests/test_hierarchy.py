import json


class HierarchyTest:
    @staticmethod
    def hierarchy_test(test_config):
        test_config.logger.info("!---Hierarchy test started for accountid {}---!".format(test_config.account_id))
        request = test_config.base_request.copy()
        fail = 0
        case = test_config.cases[0]
        test_config.logger.info('Now we process case {}'.format(case))
        request.update({"pubMatic": case})
        forecast_result = test_config.api_point.check_available_inventory(request)
        result = json.loads(forecast_result.text)
        hierarchy_list = [
            'AdUnit',
            'AdSize',
            'AdFormat'
        ]
        hierarchy_dict = dict()
        for dimension in hierarchy_list:
            tmp_result = list()
            for item in result.get(dimension).values():
                tmp_result.append(item.get('criteria').get('remoteId'))
            hierarchy_dict.update({dimension: tmp_result})
        for key in hierarchy_dict:
            if key == 'AdUnit':
                target_list = test_config.adunit_list
            elif key == 'AdSize':
                target_list = test_config.adsize_list
            else:
                target_list = test_config.adformat_list
            errors = list()
            for item in hierarchy_dict.get(key):
                if item not in target_list:
                    errors.append(item)
                else:
                    target_list.remove(item)
            if errors:
                fail = 1
                test_config.logger.error('There are some incorrect entities for dimension {} '.format(key) +
                                         'in forecast result: {}'.format(','.join(errors)))
            if target_list:
                test_config.logger.info('There are some entities for dimension {} '.format(key) +
                                        'not presented in forecast result: {}'.format(','.join(target_list)))

        if fail == 1:
            return False
        else:
            return True
