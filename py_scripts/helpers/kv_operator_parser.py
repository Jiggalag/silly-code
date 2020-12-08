class KVCaseParser:
    def __init__(self, test_case, result_case, forecast, logger):
        self.test_case = test_case
        self.result_case = result_case
        self.logger = logger
        self.example = self.result_case.get('example')
        self.greater = self.result_case.get('greater')
        self.additional = self.result_case.get('additional')
        self.operator = self.get_operator()
        self.key = self.get_key()
        self.value = self.get_value()
        self.raw_forecast = forecast
        try:
            self.forecast = list(forecast.get(self.key).keys())
            self.parsed = True
        except AttributeError:
            self.forecast = forecast
            self.parsed = True
            self.logger.warn('There is no keyname {} in forecast'.format(self.key))
            self.logger.info('Case: {}'.format(self.test_case))
            self.logger.info('Forecast: {}'.format(forecast))

    def parse_kv_operator(self):
        if self.parsed:
            try:
                self.value = int(self.value)
                result = self.parse_numeric()
            except ValueError:
                result = self.parse_str()
            return result
        else:
            if not self.forecast:
                self.logger.info('Forecast is empty')
                return True
            return False

    def parse_numeric(self):
        if self.example:
            if self.greater:
                if self.additional:
                    return self.is_all()
                else:
                    return self.bigger_than()
            else:
                if self.additional:
                    return self.lesser_than()
                else:
                    return all([self.example_presented(), self.single()])
        else:
            if self.greater:
                if self.additional:
                    return not self.example_presented()
                else:
                    return self.bigger_than()
            else:
                if self.additional:
                    return self.lesser_than()
                else:
                    return self.is_empty()

    def parse_str(self):
        if self.example:
            if self.greater:
                if self.additional:
                    return self.is_all()
                else:
                    return all([self.example_presented(), self.single()])
            else:
                if self.additional:
                    return all([self.example_presented(), not self.single()])
                else:
                    return all([self.example_presented(), self.single()])
        else:
            if self.greater:
                if self.additional:
                    return all([not self.example_presented()])
                else:
                    self.logger.error('Pay attention, this case not implemented in KVParser')
                    return True
            else:
                if self.additional:
                    return all([not self.example_presented(), len(self.forecast) >= 1])
                else:
                    return not self.example_presented()

    def get_operator(self):
        if type(self.test_case) is dict:
            return list(self.test_case.get('customKey')[0].keys())[0]
        else:
            self.logger.error('Operator cannot be calculated...')

    def get_key(self):
        part = self.test_case.get('customKey')[0].get(self.operator)[0]
        if type(part) is dict:
            return part.get('key')
        elif type(part) is str:
            return part
        else:
            self.logger.error('Error during getting key')

    def get_value(self):
        part = self.test_case.get('customKey')[0].get(self.operator)[0]
        if type(part) is dict:
            return part.get('value')
        elif type(part) is str:
            return part

    def example_presented(self):
        for item in self.forecast:
            if str(self.value) in item:
                return True
        self.logger.error('Example {} is not presented in forecast {}'.format(self.value, self.forecast))
        return False

    def single(self):
        if len(self.forecast) != 1:
            self.logger.error('Should be single element in forecast, but in fact {}'.format(len(self.forecast)))
            return False
        else:
            return True

    def bigger_than(self):
        numeric_result = list()
        for item in self.forecast:
            numeric = int(item)
            if self.value <= numeric:
                numeric_result.append(int(item))
            else:
                self.logger.error('Value {} lesser than {}'.format(numeric, self.value))
                return False
        return True

    def lesser_than(self):
        numeric_result = list()
        for item in self.forecast:
            numeric = int(item)
            if self.value >= numeric:
                numeric_result.append(int(item))
            else:
                self.logger.error('Value {} bigger than {}'.format(numeric, self.value))
                return False
        return True

    def is_empty(self):
        if not self.forecast:
            return True
        else:
            self.logger.error('Forecast is not empty:\n{}'.format(self.forecast))
            return False

    @staticmethod
    def is_all():
        return True
