from py_scripts.dbComparator import queryConstructor, process_dates, query_list_iterator
from py_scripts.helpers import dbcmp_sql_helper, converters


class Comparation:
    def __init__(self, prod_connection, test_connection, table, logger, cmp_params):
        self.prod_connection = prod_connection
        self.test_connection = test_connection
        self.table = table
        self.cmp_params = cmp_params
        self.depth_report_check = self.cmp_params.get('depth_report_check')
        self.mode = self.cmp_params.get('mode')
        self.logger = logger

    def compare_table(self, is_report, mapping, start_time, comparing_info, comparing_step):
        if is_report:
            proc_dates = process_dates.ProcessDates(self.prod_connection, self.test_connection, self.table,
                                                    self.depth_report_check, self.logger)
            dates = converters.convert_to_list(proc_dates.compare_dates(comparing_info))
            dates.sort()
            if dates:
                local_break, max_amount = self.check_amount(dates)
                self.logger.info('Will be checked dates {}'.format(dates))
                query_list = queryConstructor.InitializeQuery(self.prod_connection, mapping, self.table,
                                                              comparing_step,
                                                              self.logger).report(dates, self.mode, max_amount)
            else:
                self.logger.warn('There is not any common dates for comparing')
                query_list = []
        else:
            local_break, max_amount = self.check_amount(None)
            query_list = queryConstructor.InitializeQuery(self.prod_connection, mapping, self.table,
                                                          comparing_step, self.logger).entity(max_amount)
        if query_list:
            query_iterator = query_list_iterator.QueryListIterator(self.prod_connection, self.test_connection,
                                                                   self.table, self.logger, self.cmp_params)
            global_break, local_break = query_iterator.iterate_by_query_list(query_list, start_time,  comparing_info,
                                                                             self.cmp_params.get('service_dir'))
            return global_break
        else:
            return False

    def check_amount(self, dates):
        prod_record_amount, test_record_amount = dbcmp_sql_helper.get_amount_records(self.table, dates,
                                                                                     [self.prod_connection,
                                                                                      self.test_connection])
        if prod_record_amount == 0 and test_record_amount == 0:
            self.logger.warn("Table {} is empty on both servers!".format(self.table))
            return True, 0
        if prod_record_amount == 0:
            self.logger.warn("Table {} is empty on prod-server!".format(self.table))
            return True, 0
        if test_record_amount == 0:
            self.logger.warn("Table {} is empty on test-server!".format(self.table))
            return True, 0
        if prod_record_amount != test_record_amount:
            sub_result, instance_type, percents = self.substract(prod_record_amount, test_record_amount)
            if instance_type == 'Prod':
                base = self.prod_connection.db
            else:
                base = self.test_connection.db
            self.logger.warn(('Amount of records differs for table {}'.format(self.table) +
                              'Prod record amount: {}. '.format(prod_record_amount) +
                              'Test record amount: {}. '.format(test_record_amount) +
                              'Db {} have more records. '.format(base) +
                              'Difference equals {0}, {1:.5f} percents'.format(sub_result, percents)))
        max_amount = max(prod_record_amount, test_record_amount)
        return False, max_amount

    @staticmethod
    def substract(prod_amount, test_amount):
        if prod_amount > test_amount:
            substraction = prod_amount - test_amount
            instance_type = 'Prod'
            percents = substraction / prod_amount
        else:
            substraction = test_amount - prod_amount
            instance_type = 'Test'
            percents = substraction / test_amount
        return substraction, instance_type, percents
