import datetime
from py_scripts.dbComparator import process_uniqs
from py_scripts.helpers import dbcmp_sql_helper


class QueryListIterator():
    def __init__(self, prod_connection, test_connection, table, logger, cmp_params):
        self.prod_connection = prod_connection
        self.test_connection = test_connection
        self.table = table
        self.strings_amount = cmp_params.get('strings_amount')
        self.fail_with_first_error = cmp_params.get('fail_with_first_error')
        self.table_timeout = cmp_params.get('table_timeout')
        self.table_start_time = datetime.datetime.now()
        self.logger = logger

    def iterate_by_query_list(self, query_list, start_time, comparing_info, service_dir):
        prod_uniq = set()
        test_uniq = set()
        for query in query_list:
            percent = (query_list.index(query) / len(query_list)) * 100
            self.logger.info('Progress for table {0} {1:.2f}%'.format(self.table, percent))
            local_break, prod_tmp_uniq, test_tmp_uniq = self.get_differences(query, comparing_info, self.strings_amount,
                                                                             service_dir)

            prod_uniq = process_uniqs.merge_uniqs(prod_uniq, prod_tmp_uniq)
            test_uniq = process_uniqs.merge_uniqs(test_uniq, test_tmp_uniq)

            if prod_uniq and test_uniq:
                prod_uniq = process_uniqs.thin_uniq_list(prod_uniq, test_uniq, self.logger)
                test_uniq = process_uniqs.thin_uniq_list(test_uniq, prod_uniq, self.logger)
            if local_break:
                process_uniqs.check_uniqs(prod_uniq, test_uniq, self.strings_amount, self.table, query, service_dir,
                                          self.logger)
                return False, True
            if self.table_timeout is not None:
                self.is_timeouted(prod_uniq, test_uniq, query, service_dir)

            if not local_break and self.fail_with_first_error:
                self.logger.info(("First error founded, checking failed. " +
                                  "Comparing takes {}").format(datetime.datetime.now() - start_time))
                process_uniqs.check_uniqs(prod_uniq, test_uniq, self.strings_amount, self.table, query, service_dir,
                                          self.logger)
                return True, False

            if process_uniqs.check_uniqs(prod_uniq, test_uniq, self.strings_amount, self.table, query, service_dir,
                                         self.logger):
                return False, True
        # Hack, intended for writing all uniqs to file
        process_uniqs.check_uniqs(prod_uniq, test_uniq, 0, self.table, query_list[0], service_dir, self.logger)
        return False, False

    def is_timeouted(self, prod_uniq, test_uniq, query, service_dir):
        duration = datetime.datetime.now() - self.table_start_time
        if duration > datetime.timedelta(minutes=self.table_timeout):
            self.logger.error(('Checking table {} '.format(self.table) +
                               'exceded timeout {}. Finished'.format(self.table_timeout)))
            process_uniqs.check_uniqs(prod_uniq, test_uniq, self.strings_amount, self.table, query, service_dir,
                                      self.logger)
            return False, True

    def get_differences(self, query, comparing_info, strings_amount, service_dir):
        prod_entities, test_entities = dbcmp_sql_helper.DbCmpSqlHelper.parallel_select([self.prod_connection,
                                                                                        self.test_connection], query)
        if (prod_entities is None) or (test_entities is None):
            self.logger.warn('Table {} skipped because something going bad'.format(self.table))
            return False, set(), set()
        prod_uniq = set(prod_entities) - set(test_entities)
        test_uniq = set(test_entities) - set(prod_entities)
        if not any([len(prod_uniq) == 0, len(test_uniq) == 0]):
            # TODO: fix duplication of error message in log. Write this message in the end of checking?
            self.logger.error("Tables {} differs!".format(self.table))
            comparing_info.update_diff_data(self.table)
            if max(len(prod_uniq), len(test_uniq)) >= strings_amount:
                local_break = True
            else:
                local_break = False
            if process_uniqs.check_uniqs(prod_uniq, test_uniq, strings_amount, self.table, query, service_dir,
                                         self.logger):
                return local_break, set(), set()
            else:
                return local_break, prod_uniq, test_uniq
        else:
            return True, set(), set()
