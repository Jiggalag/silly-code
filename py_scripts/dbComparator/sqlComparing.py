import datetime
from py_scripts.helpers import dbHelper
from py_scripts.dbComparator import queryConstructor
from py_scripts.dbComparator import cmp_reports
from py_scripts.dbComparator import cmp_entities

# TODO: Days in past parameter not works for Day-summary shecking mode - fix it.


class Object:
    def __init__(self, sql_connection_properties, sql_comparing_properties, comparing_info, client=None):
        self.client = client
        self.sql_connection_properties = sql_connection_properties
        self.prod_sql = sql_connection_properties.get('prod')
        self.test_sql = sql_connection_properties.get('test')
        self.comparing_info = comparing_info
        # TODO: add checking situation if client is None - we should handle this case separately?
        self.attempts = 5
        self.comparing_step = 10000
        self.hide_columns = [
            'archived',
            'addonFields',
            'hourOfDayS',
            'dayOfWeekS',
            'impCost',
            'id'
        ]
        self.mode = 'day-sum'
        self.string_amount = 1000
        self.client_ignored_tables = []
        self.check_schema = True
        self.depth_report_check = 7
        self.fail_with_first_error = False
        self.separate_checking = 'both'
        self.schema_columns = [
            'TABLE_CATALOG',
            'TABLE_NAME',
            'COLUMN_NAME',
            'ORDINAL_POSITION',
            'COLUMN_DEFAULT',
            'IS_NULLABLE',
            'DATA_TYPE',
            'CHARACTER_MAXIMUM_LENGTH',
            'CHARACTER_OCTET_LENGTH',
            'NUMERIC_PRECISION',
            'NUMERIC_SCALE',
            'DATETIME_PRECISION',
            'CHARACTER_SET_NAME',
            'COLLATION_NAME',
            'COLUMN_TYPE',
            'COLUMN_KEY',
            'EXTRA',
            'COLUMN_COMMENT',
            'GENERATION_EXPRESSION'
        ]
        self.excluded_tables = [
            'databasechangelog',
            'download',
            'migrationhistory',
            'mntapplog',
            'reportinfo',
            'synchistory',
            'syncstage',
            'synctrace',
            'synctracelink',
            'syncpersistentjob',
            'forecaststatistics',
            'migrationhistory'
        ]
        self.table_timeout = None
        # TODO: transfer all this properties from UI
        if 'retry_attempts' in sql_comparing_properties.keys():
            self.attempts = int(sql_comparing_properties.get('retry_attempts'))
        if 'comparing_step' in sql_comparing_properties.keys():
            self.comparing_step = int(sql_comparing_properties.get('comparing_step'))
        if 'skip_columns' in sql_comparing_properties.keys():
            self.hide_columns = sql_comparing_properties.get('skip_columns').split(',')
        if 'mode' in sql_comparing_properties.keys():
            self.mode = sql_comparing_properties.get('mode')
        if 'check_schema' in sql_comparing_properties.keys():
            self.check_schema = sql_comparing_properties.get('check_schema')
        if 'depth_report_check' in sql_comparing_properties.keys():
            self.depth_report_check = int(sql_comparing_properties.get('depth_report_check'))
        if 'fail_with_first_error' in sql_comparing_properties.keys():
            self.fail_with_first_error = sql_comparing_properties.get('fail_with_first_error')
        if 'schema_columns' in sql_comparing_properties.keys():
            self.schema_columns = sql_comparing_properties.get('schema_columns').split(',')
        if 'separateChecking' in sql_comparing_properties.keys():
            self.separate_checking = sql_comparing_properties.get('separateChecking')
        if 'skip_tables' in sql_comparing_properties.keys():
            self.excluded_tables = sql_comparing_properties.get('skip_tables').split(',')
        if 'path_to_logs' in sql_comparing_properties.keys():
            self.path_to_logs = sql_comparing_properties.get('path_to_logs')
        if 'send_mail_to' in sql_comparing_properties.keys():
            self.send_mail_to = sql_comparing_properties.get('send_mail_to')
        if 'logger' in sql_comparing_properties.keys():
            self.logger = sql_comparing_properties.get('logger')
        if 'amount_checking_records' in sql_comparing_properties.keys():
            self.amount_checking_records = int(sql_comparing_properties.get('amount_checking_records'))
        if 'table_timeout' in sql_comparing_properties.keys():
            self.table_timeout = int(sql_comparing_properties.get('table_timeout'))
            if self.table_timeout == 0:
                self.table_timeout = None
        if 'string_amount' in sql_comparing_properties.keys():
            self.string_amount = int(sql_comparing_properties.get('string_amount'))
        self.sql_comparing_properties = {
            'retry_attempts': self.attempts,
            'comparing_step': self.comparing_step,
            'skip_columns': self.hide_columns,  # TODO: rename on UI-side
            'mode': self.mode,
            'check_schema': self.check_schema,
            'depth_report_check': self.depth_report_check,
            'fail_with_first_error': self.fail_with_first_error,
            'schema_columns': self.schema_columns,
            'logger': self.logger,
            'string_amount': self.string_amount,
            'separateChecking': self.separate_checking,  # TODO: rename on UI-side
            'skip_tables': self.excluded_tables,  # TODO: rename on UI-side
            'send_mail_to': self.send_mail_to,
            'amount_checking_records': self.amount_checking_records,
            'table_timeout': self.table_timeout
        }

    def complex_condition(self, table):
        booler = []
        if ('report' in table) or ('statistic' in table):
            booler.append(True)
        else:
            booler.append(False)
        if 'dt' in dbHelper.DbConnector(self.prod_sql, self.logger).get_column_list(table):
            booler.append(True)
        else:
            booler.append(False)
        if 'onlyEntities' not in self.separate_checking:
            booler.append(True)
        else:
            booler.append(False)
        if all(booler):
            return True
        else:
            return False

    def compare_data(self, global_break, start_time, service_dir, mapping):
        prod_connection = dbHelper.DbConnector(self.prod_sql, self.logger)
        test_connection = dbHelper.DbConnector(self.test_sql, self.logger)
        tables = self.comparing_info.get_tables(self.excluded_tables, self.client_ignored_tables)
        for table in tables:
            # table = 'kvkeypairreport'
            self.logger.info("Table {} processing now...".format(table))
            start_table_check_time = datetime.datetime.now()
            local_break = False
            query_object = queryConstructor.InitializeQuery(prod_connection, self.logger)
            if self.complex_condition(table):
                if not global_break:
                    cmp_reports.compare_report_table(prod_connection, test_connection, service_dir,
                                                     global_break, mapping, local_break, table, start_time,
                                                     self.comparing_info, **self.sql_comparing_properties)
                    self.logger.info("Table {} ".format(table) +
                                     "checked in {}".format(datetime.datetime.now() - start_table_check_time))
                else:
                    self.logger.info("Table {} ".format(table) +
                                     "checked in {}".format(datetime.datetime.now() - start_table_check_time))
                    break
            else:
                if 'onlyReports' in self.separate_checking:
                    continue
                prod_record_amount, test_record_amount = dbHelper.get_amount_records(table,
                                                                                     None,
                                                                                     [prod_connection, test_connection],
                                                                                     self.logger)
                if prod_record_amount == 0 and test_record_amount == 0:
                    self.logger.warn("Table {} is empty on both servers!".format(table))
                    continue
                if prod_record_amount == 0:
                    self.logger.warn("Table {} is empty on prod-server!".format(table))
                    continue
                if test_record_amount == 0:
                    self.logger.warn("Table {} is empty on test-server!".format(table))
                    continue
                max_amount = max(prod_record_amount, test_record_amount)
                query_list = query_object.entity(table, max_amount, self.comparing_step, mapping)
                if not global_break:
                    table_start_time = datetime.datetime.now()
                    for query in query_list:
                        if (not cmp_entities.compare_entity_table(prod_connection, test_connection, table, query,
                                                                  self.comparing_info,
                                                                  self.logger)) and self.fail_with_first_error:
                            self.logger.info("First error founded, checking failed. Comparing takes {}".format(
                                datetime.datetime.now() - start_time))
                            global_break = True
                            self.logger.info("Table {} ".format(table) +
                                             "checked in {}".format(datetime.datetime.now() - start_table_check_time))
                            break
                        if self.table_timeout is not None:
                            duration = datetime.datetime.now() - table_start_time
                            if duration > datetime.timedelta(minutes=self.table_timeout):
                                self.logger.error(('Checking table {} '.format(table) +
                                                   'exceded timeout {}. Finished'.format(self.table_timeout)))
                                break
                    self.logger.info("Table {} ".format(table) +
                                     "checked in {}...".format(datetime.datetime.now() - start_table_check_time))
                else:
                    self.logger.info("Table {} ".format(table) +
                                     "checked in {}...".format(datetime.datetime.now() - start_table_check_time))
                    break
        data_comparing_time = datetime.datetime.now() - start_time
        self.logger.info("Comparing finished in {}".format(data_comparing_time))
        return data_comparing_time

    def compare_metadata(self, start_time):
        prod_connection = dbHelper.DbConnector(self.prod_sql, self.logger)
        test_connection = dbHelper.DbConnector(self.test_sql, self.logger)
        tables = self.comparing_info.get_tables(self.excluded_tables, self.client_ignored_tables)
        for table in tables:
            self.logger.info("Check schema for table {}...".format(table))
            query = ("SELECT {} FROM INFORMATION_SCHEMA.COLUMNS ".format(', '.join(self.schema_columns)) +
                     "WHERE TABLE_SCHEMA = 'DBNAME' AND TABLE_NAME='TABLENAME' ".replace("TABLENAME", table) +
                     "ORDER BY COLUMN_NAME;")

            prod_columns, test_columns = dbHelper.DbConnector.parallel_select([prod_connection, test_connection], query)
            if (prod_columns is None) or (test_columns is None):
                self.logger.warn('Table {} skipped because something going bad'.format(table))
                break
            uniq_for_prod = list(set(prod_columns) - set(test_columns))
            uniq_for_test = list(set(test_columns) - set(prod_columns))
            if len(uniq_for_prod) > 0:
                return self.schema_comparing_time(table, uniq_for_prod, start_time)
            if len(uniq_for_test) > 0:
                return self.schema_comparing_time(table, uniq_for_test, start_time)
            if not all([len(uniq_for_prod) == 0, len(uniq_for_test) == 0]):
                return self.schema_comparing_time(table, None, start_time)
        schema_comparing_time = datetime.datetime.now() - start_time
        self.logger.info("Schema compared in {}".format(schema_comparing_time))
        return datetime.datetime.now() - start_time

    def schema_comparing_time(self, table, uniq_list, start_time):
        if uniq_list is None:
            self.logger.error(" [ERROR] Tables {} differs!".format(table))
        else:
            self.logger.error("Elements, unique for table {} ".format(table) +
                              "in {} db:{}".format(self.test_sql.get('db'), list(uniq_list[0])))
        if self.fail_with_first_error:
            self.logger.critical("First error founded, comparing failed. "
                                 "To find all discrepancies set failWithFirstError property in false\n")
            schema_comparing_time = datetime.datetime.now() - start_time
            self.logger.info("Schema partially compared in {}".format(schema_comparing_time))
            return schema_comparing_time
