import datetime
from py_scripts.helpers import dbcmp_sql_helper
from py_scripts.dbComparator.unified_comparing_class import Comparation


class Object:
    def __init__(self, sql_connection_properties, sql_comparing_properties, comparing_info):
        self.sql_connection_properties = sql_connection_properties
        self.prod_sql = sql_connection_properties.get('prod')
        self.test_sql = sql_connection_properties.get('test')
        self.comparing_info = comparing_info
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
        self.strings_amount = 1000
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
        self.only_tables = ''
        self.reports = True
        self.entities = True
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
            # TODO: deleted split
            self.schema_columns = sql_comparing_properties.get('schema_columns')
            if type(self.schema_columns) is str:
                self.schema_columns = self.schema_columns.split(',')
        if 'separateChecking' in sql_comparing_properties.keys():
            self.separate_checking = sql_comparing_properties.get('separateChecking')
        if 'only_tables' in sql_comparing_properties.keys():
            self.only_tables = sql_comparing_properties.get('only_tables').split(',')
        if 'skip_tables' in sql_comparing_properties.keys():
            self.excluded_tables = sql_comparing_properties.get('skip_tables').split(',')
        if 'path_to_logs' in sql_comparing_properties.keys():
            self.path_to_logs = sql_comparing_properties.get('path_to_logs')
        if 'send_mail_to' in sql_comparing_properties.keys():
            self.send_mail_to = sql_comparing_properties.get('send_mail_to')
        if 'logger' in sql_comparing_properties.keys():
            self.logger = sql_comparing_properties.get('logger')
        if 'table_timeout' in sql_comparing_properties.keys():
            self.table_timeout = int(sql_comparing_properties.get('table_timeout'))
            if self.table_timeout == 0:
                self.table_timeout = None
        if 'reports' in sql_comparing_properties.keys():
            self.reports = sql_comparing_properties.get('reports')
        if 'strings_amount' in sql_comparing_properties.keys():
            self.strings_amount = int(sql_comparing_properties.get('strings_amount'))
        self.sql_comparing_properties = {
            'check_schema': self.check_schema,
            'comparing_step': self.comparing_step,
            'depth_report_check': self.depth_report_check,
            'entities': self.entities,
            'fail_with_first_error': self.fail_with_first_error,
            'hide_columns': self.hide_columns,
            'mode': self.mode,
            'only_tables': self.only_tables,
            'schema_columns': self.schema_columns,
            'logger': self.logger,
            'reports': self.reports,
            'retry_attempts': self.attempts,
            'send_mail_to': self.send_mail_to,
            'separateChecking': self.separate_checking,  # TODO: now disabled
            'skip_tables': self.excluded_tables,
            'strings_amount': self.strings_amount,
            'table_timeout': self.table_timeout
        }

    @staticmethod
    def is_report(table, connection):
        booler = []
        query = "DESCRIBE {};".format(table)
        result = connection.select(query)
        for field in result:
            if 'dt' in field.get('Field'):
                booler.append(True)
            if 'impressions' in field.get('Field'):
                booler.append(True)
            if 'clicks' in field.get('Field'):
                booler.append(True)
        if len(booler) == 3:
            return True
        else:
            return False

    def compare_data(self, start_time, service_dir, mapping, tables):
        prod_connection = dbcmp_sql_helper.DbCmpSqlHelper(self.prod_sql, self.logger)
        test_connection = dbcmp_sql_helper.DbCmpSqlHelper(self.test_sql, self.logger)
        for table in tables:
            # table = 'campaignosreport'
            start_table_check_time = datetime.datetime.now()
            self.logger.info("Table {} processing started now...".format(table))
            is_report = self.is_report(table, prod_connection)

            # TODO: refactor this place! Rename onlyReports/Entities

            if 'onlyReports' in self.separate_checking and not is_report:
                continue
            if 'onlyEntities' in self.separate_checking and is_report:
                continue
            self.sql_comparing_properties.update({'service_dir': service_dir})
            cmp_params = self.sql_comparing_properties
            compared_table = Comparation(prod_connection, test_connection, table, self.logger, cmp_params)
            global_break = compared_table.compare_table(is_report, mapping, start_time, self.comparing_info,
                                                        self.sql_comparing_properties.get('comparing_step'))
            self.logger.info("Table {} ".format(table) +
                             "checked in {}...".format(datetime.datetime.now() - start_table_check_time))
            if global_break:
                data_comparing_time = datetime.datetime.now() - start_time
                self.logger.warn(('Global breaking is True. Comparing interrupted. ' +
                                 'Comparing finished in {}'.format(data_comparing_time)))
                return data_comparing_time
        data_comparing_time = datetime.datetime.now() - start_time
        self.logger.info("Comparing finished in {}".format(data_comparing_time))
        return data_comparing_time

    def calculate_table_list(self, connection):
        if len(self.only_tables) == 1 and self.only_tables[0] == '':
            return self.comparing_info.define_table_list(self.excluded_tables, self.client_ignored_tables,
                                                         self.reports, self.entities, connection)
        else:
            return self.only_tables

    def compare_metadata(self, start_time, tables):
        prod_connection = dbcmp_sql_helper.DbCmpSqlHelper(self.prod_sql, self.logger)
        test_connection = dbcmp_sql_helper.DbCmpSqlHelper(self.test_sql, self.logger)
        for table in tables:
            self.logger.info("Check schema for table {}...".format(table))
            query = ("SELECT {} FROM INFORMATION_SCHEMA.COLUMNS ".format(', '.join(self.schema_columns)) +
                     "WHERE TABLE_SCHEMA = 'DBNAME' AND TABLE_NAME='TABLENAME' ".replace("TABLENAME", table) +
                     "ORDER BY COLUMN_NAME;")

            prod_columns, test_columns = dbcmp_sql_helper.get_comparable_objects([prod_connection, test_connection],
                                                                                 query)
            if (prod_columns is None) or (test_columns is None):
                self.logger.warn('Table {} skipped because something going bad'.format(table))
                continue
            # TODO: Type error: unhashable type: 'dict'
            uniq_for_prod = list(set(prod_columns) - set(test_columns))
            uniq_for_test = list(set(test_columns) - set(prod_columns))
            if len(uniq_for_prod) > 0 and self.fail_with_first_error:
                return self.schema_comparing_time(table, uniq_for_prod, start_time)
            if len(uniq_for_test) > 0 and self.fail_with_first_error:
                return self.schema_comparing_time(table, uniq_for_test, start_time)
            if not all([len(uniq_for_prod) == 0, len(uniq_for_test) == 0]):
                self.logger.error(" [ERROR] Tables {} differs!".format(table))
        schema_comparing_time = datetime.datetime.now() - start_time
        self.logger.info("Schema compared in {}".format(schema_comparing_time))
        return datetime.datetime.now() - start_time

    def schema_comparing_time(self, table, uniq_list, start_time):
        self.logger.error("Elements, unique for table {} ".format(table) +
                          "in {} db:{}".format(self.test_sql.get('db'), list(uniq_list[0])))
        self.logger.critical("There are some discrepancies in schema, comparing interrupted. " +
                             "Exclude tables with different schema from comparing-list (using skip tables field in UI)")
        schema_comparing_time = datetime.datetime.now() - start_time
        self.logger.info("Schema partially compared in {}".format(schema_comparing_time))
        return schema_comparing_time
