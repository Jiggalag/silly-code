import datetime
import os
from py_scripts.helpers import dbHelper, converters
from py_scripts.dbComparator import queryConstructor


class Object:
    def __init__(self, sql_connection_properties, sql_comparing_properties, comparing_info, client=None):
        self.client = client
        self.logger = sql_comparing_properties.get('logger')
        self.sql_connection_properties = sql_connection_properties
        self.prod_sql = sql_connection_properties.get('prod')
        self.test_sql = sql_connection_properties.get('test')
        self.sql_comparing_properties = sql_comparing_properties
        self.comparing_info = comparing_info
        self.sql_comparing_properties = {}
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
            'migrationhistory']
        # TODO: transfer all this properties from UI
        if 'retry_attempts' in sql_comparing_properties.keys():
            self.attempts = sql_comparing_properties.get('retry_attempts')
        if 'comparing_step' in sql_comparing_properties.keys():
            self.comparing_step = int(sql_comparing_properties.get('comparing_step'))
        if 'skip_columns' in sql_comparing_properties.keys():
            self.hide_columns = sql_comparing_properties.get('skip_columns').split(',')
        if 'mode' in sql_comparing_properties.keys():
            self.mode = sql_comparing_properties.get('mode')
        # TODO: probably should be deleted
        if 'clientIgnoredTables' in sql_comparing_properties.keys():
            self.client_ignored_tables = sql_comparing_properties.get('clientIgnoredTables')
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
        if 'amount_checking_record' in sql_comparing_properties.keys():
            self.amount_checking_record = sql_comparing_properties.get('amount_checking_record')

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
        tables = self.comparing_info.get_tables(self.excluded_tables, self.client_ignored_tables)
        for table in tables:
            self.logger.info("Table {} processing now...".format(table))
            start_table_check_time = datetime.datetime.now()
            local_break = False
            query_object = queryConstructor.InitializeQuery(self.prod_sql, self.logger)
            if self.complex_condition(table):
                if not global_break:
                    self.compare_report_table(global_break, mapping, local_break, table, service_dir, start_time)
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
                                                                                     self.sql_connection_properties,
                                                                                     self.client,
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
                    for query in query_list:
                        if (not self.compare_entity_table(table, query, service_dir)) and self.fail_with_first_error:
                            self.logger.info("First error founded, checking failed. Comparing takes {}".format(
                                datetime.datetime.now() - start_time))
                            global_break = True
                            self.logger.info("Table {} ".format(table) +
                                             "checked in {}".format(datetime.datetime.now() - start_table_check_time))
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

    def iteration_comparing_by_queries(self, query_list, global_break, table, start_time, service_dir):
        local_break = False
        for query in query_list:
            if self.mode == "day-sum":
                if ("impressions" and "clicks") in dbHelper.DbConnector(self.prod_sql,
                                                                        self.logger).get_column_list(table):
                    if not self.compare_report_sums(table, query) and self.fail_with_first_error:
                        self.logger.critical("First error founded, checking failed. " +
                                             "Comparing takes {}.".format(datetime.datetime.now() - start_time))
                        global_break = True
                        return global_break, local_break
                else:
                    self.logger.warn("There is no impression of click column in table {}".format(table))
                    local_break = True
                    return global_break, local_break
            elif self.mode in ["section-sum", "detailed"]:
                if self.mode == "section-sum":
                    section = calculate_section_name(query)
                    self.logger.info("Check section {} for table {}".format(section, table))
                if not self.compare_reports_detailed(table, query, service_dir) and self.fail_with_first_error:
                    self.logger.critical("First error founded, checking failed. Comparing takes {}.".format(
                        datetime.datetime.now() - start_time))
                    global_break = True
                    return global_break, local_break
        return global_break, local_break

    def compare_metadata(self, start_time):
        tables = self.comparing_info.get_tables(self.excluded_tables, self.client_ignored_tables)
        for table in tables:
            self.logger.info("Check schema for table {}...".format(table))
            query = ("SELECT {} FROM INFORMATION_SCHEMA.COLUMNS ".format(', '.join(self.schema_columns)) +
                    "WHERE TABLE_SCHEMA = 'DBNAME' AND TABLE_NAME='TABLENAME' ".replace("TABLENAME", table) +
                    "ORDER BY COLUMN_NAME;")

            prod_columns, test_columns = dbHelper.DbConnector.parallel_select(self.sql_connection_properties,
                                                                              self.client, query, self.logger)
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
        if  uniq_list is None:
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

    def compare_reports_detailed(self, table, query, service_dir):
        header = get_header(query)
        prod_reports, test_reports = dbHelper.DbConnector.parallel_select(self.sql_connection_properties,
                                                                          self.client, query, self.logger)
        if (prod_reports is None) or (test_reports is None):
            self.logger.warn('Table {} skipped because something going bad'.format(table))
            return True
        prod_unique_reports = set(prod_reports) - set(test_reports)
        test_unique_reports = set(test_reports) - set(prod_reports)
        if len(prod_unique_reports) > 0:
            self.write_unique_entities_to_file(table, prod_unique_reports, "prod", header, service_dir)
        if len(test_unique_reports) > 0:
            self.write_unique_entities_to_file(table, test_unique_reports, "test", header, service_dir)
        if not all([len(prod_unique_reports) == 0, len(test_unique_reports) == 0]):
            self.logger.error("Tables {} differs!".format(table))
            self.comparing_info.update_diff_schema(table)
            return False
        else:
            return True

    def compare_report_sums(self, table, query):
        prod_reports, test_reports = dbHelper.DbConnector.parallel_select(self.sql_connection_properties,
                                                                          self.client, query, self.logger, "list")
        if (prod_reports is not None) or (test_reports is not None):
            return True
        clicks = True
        imps = True
        prod_imps = prod_reports[0]
        test_imps = test_reports[0]
        prod_clicks = prod_reports[1]
        test_clicks = test_reports[1]
        if prod_clicks != test_clicks:
            clicks = False
            self.logger.warn("There are different click sums for query {}. ".format(query) +
                             "Prod clicks={}, test clicks={}".format(prod_clicks, test_clicks))
        if prod_imps != test_imps:
            imps = False
            self.logger.warn("There are different imp sums for query {}. ".format(query) +
                             "Prod imps={}, test imps={}".format(prod_imps, test_imps))
        if not all([clicks, imps]):
            self.logger.error("Tables {} differs!".format(table))
            self.comparing_info.update_diff_data(table)
            return False
        else:
            return True

    def compare_entity_table(self, table, query, service_dir):
        header = get_header(query)
        prod_entities, test_entities = dbHelper.DbConnector.parallel_select(self.sql_connection_properties,
                                                                            self.client, query, self.logger)
        if (prod_entities is None) or (test_entities is None):
            self.logger.warn('Table {} skipped because something going bad'.format(table))
            return True
        prod_unique_entities = set(prod_entities) - set(test_entities)
        test_unique_entities = set(test_entities) - set(prod_entities)
        if len(prod_unique_entities) > 0:
            self.write_unique_entities_to_file(table, prod_unique_entities, "prod", header, service_dir)
        if len(test_unique_entities) > 0:
            self.write_unique_entities_to_file(table, test_unique_entities, "test", header, service_dir)
        if not all([len(prod_unique_entities) == 0, len(test_unique_entities) == 0]):
            self.logger.error("Tables {} differs!".format(table))
            self.comparing_info.update_diff_data(table)
            return False
        else:
            return True

    def compare_report_table(self, global_break, mapping, local_break, table, service_dir, start_time):
        dates = converters.convertToList(self.compare_dates(table))
        dates.sort()
        if dates:
            prod_record_amount, test_record_amount = dbHelper.get_amount_records(table, dates[0],
                                                                                 self.sql_connection_properties,
                                                                                 self.client,
                                                                                 self.logger)
            for dt in reversed(dates):
                if not all([global_break, local_break]):
                    max_amount = max(prod_record_amount, test_record_amount)
                    cmp_object = queryConstructor.InitializeQuery(self.prod_sql, self.logger)
                    query_list = cmp_object.report(table, dt, self.mode, max_amount,
                                                   self.comparing_step, mapping)
                    global_break, local_break = self.iteration_comparing_by_queries(query_list, global_break, table,
                                                                                    start_time, service_dir)
                else:
                    break
        else:
            self.logger.warn("Tables {} should not be compared correctly, ".format(table) +
                             "because they have no any crosses dates in reports")
            self.comparing_info.no_crossed_tables.append(table)

    def compare_dates(self, table):
        select_query = "SELECT distinct(`dt`) from {};".format(table)
        prod_dates, test_dates = dbHelper.DbConnector.parallel_select(self.sql_connection_properties,
                                                                      self.client, select_query, self.logger)
        if (prod_dates is None) or (test_dates is None):
            self.logger.warn('Table {} skipped because something going bad'.format(table))
            return []
        if all([prod_dates, test_dates]):
            return self.calculate_comparing_timeframe(prod_dates, test_dates, table)
        else:
            if not prod_dates and not test_dates:
                self.logger.warn("Table {} is empty in both dbs...".format(table))
                self.comparing_info.empty.append(table)
            elif not prod_dates:
                self.logger.warn("Table {} on {} is empty!".format(table, self.prod_sql.get('db')))
                self.comparing_info.update_empty("prod", table)
            else:
                self.logger.warn("Table {} on {} is empty!".format(table, self.test_sql.get('db')))
                self.comparing_info.update_empty("test", table)
            return []

    def calculate_comparing_timeframe(self, prod_dates, test_dates, table):
        actual_dates = set()
        days = self.depth_report_check
        for day in range(1, days):
            actual_dates.add(calculate_date(day))
        if prod_dates[-days:] == test_dates[-days:]:
            return self.get_comparing_timeframe(prod_dates)
        else:
            return self.get_timeframe_intersection(prod_dates, test_dates, table)

    def get_timeframe_intersection(self, prod_dates, test_dates, table):
        prod_set = set(prod_dates)
        test_set = set(test_dates)
        if prod_set - test_set:  # this code (4 strings below) should be moved to different function
            unique_dates = get_unique_dates(prod_set, test_set)
            self.logger.warn("This dates absent in {}: ".format(self.test_sql.get('db')) +
                             "{} in report table {}...".format(",".join(unique_dates), table))
        if test_set - prod_set:
            unique_dates = get_unique_dates(test_set, prod_set)
            self.logger.warn("This dates absent in {}: ".format(self.prod_sql.get('db')) +
                             "{} in report table {}...".format(",".join(unique_dates), table))
        return prod_set & test_set

    def get_comparing_timeframe(self, prod_dates):
        comparing_timeframe = []
        for item in prod_dates[-self.depth_report_check:]:
            comparing_timeframe.append(item.date().strftime("%Y-%m-%d"))
        return comparing_timeframe

    def write_unique_entities_to_file(self, table, list_uniqs, stage, header, service_dir):
        self.logger.error("There are {} unique elements in table {} ".format(len(list_uniqs), table) +
                          "on {}-server. Detailed list of records ".format(stage) +
                          "saved to {}{}_uniqRecords_{}".format(service_dir, table, stage))
        file_name = "{}{}_uniqRecords_{}".format(service_dir, table, stage)
        if not os.path.exists(file_name):
            write_header(file_name, header)
        with open(file_name, "a") as file:
            first_list = converters.convertToList(list_uniqs)
            first_list.sort()
            for item in first_list:
                file.write(str(item) + "\n")


def calculate_section_name(query):
    tmp_list = query.split(" ")
    for item in tmp_list:
        if "GROUP" in item:
            return tmp_list[tmp_list.index(item) + 2][2:].replace("_", "").replace("id", "")


def get_header(query):
    cut_select = query[7:]
    columns = cut_select[:cut_select.find("FROM") - 1]
    header = []
    for item in columns.split(","):
        if ' as ' in item:
            header.append(item[:item.find(' ')])
        else:
            header.append(item)
    return header


def calculate_date(days):
    return (datetime.datetime.today().date() - datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


def write_header(file_name, header):
    with open(file_name, 'w') as file:
        file.write(','.join(header) + '\n')


def get_unique_dates(first_date_list, second_date_list):
    unique_dates = []
    for item in converters.convertToList(first_date_list - second_date_list):
        unique_dates.append(item.strftime("%Y-%m-%d %H:%M:%S"))
    return unique_dates
