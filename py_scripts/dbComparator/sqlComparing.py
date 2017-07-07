import datetime
import os
from py_scripts.helpers.loggingHelper import Logger
from py_scripts.helpers import dbHelper, converters
from py_scripts.dbComparator import queryConstructor

logger = Logger(20)


class Object:
    def __init__(self, client_config, comparing_info, client):
        self.client = client
        self.client_config = client_config
        self.comparing_info = comparing_info
        self.db_properties = {
            'attempts': client_config.getProperty("sqlProperties", "retryAttempts"),
            'comparingStep': client_config.getProperty("sqlProperties", "comparingStep"),
            'hideColumns': client_config.getProperty("sqlProperties", "hideColumns"),
            'mode': client_config.getProperty("sqlProperties", "reportCheckType"),
            'clientIgnoredTables': client_config.getProperty("specificIgnoredTables", client + ".ignoreTables"),
            'enableSchemaChecking': client_config.getProperty("sqlProperties", "enableSchemaChecking"),
            'depthReportCheck': client_config.getProperty("sqlProperties", "depthReportCheck"),
            'failWithFirstError': client_config.getProperty("sqlProperties", "failWithFirstError"),
            'schemaColumns': client_config.getProperty("sqlProperties", "schemaColumns"),
            'separateChecking': client_config.getProperty("sqlProperties", "separateChecking")
        }
        self.excluded_tables = set(client_config.getProperty("sqlProperties", "tablesNotToCompare"))
        self.prod_sql = dbHelper.DbConnector(self.client_config.get_sql_connection_params("prod"), **self.db_properties)
        self.test_sql = dbHelper.DbConnector(self.client_config.get_sql_connection_params("test"), **self.db_properties)

    def compare_data(self, global_break, start_time, service_dir, mapping):
        tables = self.comparing_info.get_tables(self.excluded_tables, self.prod_sql.client_ignored_tables)
        for table in tables:
            # table = "migrationhistory"  # TODO: remove this hack after refactor finishing
            logger.info("Table {} processing now...".format(table))
            start_table_check_time = datetime.datetime.now()
            local_break = False
            query_object = queryConstructor.InitializeQuery(self.prod_sql)
            if (('report' in table) or ('statistic' in table)) and ('dt' in dbHelper.DbConnector.get_column_list(self.prod_sql, table)) and 'onlyEntities' not in self.db_properties.get('separateChecking'):
                if not global_break:
                    self.compare_report_table(global_break, mapping, local_break, table, service_dir, start_time)
                    logger.info("Table {} checked in {}..."
                                .format(table, datetime.datetime.now() - start_table_check_time))
                else:
                    logger.info("Table {} checked in {}..."
                                .format(table, datetime.datetime.now() - start_table_check_time))
                    break
            else:
                if 'onlyReports' in self.db_properties.get('separateChecking'):
                    continue
                prod_record_amount, test_record_amount = dbHelper.get_amount_records(table, None, self.client_config,
                                                                                     self.client, self.db_properties)
                if prod_record_amount == 0 and test_record_amount == 0:
                    logger.warn("Table {} is empty on both servers!".format(table))
                    continue
                if prod_record_amount == 0:
                    logger.warn("Table {} is empty on prod-server!".format(table))
                    continue
                if test_record_amount == 0:
                    logger.warn("Table {} is empty on test-server!".format(table))
                    continue
                max_amount = max(prod_record_amount, test_record_amount)
                query_list = query_object.entity(table, max_amount, self.prod_sql.comparing_step, mapping)
                if not global_break:
                    for query in query_list:
                        if (not self.compare_entity_table(table, query, service_dir)) and self.prod_sql.quick_fall:
                            logger.info("First error founded, checking failed. Comparing takes {}".format(
                                datetime.datetime.now() - start_time))
                            global_break = True
                            logger.info("Table {} checked in {}..."
                                        .format(table, datetime.datetime.now() - start_table_check_time))
                            break
                    logger.info(
                        "Table {} checked in {}...".format(table, datetime.datetime.now() - start_table_check_time))
                else:
                    logger.info(
                        "Table {} checked in {}...".format(table, datetime.datetime.now() - start_table_check_time))
                    break
        data_comparing_time = datetime.datetime.now() - start_time
        logger.info("Comparing finished in {}".format(data_comparing_time))
        return data_comparing_time

    def iteration_comparing_by_queries(self, query_list, global_break, table, start_time, service_dir):
        local_break = False
        for query in query_list:
            if self.prod_sql.mode == "day-sum":
                if ("impressions" and "clicks") in dbHelper.DbConnector.get_column_list(self.prod_sql, table):
                    if not self.compare_report_sums(table, query) and self.prod_sql.quick_fall:
                        logger.critical("First error founded, checking failed. Comparing takes {}.".format(
                            datetime.datetime.now() - start_time))
                        global_break = True
                        return global_break, local_break
                else:
                    logger.warn("There is no impression of click column in table {}".format(table))
                    local_break = True
                    return global_break, local_break
            elif self.prod_sql.mode in ["section-sum", "detailed"]:
                if self.prod_sql.mode == "section-sum":
                    section = calculate_section_name(query)
                    logger.info("Check section {} for table {}".format(section, table))
                if not self.compare_reports_detailed(table, query, service_dir) and self.prod_sql.quick_fall:
                    logger.critical("First error founded, checking failed. Comparing takes {}.".format(
                        datetime.datetime.now() - start_time))
                    global_break = True
                    return global_break, local_break
        return global_break, local_break

    def compare_metadata(self, start_time):
        tables = self.comparing_info.get_tables(self.excluded_tables, self.prod_sql.client_ignored_tables)
        for table in tables:
            logger.info("Check schema for table {}...".format(table))
            query = "SELECT {} FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'DBNAME' " \
                    "AND TABLE_NAME='TABLENAME' ORDER BY COLUMN_NAME;"\
                .replace("TABLENAME", table).format(', '.join(self.prod_sql.schema_columns))
            prod_columns, test_columns = dbHelper.DbConnector.\
                parallel_select(self.client_config, self.client, query, self.db_properties)
            uniq_for_prod = list(set(prod_columns) - set(test_columns))
            uniq_for_test = list(set(test_columns) - set(prod_columns))
            if len(uniq_for_prod) > 0:
                logger.error("Elements, unique for table {} in {} db:{}".
                             format(table, self.prod_sql.db, list(uniq_for_prod[0])))
                if self.prod_sql.quick_fall:
                    logger.critical("First error founded, comparing failed. "
                                    "To find all discrepancies set failWithFirstError property in false\n")
                    schema_comparing_time = datetime.datetime.now() - start_time 
                    logger.info("Schema partially compared in {}".format(datetime.datetime.now() - start_time))
                    return schema_comparing_time
            if len(uniq_for_test) > 0:
                logger.error("Elements, unique for table {} in {} db:{}".
                             format(table, self.test_sql.db, list(uniq_for_test[0])))
                if self.prod_sql.quick_fall:
                    logger.critical("First error founded, comparing failed. "
                                    "To find all discrepancies set failWithFirstError property in false\n")
                    schema_comparing_time = datetime.datetime.now() - start_time
                    logger.info("Schema partially compared in {}".format(schema_comparing_time))
                    return schema_comparing_time
            if not all([len(uniq_for_prod) == 0, len(uniq_for_test) == 0]):
                logger.error(" [ERROR] Tables {} differs!".format(table))
                self.comparing_info.update_diff_schema(table)
                if self.prod_sql.quick_fall:
                    logger.critical("First error founded, comparing failed. "
                                    "To find all discrepancies set failWithFirstError property in false\n")
                    schema_comparing_time = datetime.datetime.now() - start_time
                    logger.info("Schema partially compared in {}".format(schema_comparing_time))
                    return schema_comparing_time
        schema_comparing_time = datetime.datetime.now() - start_time
        logger.info("Schema compared in {}".format(schema_comparing_time))
        return datetime.datetime.now() - start_time

    def compare_reports_detailed(self, table, query, service_dir):
        header = get_header(query)
        prod_reports, test_reports = dbHelper.DbConnector.\
            parallel_select(self.client_config, self.client, query, self.db_properties)
        prod_unique_reports = prod_reports - test_reports
        test_unique_reports = test_reports - prod_reports
        if len(prod_unique_reports) > 0:
            self.write_unique_entities_to_file(table, prod_unique_reports, "prod", header, service_dir)
        if len(test_unique_reports) > 0:
            self.write_unique_entities_to_file(table, test_unique_reports, "test", header, service_dir)
        if not all([len(prod_unique_reports) == 0, len(test_unique_reports) == 0]):
            logger.error("Tables {} differs!".format(table))
            self.comparing_info.update_diff_schema(table)
            return False
        else:
            return True

    def compare_report_sums(self, table, query):
        prod_reports, test_reports = dbHelper.DbConnector.\
            parallel_select(self.client_config, self.client, query, "list")
        clicks = True
        imps = True
        prod_imps = prod_reports[0]
        test_imps = test_reports[0]
        prod_clicks = prod_reports[1]
        test_clicks = test_reports[1]
        if prod_clicks != test_clicks:
            clicks = False
            logger.warn("There are different click sums for query {}. Prod clicks={}, test clicks={}"
                        .format(query, prod_clicks, test_clicks))
        if prod_imps != test_imps:
            imps = False
            logger.warn("There are different imp sums for query {}. Prod imps={}, test imps={}"
                        .format(query, prod_imps, test_imps))
        if not all([clicks, imps]):
            logger.error("Tables {} differs!".format(table))
            self.comparing_info.update_diff_data(table)
            return False
        else:
            return True

    def compare_entity_table(self, table, query, service_dir):
        header = get_header(query)
        prod_entities, test_entities = dbHelper.DbConnector.parallel_select(self.client_config, self.client,
                                                                            query, self.db_properties)
        prod_unique_entities = set(prod_entities) - set(test_entities)
        test_unique_entities = set(test_entities) - set(prod_entities)
        if len(prod_unique_entities) > 0:
            self.write_unique_entities_to_file(table, prod_unique_entities, "prod", header, service_dir)
        if len(test_unique_entities) > 0:
            self.write_unique_entities_to_file(table, test_unique_entities, "test", header, service_dir)
        if not all([len(prod_unique_entities) == 0, len(test_unique_entities) == 0]):
            logger.error("Tables {} differs!".format(table))
            self.comparing_info.update_diff_data(table)
            return False
        else:
            return True

    def compare_report_table(self, global_break, mapping, local_break, table, service_dir, start_time):
        dates = converters.convertToList(self.compare_dates(table))
        dates.sort()
        if dates:
            prod_record_amount, test_record_amount = dbHelper.get_amount_records(table, dates[0], self.client_config,
                                                                                 self.client, self.db_properties)
            for dt in reversed(dates):
                if not all([global_break, local_break]):
                    max_amount = max(prod_record_amount, test_record_amount)
                    cmp_object = queryConstructor.InitializeQuery(self.prod_sql)
                    query_list = cmp_object.report(table, dt, self.prod_sql.mode, max_amount,
                                                   self.prod_sql.comparing_step, mapping)
                    global_break, local_break = self.iteration_comparing_by_queries(query_list, global_break, table,
                                                                                    start_time, service_dir)
                else:
                    break
        else:
            logger.warn("Tables {} should not be compared correctly, because they have no any crosses dates in reports"
                        .format(table))
            self.comparing_info.no_crossed_tables.append(table)

    def compare_dates(self, table):
        select_query = "SELECT distinct(`dt`) from {};".format(table)
        prod_dates, test_dates = dbHelper.DbConnector.parallel_select(self.client_config, self.client, select_query)
        if all([prod_dates, test_dates]):
            return self.calculate_comparing_timeframe(prod_dates, test_dates, table)
        else:
            if not prod_dates and not test_dates:
                logger.warn("Table {} is empty in both dbs...".format(table))
                self.comparing_info.empty.append(table)
            elif not prod_dates:
                logger.warn("Table {} on {} is empty!".format(table, self.prod_sql.db))
                self.comparing_info.update_empty("prod", table)
            else:
                logger.warn("Table {} on {} is empty!".format(table, self.test_sql.db))
                self.comparing_info.update_empty("test", table)
            return []

    def calculate_comparing_timeframe(self, prod_dates, test_dates, table):
        actual_dates = set()
        days = self.prod_sql.check_depth
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
            logger.warn("This dates absent in {}: {} in report table {}..."
                        .format(self.test_sql.db, ",".join(unique_dates), table))
        if test_set - prod_set:
            unique_dates = get_unique_dates(test_set, prod_set)
            logger.warn("This dates absent in {}: {} in report table {}..."
                        .format(self.prod_sql.db, ",".join(unique_dates), table))
        return prod_set & test_set

    def get_comparing_timeframe(self, prod_dates):
        comparing_timeframe = []
        for item in prod_dates[-self.prod_sql.check_depth:]:
            comparing_timeframe.append(item.date().strftime("%Y-%m-%d"))
        return comparing_timeframe

    @staticmethod
    def write_unique_entities_to_file(table, list_uniqs, stage, header, service_dir):
        logger.error("There are {0} unique elements in table {1} on {2}-server. "
                     "Detailed list of records saved to {3}{1}_uniqRecords_{2}"
                     .format(len(list_uniqs), table, stage, service_dir))
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
