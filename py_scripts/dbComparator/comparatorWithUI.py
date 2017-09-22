import datetime
import os
import shutil
from py_scripts.helpers import converters, helper, dbHelper
from py_scripts.dbComparator import tableData, sqlComparing
from py_scripts.helpers.dbHelper import DbConnector
from py_scripts.dbComparator.queryConstructor import InitializeQuery


class Backend:
    def __init__(self, sql_connection_properties, sql_comparing_properties):
        self.sql_connection_properties = sql_connection_properties
        self.sql_comparing_properties = sql_comparing_properties
        self.logger = sql_comparing_properties.get('logger')
        self.OS = sql_comparing_properties.get('os')

    def run_comparing(self):
        if self.OS == "Windows":
            service_dir = "C:\\comparator"
        else:
            service_dir = "/tmp/comparator/"
        check_service_dir(service_dir)
        prod_sql_dict = self.sql_connection_properties.get('prod')
        test_sql_dict = self.sql_connection_properties.get('test')
        comparing_info = tableData.Info(self.logger)
        comparing_info.update_table_list("prod", dbHelper.DbConnector(prod_sql_dict, self.logger).get_tables())
        comparing_info.update_table_list("test", dbHelper.DbConnector(test_sql_dict, self.logger).get_tables())
        global_break = False
        if "Linux" in self.OS:
            create_test_dir("/mxf/data/test_results/")
        else:
            create_test_dir("C:\\dbComparator\\")
        start_time = datetime.datetime.now()
        self.logger.info("Start processing!")
        mapping = InitializeQuery(DbConnector(prod_sql_dict, self.logger), self.logger).prepare_column_mapping()
        if self.sql_comparing_properties.get('check_schema'):
            schema_comparing_time = sqlComparing.Object(self.sql_connection_properties,
                                                        self.sql_comparing_properties,
                                                        comparing_info).compare_metadata(start_time)
            data_comparing_time = sqlComparing.Object(self.sql_connection_properties,
                                                      self.sql_comparing_properties,
                                                      comparing_info).compare_data(global_break,
                                                                                   start_time,
                                                                                   service_dir,
                                                                                   mapping)
        else:
            self.logger.info("Schema checking disabled...")
            schema_comparing_time = None
            data_comparing_time = sqlComparing.Object(self.sql_connection_properties,
                                                      self.sql_comparing_properties,
                                                      comparing_info).compare_data(global_break,
                                                                                   start_time,
                                                                                   service_dir,
                                                                                   mapping)
        subject = "[Test] Check databases"
        text = generate_mail_text(comparing_info, self.sql_comparing_properties,
                                  data_comparing_time, schema_comparing_time)
        helper.sendmail(text, 'do-not-reply@inventale.com', 'AKIAJHBVE2GQUQBRSQVA',
                        'pavel.kiselev@best4ad.com', subject, None)


def check_service_dir(service_dir):
    if os.path.exists(service_dir):
        shutil.rmtree(service_dir)
    os.mkdir(service_dir)


def create_test_dir(path):
    if not os.path.exists(path):
        os.mkdir(path)


def generate_mail_text(comparing_info, sql_comparing_properties, data_comparing_time, schema_comparing_time):
    text = "Initial conditions:\n\n"
    if sql_comparing_properties.get('check_schema'):
        text = text + "1. Schema checking enabled.\n"
    else:
        text = text + "1. Schema checking disabled.\n"
    if sql_comparing_properties.get('fail_with_first_error'):
        text = text + "2. Failed with first founded error.\n"
    else:
        text = text + "2. Find all errors\n"
        text = text + "3. Report checkType is " + sql_comparing_properties.get('mode') + "\n\n"
    if any([comparing_info.empty, comparing_info.diff_data, comparing_info.no_crossed_tables,
            comparing_info.prod_uniq_tables, comparing_info.test_uniq_tables]):
        text = get_test_result_text(text, comparing_info)
    else:
        text = text + "It is impossible! There is no any problems founded!"
    if sql_comparing_properties.get('check_schema'):
        text = text + "Schema checked in " + str(schema_comparing_time) + "\n"
    text = text + "Dbs checked in " + str(data_comparing_time) + "\n"
    return text


def get_test_result_text(body, comparing_info):
    body = body + "There are some problems found during checking.\n\n"
    if comparing_info.empty:
        body = body + "Tables, empty in both dbs:\n" + ",".join(comparing_info.empty) + "\n\n"
    if comparing_info.prod_empty:
        body = body + "Tables, empty on production db:\n" + ",".join(comparing_info.prod_empty) + "\n\n"
    if comparing_info.test_empty:
        body = body + "Tables, empty on test db:\n" + ",".join(comparing_info.test_empty) + "\n\n"
    if comparing_info.diff_data:
        body = body + "Tables, which have any difference:\n" + ",".join(comparing_info.diff_data) + "\n\n"
    if list(set(comparing_info.empty).difference(set(comparing_info.no_crossed_tables))):
        body = body + "Report tables, which have no crossing dates:\n" + ",".join(
            list(set(comparing_info.empty).difference(set(comparing_info.no_crossed_tables)))) + "\n\n"
    if comparing_info.get_uniq_tables("prod"):
        body = body + "Tables, which unique for production db:\n" + ",".join(
            converters.convertToList(comparing_info.prod_uniq_tables)) + "\n\n"
    if comparing_info.get_uniq_tables("test"):
        body = body + "Tables, which unique for test db:\n" + ",".join(
            converters.convertToList(comparing_info.test_uniq_tables)) + "\n\n"
    return body
