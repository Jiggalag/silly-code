import datetime
import os
import shutil
import platform
from py_scripts.helpers import configHelper, converters, helper, loggingHelper, dbHelper
from py_scripts.dbComparator import queryConstructor, tableData, sqlComparing

if "Win" in platform.system():
    OS = "Windows"
else:
    OS = "Linux"
if "Linux" in OS:
    propertyFile = os.getcwd() + "/resources/properties/sqlComparator.properties"
else:
    propertyFile = os.getcwd() + "\\resources\\properties\\sqlComparator.properties"
config = configHelper.IfmsConfigCommon(propertyFile)

logger = loggingHelper.Logger(config.getPropertyFromMainSection("loggingLevel"))

sendMailFrom = config.getPropertyFromMainSection("sendMailFrom")
sendMailTo = config.getPropertyFromMainSection("sendMailTo")
mailPassword = config.getPropertyFromMainSection("mailPassword")
check_schema = config.getProperty("sqlProperties", "enableSchemaChecking")
quick_fall = config.getProperty("sqlProperties", "quick_fall")
mode = config.getProperty("sqlProperties", "reportCheckType")


def check_service_dir(dir_name):
    if os.path.exists(dir_name):
        shutil.rmtree(dir_name)
    os.mkdir(dir_name)


def create_test_dir(path, client_name):
    if not os.path.exists(path):
        os.mkdir(path)
    if not os.path.exists(path + client_name):
        os.mkdir(path + client_name)


def generate_mail_text(comparing_info, mode):
    text = "Initial conditions:\n\n"
    if check_schema:
        text = text + "1. Schema checking enabled.\n"
    else:
        text = text + "1. Schema checking disabled.\n"
    if quick_fall:
        text = text + "2. Failed with first founded error.\n"
    else:
        text = text + "2. Find all errors\n"
        text = text + "3. Report checkType is " + mode + "\n\n"
    if any([comparing_info.empty, comparing_info.diff_data, comparing_info.no_crossed_tables,
            comparing_info.prod_uniq_tables, comparing_info.test_uniq_tables]):
        text = get_test_result_text(text, comparing_info)
    else:
        text = text + "It is impossible! There is no any problems founded!"
    if check_schema:
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


if OS == "Windows":
    service_dir = "C:\\comparator"
else:
    service_dir = "/tmp/comparator/"
check_service_dir(service_dir)
for client in config.getClients():
    client_config = configHelper.IfmsConfigClient(propertyFile, client)
    sql_property_dict = client_config.get_sql_connection_params('test')
    comparing_info = tableData.Info()
    comparing_info.update_table_list("prod",
                                     dbHelper.DbConnector(client_config.get_sql_connection_params("prod")).get_tables())
    comparing_info.update_table_list("test",
                                     dbHelper.DbConnector(client_config.get_sql_connection_params("test")).get_tables())
    global_break = False
    if "Linux" in OS:
        create_test_dir("/mxf/data/test_results/", client)
    else:
        create_test_dir("C:\\dbComparator\\", client)
    start_time = datetime.datetime.now()
    logger.info("Start {} processing!".format(client))
    mapping = queryConstructor.prepare_column_mapping(dbHelper.DbConnector(client_config.get_sql_connection_params("prod")))
    if check_schema:
        schema_comparing_time = sqlComparing.Object(client_config, comparing_info, client).compare_metadata(start_time)
        data_comparing_time = sqlComparing.Object(client_config, comparing_info, client).compare_data(global_break,
                                                                                                      start_time,
                                                                                                      service_dir,
                                                                                                      mapping)
    else:
        logger.info("Schema checking disabled...")
        data_comparing_time = sqlComparing.Object(client_config, comparing_info, client).compare_data(global_break,
                                                                                                      start_time,
                                                                                                      service_dir,
                                                                                                      mapping)
    subject = "[Test] Check databases for client {}".format(client)
    text = generate_mail_text(comparing_info, mode)
    helper.sendmail(text, sendMailFrom, sendMailTo, mailPassword, subject, None)
