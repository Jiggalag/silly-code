import argparse
import datetime
import os
import os.path
import shutil
import smtplib
import platform
from os.path import basename
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from py_scripts.helpers import configHelper, converters, logging_helper, dbcmp_sql_helper
from py_scripts.dbComparator import tableData, sqlComparing
from py_scripts.dbComparator import queryConstructor


parser = argparse.ArgumentParser(description='Utility intended to comparing databases')
parser.add_argument('--server', type=str, default='dev01.inventale.com',
                    help='Host, where script sends requests (default: dev01.inventale.com)')
parser.add_argument('--send_mail_to', type=str, default='pavel.kiselev@best4ad.com,nataly.lisitsyna@best4ad.com',
                    help=('Send mail to, comma-separated ' +
                          '(default: pavel.kiselev@best4ad.com,nataly.lisitsyna@best4ad.com)'))
parser.add_argument('--send_mail_from', type=str, default='do-not-reply@inventale.com',
                    help='Mail address send_from (default: do-not-reply@inventale.com)')
parser.add_argument('--mail_password', type=str, default='AKIAJHBVE2GQUQBRSQVA',
                    help='Password for mail_from address (default: AKIAJHBVE2GQUQBRSQVA)')
parser.add_argument('--check_schema', type=bool, default=True,
                    help='Enable comparing schema (default: True)')
parser.add_argument('--quick_fall', type=bool, default=False,
                    help='Fail after first error (default: False)')
parser.add_argument('--mode', type=str, default='day-sum',
                    help='Type of checking (default: day-sum)')
parser.add_argument('--schema_columns', type=str, default='day-sum',
                    help='Type of checking (default: day-sum)')
parser.add_argument('--logging_level', type=str, default='INFO',
                    help='''set level of displaying messages.\n
                    CRITICAL, ERROR, WARNING, INFO, DEBUG\nDefault value: INFO))''')

args = parser.parse_args()
logger = logging_helper.Logger(args.logging_level)
check_schema = args.check_schema
quick_fall = args.quick_fall
mode = args.mode


def get_os():
    if "Win" in platform.system():
        return "Windows"
    else:
        return "Linux"


def get_config(os_name):
    if "Linux" in os_name:
        property_file = os.getcwd() + "/resources/properties/sqlComparator.properties"
    else:
        property_file = os.getcwd() + "\\resources\\properties\\sqlComparator.properties"
    return configHelper.IfmsConfigCommon(property_file)


def get_client_config(os_name, client_name):
    if "Linux" in os_name:
        property_file = os.getcwd() + "/resources/properties/sqlComparator.properties"
    else:
        property_file = os.getcwd() + "\\resources\\properties\\sqlComparator.properties"
    return configHelper.IfmsConfigClient(property_file, client_name)


def check_service_dir(dir_name):
    if os.path.exists(dir_name):
        shutil.rmtree(dir_name)
    os.mkdir(dir_name)


def create_test_dir(path, client_name):
    if not os.path.exists(path):
        os.mkdir(path)
    if not os.path.exists(path + client_name):
        os.mkdir(path + client_name)


def generate_mail_text(comparing_results, comparing_mode):
    text = "Initial conditions:\n\n"
    if check_schema:
        text = text + "1. Schema checking enabled.\n"
    else:
        text = text + "1. Schema checking disabled.\n"
    if quick_fall:
        text = text + "2. Failed with first founded error.\n"
    else:
        text = text + "2. Find all errors\n"
        text = text + "3. Report checkType is " + comparing_mode + "\n\n"
    if any([comparing_results.empty, comparing_results.diff_data, comparing_results.no_crossed_tables,
            comparing_results.prod_uniq_tables, comparing_results.test_uniq_tables]):
        text = get_test_result_text(text, comparing_results)
    else:
        text = text + "It is impossible! There is no any problems founded!"
    if check_schema:
        text = text + "Schema checked in " + str(schema_comparing_time) + "\n"
    text = text + "Dbs checked in " + str(data_comparing_time) + "\n"
    return text


def sendmail(mail_body, fromaddr, toaddr, mypass, mail_subject, files):
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    if type(toaddr) is list:
        msg['To'] = ', '.join(toaddr)
    else:
        msg['To'] = toaddr
    msg['Subject'] = mail_subject
    msg.attach(MIMEText(mail_body, 'plain'))
    if files is not None:
        for attachFile in files.split(','):
            if os.path.exists(attachFile) and os.path.isfile(attachFile):
                with open(attachFile, 'rb') as file:
                    part = MIMEApplication(file.read(), Name=basename(attachFile))
                part['Content-Disposition'] = 'attachment; filename="%s"' % basename(attachFile)
                msg.attach(part)
            else:
                if attachFile.lstrip() != "":
                    logger.error("File not found {}".format(attachFile))
                    print(str(datetime.datetime.now()) + " [ERROR] File not found " + attachFile)
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    try:
        server.login(fromaddr, mypass)
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
        server.quit()
    except smtplib.SMTPAuthenticationError:
        print('Raised authentication error!')


def get_test_result_text(text, comparing_results):
    text = text + "There are some problems found during checking.\n\n"
    if comparing_results.empty:
        text = text + "Tables, empty in both dbs:\n" + ",".join(comparing_results.empty) + "\n\n"
    if comparing_results.prod_empty:
        text = text + "Tables, empty on production db:\n" + ",".join(comparing_results.prod_empty) + "\n\n"
    if comparing_results.test_empty:
        text = text + "Tables, empty on test db:\n" + ",".join(comparing_results.test_empty) + "\n\n"
    if comparing_results.diff_data:
        text = text + "Tables, which have any difference:\n" + ",".join(comparing_results.diff_data) + "\n\n"
    if list(set(comparing_results.empty).difference(set(comparing_results.no_crossed_tables))):
        text = text + "Report tables, which have no crossing dates:\n" + ",".join(
            list(set(comparing_results.empty).difference(set(comparing_results.no_crossed_tables)))) + "\n\n"
    if comparing_results.get_uniq_tables("prod"):
        text = text + "Tables, which unique for production db:\n" + ",".join(
            converters.convert_to_list(comparing_results.prod_uniq_tables)) + "\n\n"
    if comparing_results.get_uniq_tables("test"):
        text = text + "Tables, which unique for test db:\n" + ",".join(
            converters.convert_to_list(comparing_results.test_uniq_tables)) + "\n\n"
    return text


os_type = get_os()
config = get_config(os_type)
if os == "Windows":
    service_dir = "C:\\comparator"
else:
    service_dir = "/tmp/comparator/"
check_service_dir(service_dir)
for client in config.get_clients():
    client_config = get_client_config(os_type, client)
    sql_connection_properties = {
        'prod': client_config.get_sql_connection_params('prod'),
        'test': client_config.get_sql_connection_params('test')
    }
    sql_comparing_properties = {
        'comparing_step': client_config.get_property('sqlProperties', 'comparing_step'),
        'check_schema': check_schema,
        'fail_with_first_error': quick_fall,
        'send_mail_to': args.send_mail_to,
        'mode': mode,
        'excluded_tables': client_config.get_property('sqlProperties', 'tables_not_to_compare'),
        'hide_columns': client_config.get_property('sqlProperties', 'hide_columns'),
        'strings_amount': client_config.get_property('sqlProperties', 'strings_amount'),
        'logger': logger,
        'depth_report_check': config.get_property('sqlProperties', 'depth_report_check'),
        'schema_columns': config.get_property('sqlProperties', 'schema_columns'),
        'retry_attempts': config.get_property('sqlProperties', 'retry_attempts'),
        'only_tables': config.get_property('sqlProperties', 'separate_checking'),
        'reports': config.get_property('sqlProperties', 'reports'),
        'table_timeout': config.get_property('sqlProperties', 'table_timeout'),
        'os': os_type


        # TODO: support other parameters
    }
    comparing_info = tableData.Info(logger)
    comparing_info.update_table_list("prod",
                                     dbcmp_sql_helper.DbCmpSqlHelper(client_config.get_sql_connection_params("prod"),
                                                                     logger).get_tables())
    comparing_info.update_table_list("test",
                                     dbcmp_sql_helper.DbCmpSqlHelper(client_config.get_sql_connection_params("test"),
                                                                     logger).get_tables())
    global_break = False
    if "Linux" in os_type:
        create_test_dir("/mxf/data/test_results/", client)
    else:
        create_test_dir("C:\\dbComparator\\", client)
    start_time = datetime.datetime.now()
    logger.info("Start {} processing!".format(client))
    prod_sql_connection = dbcmp_sql_helper.DbCmpSqlHelper(client_config.get_sql_connection_params("prod"), logger)
    mapping = queryConstructor.prepare_column_mapping(prod_sql_connection, logger)
    excluded_tables = sql_comparing_properties.get('excluded_tables')
    client_ignored_tables = sql_comparing_properties.get('client_ignored_tables')
    reports = sql_comparing_properties.get('reports')
    entities = sql_comparing_properties.get('entities')
    tables = comparing_info.define_table_list(excluded_tables, client_ignored_tables, reports, entities,
                                              prod_sql_connection)
    if check_schema:
        # TODO: Object fixed, now this code not works
        schema_comparing_time = sqlComparing.Object(sql_connection_properties, sql_comparing_properties,
                                                    comparing_info).compare_metadata(start_time, tables)
    else:
        logger.info("Schema checking disabled...")
    # TODO: Object fixed, now this code not works
    data_comparing_time = sqlComparing.Object(sql_connection_properties, sql_comparing_properties,
                                              comparing_info).compare_data(start_time, service_dir, mapping, tables)
    subject = "[Test] Check databases for client {}".format(client)
    body = generate_mail_text(comparing_info, mode)
    sendmail(body, args.send_mail_from, args.send_mail_to, args.mail_password, subject, None)
