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
from py_scripts.helpers import configHelper, converters, logging_helper, dbHelper
from py_scripts.helpers.dbHelper import DbConnector
from py_scripts.dbComparator import tableData, sqlComparing
from py_scripts.dbComparator.queryConstructor import InitializeQuery


if "Win" in platform.system():
    OS = "Windows"
else:
    OS = "Linux"
if "Linux" in OS:
    propertyFile = os.getcwd() + "/resources/properties/sqlComparator.properties"
else:
    propertyFile = os.getcwd() + "\\resources\\properties\\sqlComparator.properties"
config = configHelper.IfmsConfigCommon(propertyFile)

logger = logging_helper.Logger(config.getPropertyFromMainSection("loggingLevel"))

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

def sendmail(body, fromaddr, toaddr, mypass, subject, files):
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    if type(toaddr) is list:
        msg['To'] = ', '.join(toaddr)
    else:
        msg['To'] = toaddr
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    if files is not None:
        for attachFile in files.split(','):
            if(os.path.exists(attachFile) and os.path.isfile(attachFile)):
                with open(attachFile, 'rb') as file:
                    part = MIMEApplication(file.read(), Name=basename(attachFile))
                part['Content-Disposition'] = 'attachment; filename="%s"' % basename(attachFile)
                msg.attach(part)
            else:
                if (attachFile.lstrip() != ""):
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
            converters.convertToList(comparing_results.prod_uniq_tables)) + "\n\n"
    if comparing_results.get_uniq_tables("test"):
        text = text + "Tables, which unique for test db:\n" + ",".join(
            converters.convertToList(comparing_results.test_uniq_tables)) + "\n\n"
    return text


if OS == "Windows":
    service_dir = "C:\\comparator"
else:
    service_dir = "/tmp/comparator/"
check_service_dir(service_dir)
for client in config.getClients():
    client_config = configHelper.IfmsConfigClient(propertyFile, client)
    sql_property_dict = client_config.get_sql_connection_params('test')
    comparing_info = tableData.Info(logger)
    comparing_info.update_table_list("prod",
                                     dbHelper.DbConnector(client_config.get_sql_connection_params("prod"),
                                                          logger).get_tables())
    comparing_info.update_table_list("test",
                                     dbHelper.DbConnector(client_config.get_sql_connection_params("test"),
                                                          logger).get_tables())
    global_break = False
    if "Linux" in OS:
        create_test_dir("/mxf/data/test_results/", client)
    else:
        create_test_dir("C:\\dbComparator\\", client)
    start_time = datetime.datetime.now()
    logger.info("Start {} processing!".format(client))
    prod = dbHelper.DbConnector(client_config.get_sql_connection_params("prod"), logger)
    mapping = InitializeQuery(DbConnector(prod, logger), logger).prepare_column_mapping()
    if check_schema:
        schema_comparing_time = sqlComparing.Object(client_config, comparing_info, client).compare_metadata(start_time)
    else:
        logger.info("Schema checking disabled...")
    data_comparing_time = sqlComparing.Object(client_config, comparing_info, client).compare_data(global_break,
                                                                                                  start_time,
                                                                                                  service_dir,
                                                                                                  mapping)
    subject = "[Test] Check databases for client {}".format(client)
    body = generate_mail_text(comparing_info, mode)
    sendmail(body, sendMailFrom, sendMailTo, mailPassword, subject, None)
