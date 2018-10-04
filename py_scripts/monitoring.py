import os
import sys
import time

import pandas
import selenium.common
from selenium import webdriver

from py_scripts.helpers.dbHelper import DbConnector
from py_scripts.helpers.logging_helper import Logger

logger = Logger('DEBUG')

target_dir = '/home/jiggalag/Downloads/'

client = 'rick'
scope = 'default'

drv = '/home/jiggalag/Downloads/chromedriver'

conn_dict = {
    'host': 'samaradb03.maxifier.com',
    'user': 'itest',
    'password': 'ohk9aeVahpiz1wi',
    'db': 'rick'
}

db_point = DbConnector(conn_dict, logger).get_connection()


def load_csv():
    driver = webdriver.Chrome(drv)
    try:
        driver.maximize_window()
    except selenium.common.exceptions.WebDriverException:
        pass
    driver.get("https://dev01.inventale.com")

    login = driver.find_element_by_id('login_form_loginActionForm_login')
    password = driver.find_element_by_id('login_form_loginActionForm_password')
    login.send_keys('pavel.kiselev')
    password.send_keys('PoKuTe713')
    login_button = driver.find_element_by_id('login_form_')
    login_button.click()

    driver.get('https://dev01.inventale.com/changeClient.action?clientScope={}.{}'.format(client, scope))
    js = driver.find_element_by_link_text('IFMS 1.0.109')
    js.click()

    time.sleep(3)

    mon = driver.find_element_by_link_text('MONITORING')
    mon.click()

    time.sleep(3)

    download = driver.find_element_by_class_name('monitoring-search__download-icon')
    download.click()
    time.sleep(5)


def get_filename(directory):
    for item in os.listdir(directory):
        if ('monitoring' in item) and (item[-4:] == 'xlsx'):
            return item
    logger.error('There is no any monitoring files in {}'.format(directory))
    return None


def drop_old_files(directory):
    files = list()
    for item in os.listdir(directory):
        if ('monitoring' in item) and ('xlsx' in item):
            files.append(item)
    for item in files:
        os.remove(directory + item)
        logger.info('File {} successfully deleted...'.format(directory + item))


def get_remoteids_from_db():
    query = ""


def wait_for_loading():
    # TODO: here I should implement logics waiting for loading monitoring*.xlsx file
    pass

# drop_old_files(target_dir)
# load_csv()
file = get_filename(target_dir)
if file is not None:
    content = pandas.read_excel(target_dir + file)
    tremoteids = list(content.icol(1))
    for column_name in tremoteids:
        if column_name == 'ID флайта':
            remoteids = set(tremoteids[tremoteids.index(column_name) + 1:])
else:
    logger.error('There is no any monitoring files')
    sys.exit(1)
print('ok')
