import sys
import pandas
import os
import time
from py_scripts.helpers.logging_helper import Logger

import selenium.common
from selenium import webdriver

logger = Logger('DEBUG')

target_dir = '/home/jiggalag/Downloads/'

client = 'rick'
scope = 'default'

drv = '/home/jiggalag/Downloads/chromedriver'

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


# drop_old_files(target_dir)
# load_csv()
file = get_filename(target_dir)
if file is not None:
    content = pandas.read_excel(target_dir + file)
    tremoteids = list(content.icol(1))
    for item in tremoteids:
        if item == 'ID флайта':
            remoteids = set(tremoteids[tremoteids.index(item) + 1:])
else:
    logger.error('There is no any monitoring files')
    sys.exit(1)
print('ok')