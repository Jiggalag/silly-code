import time

import selenium.common
from selenium import webdriver

client = 'rick'
scope = 'default'

drv = '/home/jiggalag/Downloads/chromedriver'

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

time.sleep(5)

mon = driver.find_element_by_link_text('MONITORING')
mon.click()

download = driver.find_element_by_class_name('download_figure')
download.click()

print('ok')
