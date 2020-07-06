import os

from helpers import loggingHelper
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from stem import CircStatus
from stem import Signal
from stem.control import Controller

logLevel = 20

logger = loggingHelper.Logger(logLevel)

amount = 5
ua = [
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.115 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:36.0) Gecko/20100101 Firefox/36.0",
    "Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; ASU2JS; rv:11.0) like Gecko",
    "Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "Mozilla/5.0 (compatible; Linux x86_64; Mail.RU_Bot/Fast/2.0; +http://go.mail.ru/help/robots)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586",
    "Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25",
    "Opera/9.80 (Windows NT 6.2; WOW64) Presto/2.12.388 Version/12.17",
    "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
    "Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.101 Safari/537.36 OPR/40.0.2308.62"
]

item = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.101 Safari/537.36 OPR/40.0.2308.62"

port = "8118"

service_args = [
    '--proxy=127.0.0.1:' + port,
    '--proxy-type=http',
    ]

def changeIp():
    with Controller.from_port(port=9051) as controller:
        controller.authenticate(password="pokute713")
        controller.signal(Signal.NEWNYM)

        for circ in controller.get_circuits():
            if circ.status != CircStatus.BUILT:
                continue
        exitFp, exitNickname = circ.path[-1]
        exitDesc = controller.get_network_status(exitFp, None)
        exitAddress = exitDesc.address if exitDesc else 'unknown'
        logger.info("Ip successfully changed. New address is {}".format(exitAddress))

binary = "/home/jiggalag/Downloads/tor-browser_ru/Browser/TorBrowser/Tor"
if os.path.exists(binary) is False:
    raise ValueError("The binary path to Tor firefox does not exist.")
dcap = dict(DesiredCapabilities.FIREFOX)

for i in range(amount):
    for item in ua:
        dcap["phantomjs.page.settings.userAgent"] = (item)
        driver = webdriver.PhantomJS(desired_capabilities=dcap, service_args=service_args)
        driver.set_window_size(1024, 768)
        changeIp()
        driver.get('http://inventale.com/en')
        driver.delete_all_cookies()
        driver.execute_script('localStorage.clear();')
        driver.close()
