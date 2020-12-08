import json
import threading
import time
from random import randrange

from helpers.ifmsApiHelper import IFMSApiHelper
from helpers.logging_helper import Logger


class IFMSThread(threading.Thread):
    def __init__(self, api_point, json_file, cookies, logger):
        self.api_point = api_point
        self.json_file = json_file
        self.cookies = cookies
        self.logger = logger
        threading.Thread.__init__(self)

    def run(self):
        try:
            forecast_result = self.api_point.check_available_inventory(self.json_file, self.cookies).text
            request_time = json.loads(forecast_result).get('dbgInfo').get('requestTime')
            logger.info("Json {} forecasted in {} s...".format(self.json_file, request_time))
        except AttributeError:
            forecast_result = None
        return forecast_result


logger = Logger('DEBUG')

server = 'inv-dev-02.inventale.com'
user = 'pavel.kiselev'
password = '6561bf7aacf5e58c6e03d6badcf13831'
context = 'ifms5'
client = 'rick'
scope = 'default'

while True:
    api_point = IFMSApiHelper(server, user, password, context, logger)
    cookies = api_point.change_scope(client, scope)
    json_file = {
        "startDate": "2020-04-15T21:00:00.000Z",
        "endDate": "2020-06-13T20:{0:0>2}:{0:0>2}.{0:0>3}Z".format(randrange(60), format(randrange(60)),
                                                                   randrange(500)),
        "inventoryTargeting": {
            "scheduledPages": [6435, 6437]
        },
        "frequencyTargetings": [{"scope": "LIFETIME", "impsLimit": 10}],
        "dimensions": [
            "summary",
            "page",
            "campaign"
        ],
        "priority": 20,
    }
    IFMSThread(api_point, json_file, cookies, logger).start()
    time.sleep(0.01)
print("Successfully writed!")
