import datetime
import json
import sys
import threading
import time

import requests


class Logger:
    def __init__(self, logging_type, log_file=None):
        self.types = {
            'CRITICAL': 50,
            'ERROR': 40,
            'WARNING': 30,
            'INFO': 20,
            'DEBUG': 10
        }

        if logging_type not in self.types:
            logging_type.decode('utf8')
            print('Unregistered type of message: {}'.format(logging_type))
            sys.stdout.flush()
            self.type = None
        else:
            self.type = logging_type
        self.log_file = log_file

    def _msg(self, message, msgtype):
        if self.types.get(self.type) <= self.types.get(msgtype):
            print('{} [{}] {}'.format(str(datetime.datetime.now()), msgtype, message))
            sys.stdout.flush()
            if self.log_file is not None:
                with open(self.log_file, 'a') as file:
                    file.write('{} [{}] {}\n'.format(str(datetime.datetime.now()), msgtype, message))

    def critical(self, message):
        self._msg(message, 'CRITICAL')

    def error(self, message):
        self._msg(message, 'ERROR')

    def warn(self, message):
        self._msg(message, 'WARNING')

    def info(self, message):
        self._msg(message, 'INFO')

    def debug(self, message):
        self._msg(message, 'DEBUG')


class IFMSThread(threading.Thread):
    def __init__(self, requestid, store, logger):
        self.id = requestid
        self.logger = logger
        self.store = store
        threading.Thread.__init__(self)

    def run(self):
        # self.logger.info("Send request")
        send_time = datetime.datetime.now()
        url = "https://qaxp75.api.iqdata.ai/gps"
        data = {
            "systemInfo": {
                "sdkVersion": "string",
                "platform": "ANDROID"
            },
            "idType": "AAID",
            "rows": [
                {
                    "adId": "IT",
                    "accuracy": 22.33,
                    "altitude": 64.9000015258789,
                    "altitudeAccuracy": 2,
                    "latitudeE6": 40716935,
                    "longitudeE6": -74040464,
                    "provider": "fused",
                    "idType": "AAID",
                    "timestamp": int(time.time()),
                    "polygon": "NJ"
                }],
            "userKey": {
                "deviceId": f"testJMETER-{self.id}",
                "userId": f"nastya_JMETER-{self.id}"
            }
        }
        headers = {
            "Content-Type": "application/json",
            "x-api-key": "DEF5LczCd93L22yvxWM9a3S7RNHhQGt9BrrQymy5"
        }
        # self.logger.debug(f"Run request {self.id}")
        result = requests.post(url, json=data, headers=headers)
        # self.logger.debug(result.text)
        if result.status_code != 200:
            store.append(
                f"{str(send_time)},{str(datetime.datetime.now())},{str(datetime.datetime.now() - send_time)},{str(result.status_code)},{json.loads(result.text).get('debugInfo').get('message')}")
        else:
            store.append(
                f"{str(send_time)},{str(datetime.datetime.now())},{str(datetime.datetime.now() - send_time)},{str(result.status_code)},OK")
        # self.logger.debug(f"Result for request {self.id} stored")
        return result


logger = Logger('DEBUG')

local_timer = 10  # in milliseconds
count = 0

start_time = datetime.datetime.now()

store = ["start_time,response_time,process_time,code,response_text"]

for count in range(0, 1000):
    # print(count)
    curr = datetime.datetime.now()
    time.sleep(local_timer / 1000)
    # lag = datetime.datetime.now() - curr
    # while lag < datetime.timedelta(milliseconds=local_timer):
    #     time.sleep(local_timer / 1000)
    # print(f"lag equals {lag} ms")
    IFMSThread(count, store, logger).start()
# print('stop')
with open("/home/polter/loadresult.txt", "w") as file:
    for item in store:
        file.write(str(item) + "\n")
print("Successfully writed!")
