import time

import requests


def run(id, store):
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
                "timestamp": int(time.time()) * 1000,
                "polygon": "NJ"
            }],
        "userKey": {
            "deviceId": f"testJMETER-{id}",
            "userId": f"nastya_JMETER-{id}"
        }
    }
    headers = {
        "Content-Type": "application/json",
        "x-api-key": "DEF5LczCd93L22yvxWM9a3S7RNHhQGt9BrrQymy5"
    }
    result = requests.post(url, json=data, headers=headers)
    store.update({})
    return result


store = dict()

run(1, store)
print("stop")
