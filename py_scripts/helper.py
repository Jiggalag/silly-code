import json
import requests
import sys
import time
import datetime


def apiAuthenticate(server, user, password):
    loginUrl = 'https://%s/login.action' % server
    data = {'loginActionForm.login': user, 'loginActionForm.password': password}
    loginResponse = requests.post(url=loginUrl, data=data, allow_redirects=False)
    cookies = loginResponse.cookies
    return cookies


def changeJsonDates(jsonFile, N):
    initialJson = json.loads(open(jsonFile, 'r').read())
    startDate = initialJson.get('startDate')
    startTime = startDate[startDate.rfind('T'):]
    endDate = initialJson.get('endDate')
    endTime = endDate[endDate.rfind('T'):]
    if (startDate or endDate) is None:
        print(str(datetime.datetime.now()) + " [INFO] Json " + jsonFile + " have problems with dates!\n")
        sys.exit()
    else:
        delta = datetime.datetime.strptime(endDate[endDate.rfind('T') - 10:endDate.rfind('T')], "%Y-%m-%d") - datetime.datetime.strptime(startDate[startDate.rfind('T') - 10:startDate.rfind('T')], "%Y-%m-%d")  # days between start and end date
        newDate = datetime.datetime.now() + datetime.timedelta(days=N)  # Return tomorrow date with cutting time
        newEndDate = newDate + delta
    initialJson['startDate'] = str(datetime.datetime.date(newDate)) + startTime
    initialJson['endDate'] = str(datetime.datetime.date(newEndDate)) + endTime
    writeToJson(jsonFile, initialJson)
    replaceQuotes(jsonFile)


def getResponseText(cookies, headers, jsonFile, loaded, getAvailableForecastUrl):
    while True:
        counter = 0
        response = requests.post(getAvailableForecastUrl, json=loaded, cookies=cookies, headers=headers)
        if '502' in response.text:
            if counter < 5:
                print(str(datetime.datetime.now()) + " [WARN] Check host, you've got 502 bad gateway error")
                response = requests.post(getAvailableForecastUrl, json=loaded, cookies=cookies, headers=headers)
                counter += 1
                time.sleep(3)
            else:
                print(str(datetime.datetime.now()) + " [ERROR] Bad gateway, we waste all 5 attempts.")
        if 'matched' in response.text:
            return response.text
        elif 'Unexpected' in response.text:
            print(str(datetime.datetime.now()) + " [ERROR] Check json" + '\t' + jsonFile + ". Probably json have syntax problem. " + response.text)
            sys.exit(1)
        elif 'possible' in response.text:
            print(str(datetime.datetime.now()) + " [ERROR] Check json" + '\t' + jsonFile + ". Probably json have date format problem. " + response.text)
            sys.exit(1)
        elif 'BEGIN_ARRAY' in response.text:
            print(str(datetime.datetime.now()) + " [ERROR] Check json" + '\t' + jsonFile + ". Probably json have data structure problem. " + response.text)
            sys.exit(1)
        elif 'Pages not found:' in response.text:
            print(str(datetime.datetime.now()) + " [ERROR] Check json" + '\t' + jsonFile + ". Probably you set incorrect page id. " + response.text)
            sys.exit(1)
        elif 'Last simulation cache version has changed' in response.text:
            print(str(datetime.datetime.now()) + ' ' + response.text + '\nProbably, future samples are generating now')
            sys.exit(1)
        elif 'not found:' in response.text:
            print(str(datetime.datetime.now()) + ' ' + response.text + '\nProbably, some entities in json cannot be found')
            sys.exit(1)
        else:
            progressPercent = json.loads(response.text)
            print(str(datetime.datetime.now()) + " [INFO] Forecasting", jsonFile, "Progress percent is", progressPercent.get('progressPercent'))
            time.sleep(1)


def replaceQuotes(jsonFile):
    with open(jsonFile, 'r') as file:
        text = file.read()
    with open(jsonFile, 'w') as file:
        file.write(text.replace('\'', '\"'))


def runForecast(server, jsonFile, context, user, password):
    cookies = apiAuthenticate(server, user, password)
    headers = {'Content-Type': 'application/json'}
    loaded = json.load(open(jsonFile))
    getAvailableForecastUrl = 'https://%s/%s/api/v1/checkAvailableInventory' % (server, context)
    responseText = getResponseText(cookies, headers, jsonFile, loaded, getAvailableForecastUrl)
    print(str(datetime.datetime.now()) + ' ' + jsonFile + " forecasted in " + str(json.loads(responseText).get('dbgInfo').get('requestTime')) + " seconds")
    forecastResult = json.loads(responseText)
    return forecastResult


def writeToJson(jsonFile, initialJson):
    with open(jsonFile, 'w') as file:
        file.write('{\n')
        keys = list(initialJson.keys())
        for i in ['startDate', 'endDate', 'priority', 'weight', 'pacing', 'fcType']:
            if initialJson.get(i) is None:
                line = ""
            else:
                line = '"%s": "%s",\n' % (i, initialJson.get(i))
            if i in keys:
                keys.remove(i)
            file.write(line)
        for i in keys:
            if i is keys[len(keys) - 1]:
                if initialJson.get(i) is None:
                    if i in ['positions', 'keyvalueTargeting', 'frequencyTargetings', 'usedTemplates', 'trafficAllocation', 'products', 'dimensions']:
                        line = '"%s": []\n' % i
                    else:
                        line = '"%s": {}\n' % i
                else:
                    line = '"%s": %s\n' % (i, initialJson.get(i))
                file.write(line)
            else:
                if initialJson.get(i) is None:
                    if i in ['positions', 'keyvalueTargeting', 'frequencyTargetings', 'usedTemplates', 'trafficAllocation', 'products', 'dimensions']:
                        line = '"%s": [],\n' % i
                    else:
                        line = '"%s": {},\n' % i
                else:
                    line = '"%s": %s,\n' % (i, initialJson.get(i))
                file.write(line)
        file.write('}\n')