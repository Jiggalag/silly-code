# Script sends json-query to API. Script can be used for forecast perfomance testing.

import os.path

import helper

jsonName = 'forecastQuery1.json'
resultFile = 'timediff'
server = 'mock'
context = 'mock'
user = 'mock'
salt = 'mock'

helper.changeJsonDates(jsonName, 1)

while True:
    time = helper.runForecast(server, jsonName, context, user, salt).get('dbgInfo').get('requestTime')
    newFC = helper.changeJsonFC(jsonName)
    if not os.path.exists(resultFile):
        with open(resultFile, 'w'):
            print('Result file created...')
    with open(resultFile, 'w') as file:
        file.write(str(time) + "\t" + str(int(newFC) - 1))
