import json
from helpers import configHelper, ifmsApiHelper, helper
client = 'rick'

propertyFile = './resources/properties/checkForecastViaAPI.properties'
config = configHelper.ifmsConfigClient(propertyFile, client)
server = config.getProperty('servers', client + '.host')
user = config.getProperty('servers', client + '.user')
password = config.getProperty('servers', client + '.password')
context = config.getPropertyFromMainSection('context')

resultFileName = '/home/jiggalag/loadRick1.log'

i = 10
times = []

while i < 20:
    jsonName = './resources/jsons/prodJsons/rick/forecastQuery{}.json'.format(i)
    helper.changeJsonDates(jsonName, -1)  # -1 to set startDate on today date
    cookie = ifmsApiHelper.apiAuthenticate(server, user, password)
    forecastResult = json.loads(ifmsApiHelper.checkAvailableInventory(server, jsonName, context, cookie))
    forecastTime = forecastResult.get('dbgInfo').get('requestTime')
    with open(resultFileName, 'a') as file:
        file.write('File {} forecasted in {}\n'.format(jsonName, forecastTime))
    i += 1