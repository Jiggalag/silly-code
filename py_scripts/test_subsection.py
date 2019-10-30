from py_scripts.helpers.logging_helper import Logger
from py_scripts.helpers.ifms_forecast_helper import IFMSForecast

server = 'dev01.inventale.com'
user = 'pavel.kiselev'
password = '6561bf7aacf5e58c6e03d6badcf13831'
context = 'ifms'
client = 'rick'
scope = 'default'
request = 'frc.json'
logger = Logger('INFO')

forecast = IFMSForecast(server, user, password, context, client, scope, request)
print('OK!')