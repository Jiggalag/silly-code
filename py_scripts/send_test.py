from py_scripts.helpers.ifmsApiHelper import IFMSApiHelper
from py_scripts.helpers.logging_helper import Logger
import json

server = 'dev01.inventale.com'
user = 'pavel.kiselev'
password = '6561bf7aacf5e58c6e03d6badcf13831'
context = 'ifms'
client = 'rick'
scope = 'default'
request = 'frc.json'
logger = Logger('INFO')

api_point = IFMSApiHelper(server, user, password, context, logger)

cookie = api_point.change_scope(client, scope)

result = api_point.check_available_inventory(request, cookie).text
json_result = json.loads(result)
print(json_result.get('dbgInfo'))
print(json_result.get('summary'))
frc = json_result.get('bySite')
