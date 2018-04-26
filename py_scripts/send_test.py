import helpers.ifmsApiHelper as ifmsApiHelper
import json

server = 'dev01.inventale.com'
user = 'pavel.kiselev'
password = '6561bf7aacf5e58c6e03d6badcf13831'
context = 'ifms'
client = 'rick'
scope = 'default'

request = 'frc.json'

cookie = ifmsApiHelper.change_scope(server, user, password, client, scope)

result = ifmsApiHelper.check_available_inventory(server, request, context, cookie)
print(json.loads(result.text).get('dbgInfo'))
print(json.loads(result.text).get('summary'))
frc = json.loads(result.text).get('bySite')
