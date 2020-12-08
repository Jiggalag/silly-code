import argparse
import datetime
import logging

from helpers.dbHelper import DbConnector
from helpers.octopus import OctopusApi


def prepare_arguments():
    parser = argparse.ArgumentParser(description='Utility intended to comparing databases')
    parser.add_argument('--server', type=str, default='inv-dev-02.inventale.com',
                        help='Host, where octopus located is (default: inv-dev-02.inventale.com)')
    parser.add_argument('--user', type=str, default='pavel.kiselev',
                        help='Auth user (default: pavel.kiselev)')
    parser.add_argument('--password', type=str, default='6561bf7aacf5e58c6e03d6badcf13831',
                        help='Password hash for user')
    parser.add_argument('--context', type=str, default='ifms5',
                        help='Auth user (default: ifms5)')
    parser.add_argument('--sql_host', type=str, default='eu-db-01.inventale.com',
                        help='SQL host, where octopus-db located (default: eu-db-01.inventale.com)')
    parser.add_argument('--sql_user', type=str, default='itest',
                        help='SQL user (default: itest)')
    parser.add_argument('--sql_password', type=str, default='ohk9aeVahpiz1wi',
                        help='SQL password (default: ohk9aeVahpiz1wi)')
    parser.add_argument('--sql_db', type=str, default='octopus', help='SQL db (default: octopus)')
    parser.add_argument('-d', '--debug', help="Print lots of debugging statements", action="store_const",
                        dest="loglevel", const=logging.DEBUG, default=logging.INFO)
    parser.add_argument('-v', '--verbose', help="Be verbose", action="store_const", dest="loglevel",
                        const=logging.INFO)
    return parser.parse_args()


args = prepare_arguments()
server = args.server
user = args.user
password = args.password
context = args.context

sql_host = args.sql_host
sql_user = args.sql_user
sql_password = args.sql_password
sql_db = args.sql_db

logger = logging.getLogger("tmp_octopus_test")
logger.setLevel(level=args.loglevel)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)
point = OctopusApi(server, context, user, password, logger)
connect_params = {
    'host': sql_host,
    'user': sql_user,
    'password': sql_password,
    'db': sql_db
}
sqlpoint = DbConnector(connect_params, logger)

startdate = datetime.datetime.now().date()
enddate = startdate + datetime.timedelta(days=8)

data = {
    'name': 'PKTEST',
    'updatedAt': '2019-07-24T06:13:03',
    'startDate': '{}T00:00:00'.format(startdate),
    'endDate': '{}T23:59:59'.format(enddate),
    'targeting': {
        'inventory': {
            'targets': [
                {
                    'targetValue': 10014459,
                    'exclude': False,
                    'targetLevel': 2
                }
            ]
        },
        'adSize': {
            'targets': [
                {
                    'targetValue': 74,
                    'exclude': False,
                    'targetLevel': 1
                },
                {
                    'targetValue': 125,
                    'exclude': False,
                    'targetLevel': 1
                },
                {
                    'targetValue': 126,
                    'exclude': False,
                    'targetLevel': 1
                }
            ]
        }},
    'description': '',
    'adjustmentValue': 788900,
    'accountId': 79505,
    'id': 1,
    'status': 7,
    'createdAt': '2019-08-01T06:13:03',
    'adjustmentType': 2,
    'timeZone': 'GMT'
}

update = {
    "name": 'Name1-pktest',
    'updatedAt': '2019-07-24T06:13:03',
    'startDate': '{}T00:00:00'.format(startdate),
    'endDate': '{}T23:59:59'.format(enddate),
    'targeting': {
        'adSize': {
            'targets': [{
                'targetValue': 10014459,
                'exclude': False,
                'targetLevel': 2}]
        }},
    'description': '',
    'adjustmentValue': -50,
    'accountId': 79505,
    'id': 1,
    'status': 7,
    'createdAt': '2019-08-01T06:13:03',
    'adjustmentType': 1,
    'timeZone': 'GMT'
}

patch = {
    "name": 'PatchedName'
}

get_adj = point.list_adjustments()
create_adj = point.create_adjustment(data)
after_create = point.list_adjustments()
update_adj = point.update_adjustment(1, update)
after_update = point.list_adjustments()
patch_adj = point.patch_adjustment(1, patch)
after_patch = point.list_adjustments()
get_by_id = point.get_adjustment_by_id(8)
point.delete_adjustment(8)
after_delete = point.list_adjustments()
