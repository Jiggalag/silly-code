import argparse
import datetime
import logging

from helpers.dbHelper import DbConnector
from helpers.octopus import OctopusApi


def prepare_arguments():
    parser = argparse.ArgumentParser(description='Utility intended to comparing databases')
    parser.add_argument('--server', type=str, default='inv-dev-02.inventale.com',
                        help='Host, where octopus located is (default: inv-dev-02.inventale.com)')
    parser.add_argument('--duration', type=int, default=15, help='Duration of created adjustments')
    parser.add_argument('--amount', type=int, default=30, help='Amount of created adjustments')
    parser.add_argument('--user', type=str, default='pavel.kiselev',
                        help='Auth user (default: pavel.kiselev)')
    parser.add_argument('--password', type=str, default='6561bf7aacf5e58c6e03d6badcf13831',
                        help='Password hash for user')
    parser.add_argument('--context', type=str, default='ifms5',
                        help='Auth user (default: ifms5)')
    parser.add_argument('--octopus_host', type=str, default='eu-db-01.inventale.com',
                        help='SQL host, where octopus-db located (default: eu-db-01.inventale.com)')
    parser.add_argument('--octopus_user', type=str, default='itest',
                        help='SQL user (default: itest)')
    parser.add_argument('--octopus_password', type=str, default='ohk9aeVahpiz1wi',
                        help='SQL password (default: ohk9aeVahpiz1wi)')
    parser.add_argument('--octopus_db', type=str, default='octopus', help='SQL db (default: octopus)')
    parser.add_argument('--client_host', type=str, default='eu-db-01.inventale.com',
                        help='SQL host, where octopus-db located (default: eu-db-01.inventale.com)')
    parser.add_argument('--client_user', type=str, default='itest',
                        help='SQL user (default: itest)')
    parser.add_argument('--client_password', type=str, default='ohk9aeVahpiz1wi',
                        help='SQL password (default: ohk9aeVahpiz1wi)')
    parser.add_argument('--client_db', type=str, default='pitt_cpopro_79505', help='Client sql db')
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

octopus_host = args.octopus_host
octopus_user = args.octopus_user
octopus_password = args.octopus_password
octopus_db = args.octopus_db

client_host = args.client_host
client_user = args.client_user
client_password = args.client_password
client_db = args.client_db

duration = args.duration
adjustment_amount = args.amount

logger = logging.getLogger("yakubovich_adjustments")
logger.setLevel(level=args.loglevel)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)
octopus_api = OctopusApi(server, context, user, password, logger)
octopus_connect_params = {
    'host': octopus_host,
    'user': octopus_user,
    'password': octopus_password,
    'db': octopus_db
}

client_connect_params = {
    'host': client_host,
    'user': client_user,
    'password': client_password,
    'db': client_db
}

octopus_sqlpoint = DbConnector(octopus_connect_params, logger)
client_sqlpoint = DbConnector(client_connect_params, logger)

get_pages_query = "SELECT remoteid FROM page WHERE adsdeleted IS NULL AND archived IS NULL;"
page_list = client_sqlpoint.select(get_pages_query)

startdate = datetime.datetime.now().date()
enddate = startdate + datetime.timedelta(days=duration)

for tick in range(adjustment_amount):
    page = page_list.pop()
    raw_adjustment = {
        'name': 'TestAdjustment-{}'.format(page),
        'updatedAt': '2019-07-24T06:13:03',
        'startDate': '{}T00:00:00'.format(startdate),
        'endDate': '{}T23:59:59'.format(enddate),
        'targeting': {
            'inventory': {
                'targets': [{
                    'targetValue': int(page),
                    'exclude': False,
                    'targetLevel': 2}]
            }},
        'description': '',
        'adjustmentValue': int(page),
        'accountId': 79505,
        'id': int(page),
        'status': 7,
        'createdAt': '2019-08-01T06:13:03',
        'adjustmentType': 2,
        'timeZone': 'GMT'
    }
    create_adj = octopus_api.create_adjustment(raw_adjustment)

current_list = octopus_api.list_adjustments()
