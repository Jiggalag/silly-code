from target_parser import TargetParser
from table_cloner import TableCloner
from py_scripts.helpers.dbHelper import DbConnector
from py_scripts.helpers.logging_helper import Logger


server = 'samaradb03.maxifier.com'
user = 'itest'
password = 'ohk9aeVahpiz1wi'
source_db = 'rick'
target_db = 'rick_quality'
logger = Logger('DEBUG')

connection_params = {
    'host': server,
    'user': user,
    'password': password,
    'db': None
}


connection = DbConnector(connection_params, logger).get_connection()
parser = TargetParser(connection, source_db, logger)
targeting = parser.get_targeting()
tables = list()
tt = TableCloner(connection, source_db, target_db, tables, logger)
print('debug...')
