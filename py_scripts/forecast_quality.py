import datetime

import pymysql
from helpers.dbHelper import DbConnector
from helpers.ifmsApiHelper import IFMSApiHelper
from helpers.jsonHelper import JsonQuery
from helpers.loggingHelper import Logger


# TODO: 2. iterate over this dict and get forecast for each page
# TODO: 3. Write page-map to db
# TODO: 4. write results to db


def create_page_map(connection, logger):
    get_pages_query = 'SELECT * FROM page WHERE isDeleted=0 AND archived IS null'
    pages = list()
    try:
        with connection.cursor() as cursor:
            cursor.execute(get_pages_query)
            pages = cursor.fetchall()
    except pymysql.OperationalError:
        logger.error("There are some SQL query error " + str(pymysql.OperationalError))
    finally:
        try:
            connection.close()
        except pymysql.Error:
            logger.info("Connection already closed...")
    page_map = dict()
    for page in pages:
        page_map.update({page.get('remoteId'): page.get('name')})
    return page_map

def page_map_query_list(data_list):
    query_list = list()
    for item in data_list:
        query = "INSERT INTO page VALUES ({}, {});".format(item.key(), item.values())


def create_query_list(data, order):
    pass


sql_host = 'samaradb03.maxifier.com'
sql_user = 'itest'
sql_password = 'ohk9aeVahpiz1wi'
sql_db = 'rick'
logger = Logger('INFO')

server = 'ifms3.inventale.com'
user = 'qa-rick'
password = '2c821c5131319f3b9cfa83885d552637'
context = 'ifms'
client = 'rick'
scope = 'default'

connection_params = {
    'host': sql_host,
    'user': sql_user,
    'password': sql_password,
    'db': sql_db
}

api_point = IFMSApiHelper(server, user, password, context, logger)
connection = DbConnector(connection_params, logger).get_connection()

page_map = create_page_map(connection, logger)
# connection.execute_many(query_list)

query_list = page_map_query_list(page_map)

# TODO: insert page map into database

result_list = list()

for page in page_map.keys():
    today_date = datetime.datetime.date(datetime.datetime.now())
    start_date = str(today_date) + 'T21:00:00.000Z'
    end_date = str(today_date + datetime.timedelta(days=7)) + 'T20:59:59.000Z'
    priority = 12
    query = JsonQuery(start_date, end_date, priority, page=page)
    cookies = api_point.change_scope(client, scope)
    result = api_point.check_available_inventory(query, cookies)
    result_list.append(result)

# TODO: insert results into rick_forecast table

print('Debug ok...')
