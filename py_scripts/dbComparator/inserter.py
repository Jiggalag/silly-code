from datetime import timedelta, date

from py_scripts.helpers.dbHelper import DbConnector
from py_scripts.helpers.logging_helper import Logger


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def generate_queries():
    date_list = list()
    start_date = date(2007, 1, 1)
    end_date = date(2018, 6, 29)
    for single_date in daterange(start_date, end_date):
        date_list.append(single_date.strftime("%Y-%m-%d"))
    return date_list


connection_params = {
    'host': 'samaradb03.maxifier.com',
    'user': 'itest',
    'password': 'ohk9aeVahpiz1wi',
    'db': 'rick_pk'
}

logger = Logger('DEBUG')


dates = generate_queries()
db_connection = DbConnector(connection_params, logger).get_connection()
query = "INSERT INTO rickplacereport VALUES(%s, 777, 200, 100);"
with db_connection.cursor() as cursor:
    cursor.executemany(query, dates)
    db_connection.commit()
