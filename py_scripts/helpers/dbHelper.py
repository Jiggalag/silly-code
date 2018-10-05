import pymysql
import time
from multiprocessing.dummy import Pool


class DbConnector:
    def __init__(self, connect_parameters, logger, attempts=5, timeout=10):
        self.host = connect_parameters.get('host')
        self.user = connect_parameters.get('user')
        self.password = connect_parameters.get('password')
        self.db = connect_parameters.get('db')
        self.logger = logger
        self.attempts = attempts
        self.timeout = timeout
        self.connection = self.get_connection()

    def get_connection(self):
        attempt_number = 0
        while True:
            try:
                connection = pymysql.connect(host=self.host,
                                             user=self.user,
                                             password=self.password,
                                             db=self.db,
                                             charset='utf8',
                                             cursorclass=pymysql.cursors.DictCursor)
                return connection
            except pymysql.err.OperationalError:
                attempt_number += 1
                if attempt_number > self.attempts:
                    return None
                time.sleep(self.timeout)

    def get_tables(self):
        connection = self.get_connection()
        if connection is not None:
            try:
                with connection.cursor() as cursor:
                    show_tables = "SELECT DISTINCT(table_name) FROM information_schema.columns " \
                                  "WHERE table_schema LIKE '{}';".format(self.db)
                    list_table = []
                    cursor.execute(show_tables)
                    result = cursor.fetchall()
                    for item in result:
                        list_table.append(item.get('table_name'))
            finally:
                connection.close()
            return list_table
        else:
            return None

    def select(self, query):
        connection = self.get_connection()
        if connection is not None:
            error_count = 0
            while error_count < self.attempts:
                try:
                    with connection.cursor() as cursor:
                        sql_query = query.replace('DBNAME', self.db)
                        self.logger.debug(sql_query)
                        try:
                            cursor.execute(sql_query)
                        except pymysql.err.InternalError as e:
                            self.logger.error('Error code: {}, error message: {}'.format(e.args[0], e.args[1]))
                            return None
                        result = cursor.fetchall()
                        return result
                except pymysql.OperationalError:
                    error_count += 1
                    self.logger.error("There are some SQL query error " + str(pymysql.OperationalError))
                finally:
                    try:
                        connection.close()
                    except pymysql.Error:
                        self.logger.info("Connection already closed...")
                        return []
        else:
            return None

    @staticmethod
    def parallel_select(connection_list, query):
        pool = Pool(2)  # TODO: remove hardcode, change to dynamically defining amount of threads
        result = pool.map((lambda x: x.select(query)), connection_list)  # TODO: add try/catch
        pool.close()
        pool.join()
        return result
