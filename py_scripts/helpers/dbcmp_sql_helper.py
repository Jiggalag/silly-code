import pymysql
import time
from multiprocessing.dummy import Pool

TIMEOUT = 10


class DbCmpSqlHelper:
    def __init__(self, connect_parameters, logger, **kwargs):
        self.host = connect_parameters.get('host')
        self.user = connect_parameters.get('user')
        self.password = connect_parameters.get('password')
        self.logger = logger
        self.db = connect_parameters.get('db')
        self.hide_columns = [
            'archived',
            'addonFields',
            'hourOfDayS',
            'dayOfWeekS',
            'impCost',
            'id']
        self.attempts = 5
        self.mode = 'detailed'
        self.comparing_step = 10000
        self.excluded_tables = []
        self.client_ignored_tables = []
        self.check_schema = True
        self.check_depth = 7
        self.quick_fall = False
        self.separate_checking = "both"
        self.schema_columns = [
            "TABLE_CATALOG",
            "TABLE_NAME",
            "COLUMN_NAME",
            "ORDINAL_POSITION",
            "COLUMN_DEFAULT",
            "IS_NULLABLE",
            "DATA_TYPE",
            "CHARACTER_MAXIMUM_LENGTH",
            "CHARACTER_OCTET_LENGTH",
            "NUMERIC_PRECISION",
            "NUMERIC_SCALE",
            "DATETIME_PRECISION",
            "CHARACTER_SET_NAME",
            "COLLATION_NAME",
            "COLUMN_TYPE",
            "COLUMN_KEY",
            "EXTRA",
            "COLUMN_COMMENT",
            "GENERATION_EXPRESSION"
        ]
        for key in list(kwargs.keys()):
            if 'hideColumns' in key:
                self.hide_columns = kwargs.get(key)
            if 'attempts' in key:
                self.attempts = int(kwargs.get(key))
            if 'mode' in key:
                self.mode = kwargs.get(key)
            if 'comparingStep' in key:
                self.comparing_step = kwargs.get(key)
            if 'excludedTables' in key:
                self.excluded_tables = kwargs.get(key)  # TODO: add split?
            if 'clientIgnoredTables' in key:
                self.client_ignored_tables = kwargs.get(key)  # TODO: add split?
            if 'enableSchemaChecking' in key:
                self.check_schema = kwargs.get(key)
            if 'depthReportCheck' in key:
                self.check_depth = kwargs.get(key)
            if 'failWithFirstError' in key:
                self.quick_fall = kwargs.get(key)
            if 'schemaColumns' in key:
                self.schema_columns = kwargs.get(key)  # TODO: add split?
            if 'separateChecking' in key:
                self.separate_checking = kwargs.get(key)

    @staticmethod
    def parallel_select(connection_list, query, result_type="frozenset"):
        pool = Pool(2)
        result = pool.map((lambda x: x.select(query)), connection_list)
        pool.close()
        pool.join()
        if (result[0] is None) or (result[1] is None):
            return None, None
        if result_type == "list":
            prod_result = []
            test_result = []
            for item in result[0]:
                prod_result.append(list(item))
            for item in result[1]:
                test_result.append(list(item))
        else:
            prod_result = result[0]
            test_result = result[1]
        if len(prod_result) == 1:  # TODO: strongly refactor this code
            prod = prod_result[0]
        else:
            prod = prod_result
        if len(test_result) == 1:
            test = test_result[0]
        else:
            test = test_result
        return prod, test

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
                time.sleep(TIMEOUT)

    def get_comparable_objects(self, connection_list, query, result_type="frozenset"):
        result = self.parallel_select(connection_list, query, result_type)

    # TODO: refactor, make one select function from this two methods
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

    def get_column_list(self, table):
        connection = self.get_connection()
        if connection is not None:
            try:
                with connection.cursor() as cursor:
                    column_list = []
                    query = ("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s' " % table +
                             "AND table_schema = '%s';" % self.db)
                    self.logger.debug(query)
                    cursor.execute(query)
                    column_dict = cursor.fetchall()
                    for i in column_dict:
                        element = str(i.get('column_name')).lower()  # TODO: get rid of this hack
                        column_list.append(element)
                    for column in self.hide_columns:
                        if column.replace('_', '') in column_list:
                            column_list.remove(column)
                    if not column_list:
                        return ""
                    column_string = ','.join(column_list)
                    return column_string.lower().split(',')
            finally:
                if connection.open:
                    connection.close()
        else:
            return None


def get_amount_records(table, dates, sql_connection_list):
    if dates is None:
        query = "SELECT COUNT(*) FROM `{}`;".format(table)
    else:
        query = "SELECT COUNT(*) FROM `{}` WHERE dt >= '{}';".format(table, dates[0])
    return DbCmpSqlHelper.parallel_select(sql_connection_list, query)


def get_column_list_for_sum(set_column_list):
    column_list_with_sums = []
    for item in set_column_list.split(","):
        if "clicks" in item or "impressions" in item:
            column_list_with_sums.append("sum(" + item + ")")
        else:
            column_list_with_sums.append(item)
    return column_list_with_sums
