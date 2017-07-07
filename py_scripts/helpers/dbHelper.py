import pymysql
from py_scripts.helpers import loggingHelper
from multiprocessing.dummy import Pool

logger = loggingHelper.Logger(10)


class DbConnector:
    def __init__(self, connect_parameters, **kwargs):
        self.host = connect_parameters.get('host')
        self.user = connect_parameters.get('user')
        self.password = connect_parameters.get('password')
        self.db = connect_parameters.get('db')
        self.hide_columns = None
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
    def parallel_select(client_config, client, query, result_type="frozenset"):
        pool = Pool(2)
        sql_params = pool.map(client_config.get_sql_connection_params, ["prod", "test"])
        result = pool.map((lambda x: DbConnector(x, client=client).select(query, result_type)),
                          sql_params)
        pool.close()
        pool.join()
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

    def select(self, query, result_type="frozenset"):
        connection = pymysql.connect(host=self.host,
                                     user=self.user,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.DictCursor)
        error_count = 0
        while error_count < self.attempts:
            try:
                with connection.cursor() as cursor:
                    sql_query = query.replace('DBNAME', self.db)
                    logger.debug(sql_query)
                    cursor.execute(sql_query)
                    result = cursor.fetchall()
                    processed_result = []
                    for item in result:
                        tmp_record = []
                        for key in item.keys():
                            tmp_record.append(item.get(key))
                        if len(tmp_record) == 1:
                            processed_result.append(tmp_record[0])
                        elif result_type == "list":
                            processed_result.append(tmp_record)
                        else:
                            processed_result.append(frozenset(tmp_record))
                    return processed_result
            except pymysql.OperationalError:
                error_count += 1
                logger.error("There are some SQL query error " + str(pymysql.OperationalError))
            finally:
                try:
                    connection.close()
                except pymysql.Error:
                    logger.info("Connection already closed...")
                    return []

    def get_tables(self):
        connection = pymysql.connect(host=self.host,
                                     user=self.user,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.DictCursor)
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

    def get_column_list(self, table):
        connection = pymysql.connect(host=self.host,
                                     user=self.user,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                column_list = []
                query = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s' " \
                        "AND table_schema = '%s';" % (table, self.db)
                logger.debug(query)
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
                return column_string.lower()
        finally:
            if connection.open:
                connection.close()


def get_column_list_for_sum(set_column_list):
    column_list_with_sums = []
    for item in set_column_list.split(","):
        if "clicks" in item or "impressions" in item:
            column_list_with_sums.append("sum(" + item + ")")
        else:
            column_list_with_sums.append(item)
    return column_list_with_sums


def get_amount_records(table, date, client_config, client, db_properties):
    if date is None:
        query = "SELECT COUNT(*) FROM `{}`;".format(table)
    else:
        query = "SELECT COUNT(*) FROM `{}` WHERE dt > '{}';".format(table, date)
    return DbConnector.parallel_select(client_config, client, query, db_properties)
