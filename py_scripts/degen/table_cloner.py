import datetime


class TableCloner:
    def __init__(self, connection, source_db, target_db, table_to_copy, logger):
        self.connection = connection
        self.source_db = source_db
        self.target_db = target_db
        self.table_to_copy = table_to_copy
        self.tables = self.get_table_list()
        self.today = datetime.datetime.date(datetime.datetime.today())
        self.logger = logger

    def create_tables(self):
        create_queries = list()
        insert_queries = list()
        go = self.check_date(self.today)
        self.logger.info('Check db size before cloning tables...')
        self.calculate_db_size()
        if go:
            for table in self.table_to_copy:
                table_name = "{}.{}{}".format(self.target_db, table, self.today).replace('-', '_')
                create_query = "CREATE TABLE {} LIKE {}.{};".format(table_name, self.source_db, table)
                create_queries.append(create_query)
                insert_query = "INSERT INTO {} SELECT * FROM {}.{};".format(table_name, self.source_db, table)
                insert_queries.append(insert_query)
            with self.connection.cursor() as cursor:
                for query in create_queries:
                    self.logger.debug(query)
                    cursor.execute(query)
                for query in insert_queries:
                    self.logger.debug(query)
                    cursor.execute(query)
                    self.connection.commit()
            self.logger.info('Check db size after cloning tables...')
            self.calculate_db_size()
            return True
        else:
            self.logger.warn('Tables for date {} already copied in db {}'.format(self.today, self.target_db))
            return False

    def check_date(self, today):
        string_date = self.tables[-1][-10:].replace('_', '-')
        last_date = datetime.datetime.date(datetime.datetime.strptime(string_date, "%Y-%m-%d"))
        if last_date == today:
            return False
        else:
            return True

    def get_table_list(self):
        query = "SHOW TABLES IN {};".format(self.target_db)
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            table_list = list()
            for table in result:
                table_list.append(table.get('Tables_in_{}'.format(self.target_db)))
            table_list.sort()
            return table_list

    def drop_tables(self):
        drop_list = self.find_old_tables()
        for table in drop_list:
            query = "DROP TABLE {}.{};".format(self.target_db, table)
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                self.logger('Table {}.{} successfully dropped...'.format(self.target_db, table))

    def calculate_db_size(self):
        query = ("SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 1) as Size " +
                 "FROM information_schema.tables " +
                 "WHERE table_schema = '{}' ".format(self.target_db) +
                 "GROUP BY table_schema;")
        with self.connection.cursor() as cursor:
            self.logger.debug('Run query {}...'.format(query))
            cursor.execute(query)
            size = cursor.fetchall()
        self.logger.info('Size of db {} is {} Mb...'.format(self.target_db, size[0].get('Size')))

    def find_old_tables(self):
        delete_date = self.today - datetime.timedelta(days=30)
        for table in self.tables:
            if str(delete_date) in table:
                start_index = self.tables.index(table)
                # TODO: check this code!
                return self.tables[:start_index - 1]
        self.logger.info('There is no tables for deleting...')
