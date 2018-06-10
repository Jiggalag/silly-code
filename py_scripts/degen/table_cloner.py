import datetime


class TableCloner:
    def __init__(self, connection, source_db, target_db, table_list, logger):
        self.connection = connection
        self.source_db = source_db
        self.target_db = target_db
        self.table_list = table_list
        self.logger = logger

    def create_tables(self):
        create_queries = list()
        insert_queries = list()
        for table in self.table_list:
            create_query = "CREATE TABLE {0}{1} LIKE {2}.{0}".format(table, datetime.datetime.now(), self.source_db)
            create_queries.append(create_query)
            insert_query = "INSERT INTO {0} SELECT * FROM {1}.{0}".format(table, self.source_db)
            insert_queries.append(insert_query)
        with self.connection.cursor() as cursor:
            for query in create_queries:
                cursor.execute(query)
            for query in insert_queries:
                cursor.execute(query)
                self.connection.commit()
