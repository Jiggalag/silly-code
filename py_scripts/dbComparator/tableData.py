from py_scripts.dbComparator import sqlComparing


class Info:
    def __init__(self, logger):
        self.logger = logger
        self.tables = set()
        self.excluded_tables = set()
        self.prod_list = set()
        self.test_list = set()
        self.prod_uniq_tables = []
        self.test_uniq_tables = []
        self.prod_empty = set()
        self.test_empty = set()
        self.empty = []
        self.no_crossed_tables = []
        self.diff_schema = []
        self.diff_data = []
        self.compared_tables = []
        self.prod_uniq_columns = dict()
        self.test_uniq_columns = dict()

    def update_table_list(self, stage, value):
        if stage == "prod":
            self.prod_list.update(value)
        elif stage == "test":
            self.test_list.update(value)
        else:
            self.logger.error("There is no such stage {}".format(stage))

    def update_empty(self, stage, value):
        if stage == "prod":
            self.prod_empty.update(value)
        elif stage == "test":
            self.test_empty.update(value)
        else:
            self.logger.error("There is no such stage {}".format(stage))

    def update_diff_schema(self, value):
            self.diff_schema.append(value)

    def update_diff_data(self, value):
            self.diff_data.append(value)

    def define_table_list(self, excluded_tables, client_ignored_tables, reports, entities, connection):
        self.tables = list(self.prod_list & self.test_list)
        self.tables.sort()
        for table in excluded_tables:
            if table in self.tables:
                self.tables.remove(table)
        if self.diff_schema is not None:
            for table in self.diff_schema:
                if table in self.tables:
                    self.tables.remove(table)
        if (not client_ignored_tables) and client_ignored_tables is not None:
            for table in client_ignored_tables:
                if table in self.tables:
                    self.tables.remove(table)
        table_list = []
        for table in self.tables:
            if sqlComparing.Object.is_report(table, connection):
                if reports:
                    table_list.append(table)
            else:
                if entities:
                    table_list.append(table)
        return table_list

    def get_uniq_tables(self, stage):
        if stage == "prod":
            self.prod_uniq_tables = self.prod_list - self.test_list
            return self.prod_uniq_tables
        elif stage == "test":
            self.test_uniq_tables = self.test_list - self.prod_list
            return self.test_uniq_tables
        else:
            self.logger.error("There is no such stage {}".format(stage))
