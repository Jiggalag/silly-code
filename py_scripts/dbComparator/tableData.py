from py_scripts.helpers.loggingHelper import Logger

logger = Logger(20)


class Info:
    def __init__(self):
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
            logger.error("There is no such stage {}".format(stage))

    def update_empty(self, stage, value):
        if stage == "prod":
            self.prod_empty.update(value)
        elif stage == "test":
            self.test_empty.update(value)
        else:
            logger.error("There is no such stage {}".format(stage))

    def update_nocrossed_dates(self, value):
            self.no_crossed_tables.append(value)

    def update_diff_schema(self, value):
            self.diff_schema.append(value)

    def update_diff_data(self, value):
            self.diff_data.append(value)

    def get_tables(self, excluded_tables, client_ignored_tables):
        self.tables = self.prod_list & self.test_list
        for table in excluded_tables:
            if table in self.tables:
                self.tables.remove(table)
        if self.diff_schema is not None:
            for table in self.diff_schema:
                if table in self.tables:
                    self.tables.remove(table)
        if not client_ignored_tables:
            for table in client_ignored_tables:
                if table in self.tables:
                    self.tables.remove(table)
        table_list = list(self.tables)
        table_list.sort()
        return table_list

    def get_both_empty(self):
        self.empty = self.prod_empty & self.test_empty
        return self.empty

    def get_uniq_tables(self, stage):
        if stage == "prod":
            self.prod_uniq_tables = self.prod_list - self.test_list
            return self.prod_uniq_tables
        elif stage == "test":
            self.test_uniq_tables = self.test_list - self.prod_list
            return self.test_uniq_tables
        else:
            logger.error("There is no such stage {}".format(stage))
