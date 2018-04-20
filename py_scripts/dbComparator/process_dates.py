import datetime
from py_scripts.helpers import dbcmp_sql_helper, converters


class ProcessDates:
    def __init__(self, prod_connection, test_connection, table, depth_report_check, logger):
        self.prod_connection = prod_connection
        self.test_connection = test_connection
        self.table = table
        self.depth_report_check = depth_report_check
        self.logger = logger

    def compare_dates(self, comparing_info):
        select_query = "SELECT distinct(`dt`) from {};".format(self.table)
        prod_dates, test_dates = dbcmp_sql_helper.get_comparable_objects([self.prod_connection, self.test_connection],
                                                                         select_query)
        if (prod_dates is None) or (test_dates is None):
            self.logger.warn('Table {} skipped because something going bad'.format(self.table))
            return []
        if all([prod_dates, test_dates]):
            return self.calculate_comparing_timeframe(prod_dates, test_dates)
        else:
            if not prod_dates and not test_dates:
                self.logger.warn("Table {} is empty in both dbs...".format(self.table))
                comparing_info.empty.append(self.table)
            elif not prod_dates:
                self.logger.warn("Table {} on {} is empty!".format(self.table, self.prod_connection.db))
                comparing_info.update_empty("prod", self.table)
            else:
                self.logger.warn("Table {} on {} is empty!".format(self.table, self.test_connection.db))
                comparing_info.update_empty("test", self.table)
            return []

    def calculate_comparing_timeframe(self, prod_dates, test_dates):
        actual_dates = set()
        days = self.depth_report_check
        for day in range(1, days):
            actual_dates.add(calculate_date(day))
        if prod_dates[-days:] == test_dates[-days:]:
            return self.get_comparing_timeframe(prod_dates)
        else:
            return self.get_timeframe_intersection(prod_dates, test_dates)

    def get_comparing_timeframe(self, prod_dates):
        comparing_timeframe = []
        for item in prod_dates[-self.depth_report_check:]:
            for i in item:
                comparing_timeframe.append(i.date().strftime("%Y-%m-%d"))
        return comparing_timeframe

    def get_timeframe_intersection(self, prod_dates, test_dates):
        prod_set = set(prod_dates)
        test_set = set(test_dates)
        if prod_set - test_set:
            unique_dates = get_unique_dates(prod_set, test_set)
            self.logger.warn("This dates absent in {}: ".format(self.test_connection.db) +
                             "{} in report table {}...".format(",".join(unique_dates), self.table))
        if test_set - prod_set:
            unique_dates = get_unique_dates(test_set, prod_set)
            self.logger.warn("This dates absent in {}: ".format(self.prod_connection.db) +
                             "{} in report table {}...".format(",".join(unique_dates), self.table))
        result_dates = list(prod_set & test_set)
        result_dates.sort()
        return result_dates[-self.depth_report_check:]


def calculate_date(days):
    return (datetime.datetime.today().date() - datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


def get_unique_dates(first_date_list, second_date_list):
    unique_dates = []
    for item in converters.convert_to_list(first_date_list - second_date_list):
        unique_dates.append(item.strftime("%Y-%m-%d %H:%M:%S"))
    return unique_dates
