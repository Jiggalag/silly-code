import datetime

from py_scripts.helpers import dbHelper, converters


def compare_dates(prod_connection, test_connection, table, depth_report_check, comparing_info, logger):
    select_query = "SELECT distinct(`dt`) from {};".format(table)
    prod_dates, test_dates = dbHelper.DbConnector.parallel_select([prod_connection, test_connection], select_query)
    if (prod_dates is None) or (test_dates is None):
        logger.warn('Table {} skipped because something going bad'.format(table))
        return []
    if all([prod_dates, test_dates]):
        return calculate_comparing_timeframe(prod_connection, test_connection, prod_dates,
                                             test_dates, table, depth_report_check, logger)
    else:
        if not prod_dates and not test_dates:
            logger.warn("Table {} is empty in both dbs...".format(table))
            comparing_info.empty.append(table)
        elif not prod_dates:
            logger.warn("Table {} on {} is empty!".format(table, prod_connection.db))
            comparing_info.update_empty("prod", table)
        else:
            logger.warn("Table {} on {} is empty!".format(table, test_connection.db))
            comparing_info.update_empty("test", table)
        return []


def calculate_comparing_timeframe(prod_connection, test_connection, prod_dates, test_dates, table,
                                  depth_report_check, logger):
    actual_dates = set()
    days = depth_report_check
    for day in range(1, days):
        actual_dates.add(calculate_date(day))
    if prod_dates[-days:] == test_dates[-days:]:
        return get_comparing_timeframe(prod_dates, depth_report_check)
    else:
        return get_timeframe_intersection(prod_connection, test_connection, depth_report_check,
                                          prod_dates, test_dates, table, logger)


def get_comparing_timeframe(prod_dates, depth_report_check):
    comparing_timeframe = []
    for item in prod_dates[-depth_report_check:]:
        comparing_timeframe.append(item.date().strftime("%Y-%m-%d"))
    return comparing_timeframe


def get_timeframe_intersection(prod_connection, test_connection, depth_report_check,
                               prod_dates, test_dates, table, logger):
    prod_set = set(prod_dates)
    test_set = set(test_dates)
    if prod_set - test_set:
        unique_dates = get_unique_dates(prod_set, test_set)
        logger.warn("This dates absent in {}: ".format(test_connection.db) +
                    "{} in report table {}...".format(",".join(unique_dates), table))
    if test_set - prod_set:
        unique_dates = get_unique_dates(test_set, prod_set)
        logger.warn("This dates absent in {}: ".format(prod_connection.db) +
                    "{} in report table {}...".format(",".join(unique_dates), table))
    result_dates = list(prod_set & test_set)
    result_dates.sort()
    return result_dates[-depth_report_check:]


def calculate_date(days):
    return (datetime.datetime.today().date() - datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


def get_unique_dates(first_date_list, second_date_list):
    unique_dates = []
    for item in converters.convert_to_list(first_date_list - second_date_list):
        unique_dates.append(item.strftime("%Y-%m-%d %H:%M:%S"))
    return unique_dates
