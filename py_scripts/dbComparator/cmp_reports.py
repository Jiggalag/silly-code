import datetime
from py_scripts.helpers import dbHelper, converters
from py_scripts.dbComparator import queryConstructor


def iteration_comparing_by_queries(prod_connection, test_connection, query_list, global_break,
                                   table, start_time, comparing_info, **kwargs):
    mode = kwargs.get('mode')
    logger = kwargs.get('logger')
    fail_with_first_error = kwargs.get('fail_with_first_error')
    table_timeout = kwargs.get('table_timeout')
    local_break = False
    for query in query_list:
        if mode == "day-sum":
            if ("impressions" and "clicks") in prod_connection.get_column_list(table):
                if not compare_report_sums(prod_connection, test_connection,
                                           table, query, comparing_info, logger) and fail_with_first_error:
                    logger.critical("First error founded, checking failed. " +
                                    "Comparing takes {}.".format(datetime.datetime.now() - start_time))
                    global_break = True
                    return global_break, local_break
            else:
                logger.warn("There is no impression of click column in table {}".format(table))
                local_break = True
                return global_break, local_break
        elif mode in ["section-sum", "detailed"]:
            if mode == "section-sum":
                section = calculate_section_name(query)
                logger.info("Check section {} for table {}".format(section, table))
                # TODO: add stopping of iterating by queries
            else:
                cmp_result = compare_reports_detailed(prod_connection, test_connection, table, query,
                                                      comparing_info, **kwargs)
                if cmp_result is None:
                    return global_break, local_break
                if not cmp_result and fail_with_first_error:
                    logger.critical("First error founded, checking failed. Comparing takes {}.".format(
                        datetime.datetime.now() - start_time))
                    global_break = True
                    return global_break, local_break
        if table_timeout is not None:
            if datetime.datetime.now() - start_time > datetime.timedelta(minutes=table_timeout):
                logger.error('Checking table {} exceded timeout {}. Finished'.format(table, table_timeout))
                return global_break, local_break
    return global_break, local_break


def compare_reports_detailed(prod_connection, test_connection, table, query, comparing_info, **kwargs):
    logger = kwargs.get('logger')
    prod_reports, test_reports = dbHelper.DbConnector.parallel_select([prod_connection, test_connection], query)
    if (prod_reports is None) or (test_reports is None):
        logger.warn('Table {} skipped because something going bad'.format(table))
        return True
    if len(prod_reports) == 0 or len(test_reports) == 0:
        logger.warn(('Checking table {} finished unexpectedly, '.format(table) +
                     'because at least one result set empty.' +
                     'Prod_reports_len = {}, '.format(len(prod_reports)) +
                     'test_reports_len = {}. '.format(len(test_reports))))
        return None
    prod_unique_reports = set(prod_reports) - set(test_reports)
    test_unique_reports = set(test_reports) - set(prod_reports)
    # TODO: write to file only on last stage, previously you should store uniq entities in set in memory
    # TODO: and periodically compare it
    if not all([len(prod_unique_reports) == 0, len(test_unique_reports) == 0]):
        logger.error("Tables {} differs!".format(table))
        comparing_info.update_diff_schema(table)
        return False
    else:
        return True


def compare_report_sums(prod_connection, test_connection, table, query, comparing_info, logger):
    prod_reports, test_reports = dbHelper.DbConnector.parallel_select([prod_connection, test_connection], query, "list")
    if (prod_reports is not None) or (test_reports is not None):
        return True
    clicks = True
    imps = True
    prod_imps = prod_reports[0]
    test_imps = test_reports[0]
    prod_clicks = prod_reports[1]
    test_clicks = test_reports[1]
    if prod_clicks != test_clicks:
        clicks = False
        logger.warn("There are different click sums for query {}. ".format(query) +
                    "Prod clicks={}, test clicks={}".format(prod_clicks, test_clicks))
    if prod_imps != test_imps:
        imps = False
        logger.warn("There are different imp sums for query {}. ".format(query) +
                    "Prod imps={}, test imps={}".format(prod_imps, test_imps))
    if not all([clicks, imps]):
        logger.error("Tables {} differs!".format(table))
        comparing_info.update_diff_data(table)
        return False
    else:
        return True


def compare_report_table(prod_connection, test_connection, global_break, mapping, local_break,
                         table, start_time, comparing_info, **kwargs):
    logger = kwargs.get('logger')
    comparing_step = kwargs.get('comparing_step')
    mode = kwargs.get('mode')
    depth_report_check = kwargs.get('depth_report_check')

    dates = converters.convertToList(compare_dates(prod_connection, test_connection, table, depth_report_check,
                                                   comparing_info, logger))
    # TODO: here I have problem with dates, strongly test this code and fix it
    dates.sort()
    if dates:
        prod_record_amount, test_record_amount = dbHelper.get_amount_records(table, dates[0],
                                                                             [prod_connection, test_connection],
                                                                             logger)
        if prod_record_amount != test_record_amount:
            logger.warn(('Amount of records significantly differs for table {}'.format(table) +
                         'Prod record amount: {}. '.format(prod_record_amount) +
                         'Test record amount: {}. '.format(test_record_amount)))
        for dt in reversed(dates):
            if not all([global_break, local_break]):
                max_amount = max(prod_record_amount, test_record_amount)
                cmp_object = queryConstructor.InitializeQuery(prod_connection, logger)
                query_list = cmp_object.report(table, dt, mode, max_amount,
                                               comparing_step, mapping)
                global_break, local_break = iteration_comparing_by_queries(prod_connection, test_connection,
                                                                           query_list, global_break, table,
                                                                           start_time, comparing_info,
                                                                           **kwargs)
            else:
                break
    else:
        logger.warn("Tables {} should not be compared correctly, ".format(table) +
                    "because they have no any crosses dates in reports")
        comparing_info.no_crossed_tables.append(table)


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
        return get_timeframe_intersection(prod_connection, test_connection, depth_report_check, prod_dates, test_dates, table, logger)


def get_timeframe_intersection(prod_connection, test_connection, depth_report_check, prod_dates, test_dates, table, logger):
    prod_set = set(prod_dates)
    test_set = set(test_dates)
    if prod_set - test_set:  # this code (4 strings below) should be moved to different function
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


def get_comparing_timeframe(prod_dates, depth_report_check):
    comparing_timeframe = []
    for item in prod_dates[-depth_report_check:]:
        comparing_timeframe.append(item.date().strftime("%Y-%m-%d"))
    return comparing_timeframe


def calculate_section_name(query):
    tmp_list = query.split(" ")
    for item in tmp_list:
        if "GROUP" in item:
            return tmp_list[tmp_list.index(item) + 2][2:].replace("_", "").replace("id", "")


def get_header(query):
    cut_select = query[7:]
    columns = cut_select[:cut_select.find("FROM") - 1]
    header = []
    for item in columns.split(","):
        if ' as ' in item:
            header.append(item[:item.find(' ')])
        else:
            header.append(item)
    return header


def calculate_date(days):
    return (datetime.datetime.today().date() - datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


def write_header(file_name, header):
    with open(file_name, 'w') as file:
        file.write(','.join(header) + '\n')


def get_unique_dates(first_date_list, second_date_list):
    unique_dates = []
    for item in converters.convertToList(first_date_list - second_date_list):
        unique_dates.append(item.strftime("%Y-%m-%d %H:%M:%S"))
    return unique_dates
