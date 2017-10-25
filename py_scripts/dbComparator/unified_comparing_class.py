import datetime
from py_scripts.dbComparator import process_uniqs, queryConstructor, process_dates
from py_scripts.helpers import dbHelper, converters


def compare_table(prod_connection, test_connection, table, is_report, service_dir, mapping, start_time,
                  comparing_info, **kwargs):
    logger = kwargs.get('logger')
    comparing_step = kwargs.get('comparing_step')
    depth_report_check = kwargs.get('depth_report_check')
    mode = kwargs.get('mode')
    local_break, max_amount = check_amount(prod_connection, test_connection, table, logger)
    if not local_break:
        if is_report:
            dates = converters.convertToList(process_dates.compare_dates(prod_connection, test_connection, table,
                                                                         depth_report_check, comparing_info, logger))
            dates.sort()
            query_list = queryConstructor.InitializeQuery(prod_connection, mapping, table,
                                                          comparing_step, logger).report(dates, mode, max_amount)
        else:
            query_list = queryConstructor.InitializeQuery(prod_connection, mapping, table,
                                                          comparing_step, logger).entity(max_amount)
        global_break, local_break = iterate_by_query_list(prod_connection, test_connection, query_list, table, start_time,
                                             comparing_info, service_dir, **kwargs)
        return global_break
    else:
        logger.warn('Local_break flag detected. Checking of table {} skipped.'.format(table))
        return False


def check_amount(prod_connection, test_connection, table, logger):
    prod_record_amount, test_record_amount = dbHelper.get_amount_records(table,
                                                                         None,
                                                                         [prod_connection, test_connection],
                                                                         logger)
    if prod_record_amount == 0 and test_record_amount == 0:
        logger.warn("Table {} is empty on both servers!".format(table))
        return True, 0
    if prod_record_amount == 0:
        logger.warn("Table {} is empty on prod-server!".format(table))
        return True, 0
    if test_record_amount == 0:
        logger.warn("Table {} is empty on test-server!".format(table))
        return True, 0
    if prod_record_amount != test_record_amount:
        logger.warn(('Amount of records differs for table {}'.format(table) +
                     'Prod record amount: {}. '.format(prod_record_amount) +
                     'Test record amount: {}. '.format(test_record_amount)))
    max_amount = max(prod_record_amount, test_record_amount)
    return False, max_amount


def iterate_by_query_list(prod_connection, test_connection, query_list, table, start_time, comparing_info,
                          service_dir, **kwargs):
    table_start_time = datetime.datetime.now()
    logger = kwargs.get('logger')
    strings_amount = kwargs.get('strings_amount')
    fail_with_first_error = kwargs.get('fail_with_first_error')
    table_timeout = kwargs.get('table_timeout')
    prod_uniq = set()
    test_uniq = set()
    for query in query_list:
        local_break, prod_tmp_uniq, test_tmp_uniq = get_differences(prod_connection, test_connection, table, query,
                                                            comparing_info, strings_amount, service_dir, logger)
        prod_uniq = process_uniqs.merge_uniqs(prod_uniq, prod_tmp_uniq)
        test_uniq = process_uniqs.merge_uniqs(test_uniq, test_tmp_uniq)

        # TODO: here we try to thin uniq sets
        if prod_uniq and test_uniq:
            prod_uniq = process_uniqs.thin_uniq_list(prod_uniq, test_uniq, logger)
            test_uniq = process_uniqs.thin_uniq_list(test_uniq, prod_uniq, logger)

        if table_timeout is not None:
            duration = datetime.datetime.now() - table_start_time
            if duration > datetime.timedelta(minutes=table_timeout):
                logger.error(('Checking table {} '.format(table) +
                              'exceded timeout {}. Finished'.format(table_timeout)))
                return False, True

        if not local_break and fail_with_first_error:
            logger.info(("First error founded, checking failed. " +
                         "Comparing takes {}").format(datetime.datetime.now() - start_time))
            return True, False

        if process_uniqs.check_uniqs(prod_uniq, test_uniq, strings_amount, table, query, service_dir, logger):
            return False, True
        return False, False


def get_differences(prod_connection, test_connection, table, query, comparing_info,
                    strings_amount, service_dir, logger):
    prod_entities, test_entities = dbHelper.DbConnector.parallel_select([prod_connection, test_connection], query)
    if (prod_entities is None) or (test_entities is None):
        logger.warn('Table {} skipped because something going bad'.format(table))
        return False, set(), set()
    prod_uniq = set(prod_entities) - set(test_entities)
    test_uniq = set(test_entities) - set(prod_entities)
    if not any([len(prod_uniq) == 0, len(test_uniq) == 0]):
        logger.error("Tables {} differs!".format(table))
        comparing_info.update_diff_data(table)
        if process_uniqs.check_uniqs(prod_uniq, test_uniq, strings_amount, table, query, service_dir, logger):
            return False, set(), set()
        else:
            return False, prod_uniq, test_uniq
    else:
        return True, set(), set()
