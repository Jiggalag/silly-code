from py_scripts.helpers import dbHelper

# TODO: Days in past parameter not works for Day-summary shecking mode - fix it.


def compare_entity_table(prod_connection, test_connection, table, query, comparing_info, logger):
    prod_entities, test_entities = dbHelper.DbConnector.parallel_select([prod_connection, test_connection], query)
    if (prod_entities is None) or (test_entities is None):
        logger.warn('Table {} skipped because something going bad'.format(table))
        return True
    prod_unique_entities = set(prod_entities) - set(test_entities)
    test_unique_entities = set(test_entities) - set(prod_entities)
    # TODO: write to file only on last stage, previously you should store uniq entities in set in memory
    # TODO: and periodically compare it
    if not all([len(prod_unique_entities) == 0, len(test_unique_entities) == 0]):
        logger.error("Tables {} differs!".format(table))
        comparing_info.update_diff_data(table)
        return False
    else:
        return True
