import os
from py_scripts.helpers import converters


def write_header(file_name, header):
    with open(file_name, 'w') as file:
        file.write(','.join(header) + '\n')


def write_unique_entities_to_file(table, list_uniqs, stage, header, service_dir, logger):
    logger.error("There are {} unique elements in table {} ".format(len(list_uniqs), table) +
                 "on {}-server. Detailed list of records ".format(stage) +
                 "saved to {}{}_uniqRecords_{}".format(service_dir, table, stage))
    file_name = "{}{}_uniqRecords_{}".format(service_dir, table, stage)
    if not os.path.exists(file_name):
        write_header(file_name, header)
    with open(file_name, "a") as file:
        first_list = converters.convert_to_list(list_uniqs)
        for item in first_list:
            file.write(str(item) + "\n")


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


def check_uniqs(prod_set, test_set, strings_amount, table, query, service_dir, logger):
    if len(prod_set) > strings_amount or len(test_set) > strings_amount:
        dump_uniqs(prod_set, test_set, table, query, service_dir, logger)
        return True
    else:
        return False


def dump_uniqs(prod_set, test_set, table, query, service_dir, logger):
    header = get_header(query)
    write_unique_entities_to_file(table, prod_set, 'prod', header, service_dir, logger)
    write_unique_entities_to_file(table, test_set, 'test', header, service_dir, logger)


def thin_uniq_list(target_set, second_set, logger):
    result = target_set - second_set
    if not result:
        logger.debug('Thining finished unsuccessfully')
    return result


def merge_uniqs(uniq_set, additional_set):
    for item in additional_set:
        uniq_set.update({item})
    return uniq_set
