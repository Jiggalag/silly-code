__author__ = 'pavel.kiselev'

from multiprocessing.dummy import Pool


def convert_to_list(structure_to_convert):
    result_list = []
    for item in structure_to_convert:
        if type(item) is dict:
            key = list(item.keys())[0]
            result_list.append(item.get(key))
        else:
            result_list.append(item)
    try:
        result_list.sort()
    except TypeError:
        print('Raised TypeError during list sorting')
    return result_list


def convert_to_set(list_to_convert):
    result_set = set()
    for item in list_to_convert:
        if type(item) is dict:
            key = list(item.keys())[0]
            result_set.add(item.get(key))
        else:
            result_set.add(item)
    return result_set


def parallel_convert_to_set(table_dicts):
    pool = Pool(2)
    table_sets = pool.map(convert_to_set, table_dicts)
    pool.close()
    pool.join()
    return table_sets
