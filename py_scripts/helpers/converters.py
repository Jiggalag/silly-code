__author__ = 'pavel.kiselev'

from multiprocessing.dummy import Pool

def convertToList(structureToConvert):
    resultList = []
    for item in structureToConvert:
        if type(item) is dict:
            key = list(item.keys())[0]
            resultList.append(item.get(key))
        else:
            resultList.append(item)
    # TODO: critical change, test it
    try:
        resultList.sort()
    except TypeError:
        print('Raised TypeError during list sorting')
    return resultList


def convertToSet(listToConvert):
    resultSet = set()
    for item in listToConvert:
        if type(item) is dict:
            key = list(item.keys())[0]
            resultSet.add(item.get(key))
        else:
            resultSet.add(item)
    return resultSet


def parallelConvertToSet(tableDicts):
    pool = Pool(2)
    tableSets = pool.map(convertToSet, tableDicts)
    pool.close()
    pool.join()
    return tableSets
