__author__ = 'pavel.kiselev'

def convertToList(structureToConvert):
    resultList = []
    for item in structureToConvert:
        if type(item) is dict:
            key = list(item.keys())[0]
            resultList.append(item.get(key))
        else:
            resultList.append(item)
    resultList.sort()
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