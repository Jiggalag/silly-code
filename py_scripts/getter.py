import json

jdata = json.load(open('1.json', 'r'))

def recursive_items(dictionary):
    for key, value in dictionary.items():
        if type(value) is dict:
            yield from recursive_items(value)
        else:
            yield (key, value)


recursive_items(jdata)