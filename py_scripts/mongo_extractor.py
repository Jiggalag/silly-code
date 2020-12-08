import pymongo

# mongo_point = pymongo.MongoClient('ams-0ifms-dev-cpo-bse04.inventale.com', 27017)
# mongo_point = pymongo.MongoClient('ams-0ifms-dev-cpo-bse04.inventale.com', 27017)
mongo_point = pymongo.MongoClient('ams-ifms-prd-dat-mng04.inventale.com', 27017)
dbs = mongo_point.list_database_names()
for db in dbs:
    connections = mongo_point[db].current_op(True).get('inprog')
    store = list()
    for connection in connections:
        if connection.get('active'):
            store.append(connection)
    if store:
        print(f'there is active connection on database {db}')
    else:
        print(f"database {db} have no active connections...")
print('stop')
