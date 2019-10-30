import pymongo


mongohost = 'ams-0ifms-dev-cpo-bse04.inventale.com'
mongoport = 27017

basename = 'ifms_i_suggestions'


mongo = pymongo.MongoClient(mongohost, mongoport)
base = mongo[basename]

collections = base.collection_names()
sizes = list()
for collection in collections:
    size = base.command('collstats', collection).get('storageSize') / 1024 / 1024 / 1024
    sizes.append(size)
    if size < 0.001:
        print('Collection: {}, size: ~0 GBs'.format(collection))
    else:
        print('Collection: {}, size: {} GBs'.format(collection, size))
print('stop')
