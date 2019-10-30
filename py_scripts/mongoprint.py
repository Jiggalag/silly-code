import pymongo


mongohost = 'irl-0ifms-dev-cpo-bse06.inventale.com'
mongoport = 27017

basename = 'ifms_m_suggestions'


mongo = pymongo.MongoClient(mongohost, mongoport)
base = mongo[basename]

collections = base.collection_names()
sizes = list()
for collection in collections:
    size = base.command('collstats', collection).get('storageSize') / 1024 / 1024 / 1024
    count = base.command('collstats', collection).get('count')
    sizes.append(size)
    if size < 0.001:
        # print('Collection: {}, size: ~ 0 GBs, records: {}'.format(collection, count))
        pass
    else:
        print('Collection: {}, size: {} GBs, records: {}'.format(collection, size, count))
print('stop')
