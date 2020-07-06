import pymongo

# TODO: seriously refactor this script

dbpattern = ['marv', 'ifmc', 'ifms', 'adfox', 'Adfox', 'demo_ifms']
safe = [
    'ifmc_marvin_sugg_perm',
    'ifmc_marvin_fs_perm',
    'demo_ifms_i_sugg-perm',
    'demo_ifms_i_fs-perm',
    'ifms_rick_pk_sugg_perm',
    'ifms_rick_pk_fs_perm',
    'ifmc_sugg1-perm',
    'ifmc_fs1-perm'
]

track = [
    'demo_ifms_i_fs-perm',
    'demo_ifms_i_sugg-perm',
    'ifmc_marvin_fs_perm',
    'ifmc_marvin_sugg_perm'
]

connection = pymongo.MongoClient('eu-smr-mng-01.maxifier.com', 27017)
list = connection.database_names()
print(list)
list.sort()
toDelete = []
tmp = []

for dbname in list:
    if 'perm' in dbname:
        for i in dbpattern:
            if i in dbname:
                toDelete.append(dbname)
print(len(toDelete))

for dbname in toDelete:
    for i in safe:
        if i == dbname:
            tmp.append(dbname)
            print("Database " + dbname + ' added from removelist...')
print(toDelete)
for dbname in safe:
    toDelete.remove(dbname)
    print("Removed " + dbname)

for dbname in track:
    toDelete.remove(dbname)
    print("Removed " + dbname)

for dbname in toDelete:
    for i in safe:
        if i == dbname:
            print("ERROR! " + dbname)

for dbname in toDelete:
    connection.drop_database(dbname)
    print("Database " + dbname + " deleted...")
print("OK...")

'''
'''
list = [
    'ad7-perm',
    'webteam_testoas_suggestion-perm',
    'webteam_ad_michiganlive_filestorage-perm',
    'webteam_oas_test_reco_filestorage-perm',
    'webteam_ad_michiganlive_suggestion-perm',
    'appntst-2912-perm',
    'ti11rc31-perm',
    'appnexus_test_suggestion-perm',
    'atl1642-perm',
    'fautoapn-perm',
    'wsynclib-perm',
    'or12-perm',
    'ti292-perm',
    'msynclib3-perm',
    'dfp6_test_suggestion-perm',
    'w-perm',
    'adgbs-perm',
    'at1642-perm',
    'adm4-perm',
    'dfp911-perm',
    'ban8-perm',
    'o_f_appnexus_test-perm',
    'testgbs8-perm',
    'adap11-perm',
    'w11-perm',
    'adapngbs-perm',
    'msynclib-perm',
    'mak41-perm',
    'webteam_atlantic_suggestion-perm',
    'rating-perm',
    'cmpr-frkst-sh-new-perm',
    'mak21-perm',
    'tt-perm',
    '1722-perm',
    'adap10-perm',
    '23192-quartz-16110-perm',
    'webteam_atlantic_filestorage-perm',
    'ti1130-perm',
    'appntst-2910-perm',
    'beliefnet_dfp6_test_filestorage-perm',
    'webteam_testdfpsix_suggestion-perm',
    'best-perm',
    'fogs-perm',
    'nj-perm',
    'kv-perm',
    'ad13-perm',
    'o_r_dfp6_test-perm',
    'f0805-perm',
    'atlantic-default-stor-perm',
    '23192-quartz-16110-2-perm',
    'orogs-perm',
    '23192-quartz-16111-perm',
    'o_r_appnexus_test-perm',
    'pk_chuck_suggestion-perm',
    'ti-perm',
    'crappn5-perm',
    'tgbs-perm',
    'w1synclib-perm',
    'oas97-perm',
    't1synclib-perm',
    'cox-perm',
    'best1-perm',
    'or10-perm',
    'oas911-perm',
    'or1642-perm',
    'ti1127-perm',
    'webteam_testappnexus_filestorage-perm',
    'atogs-perm',
    'orange_appnexus-default-stor1-perm',
    'beliefnet-test-perm',
    'webteam_oas_test_reco_suggestion-perm',
    'ad8-perm',
    'adogs-perm',
    'lbcogs-perm',
    '23192-quartz-thread-perm',
    'adap12-perm',
    'makgbs11-perm',
    'appnexus_test_filestorage-perm',
    'ti2914-perm',
    'w2-perm',
    'mak31-perm',
    'o_f_oas_test_reco-perm',
    'dfp6_test_filestorage-perm',
    'webteam_testappnexus_suggestion-perm',
    'tp-perm',
    'apn911-perm',
    'makgbs912-perm',
    'ad10-perm',
    'dfp11rc3-perm',
    'lbc-perm',
    'ti11rc4-perm',
    'orgbs-perm',
    'ifmc_sugg1-perm',
    'dfp97-perm',
    'adgbs9-perm',
    'atl10-perm',
    'webteam_testdfpsix_filestorage-perm',
    'appntst-2911-perm',
    'mi8-perm',
    'pk_chuck_filestorage-perm',
    'orange_appnexus-evgeny-perm',
    'o_r_oas_test_reco-perm',
    'cmpr-frkst-sh-old-perm',
    'makgbs9-perm',
    'adcr-perm',
    'belbest-perm',
    'gbs8-perm',
    'or8-perm',
    'o_f_dfp6_test-perm',
    'ad9-perm',
    'ti11rc3-perm',
    'ti11rc41-perm',
    'ti916-perm',
    'fraud-perm',
    'mak51-perm',
    'webteam_orange_filestorage-perm',
    'atl11-perm',
    'atl12-perm',
    'webteam_orange_suggestion-perm',
    'lbcgbs-perm',
    'adm11-perm',
    'webteam_testoas_filestorage-perm',
    'makgbs112-perm',
    'bsynclib-perm',
]

conf = [
    'ad_appnexus_fgenei-perm',
    'ad_appnexus_master-perm',
    'beliefnet-default-stor-perm',
    'dfp6_test-default-stor-perm',
    'oas_test_reco-default-stor-perm',
    'appnexus_test-default-stor-perm',
    'ad_michiganlive-default-stor-perm',
    'orange_appnexus-default-stor2-perm'
]

def check(conf, list):
    for dbname in conf:
        if (dbname in list) == 1:
            print("We have a problem with db " + dbname)

connection = pymongo.MongoClient('eu-smr-mng-01.maxifier.com', 27017)
check(conf, list)
count = 0
for dbname in list:
    connection.drop_database(dbname)
    print("Database " + dbname + " dropped")
    count += 1
print("Totally deleted " + str(count) + " dbs")

eval = []
connection = pymongo.MongoClient('eu-smr-mng-01.maxifier.com', 27017)
list = connection.database_names()
for dbname in list:
    if 'perm' in dbname:
        eval.append(dbname)
print(eval)
