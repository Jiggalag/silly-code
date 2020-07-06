import pymysql

host = 'eu-db-01.inventale.com'
user = 'liquibase'
password = 'thiequ0WiW6UaNgelilu'
db = 'rick_cpopro'


def get_connection(host, user, password, db):
    try:
        connection = pymysql.connect(host=host,
                                     user=user,
                                     password=password,
                                     db=db,
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.DictCursor)
        return connection
    except pymysql.err.OperationalError as e:
        print(e)
        return None


remoteId = 78979879
startdate = "2020-07-01"
enddate = "2020-07-05"

sql = get_connection(host, user, password, db)
query = (f"SELECT cmp.remoteId, sum(r.impressions) FROM rickcreativesiteplacereport r " +
         f"JOIN rickCreative crv ON crv.id = r.rickCreativeId " +
         f"JOIN rickcampaign cmp ON cmp.id = crv.rickCampaign_id " +
         f"WHERE cmp.remoteId in ({remoteId}) and r.dt BETWEEN '{startdate}' AND '{enddate}' " +
         f"group by cmp.remoteId;")
testquery = 'describe rickcampaign;'
with sql.cursor() as cursor:
    cursor.execute(query)
    result = cursor.fetchall()
print('stop')
