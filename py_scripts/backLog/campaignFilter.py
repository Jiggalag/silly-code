import pymysql.cursors
from helpers import helper
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

days = 1
accuracy = 1000
# e-mail settings
fromaddr = "haildroids@gmail.com"
toaddr = "pavel.kiselev@best4ad.com"
mypass = "m3ns3r17"
# db settings
user = 'monitor'
password = 'kqQ2YecrU0y74Qb'
# user='cpopro_10',
# password='jeiloo1eBe',
resultFileName = '/tmp/compareResult'
clients = {
    'rick': {'host': 'irl-0ifms-dev-cpo-bse06.maxifier.com', 'db': 'ifms4_cpopro'}
    # 'irving': {'host': 'ams-0ifms-dev-cpo-db01.maxifier.com', 'db': 'ifms3_uat_cpopro'},
    # 'marvin': {'host': 'ams-maxif-prd-dat-msq01.maxifier.com', 'db': 'ifms_m_cpopro'},
    # 'prisa': {'host': 'us-njc-db-01.maxifier.com', 'db': 'prisa'}
}
fieldList = [
    'Статус',
    'Дата, время окончания',
    'Размещение',
    'Уровень',
    'Приоритет',
    'Показов всего/в сутки',
    'Количество показов за период',
    'Период частоты показов'
]


def compareDeliveryAndForecast(data):
    remoteIdList = []
    with open(resultFileName, 'w'):
        print("Result file prepared...")
    for dbResult in data:
        delivery = dbResult.get('currentDelivery')
        forecast = dbResult.get('maxifierForecast')
        if (forecast or delivery) == 0:
            with open(resultFileName, 'a') as file:
                file.write("[WARN] Please, check campaign " + str(dbResult.get('campaignRemoteId')) + '. Forecast or delivery equals zero!')
                remoteIdList.append(dbResult.get('campaignRemoteId'))
        elif forecast is None or delivery is None:
            with open(resultFileName, 'a') as file:
                file.write("[WARN] Please, check campaign " + str(dbResult.get('campaignRemoteId')) + '. Forecast or delivery is NULL!')
                remoteIdList.append(dbResult.get('campaignRemoteId'))
        else:
            if delivery > forecast:
                diff = (delivery - forecast)/delivery * 100
                if diff > accuracy:
                    with open(resultFileName, 'a') as file:
                        file.write("Please, check campaign " + dbResult.get('campaignRemoteId') + '. Delivery more than forecast about ' + str(diff) + "%.\n")
                        remoteIdList.append(dbResult.get('campaignRemoteId'))
            else:
                diff = (forecast - delivery)/delivery * 100
                if diff > accuracy:
                    with open(resultFileName, 'a') as file:
                        file.write("Please, check campaign " + dbResult.get('campaignRemoteId') + '. Forecast more than delivery about ' + str(diff) + "%.\n")
                        remoteIdList.append(dbResult.get('campaignRemoteId'))
    return remoteIdList


def comparingWOWriting(data):
    remoteIdList = []
    for dbResult in data:
        delivery = dbResult.get('currentDelivery')
        forecast = dbResult.get('maxifierForecast')
        if (forecast or delivery) == 0:
            remoteIdList.append(dbResult.get('campaignRemoteId'))
        elif forecast is None or delivery is None:
            remoteIdList.append(dbResult.get('campaignRemoteId'))
        else:
            if delivery > forecast:
                diff = (delivery - forecast)/delivery * 100
                if diff > accuracy:
                    remoteIdList.append(dbResult.get('campaignRemoteId'))
            else:
                diff = (forecast - delivery)/delivery * 100
                if diff > accuracy:
                    remoteIdList.append(dbResult.get('campaignRemoteId'))
    return remoteIdList


def initializeDates():
    date = datetime.now()
    delta = timedelta(days=1)
    dates = []
    for i in range(days + 1):
        serviceDate = date - i * delta
        if serviceDate != date:
            dates.append(time.strftime("%d.%m.%y", datetime.timetuple(serviceDate)))
    if len(dates) > 1:
        dates.reverse()
    return dates

for client in clients.keys():
    connection = pymysql.connect(host=clients.get(client).get('host'),
                                 user=user,
                                 password=password,
                                 db=clients.get(client).get('db'),
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            query = 'SELECT s.campaignRemoteId, s.mainGoal, s.currentDelivery, s.maxifierForecast, (SELECT SUM(impressions) FROM standardreport r WHERE r.campaignId = c.id) totalDelivery FROM ForecastStatistics s JOIN campaign c ON c.remoteId = s.campaignRemoteId WHERE DATEDIFF(s.enddate, s.dt) = %d;' % days
            cursor.execute(query)
            data = cursor.fetchall()
    finally:
        connection.close()
    # remoteIdList = compareDeliveryAndForecast(data)
    remoteIdList = comparingWOWriting(data)
    initialSize = len(remoteIdList)
    print("There are " + str(initialSize) + " potentially problem campaigns...")
    # dates = initializeDates()
    dates = '13.10.16'
    # counter = 0
    for j in dates:
        # getRequest = 'https://login.adfox.ru/log.php?loginAccount=sup-com&loginPassword=9905f3234fda2f9ad9e15496dda3962880c40378e9377e4a8a01c1765408fcb2&pageN=0&searchString=%ssearchStartDate=%s&searchEndDate=%s&fullDay=' % (i, j, j)
        # start = datetime.today()
        # getRequest = 'https://login.adfox.ru/log.php?loginAccount=sup-com&loginPassword=9905f3234fda2f9ad9e15496dda3962880c40378e9377e4a8a01c1765408fcb2&searchString=%s&searchStartDate=%s&searchEndDate=%s&id=&object=&name=&action=&account=&ip=&x=0&y=0&newSearch=on' % (i, j, j)
        # getRequest = 'https://login.adfox.ru/log.php?loginAccount=sup-com&loginPassword=9905f3234fda2f9ad9e15496dda3962880c40378e9377e4a8a01c1765408fcb2&searchString=%s&searchStartDate=%s&searchEndDate=%s&id=&object=&name=&action=&account=&ip=&x=0&y=0&newSearch=on' % (i, j, j)
        getRequest = "https://login.adfox.ru/log.php?loginAccount=sup-com&loginPassword=9905f3234fda2f9ad9e15496dda3962880c40378e9377e4a8a01c1765408fcb2&searchString=&searchStartDate=%s&searchEndDate=%s&id=&object=&name=&action=&account=&ip=&x=0&y=0&fullDay=1" % (j, j)
        text = helper.getHistory(getRequest)
        soup = BeautifulSoup(text, "lxml")
        tags = soup.find_all("tr", {"valign": "middle"})
        isFlight = 0
        for k in tags:
            if len(k.find_all("td")) == 12:
                if (k.find_all("td")[2].text in 'Флайт') or (k.find_all("td")[2].text is '' and isFlight == 1):
                    isFlight = 1
                    if k.find_all("td")[2].text in 'Флайт':
                        idFlight = k.find_all("td")[1].text
                        for c in range(11):
                            print(k.find_all("td")[c].text)
                    if k.find_all("td")[7].text in fieldList:
                        try:
                            remoteIdList.remove(k.find_all("td")[1].text)
                            print("Campaign removed from list because it dramatically changed in " + str(j) + "...")
                        except ValueError:
                            print("remoteId " + k.find_all("td")[1].text + " not in list of problem flight...")
                else:
                    isFlight = 0
                    # requestTime = datetime.today() - start
                    # print("Checked flight " + str(i) + " for date " + str(j) + " " + str(counter) + "/" + str(len(remoteIdList)) + " in " + str(requestTime) + "...")
                    # counter += 1
    # TODO: remove excess ids from remoteIdList
    with open(resultFileName, 'w') as file:
        for i in remoteIdList:
            file.write(str(remoteIdList) + '\n')
    subject = "[Test] Discrepancies between delivery and forecast on %s" % client
    body = 'Hello!\n There are some discrepancies between forecast and delivery founded on %s for %d days.\n More information in attach file.' % (client, days)
    helper.sendmail(body, fromaddr, toaddr, mypass, subject, resultFileName)
    print("Client " + client + " processed...")
