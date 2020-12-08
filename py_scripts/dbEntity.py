import difflib
import email
import filecmp
import os
import smtplib
import subprocess
import time
from email.header import Header
from email.mime.multipart import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pymysql.cursors

# TODO: add dict with pairs client - rmiPort after adding all appropriate sync to dev02
path = '/home/jiggalag/'
cliPath = '/home/jiggalag/Downloads/maxifier-cli-1.12/'
dbases = ['mock', 'mock', 'mock']
host = 'mock'
rmiPort = '9035'
jobs = ['DownloadEntity(-start)', 'Common()', 'Geo()', 'Inventory()', 'Campaign(-f MODIFIED)', 'DownloadEntity(-end)']
irvingJobs = ['DownloadEntity(-start)', 'OpenParseEntities()', 'OpenLoadEntities()', 'OpenParseReport()',
              'OpenLoadReport(-cfg PageReport,StandardReport,KVKeyPairReport,CampaignGeoReport,GeoPageReport,CampaignCountryReport)',
              'DownloadEntity(-end)']

# e-mail settings
fromaddr = "mock"
toaddr = "mock"
mypass = "mock"

# Note: target table now not checked
entities = [
    "browsers",
    "deletedBrowsers",
    "pages",
    "deletedPages",
    "sites",
    "deletedSites",
    "creatives",
    "deletedCreatives",
    "campaignPages",
    "countries",
    "states",
    "cities",  # only for irving
    "campaigns",
    "deletedCampaigns",
    "keyname",
    "keyvalue",
    "insertionOrders",
    "deletedInsertionOrders",
    "posit",
    "deletedPosit",
    "customCampaignType"  # for irving only
]
queries = [
    "select name from browser where isDeleted=0;",
    "select name from browser where isDeleted=1;",
    "select remoteId from page where isDeleted=0 and archived is null;",
    "select remoteId from page where isDeleted=0 or archived is not null;",
    "select name from site where isDeleted=0 and archived is null;",
    "select name from site where isDeleted=1 or archived is not null;",
    "select remoteid from creative where isDeleted=0 and archived is null;",
    "select remoteid from creative where isDeleted=1 or archived is not null;",
    "select * from campaign_pages order by campaignId asc;",
    "select name from country;",
    "select name from state;",
    "select name from city;",  # only for irving
    "select remoteId from campaign where isDeleted=0 and archived is null;",
    "select remoteId from campaign where isDeleted=1 or archived is not null;",
    "select name from keyname where isDeleted=0 and archived is null;",
    "select name from keyvalue where isDeleted=0 and archived is null;",
    "select name from InsertionOrder where isDeleted=0 and archived is null;",
    "select name from InsertionOrder where isDeleted=1 and archived is not null;",
    "select name from posit where isDeleted=0 and archived is null;",
    "select name from posit where isDeleted=1 and archived is not null;",
    "select name from customcampaigntype where isDeleted=0 and archived is null;"  # for irving only
]


def findDiff(entity):
    oldFile = '/tmp/%s0' % entity
    newFile = '/tmp/%s1' % entity
    with open(oldFile, 'r') as file:
        oldEntities = file.readlines()
    with open(newFile, 'r') as file:
        newEntities = file.readlines()
    if filecmp.cmp(oldFile, newFile):
        print("Files are same..." + oldFile + '    ' + newFile)
    else:
        d = difflib.HtmlDiff()
        diff = d.make_table(oldEntities, newEntities)
        filename = '/tmp/%sDiff.html' % entity
        with open(filename, 'w') as file:
            file.write(str(diff))
        body = "Hello!\nThere a some differences founded for entities %s.\n" % entity
        sendmail(body, fromaddr, toaddr, mypass, filename)


def sendmail(body, fromaddr, toaddr, mypass, filename):
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "[Test] Entity differences"
    msg.attach(MIMEText(body, 'plain'))

    if os.path.exists(filename) and os.path.isfile(filename):
        with open(filename, 'rb') as file:
            attachment = MIMEBase('application', "octet-stream")
            attachment.set_payload(file.read())
            email.encoders.encode_base64(attachment)
        nameAttach = Header(os.path.basename(filename), 'utf-8')
        attachment.add_header('Content-Disposition','attachment; filename="%s"' % nameAttach)
        msg.attach(attachment)
    else:
        if filename.lstrip() != "":
            print("File for attach not found - " + filename)

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(fromaddr, mypass)
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()


def importData(i):
    for query in queries:
        cursor.execute(query)
        result = cursor.fetchall()
        fileName = '/tmp/%s%d' % (entities[queries.index(query)], i)
        with open(fileName, 'w') as file:
            for item in result:
                file.write("%s\n" % item)


def runUpdate(jobs, cliPath, host, rmiPort):
    for job in jobs:
        while True:
            checkRunningJobsCommand = '%s./cli.sh jmx:%s:%s sync -c w' % (cliPath, host, rmiPort)
            runningJobs = subprocess.check_output(checkRunningJobsCommand, shell=True, universal_newlines=True)
            if 'pid' in runningJobs:
                print('Wait...')
                time.sleep(10)
            else:
                print(job + "finished...")
                break
        runJobsCommand = '%s./cli.sh jmx:%s:%s sync -c s "%s"' % (cliPath, host, rmiPort, job)
        subprocess.check_output(runJobsCommand, shell=True, universal_newlines=True)
        print(runJobsCommand)

# Connect to the database
for db in dbases:
    connection = pymysql.connect(host='mock',
                                 user='mock',
                                 password='mock',
                                 db=db,
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            importData(0)
            if db is 'irving':
                runUpdate(irvingJobs, cliPath, host, rmiPort)
            else:
                runUpdate(jobs, cliPath, host, rmiPort)
            importData(1)
            for entity in entities:
                findDiff(entity)
    finally:
        connection.close()

    for i in entities:
        for j in range(1):
            fileToRemove = '/tmp/%s%d' % (i, j)
            if os.path.exists(fileToRemove):
                os.remove(fileToRemove)
                print("File " + fileToRemove + " deleted...")
        fileToRemove = '/tmp/%sDiff.html' % i
        if os.path.exists(fileToRemove):
            os.remove(fileToRemove)
            print("File " + fileToRemove + " deleted...")
    print("Client " + db + ' processed...')
