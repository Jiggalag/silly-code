import json
import os.path
import smtplib
import subprocess
import sys
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from os.path import basename


def changeJsonDates(jsonFile, N):
    initialJson = json.loads(open(jsonFile, 'r').read())
    startDate = initialJson.get('startDate')
    startTime = startDate[startDate.rfind('T'):]
    endDate = initialJson.get('endDate')
    endTime = endDate[endDate.rfind('T'):]
    if (startDate or endDate) is None:
        print(str(datetime.datetime.now()) + " [INFO] Json " + jsonFile + " have problems with dates!\n")
        sys.exit()
    else:
        delta = datetime.datetime.strptime(endDate[endDate.rfind('T') - 10:endDate.rfind('T')], "%Y-%m-%d") - datetime.datetime.strptime(startDate[startDate.rfind('T') - 10:startDate.rfind('T')], "%Y-%m-%d")  # days between start and end date
        newDate = datetime.datetime.now() + datetime.timedelta(days=N)  # Return tomorrow date with cutting time
        newEndDate = newDate + delta
    initialJson['startDate'] = str(datetime.datetime.date(newDate)) + startTime
    initialJson['endDate'] = str(datetime.datetime.date(newEndDate)) + endTime
    writeToJson(jsonFile, initialJson)
    replaceQuotes(jsonFile)


def changeJsonFC(jsonFile):
    initialJson = json.loads(open(jsonFile, 'r').read())
    if 'frequencyTargetings' not in list(initialJson.keys()):
        print(str(datetime.datetime.now()) + ' [ERROR] There is no FC in json ' + jsonFile)
        return False
    newFC = initialJson.get('frequencyTargetings')[0].get('impsLimit') + 1
    initialJson['frequencyTargetings'][0]['impsLimit'] = newFC
    writeToJson(jsonFile, initialJson)
    replaceQuotes(jsonFile)
    return newFC


def getInstanceInformation(cliPath, server, rmiPort):
    instanceInformation = {}
    getInformationCommand = '%s jmx:%s:%d sync -c i' % (cliPath, server, rmiPort)
    rawInformation = subprocess.check_output(getInformationCommand, shell=True, universal_newlines=True)
    for data in rawInformation.split('\n'):
        instanceInformation.update({data[:data.find(':')]: data[data.find(':') + 2:]})
    return instanceInformation


def replaceQuotes(jsonFile):
    with open(jsonFile, 'r') as file:
        text = file.read()
    with open(jsonFile, 'w') as file:
        file.write(text.replace('\'', '\"'))


def sendmail(body, fromaddr, toaddr, mypass, subject, files):
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    if type(toaddr) is list:
        msg['To'] = ', '.join(toaddr)
    else:
        msg['To'] = toaddr
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    if files is not None:
        for attachFile in files.split(','):
            if(os.path.exists(attachFile) and os.path.isfile(attachFile)):
                with open(attachFile, 'rb') as file:
                    part = MIMEApplication(file.read(), Name=basename(attachFile))
                part['Content-Disposition'] = 'attachment; filename="%s"' % basename(attachFile)
                msg.attach(part)
            else:
                if (attachFile.lstrip() != ""):
                    print(str(datetime.datetime.now()) + " [ERROR] File not found " + attachFile)
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(fromaddr, mypass)
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()


def writeToJson(jsonFile, initialJson):
    with open(jsonFile, 'w') as file:
        file.write('{\n')
        keys = list(initialJson.keys())
        for i in ['startDate', 'endDate', 'priority', 'weight', 'pacing', 'fcType']:
            if initialJson.get(i) is None:
                line = ""
            else:
                line = '"%s": "%s",\n' % (i, initialJson.get(i))
            if i in keys:
                keys.remove(i)
            file.write(line)
        for i in keys:
            if i is keys[len(keys) - 1]:
                if initialJson.get(i) is None:
                    if i in ['positions', 'keyvalueTargeting', 'frequencyTargetings', 'usedTemplates', 'trafficAllocation', 'products', 'dimensions']:
                        line = '"%s": []\n' % i
                    else:
                        line = '"%s": {}\n' % i
                else:
                    line = '"%s": %s\n' % (i, initialJson.get(i))
                file.write(line)
            else:
                if initialJson.get(i) is None:
                    if i in ['positions', 'keyvalueTargeting', 'frequencyTargetings', 'usedTemplates', 'trafficAllocation', 'products', 'dimensions']:
                        line = '"%s": [],\n' % i
                    else:
                        line = '"%s": {},\n' % i
                else:
                    line = '"%s": %s,\n' % (i, initialJson.get(i))
                file.write(line)
        file.write('}\n')
