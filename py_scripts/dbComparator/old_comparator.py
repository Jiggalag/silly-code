__author__ = 'pavel.kiselev'

# TODO: remove after stabilization of master
# TODO: 1. add meta-data comparing
# TODO: 2. add kdiff-like visualisation?
# TODO: 3. add intellectual comparing of reports. If amount of dates in report tables differs, we should select minimum date
# TODO: from both tables and start comparing from this date. Also, we should modify select query in case of report table
# TODO: comparing. We should compare tables by dates too, not only with limit.
# TODO: 4. Add mechanism for quick comparing all data of huge reports. Probably, checking sums.

import configparser
import datetime
import os
import pymysql
from helpers import helper
from multiprocessing.dummy import Pool as ThreadPool

config = configparser.ConfigParser()
propertyFile = './resources/properties/dbComparing.properties'
config.read(propertyFile)

# e-mail settings
fromaddr = "do-not-reply@inventale.com"
toaddr = config['main']['sendMailTo'].split(',')
mypass = "AKIAJHBVE2GQUQBRSQVA"

step = config.getint('main', 'comparingStep')  # step of limit
limit = 0
differTables = []
attempts = config.getint('main', 'retryAttempts')  # amount of attempts of retrying SQL-query
errorLog = []

def checkDataSetLength(prodResult, testResult):
    return not (len(prodResult) < step) or (len(testResult) < step)

def cmpDicts(prod, test):
    diff = 0
    prodKeys = list(prod)
    prodKeys.sort()
    testKeys = list(test)
    testKeys.sort()
    if prodKeys != testKeys:
        if len(prodKeys) > len(testKeys):
            server = 'production'
        else:
            server = 'test'
        errorLog.append("[ERROR] There are different amount of columns in table " + tableName + ". Columns " + ', '.join(map(str, list(set(prodKeys) ^ set(testKeys)))) + " are excess on " + server)
        diff = 1
    else:
        for key in prodKeys:
            if prod.get(key) != test.get(key):
                errorLog.append("Data differs in column " + key + ". Prod value is " + str(prod.get(key)) + " , test value is " + str(test.get(key)))
                diff = 1
                break
    return diff != 1

def compareDataLength(prodResult, testResult):
    if len(prodResult) != len(testResult):
        errorLog.append("[ERROR] Tables " + tableName + " differs because there are different data length we have got...")
        differTables.append(tableName)
        return False
    else:
        return True

def countReportDate(environment):
    connection = pymysql.connect(host=config[environment][client + '.host'],
                                 user=config[environment][client + '.user'],
                                 password=config[environment][client + '.password'],
                                 db=config[environment][client + '.db'],
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            selectQuery = 'SELECT distinct(dt) FROM %s ORDER BY dt DESC LIMIT 0,%d;' % (tableName, int(config['main']['depthReportCheck']))
            cursor.execute(selectQuery)
            result = cursor.fetchall()
            if len(result) == 0:
                return False
            else:
                return result[0].get('dt').strftime('%Y-%m-%d')
    finally:
        connection.close()

def deleteServiceTables(environment):
    listTables = getTables(environment)
    for table in config['main']['tablesNotToCompare'].split(','):
        if table in listTables:
            listTables.remove(table)
        else:
            errorLog.append("[WARN] There is no table " + table + " in table list")
    return listTables

def getColumnList(environment):
    connection = pymysql.connect(host=config[environment][client + '.host'],
                                 user=config[environment][client + '.user'],
                                 password=config[environment][client + '.password'],
                                 db=config[environment][client + '.db'],
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            columnList = []
            queryGetColumnList = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s' AND table_schema = '%s';" % (tableName, config[environment][client + '.db'])
            cursor.execute(queryGetColumnList)
            columnDict = cursor.fetchall()
            for i in columnDict:
                element = 't.' + str(i.get('column_name')).lower()  # It's neccessary to make possible queries like "select key form keyname;"
                columnList.append(element)
            for column in config['main']['hideColumns'].split(','):
                if 't.' + column.replace('_', '') in columnList:
                    columnList.remove('t.' + column)
            if columnList == []:
                return False
            columnString = ','.join(columnList)
            return columnString.lower()
    finally:
        connection.close()

def getTables(environment):
    connection = pymysql.connect(host=config[environment][client + '.host'],
                                 user=config[environment][client + '.user'],
                                 password=config[environment][client + '.password'],
                                 db=config[environment][client + '.db'],
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            showTables = "SELECT distinct(table_name) FROM information_schema.columns WHERE table_schema LIKE '%s';" % config[environment][client + '.db']
            listTable = []
            cursor.execute(showTables)
            result = cursor.fetchall()
            for item in result:
                listTable.append(item.get('table_name'))
    finally:
        connection.close()
    return listTable

def isDataDiffers(prodResult, testResult, i):
    if not cmpDicts(prodResult[i], testResult[i]):
        errorLog.append("[ERROR] Tables " + tableName + " differs!")
        differTables.append(tableName)
        return True
    else:
        return False

def iterateByData(prodResult, testResult):
    for i in range(0, min(len(prodResult), len(testResult))):
        if isDataDiffers(prodResult, testResult, i):
            saveDiffData(prodResult, testResult)
            return False
        else:
            return True

def parallelComparing(array):
    firstLimit = 0
    prodToCompare = []
    while True:
        if config.getboolean('main', 'quickCheck'):
            if firstLimit > int(config.getint('main', 'amountCheckingRecords')):
                break
        firstLimit = firstLimit + step
        selectResults = pool.map(runSelect, [[array[0], firstLimit + 1, firstLimit + step], [array[1], firstLimit + 1, firstLimit + step]])
        prodResult = selectResults[0]
        testResult = selectResults[1]
        if prodResult == [] and testResult == []:
            print("Both tables " + tableName + " are empty...")
        if (prodResult == [] and testResult != []) or (testResult == [] and prodResult != []):
            if prodResult == []:
                errorLog.append("[WARN] Table " + tableName + " on production server is empty...")
                break
            else:
                errorLog.append("Table " + tableName + " on test server is empty...")
                break
        if prodToCompare == prodResult:
            break
        prodToCompare = selectResults[0]
        if not compareDataLength(prodResult, testResult) or not iterateByData(prodResult, testResult) or not checkDataSetLength(prodResult, testResult):
            break

def prepareColumnMapping(environment):
    connection = pymysql.connect(host=config[environment][client + '.host'],
                                 user=config[environment][client + '.user'],
                                 password=config[environment][client + '.password'],
                                 db=config[environment][client + '.db'],
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            columnList = []
            mapping = {}
            queryGetColumn = "SELECT distinct(column_name) FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s' AND column_name LIKE '%%id' OR column_name LIKE '%%remoteid' ORDER BY column_name asc;" % config[environment][client + '.db']
            cursor.execute(queryGetColumn)
            rawColumnList = cursor.fetchall()
            for i in rawColumnList:
                columnList.append(i.get('column_name').lower())
            for column in columnList:
                if '_' in column:
                    column = column.replace(column[column.rfind('_')], '')
                if 'remoteid' in column:
                    if column[:-8] in listTables:
                        mapping.update({column: column[:-8]})
                else:
                    if column[:-2] in listTables:
                        mapping.update({column: column[:-2]})
                    else:
                        errorLog.append("[WARN] Column " + column + " have no appropriate table...")
            return mapping
    finally:
        connection.close()

def printSummary(errorLog):
    print("[SUMMARY] This tables differs " + ', '.join(differTables))
    print("Detalization:\n")
    for string in errorLog:
        print(string)

def queryReportConstruct(environment):
    columnString = getColumnList(environment)
    setColumnList = ''
    setJoinSection = ''
    tmpOrderList = []
    for column in columnString.split(','):
        if column[2:] in list(mapping):
            setColumnList = setColumnList + mapping.get(column[2:]) + '.remoteid as ' + column[2:] + ','
            setJoinSection = setJoinSection + 'JOIN ' + mapping.get(column[2:]) + ' ON t.' + column[2:] + '=' + mapping.get(column[2:]) + '.id '
        else:
            setColumnList = setColumnList + column + ','
    if setColumnList[-1:] == ',':
        setColumnList = setColumnList[:-1]
    for i in setColumnList.split(','):
        if ' as ' in i:
            tmpOrderList.append(i[i.rfind(' '):])
        else:
            tmpOrderList.append(i)
    setOrderList = ', '.join(tmpOrderList)
    dt = countReportDate(environment)
    if not dt:
        return False
    else:
        getReportQuery = "SELECT %s FROM %s AS t %s WHERE t.dt>='%s' ORDER BY %s LIMIT " % (setColumnList, tableName, setJoinSection, dt, setOrderList)
    return getReportQuery

def saveDiffData(prodResult, testResult):
    prodFile = '/mxf/data/test_results/%s/prod_%s.tsv' % (client, tableName)
    testFile = '/mxf/data/test_results/%s/test_%s.tsv' % (client, tableName)
    prodHeader = list(prodResult[0])
    testHeader = list(testResult[0])
    prodHeader.sort()
    testHeader.sort()
    with open(prodFile, 'w') as file:
        for header in prodHeader:
            file.write(header + "\t")
        file.write("\n")
        for data in prodResult:
            for header in prodHeader:
                file.write(str(data.get(header)) + "\t")
            file.write("\n")
        print("File " + prodFile + " successfully saved...")
    with open(testFile, 'w') as file:
        for header in prodHeader:
            file.write(header + "    \n")
        file.write("\n")
        for data in testResult:
            for header in prodHeader:
                file.write(str(data.get(header)) + "\t")
            file.write("\n")
        print("File " + testFile + " successfully saved...")

def runSelect(args):
    connection = pymysql.connect(host=config[args[0]][client + '.host'],
                                 user=config[args[0]][client + '.user'],
                                 password=config[args[0]][client + '.password'],
                                 db=config[args[0]][client + '.db'],
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)
    errorCount = 0
    while errorCount < attempts:
        try:
            with connection.cursor() as cursor:
                columnList = getColumnList(args[0])
                if not columnList:
                    print("Column list for table " + tableName + ' is empty')
                    return []
                if ('impression' and 'dt' in columnList) and 'report' in tableName:
                    rawSelect = queryReportConstruct(args[0])
                    if not rawSelect:
                        print("Report table " + tableName + " is empty...")
                        return []
                    selectQuery = rawSelect + '%s, %s;' % (args[1], args[2])
                    if not config.getboolean('main', 'hideSQLQueries'):
                        print(selectQuery)
                else:
                    selectQuery = 'SELECT %s FROM %s AS t ORDER BY %s ASC LIMIT %s,%s;' % (columnList, tableName, columnList, args[1], args[2])
                    if not config.getboolean('main', 'hideSQLQueries'):
                        print(selectQuery)
                cursor.execute(selectQuery)
                result = cursor.fetchall()
                return result
        except pymysql.OperationalError:
            errorCount += 1
            errorLog.append("[ERROR] Lost connection to MySQL server during query. Start attempt " + str(errorCount))
        finally:
            try:
                connection.close()
            except pymysql.Error:
                print("Connection already closed...")
                return []

for client in config['main']['clients'].split(','):
    os.mkdir('/mxf/data/test_results/' + client)
    print('\nStart ' + client + ' processing!\n')
    pool = ThreadPool(2)
    listTables = deleteServiceTables('test-servers')
    mapping = prepareColumnMapping('prod-servers')
    startTime = datetime.datetime.today()
    for tableName in listTables:
        print("Table " + tableName + " in progress...")
        parallelComparing(['prod-servers', 'test-servers'])
    errorLog.append('Tables, which differs are: ' + str(differTables))
    printSummary(errorLog)
    subject = "[Test] Check databases for client %s" % client
    helper.sendmail('\n'.join(errorLog), fromaddr, toaddr, mypass, subject, None)
    print("Client " + client + " processed in " + str(datetime.datetime.today() - startTime))
