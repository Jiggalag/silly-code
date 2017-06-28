import datetime
import configparser
import os
import shutil
import sys
sys.path.append(os.getcwd() + '/py_scripts')
from py_scripts.helpers import configHelper, converters, dbHelper, helper, loggingHelper

propertyFile = os.getcwd() + "/resources/properties/sqlComparator.properties"
logFile = "/home/jiggalag/comparatorLog.txt"
config = configHelper.ifmsConfigCommon(propertyFile)

logger = loggingHelper.Logger(config.getPropertyFromMainSection("loggingLevel"))

sendMailFrom = config.getPropertyFromMainSection("sendMailFrom")
sendMailTo = config.getPropertyFromMainSection("sendMailTo")
mailPassword = config.getPropertyFromMainSection("mailPassword")

# sqlProperties
attempts = config.getProperty("sqlProperties", "retryAttempts")  # amount of attempts of retrying SQL-query
comparingStep = config.getProperty("sqlProperties", "comparingStep")
depthReportCheck = config.getProperty("sqlProperties", "depthReportCheck")
enableSchemaChecking = config.getProperty("sqlProperties", "enableSchemaChecking")
excludedTables = config.getProperty("sqlProperties", "tablesNotToCompare")
failWithFirstError = config.getProperty("sqlProperties", "failWithFirstError")
mode = config.getProperty("sqlProperties", "reportCheckType")
schemaColumns = config.getProperty("sqlProperties", "includeSchemaColumns")
hideColumns = config.getProperty("sqlProperties", "hideColumns")
serviceDir = config.getPropertyFromMainSection("serviceDir")

dbProperties = {
    'attempts': attempts,
    'comparingStep': comparingStep,
    'hideColumns': hideColumns,
    'mode': mode
}


def calculateDate(days):
    return (datetime.datetime.today().date() - datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


def checkDateList(table, emptyTables, emptyProdTables, emptyTestTables, client):
    selectQuery = "SELECT distinct(`dt`) from {};".format(table)
    prodDates, testDates = dbHelper.dbConnector.runParallelSelect(clientConfig, client, selectQuery, dbProperties)
    if all([prodDates, testDates]):
        return calculateComparingTimeframe(prodDates, testDates, table)
    else:
        if not prodDates and not testDates:
            logger.warn("Table {} is empty in both dbs...".format(table))
            emptyTables.append(table)
        elif not prodDates:
            prodDb = config.getProperty('sqlParameters', 'prod.' + client + '.sqlDb')
            logger.warn("Table {} on {} is empty!".format(table, prodDb))
            emptyProdTables.append(table)
        else:
            testDb = config.getProperty('sqlParameters', 'test.' + client + '.sqlDb')
            logger.warn("Table {} on {} is empty!".format(table, testDb))
            emptyTestTables.append(table)
        return []


def calculateComparingTimeframe(prodDates, testDates, table):
    actualDates = set()
    for days in range(1, depthReportCheck):
        actualDates.add(calculateDate(days))
    if prodDates[-depthReportCheck:] == testDates[-depthReportCheck:]:
        return getComparingTimeframe(prodDates)
    else:
        return getTimeframeIntersection(prodDates, testDates, table)


def calculateSectionName(query):
    tmpList = query.split(" ")
    for item in tmpList:
        if "GROUP" in item:
            return tmpList[tmpList.index(item) + 2][2:].replace("_", "").replace("id", "")


def checkServiceDir(serviceDir):
    if os.path.exists(serviceDir):
        shutil.rmtree(serviceDir)
    os.mkdir(serviceDir)


def cmpReports(emptyProdTables, emptyTables, emptyTestTables, globalBreak, mapping, noCrossedDatesTables,
               stopCheckingThisTable, table):
    dates = converters.convertToList(checkDateList(table, emptyTables, emptyProdTables, emptyTestTables, client))
    dates.sort()
    if dates:
        prodRecordAmount, testRecordAmount = countTableRecords(table, dates[0])
        for dt in reversed(dates):
            if not all([globalBreak, stopCheckingThisTable]):
                maxAmountOfRecords = max(prodRecordAmount, testRecordAmount)
                queryList = queryReportConstruct(table, dt, mode, maxAmountOfRecords, comparingStep, mapping)
                globalBreak, stopCheckingThisTable = iterationComparingByQueries(queryList, globalBreak, table)
            else:
                break
    else:
        logger.warn(
            "Tables {} should not be compared correctly, because they have no any crosses dates in reports".format(
                table))
        noCrossedDatesTables.append(table)


def compareData(tables, tablesWithDifferentSchema, globalBreak, noCrossedDatesTables, emptyTables, emptyProdTables, emptyTestTables, differingTables):
    tables = prepareTableList(tables, tablesWithDifferentSchema)
    mapping = prepareColumnMapping("prod")
    for table in tables:
        table = "creative" # TODO: remove this hack after refactor finishing
        logger.info("Table {} processing now...".format(table))
        startTableCheckTime = datetime.datetime.now()
        stopCheckingThisTable = False
        if (('report' in table) or ('statistic' in table)) and ('dt' in getColumnList('prod', table)):
            if not globalBreak:
                cmpReports(emptyProdTables, emptyTables, emptyTestTables, globalBreak, mapping, noCrossedDatesTables, stopCheckingThisTable, table)
                logger.info("Table {} checked in {}...".format(table, datetime.datetime.now() - startTableCheckTime))
            else:
                logger.info("Table {} checked in {}...".format(table, datetime.datetime.now() - startTableCheckTime))
                break
        else:
            prodRecordAmount, testRecordAmount = countTableRecords(table, None)
            if prodRecordAmount == 0 and testRecordAmount == 0:
                logger.warn("Table {} is empty on both servers!".format(table))
                continue
            if prodRecordAmount == 0:
                logger.warn("Table {} is empty on prod-server!".format(table))
                continue
            if testRecordAmount == 0:
                logger.warn("Table {} is empty on test-server!".format(table))
                continue
            maxAmountOfRecords = max(prodRecordAmount, testRecordAmount)
            queryList = queryEntityConstructor(table, maxAmountOfRecords, comparingStep, mapping)
            if not globalBreak:
                for query in queryList:
                    if (not compareEntityTable(table, query, differingTables)) and failWithFirstError:
                        logger.info("First error founded, checking failed. Comparing takes {}".format(datetime.datetime.now() - startTime))
                        globalBreak = True
                        logger.info("Table {} checked in {}...".format(table, datetime.datetime.now() - startTableCheckTime))
                        break
                logger.info("Table {} checked in {}...".format(table, datetime.datetime.now() - startTableCheckTime))
            else:
                logger.info("Table {} checked in {}...".format(table, datetime.datetime.now() - startTableCheckTime))
                break
    dataComparingTime = datetime.datetime.now() - startTime
    logger.info("Comparing finished in {}".format(dataComparingTime))
    return dataComparingTime


def compareEntityTable(table, query, differingTables):
    header = getHeader(query)
    prodEntities, testEntities = dbHelper.dbConnector.runParallelSelect(clientConfig, client, query, dbProperties)
    prodUniqueEntities = set(prodEntities) - set(testEntities)
    testUniqueEntities = set(testEntities) - set(prodEntities)
    if len(prodUniqueEntities) > 0:
        writeUniqueEntitiesToFile(table, prodUniqueEntities, "prod", header)
    if len(testUniqueEntities) > 0:
        writeUniqueEntitiesToFile(table, testUniqueEntities, "test", header)
    if not all([len(prodUniqueEntities) == 0, len(testUniqueEntities) == 0]):
        logger.error("Tables {} differs!".format(table))
        differingTables.append(table)
        return False
    else:
        return True


def compareReportSums(table, query, differingTables):
    prodReports, testReports = dbHelper.dbConnector.runParallelSelect(clientConfig, client, query, dbProperties, "list")
    clicks = True
    imps = True
    prodImps = prodReports[0]
    testImps = testReports[0]
    prodClicks = prodReports[1]
    testClicks = testReports[1]
    if prodClicks != testClicks:
        clicks = False
        logger.warn("There are different click sums for query {}. Prod clicks={}, test clicks={}".format(query, prodClicks, testClicks))
    if prodImps != testImps:
        imps = False
        logger.warn("There are different imp sums for query {}. Prod imps={}, test imps={}".format(query, prodImps, testImps))
    if not all([clicks, imps]):
        logger.error("Tables {} differs!".format(table))
        differingTables.append(table)
        return False
    else:
        return True


def compareReportDetailed(table, query):
    header = getHeader(query)
    prodReports, testReports = dbHelper.dbConnector.runParallelSelect(clientConfig, client, query, dbProperties)
    prodUniqueReports = prodReports - testReports
    testUniqueReports = testReports - prodReports
    if len(prodUniqueReports) > 0:
        writeUniqueEntitiesToFile(table, prodUniqueReports, "prod", header)
    if len(testUniqueReports) > 0:
        writeUniqueEntitiesToFile(table, testUniqueReports, "test", header)
    if not all([len(prodUniqueReports) == 0, len(testUniqueReports) == 0]):
        logger.error("Tables {} differs!".format(table))
        differingTables.append(table)
        return False
    else:
        return True


def compareTableLists():
    selectQuery = "SELECT DISTINCT(TABLE_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'DBNAME';"
    prodTables, testTables = dbHelper.dbConnector.runParallelSelect(clientConfig, client, selectQuery, dbProperties)
    if prodTables == testTables:
        return prodTables
    else:
        return getIntersectedTables(prodTables, testTables)


def compareTablesMetadata(tables):
    tablesWithDifferentSchema = []
    for table in tables:
        logger.info("Check schema for table {}...".format(table))
        selectQuery = "SELECT {} FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'DBNAME' and TABLE_NAME='TABLENAME' ORDER BY COLUMN_NAME;".replace("TABLENAME", table).format(', '.join(schemaColumns))
        prodColumns, testColumns = dbHelper.dbConnector.runParallelSelect(clientConfig, client, selectQuery, dbProperties)
        uniqForProd = set(prodColumns) - set(testColumns)
        uniqForTest = set(testColumns) - set(prodColumns)
        if len(uniqForProd) > 0:
            prodDb = config.getProperty('sqlParameters', 'prod.' + client + '.sqlDb')
            logger.error("Elements, unique for table {} in {} db:{}".format(table, prodDb, uniqForProd))
        if len(uniqForTest) > 0:
            testDb = config.getProperty('sqlParameters', 'test.' + client + '.sqlDb')
            logger.error("Elements, unique for table {} in {} db:{}".format(table, testDb, uniqForTest))
        if not all([len(uniqForProd) == 0, len(uniqForTest) == 0]):
            logger.error(" [ERROR] Tables {} differs!".format(table))
            tablesWithDifferentSchema.append(table)
            if failWithFirstError:
                logger.critical("First error founded, checking failed...")
                return False
    return tablesWithDifferentSchema


def countTableRecords(table, date):
    if date is None:
        query = "SELECT COUNT(*) FROM `{}`;".format(table)
    else:
        query = "SELECT COUNT(*) FROM `{}` WHERE dt > '{}';".format(table, date)
    return dbHelper.dbConnector.runParallelSelect(clientConfig, client, query, dbProperties)


def createTestDir(path, client):
    if not os.path.exists(path):
        os.mkdir(path)
    if not os.path.exists(path + client):
        os.mkdir(path + client)


def generateMailText(emptyTables, differingTables, noCrossedDatesTables, prodUniqueTables, testUniqueTables):
    body = "Initial conditions:\n\n"
    if enableSchemaChecking:
        body = body + "1. Schema checking enabled.\n"
    else:
        body = body + "1. Schema checking disabled.\n"
    if failWithFirstError:
        body = body + "2. Failed with first founded error.\n"
    else:
        body = body + "2. Find all errors\n"
    body = body + "3. Report checkType is " + mode + "\n\n"
    if any([emptyTables, differingTables, noCrossedDatesTables, prodUniqueTables, testUniqueTables]):
        body = getTestResultText(body, differingTables, emptyTables, noCrossedDatesTables,
                                 prodUniqueTables, testUniqueTables)
    else:
        body = body + "It is impossible! There is no any problems founded!"
    if enableSchemaChecking:
        body = body + "Schema checked in " + str(schemaComparingTime) + "\n"
    body = body + "Dbs checked in " + str(dataComparingTime) + "\n"
    return body


def getColumnList(stage, table):
    # Function returns column list for sql-query for report table
    sql = dbHelper.dbConnector(clientConfig.getSQLConnectParams(stage))
    try:
        with sql.connection.cursor() as cursor:
            columnList = []
            queryGetColumnList = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{}' AND table_schema = '{}';".format(table, sql.db)
            logger.debug(queryGetColumnList)
            cursor.execute(queryGetColumnList)
            columnDict = cursor.fetchall()
            for item in columnDict:
                for key in item.keys():
                    columnList.append("`{}`".format(item.get(key).lower()))
            for column in hideColumns:
                if "`{}`".format(column) in columnList:
                    columnList.remove("`{}`".format(column))
            if columnList == []:
                return []
            columnString = ",".join(columnList)
            return columnString
    finally:
        sql.connection.close()


def getColumnListForSum(setColumnList):
    columnListWithSums = []
    for item in setColumnList.split(","):
        if "clicks" in item or "impressions" in item:
            columnListWithSums.append("sum(" + item + ")")
        else:
            columnListWithSums.append(item)
    return columnListWithSums


def getComparingTimeframe(prodDates):
    comparingTimeframe = []
    for item in prodDates[-depthReportCheck:]:
        comparingTimeframe.append(item.date().strftime("%Y-%m-%d"))
    return comparingTimeframe


def getHeader(query):
    cutSelect = query[7:]
    columns = cutSelect[:cutSelect.find("FROM") - 1]
    header = []
    for item in columns.split(","):
        if ' as ' in item:
            header.append(item[:item.find(' ')])
        else:
            header.append(item)
    return header


def getIntersectedTables(prodTables, testTables):
    prodSet = set(prodTables)
    testSet = set(testTables)
    prodUniqueTables = prodSet - testSet
    testUniqueTables = testSet - prodSet
    if len(prodUniqueTables) > 0:
        prodDb = config.getProperty('sqlParameters', 'prod.' + client + '.sqlDb')
        logger.warn("Tables, which unique for {} db {}.".format(prodDb, prodUniqueTables))
    if len(testUniqueTables) > 0:
        testDb = config.getProperty('sqlParameters', 'test.' + client + '.sqlDb')
        logger.warn("Tables, which unique for {} db {}.".format(testDb, testUniqueTables))
    if len(prodSet) >= len(testSet):
        for item in prodUniqueTables:
            prodSet.remove(item)
        return converters.convertToList(prodSet)
    else:
        for item in testUniqueTables:
            testSet.remove(item)
        return converters.convertToList(testSet)


def getTestResultText(body, differingTables, emptyTables, noCrossedDatesTables,
                      prodUniqueTables, testUniqueTables):
    body = body + "There are some problems found during checking.\n\n"
    if emptyTables:
        body = body + "Tables, empty in both dbs:\n" + ",".join(emptyTables) + "\n\n"
    if emptyProdTables:
        body = body + "Tables, empty on production db:\n" + ",".join(emptyProdTables) + "\n\n"
    if emptyTestTables:
        body = body + "Tables, empty on test db:\n" + ",".join(emptyTestTables) + "\n\n"
    if differingTables:
        body = body + "Tables, which have any difference:\n" + ",".join(differingTables) + "\n\n"
    if list(set(emptyTables).difference(set(noCrossedDatesTables))):
        body = body + "Report tables, which have no crossing dates:\n" + ",".join(
            list(set(emptyTables).difference(set(noCrossedDatesTables)))) + "\n\n"
    if prodUniqueTables:
        body = body + "Tables, which unique for production db:\n" + ",".join(
            converters.convertToList(prodUniqueTables)) + "\n\n"
    if testUniqueTables:
        body = body + "Tables, which unique for test db:\n" + ",".join(
            converters.convertToList(testUniqueTables)) + "\n\n"
    return body


def getTimeframeIntersection(prodDates, testDates, table):
    prodSet = set(prodDates)
    testSet = set(testDates)
    if (prodSet - testSet):  # this code (4 strings below) should be moved to different function
        uniqueDates = getUniqueReportDates(prodSet, testSet)
        testDb = config.getProperty('sqlParameters', 'test.' + client + '.sqlDb')
        logger.warn("This dates absent in {}: {} in report table {}...".format(testDb, ",".join(uniqueDates), table))
    if (testSet - prodSet):
        uniqueDates = getUniqueReportDates(testSet, prodSet)
        prodDb = config.getProperty('sqlParameters', 'prod.' + client + '.sqlDb')
        logger.warn("This dates absent in {}: {} in report table {}...".format(prodDb, ",".join(uniqueDates), table))
    return prodSet & testSet


def getUniqueReportDates(firstDateList, secondDateList):
    uniqueDates = []
    for item in converters.convertToList(firstDateList - secondDateList):
        uniqueDates.append(item.strftime("%Y-%m-%d %H:%M:%S"))
    return uniqueDates


def iterationComparingByQueries(queryList, globalBreak, table):
    stopCheckingThisTable = False
    for query in queryList:
        if mode == "day-sum":
            if ("impressions" and "clicks") in getColumnList("prod", table):
                if not compareReportSums(table, query, differingTables) and failWithFirstError:
                    logger.critical("First error founded, checking failed. Comparing takes {}.".format(datetime.datetime.now() - startTime))
                    globalBreak = True
                    return globalBreak, stopCheckingThisTable
            else:
                logger.warn("There is no impression of click column in table {}".format(table))
                stopCheckingThisTable = True
                return globalBreak, stopCheckingThisTable
        elif mode == "section-sum" or mode == "detailed":
            if mode == "section-sum":
                section = calculateSectionName(query)
                logger.info("Check section {} for table {}".format(section, table))
            if not compareReportDetailed(table, query) and failWithFirstError:
                logger.critical("First error founded, checking failed. Comparing takes {}.".format(datetime.datetime.now() - startTime))
                globalBreak = True
                return globalBreak, stopCheckingThisTable
    return globalBreak, stopCheckingThisTable


def prepareColumnMapping(stage):
    sql = dbHelper.dbConnector(clientConfig.getSQLConnectParams(stage))
    try:
        with sql.connection.cursor() as cursor:
            columnDict = {}
            queryGetColumn = "select column_name, referenced_table_name from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where constraint_name not like 'PRIMARY' and referenced_table_name is not null and table_schema = '{}';".format(sql.db)
            logger.debug(queryGetColumn)
            cursor.execute(queryGetColumn)
            rawColumnList = cursor.fetchall()
            for item in rawColumnList:
                columnDict.update({"`{}`".format(item.get('column_name').lower()): "`{}`".format(item.get('referenced_table_name').lower())})
            return columnDict
    finally:
        sql.connection.close()


def prepareQuerySections(table, mapping):
    columnString = getColumnList("prod", table)
    setColumnList = "" # ?
    setJoinSection = "" # ?
    tmpOrderList = []
    setColumnList, setJoinSection = constructColumnAndJoinSection(columnString, mapping, setColumnList, setJoinSection, table)
    if setColumnList[-1:] == ",":
        setColumnList = setColumnList[:-1]
    setOrderList = constructOrderList(setColumnList, tmpOrderList)
    columns = ",".join(setOrderList)
    return columnString, setColumnList, setJoinSection, columns


def constructOrderList(setColumnList, tmpOrderList):
    for i in setColumnList.split(","):
        if " as " in i:
            tmpOrderList.append(i[i.rfind(" "):])
        else:
            tmpOrderList.append(i)
    setOrderList = []
    if "dt" in tmpOrderList:
        setOrderList.append("dt")
    if "campaignid" in tmpOrderList:
        setOrderList.append("campaignid")
    for item in tmpOrderList:
        if "id" in item and "campaignid" not in item:
            setOrderList.append(item)
    for item in tmpOrderList:
        if item not in setOrderList:
            setOrderList.append(item)
    return setOrderList


def constructColumnAndJoinSection(columnString, mapping, setColumnList, setJoinSection, table):
    for column in columnString.split(","):
        if column in list(mapping.keys()):
            linkedTable = mapping.get(column)
            if "remoteid" in column:
                # if "remoteid" in getColumnList("prod", column[:-8]):
                #     setColumnList = setColumnList + linkedTable + ".`remoteid` as " + column + ","
                # else:
                setColumnList = setColumnList + linkedTable + ".`remoteid` as " + column + ","
            elif "id" in column:
                if "remoteid" in getColumnList("prod", linkedTable):
                    setColumnList = setColumnList + linkedTable + ".`remoteid` as " + column + ","
                else:
                    setColumnList = setColumnList + linkedTable + ".`id` as " + column + ","
            else:
                if "remoteid" in getColumnList("prod", column):
                    setColumnList = setColumnList + linkedTable + ".`remoteid` as " + column + ","
                else:
                    setColumnList = setColumnList + linkedTable + ".`id` as " + column + ","
            if "`{}`".format(table) == linkedTable:
                setJoinSection = ""
            else:
                setJoinSection = setJoinSection + "JOIN " + linkedTable + " ON " + column + "=" + linkedTable + ".`id` "
        else:
            setColumnList = setColumnList + table + "." + column + ","
    return setColumnList, setJoinSection


def prepareTableList(tables, tablesWithDifferentSchema):
    for table in excludedTables:
        if table in tables:
            tables.remove(table)
    if tablesWithDifferentSchema is not None:
        for table in tablesWithDifferentSchema:
            if table in tables:
                tables.remove(table)
    try:
        if len(clientIgnoreTables) > 0:
            for table in clientIgnoreTables.split(","):
                if table in tables:
                    tables.remove(table)
    except configparser.NoOptionError:
        logger.warn("Property {}.ignoreTables in section [specificIgnoredTables] absent.".format(client))
    return tables


def prepareToTest(client):
    createTestDir("/mxf/data/test_results/", client)
    startTime = datetime.datetime.now()
    logger.info("Start {} processing!".format(client))
    tables = converters.convertToList(compareTableLists())
    return startTime, tables


def queryEntityConstructor(table, threshold, comparingStep, mapping):
    queryList = []
    columnString, setColumnList, setJoinSection, setOrderList = prepareQuerySections(table, mapping)
    query = "SELECT {} FROM `{}` ".format(setColumnList, table)
    if setJoinSection:
        query = query + " {}".format(setJoinSection)
    if setOrderList:
        query = query + " ORDER BY {}".format(setOrderList)
    if threshold > comparingStep:
        offset = 0
        while offset < threshold:
            queryWithLimit = query + " LIMIT {},{};".format(offset, comparingStep)
            offset = offset + comparingStep
            queryList.append(queryWithLimit)
    else:
        queryList.append(query + ";")
    return queryList


def queryReportConstruct(table, dt, mode, threshold, comparingStep, mapping):
    queryList = []
    if mode == "day-sum":
        query = "SELECT SUM(IMPRESSIONS), SUM(CLICKS) FROM {} WHERE dt = '{}';".format(table, dt)
        queryList.append(query)
    elif mode == "section-sum":
        sections = []  # Sections for imp-aggregating
        columnString, setColumnList, setJoinSection, setOrderList = prepareQuerySections(table, mapping)
        for column in columnString.split(","):
            if "id" == column[-2:]:
                sections.append(column)
                columnListWithSums = getColumnListForSum(setColumnList)
                query = "SELECT {} FROM `{}` {} WHERE dt = '{}' GROUP BY {} ORDER BY {};".format(",".join(columnListWithSums), table, setJoinSection, dt, column, setOrderList)
                queryList.append(query)
    elif mode == "detailed":
        offset = 0
        while offset < threshold:
            columnString, setColumnList, setJoinSection, setOrderList = prepareQuerySections(table, mapping)
            query = "SELECT {} FROM `{}` {} WHERE t.dt>='{}' ORDER BY {} LIMIT {},{};".format(setColumnList, table, setJoinSection, dt, setOrderList, offset, comparingStep)
            offset = offset + comparingStep
            queryList.append(query)
    else:
        logger.error("Property reportCheckType has incorrect value {}. Please, set any of this value: day-sum, section-sum, detailed.".format(mode))
        sys.exit(1)
    return queryList


def writeHeader(fileName, header):
    with open(fileName, 'w') as file:
        file.write(','.join(header) + '\n')


def writeUniqueEntitiesToFile(table, listUniqs, stage, header):
    logger.error("There are {0} unique elements in table {1} on {2}-server. Detailed list of records saved to {3}{1}_uniqRecords_{2}".format(len(listUniqs), table, stage, serviceDir))
    fileName = "{}{}_uniqRecords_{}".format(serviceDir, table, stage)
    if not os.path.exists(fileName):
        writeHeader(fileName, header)
    with open(fileName, "a") as file:
        firstList = converters.convertToList(listUniqs)
        firstList.sort()
        for item in firstList:
            file.write(str(item) + "\n")


checkServiceDir(serviceDir)
for client in config.getClients():
    clientConfig = configHelper.ifmsConfigClient(propertyFile, client)
    sqlPropertyDict = clientConfig.getSQLConnectParams('test')
    clientIgnoreTables = config.getProperty("specificIgnoredTables", client + ".ignoreTables")
    if clientIgnoreTables is None:
        clientIgnoreTables = 0
    noCrossedDatesTables = []
    emptyTables = []
    differingTables = []
    emptyProdTables = emptyTestTables = []
    prodUniqueTables = testUniqueTables = set()
    globalBreak = False
    startTime, tables = prepareToTest(client)
    if enableSchemaChecking:
        tablesWithDifferentSchema = compareTablesMetadata(tables)
        if not tablesWithDifferentSchema and failWithFirstError:
            schemaComparingTime = str(datetime.datetime.now() - startTime)
            logger.info("Schema partially compared in {}".format(schemaComparingTime))
        else:
            schemaComparingTime = str(datetime.datetime.now() - startTime)
            logger.info("Schema compared in {}".format(schemaComparingTime))
            dataComparingTime = compareData(tables, tablesWithDifferentSchema, globalBreak, noCrossedDatesTables, emptyTables, emptyProdTables, emptyTestTables, differingTables)
    else:
        logger.info("Schema checking disabled...")
        tablesWithDifferentSchema = []
        dataComparingTime = compareData(tables, [], globalBreak, noCrossedDatesTables, emptyTables, emptyProdTables, emptyTestTables, differingTables)
    subject = "[Test] Check databases for client {}".format(client)
    body = generateMailText(emptyTables, differingTables, noCrossedDatesTables, prodUniqueTables, testUniqueTables)
    helper.sendmail(body, sendMailFrom, sendMailTo, mailPassword, subject, None)
