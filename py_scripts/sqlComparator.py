# TODO: add structure to store any information about something db-lists
# TODO: add structure to store any differences

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

tableStructure = {
    "ignoredTables": set(),
    "prodEmpty": set(),
    "testEmpty": set(),
    "prodList": [],
    "testList": [],
    "crossedList": [],
    "prodUnique": set(),
    "testUnique": set(),
    "prodUniqueColumns": set(),
    "testUniqueColumns": set(),
    "noCrossedDates": set(),
    "diffSchema": set(),
    "diffData": set()
    }



def calculateDate(days):
    return (datetime.datetime.today().date() - datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

# TODO: too many args
def checkDateList(table, emptyTables, emptyProdTables, emptyTestTables, client):
    selectQuery = "SELECT distinct(`dt`) from {};".format(table)
    dateList = dbHelper.dbConnector.runParallelSelect(clientConfig, client, selectQuery, dbProperties)
    if all(dateList):
        return calculateComparingTimeframe(dateList, table)
    else:
        if not dateList[0] and not dateList[1]:
            logger.warn("Table {} is empty in both dbs...".format(table))
            emptyTables.append(table)
        elif not dateList[0]:
            prodDb = config.getProperty('sqlParameters', 'prod.' + client + '.sqlDb')
            logger.warn("Table {} on {} is empty!".format(table, prodDb))
            emptyProdTables.append(table)
        else:
            testDb = config.getProperty('sqlParameters', 'test.' + client + '.sqlDb')
            logger.warn("Table {} on {} is empty!".format(table, testDb))
            emptyTestTables.append(table)
        return []


def calculateComparingTimeframe(dateList, table):
    actualDates = set()
    for days in range(1, depthReportCheck):
        actualDates.add(calculateDate(days))
    if dateList[0][-depthReportCheck:] == dateList[1][-depthReportCheck:]:
        return getComparingTimeframe(dateList)
    else:
        return getTimeframeIntersection(dateList, table)


def calculateSectionName(query):
    tmpList = query.split(" ")
    for item in tmpList:
        if "GROUP" in item:
            return tmpList[tmpList.index(item) + 2][2:].replace("_", "").replace("id", "")


def checkServiceDir(serviceDir):
    if os.path.exists(serviceDir):
        shutil.rmtree(serviceDir)
    os.mkdir(serviceDir)

# TODO: too many args
def cmpReports(emptyProdTables, emptyTables, emptyTestTables, globalBreak, mapping, noCrossedDatesTables,
               stopCheckingThisTable, table):
    dates = converters.convertToList(checkDateList(table, emptyTables, emptyProdTables, emptyTestTables, client))
    dates.sort()
    if dates:
        amountRecords = countTableRecords(table, dates[0])
        for dt in reversed(dates):
            if not all([globalBreak, stopCheckingThisTable]):
                maxAmountOfRecords = max(amountRecords[0][0].get("COUNT(*)"), amountRecords[1][0].get("COUNT(*)"))
                queryList = queryReportConstruct(table, dt, mode, maxAmountOfRecords, comparingStep, mapping)
                globalBreak, stopCheckingThisTable = iterationComparingByQueries(queryList, globalBreak, table)
            else:
                break
    else:
        logger.warn(
            "Tables {} should not be compared correctly, because they have no any crosses dates in reports".format(
                table))
        noCrossedDatesTables.append(table)

# TODO: too many args
def compareData(tableStructure, globalBreak):
    prepareTableList(tableStructure)
    mapping = prepareColumnMapping("prod")
    for table in tableStructure.get("crossedList"):
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
            amountRecords = countTableRecords(table, None)
            maxAmountOfRecords = max(amountRecords[0][0].get("COUNT(*)"), amountRecords[1][0].get("COUNT(*)"))
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
    listEntities = getTableData(dbHelper.dbConnector.runParallelSelect(clientConfig, client, query, dbProperties), header)
    uniqFor0 = listEntities[0] - listEntities[1]
    uniqFor1 = listEntities[1] - listEntities[0]
    if len(uniqFor0) > 0:
        writeUniqueEntitiesToFile(table, uniqFor0, "prod", header)
    if len(uniqFor1) > 0:
        writeUniqueEntitiesToFile(table, uniqFor1, "test", header)
    if not all([len(uniqFor0) == 0, len(uniqFor1) == 0]):
        logger.error("Tables {} differs!".format(table))
        differingTables.append(table)
        return False
    else:
        return True


def compareReportSums(table, query, differingTables):
    listReports = dbHelper.dbConnector.runParallelSelect(clientConfig, client, query, dbProperties)
    clicks = imps = True
    prodClicks = int(listReports[0][0].get("SUM(CLICKS)"))
    testClicks = int(listReports[1][0].get("SUM(CLICKS)"))
    prodImps = int(listReports[0][0].get("SUM(CLICKS)"))
    testImps = int(listReports[1][0].get("SUM(CLICKS)"))
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
    txtReports = getTableData(dbHelper.dbConnector.runParallelSelect(clientConfig, client, query, dbProperties), header)
    uniqFor0 = txtReports[0] - txtReports[1]
    uniqFor1 = txtReports[1] - txtReports[0]
    if len(uniqFor0) > 0:
        writeUniqueEntitiesToFile(table, uniqFor0, "prod", header)
    if len(uniqFor1) > 0:
        writeUniqueEntitiesToFile(table, uniqFor1, "test", header)
    if not all([len(uniqFor0) == 0, len(uniqFor1) == 0]):
        logger.error("Tables {} differs!".format(table))
        differingTables.append(table)
        return False
    else:
        return True


def compareTableLists(tableStructure):
    selectQuery = "SELECT DISTINCT(TABLE_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'DBNAME';"
    prodTableList, testTableList = dbHelper.dbConnector.runParallelSelect(clientConfig, client, selectQuery, dbProperties)
    # TODO: remove all callings like tableDict[1] - it's not clear
    tableStructure.update({"prodList": prodTableList})
    tableStructure.update({"testList": testTableList})
    if prodTableList != testTableList:
        getIntersectedTables(tableStructure)


def compareTablesMetadata(tableStructure, failWithFirstError):
    for table in tableStructure.get("crossedList"):
        logger.info("Check schema for table {}...".format(table))
        selectQuery = "SELECT {} FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'DBNAME' and TABLE_NAME='TABLENAME' ORDER BY COLUMN_NAME;".replace("TABLENAME", table).format(', '.join(schemaColumns))
        prodColumns, testColumns = dbHelper.dbConnector.runParallelSelect(clientConfig, client, selectQuery, dbProperties)
        prodUniqueColumns = prodColumns - testColumns
        tableStructure.update({"prodUniqueColumns": prodUniqueColumns})
        testUniqueColumns = testColumns - prodColumns
        tableStructure.update({"testUniqueColumns": testUniqueColumns})
        if len(prodUniqueColumns) > 0:
            prodDb = config.getProperty('sqlParameters', 'prod.' + client + '.sqlDb')
            logger.error("Elements, unique for table {} in {} db:{}".format(table, prodDb, prodUniqueColumns))
        if len(testUniqueColumns) > 0:
            testDb = config.getProperty('sqlParameters', 'test.' + client + '.sqlDb')
            logger.error("Elements, unique for table {} in {} db:{}".format(table, testDb, testUniqueColumns))
        if not all([len(prodUniqueColumns) == 0, len(testUniqueColumns) == 0]):
            logger.error(" [ERROR] Tables {} differs!".format(table))
            diffSchema = tableStructure.get("diffSchema")
            diffSchema.add(table)
            tableStructure.update({"diffSchema": diffSchema})
            if failWithFirstError:
                logger.critical("First error founded, checking failed...")


def countTableRecords(table, date):
    if date is None:
        query = "SELECT COUNT(*) FROM `{}`;".format(table)
    else:
        query = "SELECT COUNT(*) FROM `{}` WHERE dt > '{}';".format(table, date)
    amountRecords = dbHelper.dbConnector.runParallelSelect(clientConfig, client, query, dbProperties)
    return amountRecords


def createTestDir(path, client):
    if not os.path.exists(path):
        os.mkdir(path)
    if not os.path.exists(path + client):
        os.mkdir(path + client)

# TODO: too many args
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
            for i in columnDict:
                element = str(i.get("column_name")).lower()
                columnList.append("`{}`".format(element))
            for column in hideColumns:
                if column in columnList:
                    columnList.remove(column)
            if columnList == []:
                return False
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


def getComparingTimeframe(dateList):
    comparingTimeframe = []
    for item in dateList[0][-depthReportCheck:]:
        comparingTimeframe.append(item.get("dt").date().strftime("%Y-%m-%d"))
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


def getIntersectedTables(tableStructure):
    prodUniqueTables = set(tableStructure.get("prodList")) - set(tableStructure.get("testList")) # TODO: test it!
    testUniqueTables = set(tableStructure.get("testList")) - set(tableStructure.get("prodList")) # TODO: test it!
    tableStructure.update({"prodUnique": prodUniqueTables})
    tableStructure.update({"testUnique": testUniqueTables})
    if len(prodUniqueTables) > 0:
        prodDb = config.getProperty('sqlParameters', 'prod.' + client + '.sqlDb')
        logger.warn("Tables, which unique for {} db {}.".format(prodDb, prodUniqueTables))
    if len(testUniqueTables) > 0:
        testDb = config.getProperty('sqlParameters', 'test.' + client + '.sqlDb')
        logger.warn("Tables, which unique for {} db {}.".format(testDb, testUniqueTables))
    prodTableList = tableStructure.get("prodList")
    testTableList = tableStructure.get("testList")
    if len(prodUniqueTables) >= len(testUniqueTables):
        for item in prodUniqueTables:
            prodTableList.remove(item)
        tableStructure.update({"crossedList": prodTableList})
    else:
        for item in testUniqueTables:
            testTableList.remove(item)
        tableStructure.update({"crossedList": testTableList})


def getTableData(listReports, header):
    txtReports = []
    for record in listReports:
        instance = set()
        for item in record:
            section = []
            for key in header:
                try:
                    section.append(str(int(item.get(key))))
                except (TypeError, ValueError):
                    section.append(str(item.get(key)))
            instance.add(",".join(section))
        txtReports.append(instance)
    return txtReports


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


def getTimeframeIntersection(dateList, table):
    dateSet = converters.parallelConvertToSet(dateList)
    if (dateSet[0] - dateSet[1]):  # this code (4 strings below) should be moved to different function
        uniqueDates = getUniqueReportDates(dateSet[0], dateSet[1])
        testDb = config.getProperty('sqlParameters', 'test.' + client + '.sqlDb')
        logger.warn("This dates absent in {}: {} in report table {}...".format(testDb, ",".join(uniqueDates), table))
    if (dateSet[1] - dateSet[0]):
        uniqueDates = getUniqueReportDates(dateSet[1], dateSet[0])
        prodDb = config.getProperty('sqlParameters', 'prod.' + client + '.sqlDb')
        logger.warn("This dates absent in {}: {} in report table {}...".format(prodDb, ",".join(uniqueDates), table))
    return dateSet[0] & dateSet[1]


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
                columnDict.update({item.get('column_name').lower(): "`{}`".format(item.get('referenced_table_name').lower())})
            return columnDict
    finally:
        sql.connection.close()


def prepareQuerySections(table, mapping):
    columnString = getColumnList("prod", table)
    setColumnList = ""
    setJoinSection = ""
    tmpOrderList = []
    setColumnList, setJoinSection = constructColumnAndJoinSection(columnString, mapping, setColumnList, setJoinSection)
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


def constructColumnAndJoinSection(columnString, mapping, setColumnList, setJoinSection):
    for column in columnString.split(","):
        if column in list(mapping.keys()):
            linkedTable = mapping.get(column)
            if "remoteid" in column:
                if "remoteid" in getColumnList("prod", column[:-8]):
                    setColumnList = setColumnList + linkedTable + ".`remoteid` as " + column + ","
                else:
                    setColumnList = setColumnList + linkedTable + ".`id` as " + column + ","
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
            setJoinSection = setJoinSection + "JOIN " + linkedTable + " ON " + column + "=" + linkedTable + ".`id` "
        else:
            setColumnList = setColumnList + column + ","
    return setColumnList, setJoinSection


def prepareTableList(tableStructure):
    crossedList = tableStructure.get("crossedList")
    diffSchema = tableStructure.get("diffSchema")
    ignoredTables = tableStructure.get("ignoredTables")
    for table in tableStructure.get("ignoredTables"):
        if table in crossedList:
            crossedList.remove(table)
    if tableStructure.get("diffSchema") is not None:
        for table in diffSchema:
            if table in crossedList:
                crossedList.remove(table)
    try:
        if len(ignoredTables) > 0:
            for table in ignoredTables:
                if table in crossedList:
                    crossedList.remove(table)
    except configparser.NoOptionError:
        logger.warn("Property {}.ignoreTables in section [specificIgnoredTables] absent.".format(client))
    tableStructure.update({"crossedList": crossedList})


def prepareToTest(client, tableStructure):
    createTestDir("/mxf/data/test_results/", client)
    startTime = datetime.datetime.now()
    logger.info("Start {} processing!".format(client))
    compareTableLists(tableStructure)
    return startTime


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
            file.write(item + "\n")


checkServiceDir(serviceDir)
for client in config.getClients():
    clientConfig = configHelper.ifmsConfigClient(propertyFile, client)
    sqlPropertyDict = clientConfig.getSQLConnectParams('test')
    tableStructure.update({"ignoredTables": set(config.getProperty("specificIgnoredTables", client + ".ignoreTables").split(","))})
    globalBreak = False
    startTime = prepareToTest(client, tableStructure)
    if enableSchemaChecking:
        compareTablesMetadata(tableStructure, failWithFirstError)
        if not tableStructure.get("diffSchema") and failWithFirstError:
            schemaComparingTime = str(datetime.datetime.now() - startTime)
            logger.info("Schema partially compared in {}".format(schemaComparingTime))
        else:
            schemaComparingTime = str(datetime.datetime.now() - startTime)
            logger.info("Schema compared in {}".format(schemaComparingTime))
            dataComparingTime = compareData(tableStructure, globalBreak)
    else:
        logger.info("Schema checking disabled...")
        tablesWithDifferentSchema = []
        dataComparingTime = compareData(tableStructure, globalBreak)
    subject = "[Test] Check databases for client {}".format(client)
    body = generateMailText(emptyTables, differingTables, noCrossedDatesTables, prodUniqueTables, testUniqueTables)
    helper.sendmail(body, sendMailFrom, sendMailTo, mailPassword, subject, None)
