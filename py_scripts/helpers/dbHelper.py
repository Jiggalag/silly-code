import sys
import os
sys.path.append(os.getcwd() + '/py_scripts/helpers')
from py_scripts.helpers import loggingHelper
from multiprocessing.dummy import Pool
import pymysql

logger = loggingHelper.Logger(10)

class dbConnector:
    def __init__(self, connectParameters, **kwargs):
        self.host = connectParameters.get('host')
        self.user = connectParameters.get('user')
        self.password = connectParameters.get('password')
        self.db = connectParameters.get('db')
        self.connection = pymysql.connect(host=self.host,
                                          user=self.user,
                                          password=self.password,
                                          db=self.db,
                                          charset='utf8',
                                          cursorclass=pymysql.cursors.DictCursor)

        # sql-property section
        self.hideColumns = None
        self.attempts = 5
        self.mode = 'detailed'
        self.comparingStep = 10000
        for key in list(kwargs.keys()):
            if 'hideColumns' in key:
                self.hideColumns = kwargs.get(key)
            if 'attempts' in key:
                self.attempts = int(kwargs.get(key))
            if 'mode' in key:
                self.mode = kwargs.get(key)
            if 'comparingStep' in key:
                self.comparingStep = kwargs.get(key)

    @staticmethod
    def runParallelSelect(clientConfig, client, query, dbProperties, resultType="frozenset"):
        pool = Pool(2)
        sqlParamArray = pool.map(clientConfig.getSQLConnectParams, ["prod", "test"])
        resultArray = pool.map((lambda x: dbConnector(x, client=client, **dbProperties).runSelect(query, resultType)), sqlParamArray)
        pool.close()
        pool.join()
        if resultType == "list":
            prodResult = []
            testResult = []
            for item in resultArray[0]:
                prodResult.append(list(item))
            for item in resultArray[1]:
                testResult.append(list(item))
        else:
            prodResult = resultArray[0]
            testResult = resultArray[1]
        # return value instead of
        if len(prodResult) == 1: # TODO: strongly refactor this code
            prod = prodResult[0]
        else:
            prod = prodResult
        if len(testResult) == 1:
            test = testResult[0]
        else:
            test = testResult
        return prod, test


    def runSelect(self, query, resultType="frozenset"):
        errorCount = 0
        while errorCount < self.attempts:
            try:
                with self.connection.cursor() as cursor:
                    sqlQuery = query.replace('DBNAME', self.db)
                    logger.debug(sqlQuery)
                    cursor.execute(sqlQuery)
                    result = cursor.fetchall()
                    processedResult = []
                    for item in result:
                        tmpRecord = []
                        for key in item.keys():
                            tmpRecord.append(item.get(key))
                        if len(tmpRecord) == 1:
                            processedResult.append(tmpRecord[0])
                        elif resultType == "list":
                            processedResult.append(tmpRecord)
                        else:
                            processedResult.append(frozenset(tmpRecord))
                    return processedResult
            except pymysql.OperationalError:
                errorCount += 1
                logger.error("There are some SQL query error " + str(pymysql.OperationalError))
            finally:
                try:
                    self.connection.close()
                except pymysql.Error:
                    logger.info("Connection already closed...")
                    return []


    def getColumnList(self, table):
        # Function returns column list for sql-query for report table
        try:
            with self.connection.cursor() as cursor:
                columnList = []
                queryGetColumnList = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s' AND table_schema = '%s';" % (table, self.db)
                logger.debug(queryGetColumnList)
                cursor.execute(queryGetColumnList)
                columnDict = cursor.fetchall()
                for i in columnDict:
                    element = 't.' + str(i.get('column_name')).lower()  # It's neccessary to make possible queries like "select key form keyname;"
                    columnList.append(element)
                for column in self.hideColumns:
                    if 't.' + column.replace('_', '') in columnList:
                        columnList.remove('t.' + column)
                if columnList == []:
                    return False
                columnString = ','.join(columnList)
                return columnString.lower()
        finally:
            self.connection.close()
