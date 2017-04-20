import configparser
import datetime
import os.path
import sys
sys.path.append(os.getcwd() + '/py_scripts/helpers')
from py_scripts.helpers import loggingHelper

logger = loggingHelper.Logger(20)

class ifmsConfigCommon:
    def __init__(self, configName):
        if not os.path.exists(configName):
            logger.error('Property file {} does not exist!'.format(configName))
            sys.exit(1)
        self.config = configparser.ConfigParser()
        self.config.read(configName)


    def getClients(self):
        return self.config['main']['clients'].split(',')


    def getDate(self, section, propertyName):
        value = self.config[section][propertyName]
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()


    def getProperty(self, section, propertyName):
        try:
            propertyValue = self.config[section][propertyName]
            if 'True' in propertyValue:
                return True
            if 'False' in propertyValue:
                return False
            try:
                return int(propertyValue)
            except ValueError:
                pass
            try:
                return float(propertyValue)
            except ValueError:
                if ',' in propertyValue:
                    return propertyValue.split(',')
                else:
                    return propertyValue
        except KeyError:
            return None


    def getPropertyFromMainSection(self, propertyName):
        return ifmsConfigCommon.getProperty(self, 'main', propertyName)


    def getTimedeltaProperty(self, section, propertyName):
        timeProperty = self.config[section][propertyName]
        days = 0
        hours = 0
        minutes = 0
        for item in timeProperty.split(':'):
            if 'd' in item:
                days = int(item.replace('d', ''))
            if 'h' in item:
                hours = int(item.replace('h', ''))
            if 'm' in item:
                minutes = int(item.replace('m', ''))
        return datetime.timedelta(days=days, hours=hours, minutes=minutes)


class ifmsConfigClient(ifmsConfigCommon):
    def __init__(self, configName, client):
        super().__init__(configName)
        self.client = client


    def getMongoConnectParams(self, stage):
        paramArray = ['mongohost', 'mongodb', 'mongoport']
        connectionDict = {}
        if stage is None:
            stage = ''
        if self.client is None:
            client = ''
        else:
            client = self.client
        for item in self.config.items('mongoParameters'):
            if (stage in item[0]) and (client in item[0]):
                key = item[0].replace(stage, '').replace(client, '').replace('.', '')
                if key in paramArray:
                    connectionDict.update({key.replace('mongo', ''): item[1]})
            if 'common' in item[0]:
                key = item[0].replace('common', '').replace('.', '')
                if key in paramArray:
                    connectionDict.update({key.replace('mongo', ''): item[1]})
        return connectionDict


    def getSQLConnectParams(self, stage):
        paramArray = ['sqlhost', 'sqluser', 'sqlpassword', 'sqldb']
        connectionDict = {}
        if stage is None:
            stage = ''
        if self.client is None:
            self.client = ''
        for item in self.config.items('sqlParameters'):
            if (stage in item[0]) and (self.client in item[0]):
                key = item[0].replace(stage, '').replace(self.client, '').replace('.', '')
                if key in paramArray:
                    connectionDict.update({key.replace('sql', ''): item[1]})
        return connectionDict


    def getSQLHostForClientAndStage(self, stage):
        return self.getSQLConnectParams(stage).get('host')


    def getSQLUserForClientAndStage(self, stage):
        return self.getSQLConnectParams(stage).get('user')


    def getSQLPasswordForClientAndStage(self, stage):
        return self.getSQLConnectParams(stage).get('password')


    def getSQLDbForClientAndStage(self, stage):
        return self.getSQLConnectParams(stage).get('db')









