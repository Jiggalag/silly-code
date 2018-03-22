import configparser
import datetime
import os.path
import sys
from py_scripts.helpers import logging_helper

logger = logging_helper.Logger('INFO')


class IfmsConfigCommon:
    def __init__(self, config_name):
        if not os.path.exists(config_name):
            logger.error('Property file {} does not exist!'.format(config_name))
            sys.exit(1)
        self.config = configparser.ConfigParser()
        self.config.read(config_name)

    def get_clients(self):
        return self.config['main']['clients'].split(',')

    def get_date(self, section, property_name):
        value = self.config[section][property_name]
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()

    def get_property(self, section, property_name):
        try:
            property_value = self.config[section][property_name]
            if 'True' in property_value:
                return True
            if 'False' in property_value:
                return False
            try:
                return int(property_value)
            except ValueError:
                pass
            try:
                return float(property_value)
            except ValueError:
                if ',' in property_value:
                    return property_value.split(',')
                else:
                    return property_value
        except KeyError:
            return None

    def get_property_from_main_section(self, property_name):
        return IfmsConfigCommon.get_property(self, 'main', property_name)

    def get_timedelta_property(self, section, property_name):
        time_property = self.config[section][property_name]
        days = 0
        hours = 0
        minutes = 0
        for item in time_property.split(':'):
            if 'd' in item:
                days = int(item.replace('d', ''))
            if 'h' in item:
                hours = int(item.replace('h', ''))
            if 'm' in item:
                minutes = int(item.replace('m', ''))
        return datetime.timedelta(days=days, hours=hours, minutes=minutes)


class IfmsConfigClient(IfmsConfigCommon):
    def __init__(self, config_name, client):
        super().__init__(config_name)
        self.client = client

    def get_mongo_connection_params(self, stage):
        param_array = ['mongohost', 'mongodb', 'mongoport']
        connection_dict = {}
        if stage is None:
            stage = ''
        if self.client is None:
            client = ''
        else:
            client = self.client
        for item in self.config.items('mongoParameters'):
            if (stage in item[0]) and (client in item[0]):
                key = item[0].replace(stage, '').replace(client, '').replace('.', '')
                if key in param_array:
                    connection_dict.update({key.replace('mongo', ''): item[1]})
            if 'common' in item[0]:
                key = item[0].replace('common', '').replace('.', '')
                if key in param_array:
                    connection_dict.update({key.replace('mongo', ''): item[1]})
        return connection_dict

    def get_sql_connection_params(self, stage):
        param_array = ['sqlhost', 'sqluser', 'sqlpassword', 'sqldb']
        connection_dict = {}
        if stage is None:
            stage = ''
        if self.client is None:
            self.client = ''
        for item in self.config.items('sqlParameters'):
            if (stage in item[0]) and (self.client in item[0]):
                key = item[0].replace(stage, '').replace(self.client, '').replace('.', '')
                if key in param_array:
                    connection_dict.update({key.replace('sql', ''): item[1]})
        return connection_dict

    def get_sql_host_for_client_and_stage(self, stage):
        return self.get_sql_connection_params(stage).get('host')

    def get_sql_user_for_client_and_stage(self, stage):
        return self.get_sql_connection_params(stage).get('user')

    def get_sql_password_for_client_and_stage(self, stage):
        return self.get_sql_connection_params(stage).get('password')

    def get_sql_db_for_client_and_stage(self, stage):
        return self.get_sql_connection_params(stage).get('db')
