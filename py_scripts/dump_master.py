import argparse
import datetime
import sys

import paramiko

from py_scripts.helpers.dbHelper import DbConnector
from py_scripts.helpers.logging_helper import Logger


# TODO: add support of 'last' value for date. If date = last, script should find dump with newer available date


class DumpMaster:
    def __init__(self):
        self.args = self.prepare_arguments()
        self.hostmap = {
            'irving': {'host': 'ams-ifms-prd-dat-msq01.inventale.com', 'db': 'irving_cpopro'},
            'rick': {'host': 'irl-0ifms-dev-cpo-bse06.inventale.com', 'db': 'rick_cpopro'}
        }
        self.hostname = self.args.dumphost
        self.client = self.args.client
        self.db = self.hostmap.get(self.client).get('db')
        self.target_host = self.args.targethost
        self.username = self.args.username
        self.password = self.args.password
        self.date = self.args.date
        self.kv = self.args.kv
        self.logger = Logger(self.args.logging_level)
        self.dirname = '/home/dba/backups/db/daily/{}'.format(self.hostmap.get(self.client).get('host'))

    @staticmethod
    def prepare_arguments():
        parser = argparse.ArgumentParser(description='Check forecast on production via api')
        parser.add_argument('client', type=str, help='Client name')
        parser.add_argument('--dumphost', type=str, default='eu-dba.maxifier.com', help='Host where dumps located')
        parser.add_argument('--targethost', type=str, default='eu-db-01.inventale.com',
                            help='Host where you want create db from dump')
        parser.add_argument('--target_user', type=str, default='liquibase', help='SQL-username on target host')
        parser.add_argument('--target_password', type=str, default='thiequ0WiW6UaNgelilu',
                            help='SQL-password on target host')
        parser.add_argument('--username', type=str, default='backups', help='User for ssh-connection')
        parser.add_argument('--password', type=str, default='5kTAAku6ZNpN', help='Password for ssh-connection')
        parser.add_argument('date', type=str, help='Creation date of dump')
        parser.add_argument('--kv', type=bool, default=False, help='Restore kv from dump to db or not. Default - false')
        parser.add_argument('--logging_level', type=str, default='INFO', help='''Set level of displaying messages:\n
                                CRITICAL, ERROR, WARNING, INFO, DEBUG. Default value: INFO))''')
        return parser.parse_args()

    @staticmethod
    def get_names(rawdata):
        names = list()
        for i in rawdata:
            names.append(i.replace('\n', ''))
        return names

    def get_date_list(self, sshc):
        ls_command = 'ls {}'.format(self.dirname)
        stdin, stdout, stderr = sshc.exec_command(ls_command)
        rawresult = stdout.readlines()
        result = list()
        for item in rawresult:
            result.append(item.replace('\n', ''))
        result.reverse()
        return result

    def check_dump_consistency(self, sshc, dump_date):
        ls_cmd = 'ls {}/{} | grep {}'.format(self.dirname, dump_date, self.db)
        stdin, stdout, stderr = sshc.exec_command(ls_cmd)
        result = stdout.readlines()
        dumpfiles = list()
        for item in result:
            dumpfiles.append(item.replace('\n', ''))
        if self.args.kv:
            file_amount = 4
        else:
            file_amount = 3
        self.logger.debug('Kv property is {}, file_amount value is {}'.format(self.args.kv, file_amount))
        if len(dumpfiles) >= file_amount:
            self.logger.warn('Some dump file absent for date {}. Exist only files: {}'.format(dump_date,
                                                                                              '\n'.join(dumpfiles)))
            return False
        else:
            self.logger.info('Founded full pack of dumps for {}:\n{}'.format(dump_date, '\n'.join(dumpfiles)))
            return True

    def get_last_update(self, sshc):
        dir_list = self.get_date_list(sshc)
        for dt in dir_list:
            if self.check_dump_consistency(sshc, dt):
                if dir_list.index(dt) != 0:
                    self.logger.info('Script stopped due to error above')
                return dt
        self.logger.error('There is no any dumps for this db, script stopped')
        sys.exit(1)

    def get_dump_date(self, sshc):
        if 'last' in self.date:
            return self.get_last_update(sshc)
        else:
            return self.date

    def copy_dumps(self):
        sshcon = paramiko.SSHClient()
        sshcon.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        sshcon.connect(self.hostname, username=self.username, password=self.password)
        date = self.get_dump_date(sshcon)
        self.logger.debug('Dump date is {}'.format(date))
        ls_command = 'ls {}/{} | grep {}'.format(self.dirname, date, self.hostmap.get(self.client).get('db'))
        stdin, stdout, stderr = sshcon.exec_command(ls_command)
        dumplist = stdout.readlines()
        dumps = self.get_names(dumplist)
        for dump in dumps:
            scp_command = 'scp -3 {0}/{2}/{1} eu-db-01.inventale.com:~/{1}'.format(self.dirname, dump, date)
            self.logger.info('Started copying of {}...'.format(dump))
            sshcon.exec_command(scp_command)
        sshcon.close()
        return dumps, date

    def is_creation_enabled(self, sql, db):
        with sql.cursor() as cursor:
            show_query = 'SHOW DATABASES;'
            cursor.execute(show_query)
            result = cursor.fetchall()
        for item in result:
            if db == item.get('Database'):
                self.logger.error('Database {} already exists, script stopped!'.format(db))
                sys.exit(1)
        return True

    def create_db(self, sql, db):
        with sql.cursor() as cursor:
            create_db = "CREATE DATABASE {};".format(db)
            cursor.execute(create_db)
        self.logger.info('Database {} successfully created...'.format(db))

    def restore_from_dump(self, file_list, db):
        target_sshcon = paramiko.SSHClient()
        target_sshcon.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        target_sshcon.connect(self.target_host, username=self.username, password=self.password)

        for item in ['ddl', 'model', 'reports']:
            for name in file_list:
                if item in name:
                    file_list.remove(name)
                    self.logger.info('Processing of {} started...'.format(name))
                    from_dump = 'bzcat {} | mysql {}'.format(name, db)
                    stdin, stdout, stderr = target_sshcon.exec_command(from_dump)
                    exit_status = stdout.channel.recv_exit_status()
                    self.logger.debug('Command is {}'.format(from_dump))
                    self.logger.debug('Exit status is {}'.format(exit_status))

        if self.kv:
            self.logger.info('Processing of {} started...'.format(file_list[0]))
            kv_dump = 'bzcat {} | mysql {}'.format(file_list[0], db)
            stdin, stdout, stderr = target_sshcon.exec_command(kv_dump)
            exit_status = stdout.channel.recv_exit_status()
            self.logger.debug('Command is {}'.format(kv_dump))
            self.logger.debug('Exit status is {}'.format(exit_status))

    def run_process(self):
        start = datetime.datetime.now()
        dump_list, date = self.copy_dumps()

        newdbname = '{}_{}'.format(self.db, date.replace('-', ''))
        connection_params = {
            'host': self.target_host,
            'user': self.args.target_user,
            'password': self.args.target_password,
            'db': newdbname
        }

        sql_point = DbConnector(connection_params, self.logger).get_sql_connection()
        if self.is_creation_enabled(sql_point, newdbname):
            self.create_db(sql_point, newdbname)

        self.restore_from_dump(dump_list, newdbname)
        result_time = datetime.datetime.now() - start
        self.logger.info('Dump restored in {}'.format(result_time))


if __name__ == '__main__':
    DumpMaster().run_process()
