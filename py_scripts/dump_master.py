import argparse

import paramiko


def get_tables(rawdata):
    names = list()
    for i in rawdata:
        names.append(i.replace('\n', ''))
    return names


def prepare_arguments():
    parser = argparse.ArgumentParser(description='Check forecast on production via api')
    parser.add_argument('client', type=str, help='Client name')
    parser.add_argument('--dumphost', type=str, default='eu-dba.maxifier.com', help='Host where dumps located')
    parser.add_argument('--targethost', type=str, default='eu-dba.maxifier.com', help='Host where you want create db '
                                                                                      'from dump')
    parser.add_argument('--username', type=str, default='backups', help='User for ssh-connection')
    parser.add_argument('--password', type=str, default='5kTAAku6ZNpN', help='Password for ssh-connection')
    parser.add_argument('date', type=str, help='Creation date of dump')
    parser.add_argument('--kv', type=bool, default=False, help='Restore kv from dump to db or not. Default - false.')
    return parser.parse_args()


hostmap = {
    'irving': {'host': 'ams-ifms-prd-dat-msq01.inventale.com', 'db': 'irving_cpopro'},
    'rick': {'host': 'irl-0ifms-dev-cpo-bse06.inventale.com', 'db': 'rick_cpopro'}
}

args = prepare_arguments()

hostname = args.dumphost
target_host = args.targethost
username = args.username
password = args.password
client = args.client
date = args.date
kv = args.kv

# sshcon = paramiko.SSHClient()
# sshcon.set_missing_host_key_policy(paramiko.AutoAddPolicy())
# sshcon.connect(hostname, username=username, password=password)
#
# dirname = '/home/dba/backups/db/daily/{}/{}'.format(hostmap.get(client).get('host'), date)
#
# ls_command = 'ls {} | grep {}'.format(dirname, hostmap.get(client).get('db'))
# stdin, stdout, stderr = sshcon.exec_command(ls_command)
# dumplist = stdout.readlines()
#
# # TODO: check if exist proper archives in stdout
# dumps = get_tables(dumplist)
# for dump in dumps:
#     scp_command = 'scp -3 {0}/{1} eu-db-01.inventale.com:~/{1}'.format(dirname, dump)
#     stdin, stdout, stderr = sshcon.exec_command(scp_command)
#
# sshcon.close()

target_sshcon = paramiko.SSHClient()
target_sshcon.set_missing_host_key_policy(paramiko.AutoAddPolicy())
target_sshcon.connect(target_host, username=username, password=password)

dbname = hostmap.get(client).get('db')
newdbname = '{}_{}'.format(dbname, date.replace('-', ''))
create_db = 'mysql -e "CREATE DATABASE {};"'.format(newdbname)
stdin, stdout, stderr = target_sshcon.exec_command(create_db)
error = stderr.readlines()[0]
if 'ERROR' in error:
    print('Creating database failed:\n')
    print(error)

for item in ['ddl', 'model', 'reports']:
    from_dump = 'bzcat {0}_{1}.sql.bz2 | mysql {0}'.format(dbname, item)
    stdin, stdout, stderr = target_sshcon.exec_command(from_dump)

if kv:
    kv_dump = 'bzcat {0}_kv.sql.bz2 | mysql {0}'.format(dbname)
    stdin, stdout, stderr = target_sshcon.exec_command(kv_dump)
