import paramiko

hostname = 'dcos-slv-01.inventale.com'
user = 'pavel.kiselev'
pubkey = '/home/polter/.ssh/id_rsa'


def get_info(cmd):
    stdin, stdout, stderr = sshcon.exec_command(cmd)
    rawresult = stdout.readlines()[0].split(' ')
    return rawresult[-1].replace('\n', '')


sshcon = paramiko.SSHClient()
sshcon.set_missing_host_key_policy(paramiko.AutoAddPolicy())
sshcon.connect(hostname, username=user, key_filename=pubkey)
get_hdfs = 'whereis hdfs'
stdin, stdout, stderr = sshcon.exec_command(get_hdfs)
rawresult = stdout.readlines()[0].split(' ')
hdfs = rawresult[1]
check_logs = '{} dfs -ls /logs/rick_dsp/csv/logs | tail -n 1'.format(hdfs)
result = get_info(check_logs)
if 'http.lock' in result:
    check_logs = '{} dfs -ls /logs/rick_dsp/csv/logs | tail -n 2'.format(hdfs)
    result = get_info(check_logs)
print('Last downloaded log: {}'.format(result))

check_parquets = '{} dfs -ls /logs/rick_dsp/parquet/logs | tail -n 1'.format(hdfs)
result = get_info(check_parquets)
print('Last parquet: {}'.format(result))

check_hdfs_sample = '{} dfs -ls /logs/rick_dsp/parquet/sample | tail -n 1'.format(hdfs)
result = get_info(check_hdfs_sample)
print('Last hdfs-sample: {}'.format(result))

check_hdfs_reports = '{} dfs -ls /logs/rick_dsp/parquet/reports | tail -n 1'.format(hdfs)
result = get_info(check_hdfs_reports)
print('Last hdfs-report: {}'.format(result))

sshcon.close()

hostname = 'irl-0ifms-dev-cpo-bse06.inventale.com'
sshcon.connect(hostname, username=user, key_filename=pubkey)

check_sample = 'ls /mxf/data/rick/dsp_sampling | tail -n 1'
result = get_info(check_sample)
print('Last sample is: {}'.format(result))

check_reports = 'ls /mxf/data/rick/dsp_reports | tail -n 1'
result = get_info(check_reports)
print('Last report: {}'.format(result))

sshcon.close()
