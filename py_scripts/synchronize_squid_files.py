import os

import paramiko


class Syncronizer:
    def __init__(self, hostname, user, password, from_dir, to_dir, filemask):
        self.hostname = hostname
        self.user = user
        self.password = password
        self.from_dir = from_dir
        self.to_dir = to_dir
        self.filemask = filemask

    def get_files(self, sftp, diff):
        for item in diff:
            sftp.get(self.from_dir + item, self.to_dir + item)

    def get_diff(self, ftppoint):
        from_list = ftppoint.listdir(self.from_dir)
        to_list = os.listdir(self.to_dir)
        result = set(from_list) - set(to_list)
        if self.filemask == 'all':
            return result
        else:
            processed_result = set()
            for item in result:
                if self.filemask in item:
                    processed_result.update(item)
            return processed_result

    def go(self):
        sshcon = paramiko.SSHClient()
        sshcon.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        sshcon.connect(self.hostname, username=self.user, password=self.password)
        sftp = sshcon.open_sftp()
        diff = self.get_diff(sftp)
        if diff:
            self.get_files(sftp, diff)
            print('Synchronization finished successfully...')
        else:
            print('Everything already synchronized...')


sync = Syncronizer('irl-0ifms-dev-cpo-bse06.maxifier.com', 'sftpifms', 'djf8Djk32478wEh24sdf',
                   '/mxf/data/rick/prod/dsp_sampling/', '/mxf/data/rick/prod/dsp_sampling/', 'all')
sync.go()
