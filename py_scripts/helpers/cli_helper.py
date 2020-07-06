import subprocess
import time

from py_scripts.helpers.logging_helper import Logger

logger = Logger(20)


class CliConnection:
    def __init__(self, cli_path, host, rmi_port):
        self.cli_path = cli_path
        self.host = host
        self.rmi_port = rmi_port

    def run_job(self, job):
        if not self.is_job_running():
            run_jobs_command = '{} jmx:{}:{} sync -c s "{}"'.format(self.cli_path, self.host, self.rmi_port, job)
            subprocess.check_output(run_jobs_command, shell=True, universal_newlines=True)
            logger.debug(run_jobs_command)
        else:
            self.wait_for_job_finished()

    def is_job_running(self):
        while True:
            check_running_jobs_command = '{} jmx:{}:{} sync -c w'.format(self.cli_path, self.host, self.rmi_port)
            running_jobs = subprocess.check_output(check_running_jobs_command, shell=True, universal_newlines=True)
            if 'pid' in running_jobs:
                return True
            else:
                return False

    def get_instance_information(self):
        instance_information = {}
        get_information_command = '%s jmx:%s:%d sync -c i' % (self.cli_path, self.host, self.rmi_port)
        raw_information = subprocess.check_output(get_information_command, shell=True,
                                                  universal_newlines=True).decode('utf-8')
        for data in raw_information.split('\n'):
            instance_information.update({data[:data.find(':')]: data[data.find(':') + 2:]})
        return instance_information

    def wait_for_job_finished(self):
        while True:
            check_running_jobs_command = '{} jmx:{}:{} sync -c w'.format(self.cli_path, self.host, self.rmi_port)
            running_jobs = subprocess.check_output(check_running_jobs_command, shell=True, universal_newlines=True)
            if 'pid' in running_jobs:
                logger.debug("Wait...")
                time.sleep(10)
            else:
                break
