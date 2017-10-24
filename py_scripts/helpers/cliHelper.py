import subprocess
import time
from .loggingHelper import Logger

logger = Logger(20)


host = 'dev01.inventale.com'
cliPath = 'cli'
rmiPort = 9047
jobs = ['1', '2']

class cliConnection:
    def __init__(self, cliPath, host, rmiPort):
        self.cliPath = cliPath
        self.host = host
        self.rmiPort = rmiPort


    def runJob(self, job):
        if not self.isJobRunning():
            runJobsCommand = '{} jmx:{}:{} sync -c s "{}"'.format(cliPath, host, rmiPort, job)
            subprocess.check_output(runJobsCommand, shell=True, universal_newlines=True)
            logger.debug(runJobsCommand)
        else:
            self.waitForJobFinished()


    def isJobRunning(self):
        while True:
            checkRunningJobsCommand = '{} jmx:{}:{} sync -c w'.format(cliPath, host, rmiPort)
            runningJobs = subprocess.check_output(checkRunningJobsCommand, shell=True, universal_newlines=True)
            if 'pid' in runningJobs:
                return True
            else:
                return False

    def getInstanceInformation(self):
        instanceInformation = {}
        getInformationCommand = '%s jmx:%s:%d sync -c i' % (self.cliPath, self.host, self.rmiPort)
        rawInformation = subprocess.check_output(getInformationCommand, shell=True, universal_newlines=True)
        for data in rawInformation.split('\n'):
            instanceInformation.update({data[:data.find(':')]: data[data.find(':') + 2:]})
        return instanceInformation


    def waitForJobFinished(self):
        while True:
            checkRunningJobsCommand = '{} jmx:{}:{} sync -c w'.format(cliPath, host, rmiPort)
            runningJobs = subprocess.check_output(checkRunningJobsCommand, shell=True, universal_newlines=True)
            if 'pid' in runningJobs:
                logger.debug("Wait...")
                time.sleep(10)
            else:
                break
