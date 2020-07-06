import datetime
import os
import shutil
import subprocess
import time


class cliConnection:
    def __init__(self, cliPath, host, rmiPort):
        self.cliPath = cliPath
        self.host = host
        self.rmiPort = rmiPort


    def runJob(self, job):
        if not self.isJobRunning():
            runJobsCommand = '{} jmx:{}:{} sync -c s "{}"'.format(self.cliPath, self.host, self.rmiPort, job)
            subprocess.check_output(runJobsCommand, shell=True, universal_newlines=True)
            print(runJobsCommand)
        else:
            self.waitForJobFinished()


    def isJobRunning(self):
        while True:
            checkRunningJobsCommand = '{} jmx:{}:{} sync -c w'.format(self.cliPath, self.host, self.rmiPort)
            runningJobs = subprocess.check_output(checkRunningJobsCommand, shell=True, universal_newlines=True)
            if 'pid' in runningJobs:
                return True
            else:
                return False


    def waitForJobFinished(self):
        while True:
            checkRunningJobsCommand = '{} jmx:{}:{} sync -c w'.format(self.cliPath, self.host, self.rmiPort)
            runningJobs = subprocess.check_output(checkRunningJobsCommand, shell=True, universal_newlines=True)
            if 'pid' in runningJobs:
                print("Wait...")
                time.sleep(10)
            else:
                break


date = datetime.date.today() - datetime.timedelta(days=1)

jobs = [
    "SampleToCSV(-sDt {0} -eDt {0})".format(date),
    "SimulationToCSV(--current)"
]

host = 'dev01.inventale.com'
cliPath = 'cli'
rmiPort = 9047
targetPath = "/mxf/data/rick/CSVs/"

connection = cliConnection(cliPath, host, rmiPort)

for job in jobs:
    if not connection.isJobRunning():
        connection.runJob(job)
    while connection.isJobRunning():
        time.sleep(30)
    if "Sample" in job:
        filePath = "/mxf/apps/sync/1.3.54-sim-to-csv.2/instances/rick/untochable/work/"
        fileList = os.listdir(filePath)
        for filename in fileList:
            if str(date) in filename:
                shutil.move(filePath + filename, targetPath)
    elif "Simulation":
        filePath = "/mxf/apps/ifms/1.3.54-sim-to-csv.2/instances/rick/untochable/work/"
        fileList = os.listdir(filePath)
        for filename in fileList:
            if str(date) in filename:
                shutil.move(filePath + filename + "/sample.tsv.gz", targetPath)
    else:
        pass
