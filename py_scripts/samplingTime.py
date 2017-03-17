# Script aggregates sampling time from production to separate file and calculates average sampling time.
# TODO: remove bash

import subprocess

version = 'mock'
client = 'mock'
server = 'mock'
user = 'mock'
copyLogsCommand = 'scp %s@%sPWD_MOCK/LogName.* ./logs' % (user, server)

createDirectory = subprocess.call("mkdir logs && chmod 777 logs", shell=True)
print("Making directory and chmoding is OK...")
createResultFile = subprocess.call("touch resultSamplingTime", shell=True)
print("Result file successfully created...")
copyLogs = subprocess.call(copyLogsCommand, shell=True)
print("Copying sync logs is OK...")

processingLogs = subprocess.call("zgrep Sampling:\ Loaded\ log\ for ./logs/LogName.* | cut -f 17 -d ' '  > 1", shell=True)
with open('resultSamplingTime', 'w') as resultFile, open('1', 'r') as tempFile:
    averageTime = 0
    tick = 0
    for i in tempFile:
        print(round(int(i) / 3600000, 2), 'hours', file=tempFile)
        averageTime += int(i) / 3600000
        tick += 1
    print("\n", "Total amount of successful sampling", tick, file=resultFile)
    print("Average time of sampling is", averageTime / tick, "hours", file=resultFile)
deleteTmpFiles = subprocess.call("rm -r ./logs && rm 1", shell=True)
print("Garbage collecting is OK...")
