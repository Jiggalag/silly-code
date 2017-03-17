import subprocess

# Script collects forecast time from production in separate file and counts average forecast time.
# TODO: remove bash

version = 'mock'
client = 'mock'
scope = 'mock'
server = 'mock'
user = 'mock'
averageTime = 0
tick = 0

copyLogsCommand = 'scp %s@%s.PWD_MOCK/LogName.* ./logs' % (user, server)

createDirectory = subprocess.call("mkdir logs && chmod 777 logs", shell=True)
print("Making directory and chmoding is OK...")
copyLogs = subprocess.call(copyLogsCommand, shell=True)
print("Copying sync logs is OK...")
processingLogs = subprocess.call("zgrep finished\ in ./logs/LogName.* | cut -f 13 -d ' '  > 1", shell=True)
createResultFile = subprocess.call("touch ./resultInvTime", shell=True)

with open('1', 'r') as tempFile:
    rawData = tempFile.readlines()
with open('resultInvTime', 'w') as resultFile:
    for i in rawData:
        print(str(round(int(i) / 1000, 2)), 'seconds', file=resultFile)
        averageTime += int(i) / 1000
        tick += 1
    print("\n", "Total amount of successful inventory forecast ", tick, file=resultFile)
    print("Average time of inventory forecast is ", averageTime / tick, " seconds", file=resultFile)
    deleteTmpFiles = subprocess.call("rm -r ./logs && rm 1", shell=True)
    print("Garbage collecting is OK...")