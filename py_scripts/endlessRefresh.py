# Script refreshes sync in endless cycle and run forecast after refresh

import subprocess
import datetime
import time

cliPath = '/home/jiggalag/Downloads/maxifier-cli-1.11/'
user = 'mock'
server = 'dev02'
version = 'mock'
client = 'mock'
scope = 'mock'
rmiPort = '60035'
grepString = "ssh %s@%s.maxifier.com 'grep CORRECTION\ FINISHED /mxf/apps/ifms/%s/instances/%s/%s/logs/maxifier.log | wc -l'" % (user, server, version, client, scope)
refreshCommand = '%s./cli.sh jmx:%s.maxifier.com:%s ifms -c refresh' % (cliPath, server, rmiPort)
tick = 0
j = 0

howManyRefreshes = int(subprocess.check_output(grepString, shell=True, universal_newlines=True))
createTimeFile = subprocess.call('touch ./timed', shell=True)

print('howManyRefreshes is ---> ', howManyRefreshes)
while True:
    # TODO: already not works
    # forecastResult = helper.runForecast('mock', 'mock', 'mock', 'mock', 'mock')
    subprocess.call('curl -c cookies.txt -X POST -d "loginActionForm.login=MOCK&loginActionForm.password=MOCK" https://MOCK/login.action', shell=True)
    while True:
        if tick is 0:
            startTime = datetime.datetime.now()
            tick += 1
            print('Go', tick)
        else:
            print('Do forecastQuery1.json.json...')
            getForecastCommand = r'curl -H "Content-Type:application/json" -b cookies.txt -X POST -d @forecastQuery1.json.json MOCKforForecastCommend'
            forecastResult = subprocess.check_output(getForecastCommand, shell=True, universal_newlines=True)
            print(forecastResult)
            time.sleep(1)
            if 'matched' not in forecastResult:
                print("Wait")
            else:
                timeDelta = str(datetime.datetime.now() - startTime)
                with open('timed', 'a') as resultFile:
                    print(str(startTime).ljust(8), timeDelta, file=resultFile)
                    print('Ended & breaked...')
                    tick = 0
                    break
            print("Let's refresh!")
            runRefresh = subprocess.call(refreshCommand, shell=True)
            print("Refreshed!")
            howManyRefreshes += 1
            while True:
                print ("Grep!")
                grepResult = subprocess.check_output(grepString, shell=True, universal_newlines=True)
                print(grepResult, howManyRefreshes)
                if int(grepResult) == howManyRefreshes:
                    print("Break!")
                    j = 0
                    break
                else:
                    time.sleep(60)
                    print('Ping ', j)
                    j += 1
