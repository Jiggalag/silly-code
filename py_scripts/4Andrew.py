import datetime
import os.path
import shutil

listDir = "/mxf/apps/ifms"
client = "rick"
scope = "default"
targetPath = "/mxf/data/rick/futureSamples"


dictTypes = [
    "BrowserRemoteId",
    "BrowserVRemoteId",
    "CampaignRemoteId",
    "CityRemoteId",
    "ContinentRemoteId",
    "CountryRemoteId",
    "ExternalEvent",
    "OSRemoteId",
    "PageRemoteId",
    "SearchCriteria",
    "StateRemoteId",
    "UserId",
    "YaGeoId",
    "sample"
]


def getLastIFMSVersion(listDir):
    listVersion = []
    rawList = os.listdir(listDir)
    for item in rawList:
        if len(item) == 6:
            listVersion.append(item)
    return listVersion[len(listVersion) - 1]


def getSampleList(listDir, version, client, scope):
    pathToSamples = "{}/{}/instances/{}/{}/tmp/".format(listDir, version, client, scope)
    sampleList = os.listdir(pathToSamples)
    return sampleList


def getUniqueDate(fileList):
    tmpDateList = []
    for item in fileList:
        tmpDateList.append(item[26:36])
    uniqDates = []
    for item in tmpDateList:
        if item not in uniqDates:
            uniqDates.append(item)
    uniqDates.sort()
    return uniqDates

def getLastFileVersion(fileList, pathToSamples):
    if fileList:
        for filename in fileList:
            if fileList.index(filename) < len(fileList) - 1:
                if os.path.getctime(pathToSamples + filename) > os.path.getctime(pathToSamples + fileList[fileList.index(filename) + 1]):
                    file = filename
                else:
                    file = fileList[fileList.index(filename) + 1]
            else:
                print(filename + '\n')
                return filename
            print(filename + '\n')
        return file
    else:
        print("Function: getLastFileVersion. Error: fileList is empty!\n")
        return ''


def getDownloadList(fileList, dateList, pathToSamples):
    downloadList = []
    for date in dateList:
        for dictType in dictTypes:
            tmpList = []
            for filename in fileList:
                if (date in filename) and (dictType in filename):
                    tmpList.append(filename)
            downloadList.append(getLastFileVersion(tmpList, pathToSamples))
    return downloadList

def moveFiles(fileList, pathToSamples, targetPath):
    for file in fileList:
        shutil.move(pathToSamples + file, targetPath)

startTime = datetime.datetime.now()
version = getLastIFMSVersion(listDir)
pathToSamples = "{}/{}/instances/{}/{}/tmp/".format(listDir, version, client, scope)
sampleList = getSampleList(listDir, version, client, scope)
uniqDates = getUniqueDate(sampleList)
downloadList = getDownloadList(sampleList, uniqDates, pathToSamples)
moveFiles(sampleList, pathToSamples, targetPath)
print("Files moved to {} in {}".format(targetPath, datetime.datetime.now() - startTime))
