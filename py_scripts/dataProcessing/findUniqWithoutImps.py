import csv
from collections import Counter

csvWithClicks = 'mock'  # file should contains strings in format click - uniqId
csvWithImps = 'mock'  # file should contains strings in format imp - uniqId
clicks = csv.reader(open(csvWithClicks, "r"))
imps = csv.reader(open(csvWithImps, "r"))
clickList = []
impList = []
c = 0
for row in clicks:
    clickList.append(row[0])
print("Clicklist prepared...\n")
for row in imps:
    impList.append(row[0])
print("Implist prepared...\n")
clickList.sort()
print("clicklist sorted...\n")
impList.sort()
print("implist sorted ...\n")

cl = Counter(clickList)
im = Counter(impList)
for i in cl.keys():
    if im.get(i) is None:
        print("impressions equals 0 for uniq " + str(i) + "\n")
        ctr = 'Inf'
    else:
        print("Uniq id is " + str(i) + "\n")
        ctr = cl.get(i)/im.get(i)
    with open('/tmp/RESULT', 'a')as file:
        file.write(str(i) + " " + str(ctr) + '\n')
