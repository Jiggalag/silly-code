uniqList = []
with open("/tmp/track", 'r') as file:
    for i in file:
        uniqList.append(i[:-1])

uniqList.sort()

with open("/media/jiggalag/48336a84-121b-42d9-b0d5-b57a1201c093/gpmd/pr-output.csv", "r") as fileRead:
    with open('/tmp/ok-result', 'w') as fileWrite:
        for s in fileRead:
            stringArray = s.split(',')
            if stringArray[1].replace('"', '') in uniqList:
                fileWrite.write(s)

# TODO: define, which version better

'''
uniqList = []
with open("/tmp/track", 'r') as file:
    for i in file:
        uniqList.append(i[:-1])
uniqList.sort()
ss = set()
for i in uniqList:
    ss.add(i)

with open("/media/jiggalag/48336a84-121b-42d9-b0d5-b57a1201c093/gpmd/pr-output.csv", "r") as fileRead:
    with open('/tmp/ok-result3', 'w') as fileWrite:
        for s in fileRead:
            stringArray = s.split(',')
            if stringArray[1][1:-1] in ss:
                fileWrite.write(s)

'''