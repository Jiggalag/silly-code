with open('/home/jiggalag/Desktop/remoteids_from_db.csv', 'r') as f:
    db = f.readlines()

with open('/home/jiggalag/Desktop/remoteids_from_ui', 'r') as f:
    ui = f.readlines()

dbp = list()
uip = list()
strange = list()
for i in db:
    dbp.append(i.replace('\n', ''))

for i in ui:
    uip.append(i.replace('\n', ''))

for i in uip:
    if i not in dbp:
        strange.append(i)

print('stop')
