with open('/home/polter/zlessresult', 'r') as file:
    content = file.readlines()

big = dict()

for row in content:
    row = row.replace('\n', '')
    time = row.split(' ')[0]
    id = row.split(' ')[1]
    try:
        duration = int(row.split(' ')[2])
        if duration > 200000:
            big.update({time: duration})
    except IndexError as e:
        print(e)
print('kek')
