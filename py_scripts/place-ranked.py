import csv

result = list()

with open('/home/polter/Downloads/place-ranked.csv', 'r') as places:
    reader = csv.reader(places, delimiter=' ')
    for row in reader:
        remoteid = row[1]
        type = row[2]
        diff = float(row[3])
        default = int(row[4])
        dcos = int(row[5])
        absdiff = float(row[6])
        if (default > 100000) and (dcos > 100000):
            if diff > 5:
                result.append(row)
                print(row)
print('stop')