import json

files = [
    'pub.json',
    'def.json'
]


def read_file(filename):
    with open('/home/polter/irv/' + filename, 'r') as f:
        return json.loads(f.read())


results = list()

for filename in files:
    results.append(read_file(filename))
    print(f'File {filename} processed')
correction0 = results[0].get('corrections')
correction1 = results[0].get('corrections')
for item in correction0:
    index = correction0.index(item)
    item2 = correction1[index]
    if item != item2:
        print('Oops!')
print('ok')
