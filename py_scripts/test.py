import json

with open('/tmp/default', 'r') as file:
    default = json.loads(file.read())

with open('/tmp/dcos', 'r') as file:
    dcos = json.loads(file.read())

default_flights = ['534615554',
                   '534057820',
                   '534615480',
                   '534059736',
                   '534615540',
                   '534728928',
                   '533580023',
                   '534059708',
                   '534615510',
                   '534683376',
                   '534681552']

default_flights2 = ['534728928',
                    '534059736',
                    '534615510',
                    '534615480',
                    '534059708',
                    '534057820',
                    '534681552',
                    '534683376',
                    '534615554',
                    '534615540',
                    '533580023']


dcos_flights = ['534623012',
                '534779372',
                '534641810',
                '534774834',
                '534615584',
                '534734254',
                '534748656',
                '534623642',
                '534778532',
                '534342116',
                '534710530',
                '534059778',
                '534778196',
                '534753208',
                '534683680',
                '534749076',
                '534645110',
                '534742360',
                '534024505',
                '534742374',
                '532186349',
                '534059750']

default_interest = ['14891','14873']
dcos_interest = ['3379','3937']
default_subsection = ['5307']


print('Default flights:')
for item in default_flights:
    print(f'{item}: {default.get("Flight").get(item)}')

print('Dcos flights:')
for item in dcos_flights:
    print(f'{item}: {dcos.get("Flight").get(item)}')

print('Default interest:')
for item in default_interest:
    print(f'{item}: {default.get("Interest").get(item)}')

print('Dcos interest:')
for item in dcos_interest:
    print(dcos.get('Interest').get(item))

print('Default subsection:')
for item in default_subsection:
    print(default.get('SubSection').get(item))

print('stop')