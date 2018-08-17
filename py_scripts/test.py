import argparse
import time
import py_scripts.rfc_helper as rcf

parser = argparse.ArgumentParser(description='Intended to test RFC API')
parser.add_argument('login', type=str, help='Login')
parser.add_argument('password_hash', type=str, help='password_hash')
parser.add_argument('server', type=str, help='Host, where script sends requests')


args = parser.parse_args()

server = args.server
login = args.login
password_hash = args.password_hash
start = time.clock()

api_point = rcf.RFCApi(server, login, password_hash)

print('Time taken to login {}'.format(time.clock() - start))

# result = api_point.get_matching_list(100)

# r2 = api_point.get_matching_list(**ddict)

offset = 0

result = {
    'mismatch': 0,
    'match': 0
}

ddict = {
    # 'limit': 1000
    # 'offset': offset,
    # 'beginDate': '2018-08-15T00:00:00',
    # 'endDate': '2018-08-15T07:00:00'
    'beginDate': '2018-08-15',
    'endDate': '2018-08-16'
}
start = time.clock()
r1 = api_point.get_track_list(**ddict)
print('Duration is {}'.format(time.clock() - start))
offset += 100
for item in r1.get('items'):
    tmp_match = 0
    tmp_mismatch = 0
    fio = item.get('fio')
    if fio == '<Не найден в базе профилей>':
        tmp_mismatch += 1
    else:
        tmp_match += 1
    new_mismatch = result.get('mismatch') + tmp_mismatch
    result.update({'mismatch': new_mismatch})
    new_match = result.get('match') + tmp_match
    result.update({'match': new_match})
print('Mismatch = {}'.format(result.get('mismatch')))
print('Match = {}'.format(result.get('match')))
print('OK')
