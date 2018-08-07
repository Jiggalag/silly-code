import argparse
import py_scripts.recfaces_helper as rcf

parser = argparse.ArgumentParser(description='Intended to test RecFaces API')
parser.add_argument('login', type=str, help='Login')
parser.add_argument('password_hash', type=str, help='password_hash')
parser.add_argument('--server', type=str, default='https://cloud.recfaces.com',
                    help='Host, where script sends requests (default: https://cloud.recfaces.com)')


args = parser.parse_args()

server = args.server
login = args.login
password_hash = args.password_hash

api_point = rcf.RecFacesApi(server, login, password_hash)


# result = api_point.get_matching_list(100)
r1 = api_point.get_track_list(100)