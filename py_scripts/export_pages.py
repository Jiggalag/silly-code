import argparse
import subprocess

parser = argparse.ArgumentParser(description="Script returns page ids list on which we run our forecast")
parser.add_argument('rmi_port', type=int,
                    help='rmi port of target instance')
parser.add_argument('--server', type=str, default='dev01.inventale.com',
                    help='host, where target instance located')
parser.add_argument('--cli_path', type=str, default='cli',
                    help='path to cli')
parser.add_argument('rmi_port', type=int,
                    help='rmi port of target instance')

args = parser.parse_args()

p_host = args.server
p_cli_path = args.cli_path
p_rmi_port = args.rmi_port

def get_property_value(cli_path, host, rmi_port):
    command = '{} jmx:{}:{} ifms -c i forecasting.ifms.directory-include-pages'.format(cli_path, host, rmi_port)
    return subprocess.check_output(command, shell=True, universal_newlines=True)
