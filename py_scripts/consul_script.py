import os
import socket

import argparse
import time

import consulate as cons
import statsd as stat
import sys

parser = argparse.ArgumentParser()

parser.add_argument("--client", default="rick")
parser.add_argument("--scope", default="default")

parser.add_argument("--auth_user", default="test-api-prisa")
parser.add_argument("--auth_password", default="b0b2a74849b62b4da005cdd79b8b9981")
parser.add_argument("--web_url", default="dev01.inventale.com/ifms")

# ---not environment variable
parser.add_argument("--consul_kv", default="storage")
parser.add_argument("--consul_host", default="127.0.0.1")
parser.add_argument("--consul_port", default=8500)

parser.add_argument("--statsd_host", default="localhost")
parser.add_argument("--statsd_port", default=8125)

parser.add_argument("--limit_seconds", default=10)
parser.add_argument("--polling_period", default=0.5)
parser.add_argument("--limit_minutes_per_query", default=60)
# ----

args = parser.parse_args()


client = os.environ.get('AUTH_CLIENT', args.client)
scope = os.environ.get('AUTH_SCOPE', args.scope)

auth_user = os.environ.get('AUTH_USER', args.auth_user)
auth_password = os.environ.get('AUTH_PASSWORD', args.auth_password)
web_url = os.environ.get('WEB_URL', args.web_url)

server = web_url.split('/')[0]
web_context = web_url.split('/')[1]

graphite_prefix = '{}.internal.monitoring.{}'.format(socket.getfqdn().split('.')[0], client)
consul_kv = 'monitoring/{}/{}'.format(client, args.consul_kv)

max_time_for_request_in_seconds = 60 * 60 * 60

consul = cons.Consul(host=args.consul_host, port=args.consul_port)
statsd = stat.StatsClient(host=args.statsd_host, port=args.statsd_port, prefix=graphite_prefix)


def collect_metric(metric_name, value):
    print("name:{} value:{} time:{}".format(metric_name, value, time.time()))
    statsd.gauge(metric_name, value)


collect_metric("request.responseCode3xx", 300)
collect_metric("request.responseCode4xx", 400)
collect_metric("request.responseCode5xx", 500)

sys.exit(0)
