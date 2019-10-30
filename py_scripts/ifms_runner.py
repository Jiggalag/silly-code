import datetime
import os
import psutil
import subprocess
import time

# TODO: it's only mold for ifms-python-runner

hard_properties = [
    '-Xms512M',
    '-XX:ReservedCodeCacheSize=100m',
    '-XX:+UseParallelGC',
    '-XX:+UseParallelOldGC',
    '-XX:+CMSClassUnloadingEnabled',
    '-XX:+UseCompressedOops',
    '-XX:-PartialPeelLoop',
    '-Dfile.encoding=UTF-8',
    '-Dcom.sun.management.jmxremote.authenticate=false',
    '-Dcom.sun.management.jmxremote.ssl=false',
    '-Djava.awt.headless=true',
    '-Djava.io.tmpdir=./work',
    '-Dmail.smtp.debug',
    '-Dmail.smtp.socketFactory.port=465',
]

standard_properties = {
    '-Dhibernate.connection.url': 'jdbc:mysql://samaradb03.maxifier.com/rick',
    '-Dhibernate.connection.username': 'itest',
    '-Dhibernate.connection.password': 'ohk9aeVahpiz1wi',
    '-Dmongo.connection.url': 'mongodb://eu-smr-mng-01.maxifier.com',
    '-Dmongo.connection.db': 'ifms_rickmaster2_sugg-perm',
    '-Dauth.user': 'app_default',
    '-Dauth.scope': 'default',
    '-Dcpo.properties.auth.only': 'false',
    # TODO: config should be changed!
    '-Dauth.service.config': '/mxf/etc/ifms/open.cfg',
    '-Dcpo.registry.file': 'none',
    '-Dauth.client': 'rick', # TODO: value should be changed
    '-Drabbitmq.uri': 'amqp://msp4_auth:msp4@eu-smr-rmq-01.maxifier.com:5672/msp4',
    '-Dmail.smtp.user': 'AKIAINQZVBQJ5FJJBNKQ',
    '-Dmail.smtp.host': 'email-smtp.us-east-1.amazonaws.com',
    '-Dmail.smtp.port': '465',
    '-Dmail.smtp.pass': 'AozX4RZRnXUlhRxbzAPgriH0AnVkJhYcQ5aQYJCi/0d/',
    '-Dmail.smtp.starttls.enable': 'true',
    '-Dmail.smtp.auth': 'true',
    '-Dmail.smtp.socketFactory.class': 'javax.net.ssl.SSLSocketFactory',
    '-Dmail.smtp.socketFactory.fallback': 'false',
    '-Dgraphite.host': 'graphite.inventale.com',
    '-Dgraphite.port': '2003',
    # '-Dgraphite.prefix=eu-smr-dev-01.internal.ifms.rick',
    # '-Dsentry.dsn=https://f105285043a34986b01c361fec593044:bbaf41e391a34092ae1ad61937078a03@sentry.maxifier.com/33',
    # '-Dsentry.servername=eu-smr-dev-01',
    # '-Dsentry.environment=internal',
    # '-Dsentry.tags=app:ifms,client:rick',
    # '-Dcom.sun.management.jmxremote',
    # '-Dcom.sun.management.jmxremote.port=60037 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.awt.headless=true -Djava.rmi.server.hostname=eu-smr-dev-01.inventale.com -Djava.net.preferipv4stack=true -Dmongo.connection.bucket= -Dforecasting.sampling.directory=/mxf/data/rick/sampling -Dmonitoring.order.hide-tab=true -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/mxf/data/rick/dumps -Dforecasting.ifms.default-weight=0 -Dforecasting.date-profile.spike-enabled=false -Dforecasting.sampling.cumulative-days=7 -Dforecasting.sampling.uniques-extrapolation.stat-days=14 -Dforecasting.searchterm.allowed-keynames=1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,59 -Dforecasting.ifms.keynames-with-undefined=3,4,6,12,13,18,24,30,31,49,50,55,57,59 -Dforecasting.ifms.keynames-for-breakdown=3,4,6,12,13,18,24,30,31,49,50,55,57,59 -Dforecasting.ifms.ui-allowed-keynames=3,4,6,12,13,18,24,30,31,49,50,55,57,59 -jar ./lib/maxifier-ifms.jar'
}


class IfmsRunner:
    def __init__(self, jar_path, redefined_properties, logger):
        self.sh_path = jar_path
        self.redefined_properties = redefined_properties
        self.logger = logger
        if redefined_properties is not None:
            self.result_properties = self.prepare_properties()
        else:
            self.result_properties = standard_properties
        self.deserialized_properties = self.deserialize_properties()
        if 'api' in jar_path:
            command = ['cd {} && ./nikola-api.sh {}'.format(self.sh_path, self.deserialized_properties)]
        else:
            command = ['cd {} && ./nikola.sh {}'.format(self.sh_path, self.deserialized_properties)]
        self.logger.info('Properties: {}'.format(self.deserialized_properties))
        self.logger.debug(command[0])
        self.process = subprocess.Popen(command, shell=True)
        self.pid = self.get_pid()
        if 'api' in jar_path:
            filename = '/tmp/nikola_api_pid'
        else:
            filename = '/tmp/nikola_pid'
        with open(filename, 'w') as f:
            f.write(str(self.pid))
        self.wait_for_running(60, 5)

    def get_pid(self):
        pid = self.process.pid
        # TODO: get rid of this ugly-hacked timeout
        time.sleep(10)
        while pid:
            pgrep = 'pgrep -P {}'.format(pid)
            try:
                pid = subprocess.check_output(pgrep, shell=True).decode('utf8')[:-1]
            except subprocess.CalledProcessError:
                return int(pid)

    def wait_for_running(self, timeout, attempts):
        # TODO: remove hardcode
        while True:
            result = self.retry_check_running(timeout, attempts)
            while True:
                # TODO: except situation when there aren't any start records in logs
                if len(result) > 1:
                    break
            for item in reversed(result):
                if item == '':
                    continue
                else:
                    tmp_array = item.split(' ')
                    break
            if tmp_array:
                raw_time = tmp_array[0] + ' ' + tmp_array[1]
                start_time = datetime.datetime.strptime(raw_time, '%Y-%m-%d %H:%M:%S.%f')
            if datetime.datetime.now() - start_time < datetime.timedelta(seconds=30):
                self.logger.info('Application has been launched')
                break
            else:
                self.logger.debug('Wait for starting app 5 sec...')
                time.sleep(5)

    def prepare_properties(self):
        for prop in list(self.redefined_properties):
            standard_properties.update({prop: self.redefined_properties.get(prop)})
        if 'api' in self.sh_path:
            standard_properties.update({'-Dlog.directory': '/var/log/nikola-api/'})
            standard_properties.update({'-Drest_api.port': '9191'})
            standard_properties.update({'-Dcom.sun.management.jmxremote.port': '1235'})
        else:
            standard_properties.update({'-Dlog.directory': '/var/log/nikola/'})
            standard_properties.update({'-Dcom.sun.management.jmxremote.port': '1234'})
        return standard_properties

    def deserialize_properties(self):
        result_string = ' '.join(hard_properties) + ' '
        if 'api' in self.sh_path:
            result_string = result_string + ' -Xmx2G '
        else:
            result_string = result_string + ' -Xmx10G '
        for prop in self.result_properties:
            result_string = result_string + '{}={} '.format(prop, self.result_properties.get(prop))
        return result_string

    def stop_app(self):
        proc = psutil.Process(self.pid)
        proc.terminate()

    def retry_check_running(self, timeout, attempts):
        if 'api' in self.sh_path:
            grep = 'grep has\\ started\\ for /var/log/nikola-api/nikola-api.log'
        else:
            grep = 'grep has\\ started\\ for /var/log/nikola/nikola.log'
        counter = 0
        while counter <= attempts:
            self.logger.debug('Wait for {} seconds, attempt {}'.format(timeout, counter))
            time.sleep(timeout)
            try:
                return subprocess.check_output(grep, shell=True, universal_newlines=True).split('\n')
            except subprocess.CalledProcessError as e:
                try:
                    print(e.stdout, e.stderr)
                except AttributeError:
                    print(e.output)
            counter += 1


def install_deb_package(start_dir, app_type):
    file_list = os.listdir('{}/nikola-{}/target/'.format(start_dir, app_type))
    deb_package = ''
    for item in file_list:
        if 'deb' in item:
            deb_package = item
            break
    subprocess.call('dpkg -i {}/nikola-{}/target/{}'.format(start_dir, app_type, deb_package), shell=True)
    time.sleep(30)


def remove_app():
    subprocess.call('apt-get remove -y nikola-*', shell=True)
    time.sleep(30)
