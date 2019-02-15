from py_scripts.helpers import jarvis_helper
from py_scripts.helpers import logging_helper

class Instances:
    def __init__(self):
        self.apipoint = jarvis_helper.JarvisApi('adm.inventale.com', 'eu-dev-01.inventale.com')
        self.logger = logging_helper.Logger('DEBUG')

    def get_all_runned_instances(self, instance_type):
        result = list()
        for instance in self.apipoint.get_all_apps(instance_type):
            if instance.get('config').get('status') == 'running':
                result.append(instance)
        return result

    def get_instance(self, instance_type, configname):
        for instance in self.apipoint.get_all_apps(instance_type):
            if instance.get('value') == configname:
                return instance
        self.logger.warn(f'Something went wrong. There is no {instance_type} instance with config {configname}')
        return None


class Instance(Instances):
    def __init__(self, instance_type, configname):
        super().__init__()
        self.instance_type = instance_type
        self.configname = configname
        self.instance = self.get_instance(self.instance_type, self.configname)
        self.status = self.get_status()
        self.instance_parameters = self.instance.get('config')

    def get_status(self):
        for instance in self.apipoint.get_all_apps(self.instance_type):
            if instance.get('value') == self.configname:
                return instance.get('config').get('status')
        self.logger.warn('Something went wrong...')
        return None

    def add_java_custom_param(self, param):
        pass

    def update_version(self, version):
        pass


inst = Instances()
print('kek')
