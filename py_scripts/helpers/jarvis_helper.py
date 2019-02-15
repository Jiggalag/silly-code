import json
import requests


class JarvisApi:
    def __init__(self, service_host, target_host):
        self.service_host = service_host
        self.target_host = target_host
        self.env = 'production'
        self.ifms_list = self.get_all_apps('ifms')
        self.fsync_list = self.get_all_apps('fsync')
        self.ifmsjs_list = self.get_all_apps('ifmsjs')


    def list_application_types(self):
        result = json.loads(requests.get(f'https://{self.service_host}/api/apps/').text).get('apps')
        return result

    def get_all_apps(self, app):
        result = json.loads(requests.get(f'https://{self.service_host}/api/apps/{self.env}/{app}/{self.target_host}').text).get(app)
        return result

    def get_proper_config(self, app, config):
        result = json.loads(requests.get(f'https://{self.service_host}/api/apps/{self.env}/{app}/{self.target_host}/{config}').text).get(app)
        return result

    def update_config(self, config, params):
        headers = {'content-type': 'application/json'}
        result = requests.post(f'https://{self.service_host}/api/apps/{self.env}/{app}/{self.target_host}/{config}/update', headers=headers, params=params)
        return result.code

    def puppet_status(self, dc):
        result = requests.get(f'https://{self.service_host}/api/puppet/{self.env}/{dc}/{self.target_host}/puppet/status')
        return result


# point = JarvisApi('adm.inventale.com', 'eu-dev-01.inventale.com')
# res = point.get_proper_config('ifms', 'ssp_mrv')
# res = point.puppet_status('dev')
