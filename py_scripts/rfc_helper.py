import json

import requests


class RFCApi:
    def __init__(self, server, login, password):
        self.server = server
        self.login = login
        self.password = password
        self.session_id = self.get_session_id()

    def get_session_id(self):
        login_url = '{}/ui/v1/login/'.format(self.server)
        headers = {'Content-Type': 'application/json'}
        data = {'login': self.login, 'password': self.password}
        try:
            login_response = requests.post(url=login_url, data=json.dumps(data), headers=headers)
        except requests.exceptions.ConnectionError:
            login_response = requests.post(url=login_url, data=json.dumps(data), headers=headers)
        return json.loads(login_response.text).get('sessionId')

    def generate_url(self, url, **kwargs):
        if 'offset' in kwargs:
            offset = kwargs.get('offset')
            url = url + '?offset={}'.format(offset)
        if 'limit' in kwargs:
            limit = kwargs.get('limit')
            if '?' in url:
                url = url + '&limit={}'.format(limit)
            else:
                url = url + '?limit={}'.format(limit)
        if 'beginDate' in kwargs:
            beginDate = kwargs.get('beginDate')
            if '?' in url:
                url = url + '&beginDate={}'.format(beginDate)
            else:
                url = url + '?beginDate={}'.format(beginDate)
        if 'endDate' in kwargs:
            endDate = kwargs.get('endDate')
            if '?' in url:
                url = url + '&endDate={}'.format(endDate)
            else:
                url = url + '?endDate={}'.format(endDate)
        return url

    def get_matching_list(self, **kwargs):
        url = '{}/api/v1/registry/matching-hit'.format(self.server)
        url = self.generate_url(url, **kwargs)
        headers = {
            'Content-Type': 'application/json',
            'sessionid': self.session_id
        }
        print(url)
        try:
            login_response = requests.get(url=url, headers=headers)
        except requests.exceptions.ConnectionError:
            login_response = requests.get(url=url, headers=headers)
        return json.loads(login_response.text)

    def get_matching_list_by_id(self, id):
        url = '{}/api/v1/registry/matching-hit/{}'.format(self.server, id)
        headers = {
            'Content-Type': 'application/json',
            'sessionid': self.session_id
        }
        try:
            login_response = requests.get(url=url, headers=headers)
        except requests.exceptions.ConnectionError:
            login_response = requests.get(url=url, headers=headers)
        return json.loads(login_response.text)

    def create_hit(self):
        url = '{}/api/v1/registry/matching-hit'.format(self.server)
        pass

    def get_mismatching_list(self, **kwargs):
        url = '{}/api/v1/registry/matching-not-hit'.format(self.server)
        url = self.generate_зurl(url, **kwargs)
        headers = {
            'Content-Type': 'application/json',
            'sessionid': self.session_id
        }
        print(url)
        try:
            login_response = requests.get(url=url, headers=headers)
        except requests.exceptions.ConnectionError:
            login_response = requests.get(url=url, headers=headers)
        return json.loads(login_response.text)

    def get_mismatching_list_by_id(self, id):
        url = '{}/api/v1/registry/matching-not-hit/{}'.format(self.server, id)
        headers = {
            'Content-Type': 'application/json',
            'sessionid': self.session_id
        }
        try:
            login_response = requests.get(url=url, headers=headers)
        except requests.exceptions.ConnectionError:
            login_response = requests.get(url=url, headers=headers)
        return json.loads(login_response.text)

    def get_track_list(self, **kwargs):
        url = '{}/api/v1/registry/track'.format(self.server)
        url = self.generate_url(url, **kwargs)
        headers = {
            'Content-Type': 'application/json',
            'sessionid': self.session_id
        }
        print(url)
        try:
            login_response = requests.get(url=url, headers=headers)
        except requests.exceptions.ConnectionError:
            login_response = requests.get(url=url, headers=headers)
        return json.loads(login_response.text)

    def get_business_notice(self, **kwargs):
        url = '{}/api/v1/registry/business-notice'.format(self.server)
        url = self.generate_url(url, **kwargs)
        headers = {
            'Content-Type': 'application/json',
            'sessionid': self.session_id
        }
        try:
            login_response = requests.get(url=url, headers=headers)
        except requests.exceptions.ConnectionError:
            login_response = requests.get(url=url, headers=headers)
        return json.loads(login_response.text)

    def get_business_notice_by_id(self, id):
        url = '{}/api/v1/registry/business-notice/{}'.format(self.server, id)
        headers = {
            'Content-Type': 'application/json',
            'sessionid': self.session_id
        }
        try:
            login_response = requests.get(url=url, headers=headers)
        except requests.exceptions.ConnectionError:
            login_response = requests.get(url=url, headers=headers)
        return json.loads(login_response.text)

    def get_passage_facts(self, **kwargs):
        url = '{}/api/v1/registry/passage-fact'.format(self.server)
        url = self.generate_url(url, **kwargs)
        headers = {
            'Content-Type': 'application/json',
            'sessionid': self.session_id
        }
        try:
            login_response = requests.get(url=url, headers=headers)
        except requests.exceptions.ConnectionError:
            login_response = requests.get(url=url, headers=headers)
        return json.loads(login_response.text)

    def get_passage_facts_by_id(self, id):
        url = '{}/api/v1/registry/business-notice/{}'.format(self.server, id)
        headers = {
            'Content-Type': 'application/json',
            'sessionid': self.session_id
        }
        try:
            login_response = requests.get(url=url, headers=headers)
        except requests.exceptions.ConnectionError:
            login_response = requests.get(url=url, headers=headers)
        return json.loads(login_response.text)

    def create_profile(self, last_name, first_name, middle_name):
        url = '{}/api/v1/photo-profile'.format(self.server, id)
        headers = {
            'Content-Type': 'application/json',
            'sessionid': self.session_id
        }
        data = {
            'lastName': last_name,
            'firstName': first_name,
            'middleName': middle_name,
            'clientId': 'null'
        }

        try:
            login_response = requests.get(url=url, headers=headers, data=json.dumps(data))
        except requests.exceptions.ConnectionError:
            login_response = requests.get(url=url, headers=headers, data=json.dumps(data))
        return json.loads(login_response.text)

    def create_profile_by_photo(self, photo_id):
        url = '{}/api/v1/photo-profile/create'.format(self.server, id)
        data = {
            'photoId': photo_id
        }

    def redact_profile(self):
        # PUT /api/v1/photo-profile/{ProfileID}
        pass

    def add_photo(self):
        # POST /api/v1/photo-profile/add
        pass

    def load_video(self):
        # POST /api/v1/archive/upload-video?fileName=video1.mp4
        pass
