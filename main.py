#!/usr/bin/env python3
"""
Simple script to test running algorithms in a central container.
"""
import sys
import time
import json
import requests

class Client(object):

    def __init__(self, host, collaboration_id, username, password, api_path='/api'):
        self.host = host
        self.api_path = api_path
        self.collaboration_id = collaboration_id
        self.username = username
        self.password = password
        self.access_token = None
        self.refresh_token = None

    @property
    def _baseURL(self):
        return self.host + self.api_path
    

    def authenticate(self):
        url = f'{self.host}{self.api_path}/token/user'
        credentials = {
            'username': self.username,
            'password': self.password
        }
        r = requests.post(url, json=credentials).json()
        self.access_token = r['access_token']
        self.refresh_token = r['refresh_token']

    def create_task(self, task):
        return self.post('/task', task)

    def wait_for_results(self, task_id):
        path = f'/task/{task_id}'

        while True:
            result = self.get(path)
            if result['complete']:
                break

            print('.', sep='', end='')
            sys.stdout.flush()
            time.sleep(5)

        print()

        result = self.get(path + '?include=results')
        return [json.loads(r['result']) for r in result['results']]

    def get(self, url):
        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }
        result = requests.get(self._baseURL + url, headers=headers)
        return result.json()

    def post(self, url, task):
        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }
        result = requests.post(self._baseURL + url, json=task, headers=headers)
        return result.json()


def run(host, api_path, username, password, collaboration_id):
    client = Client(host, collaboration_id, username, password, api_path)
    client.authenticate()

    task = {
        'name':  'RPC call from Python',
        'image': 'docker-registry.distributedlearning.ai/dl_test',
        'collaboration_id': collaboration_id,
        'input': {
            'role': 'master',
            'host': host,
            'api_path': api_path,
            'collaboration_id': collaboration_id,
        },
        'database': '',
        'description': '',
    }

    response = client.create_task(task)
    return client.wait_for_results(response['id'])


if __name__ == '__main__':
    run()
