# -*- coding: utf-8 -*-
"""
Created on Fri Mar 23 11:33:24 2018

@author: stuart
"""

#single file processing tools for nanowire-plugin

import datetime
import inspect
import json
import logging
import os
import sys
import time
import traceback

import requests

from cerberus import Validator
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import logging
logger = logging.getLogger(__name__)

class Worker(object):
    task_handler = None
    controller_base_uri = None
    plugin_id = None
    pod_name = None

    is_connected = False

    def _retriable_request_session(self, retries=3, backoff_factor=0.3,
                status_forcelist=(500, 502, 504), session=None):

        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def __init__(self, task_handler):
        logging.debug("Constructing new single file worker for " + str(task_handler))
        self.plugin_instance = os.environ['POD_NAME']
        self.controller_base_uri = os.environ['CONTROLLER_BASE_URI']
        self.plugin_id = os.environ['PLUGIN_ID']
        self.task_handler = task_handler

    def report_task_failure(self, taskId, errorMessage):
        request_url = "{}/v1/tasks/{}".format(self.controller_base_uri, taskId)
        requests.put(request_url, json={
            'pluginInstance': self.plugin_instance,
            'status': 'failure',
            'error': errorMessage
        })

    def report_task_success(self, taskId, jsonld):
        request_url = "{}/v1/tasks/{}".format(self.controller_base_uri, taskId)
        requests.put(request_url, json={
            'pluginInstance': self.plugin_instance,
            'status': 'success',
            'jsonld': jsonld
        })
    
    def _validate_task_meta(self, meta):
        raise NotImplementedError

    def run(self):
        logging.info("Handling tasks for {} as {}".format(self.plugin_id, self.plugin_instance))
        while True:
            try:
                request_url = "{}/v1/tasks/?pluginId={}&pluginInstance={}".format(
                    self.controller_base_uri,
                    self.plugin_id,
                    self.plugin_instance
                    )

                response = requests.get(request_url)

                if not self.is_connected:
                    logger.info("Connected to controller at {}".format(self.controller_base_uri))
                    self.is_connected = True

                if response.status_code == 404:
                    time.sleep(1)
                    continue

                if response.status_code != 200:
                    raise Exception('Received unexpected response from controller' + str(response))

                payload = response.json()
                self._validate_task_meta(payload['metadata'])
                meta = payload['metadata']
                jsonld = payload['jsonld']
                url = payload.get('url', None)

                try:
                    result = self.task_handler(meta, jsonld, url)
                    self.report_task_success(meta['task']['_id'], result)
                except Exception as exp:
                    self.report_task_failure(meta['task']['_id'], str(exp))

            except Exception as e:
                logger.error(e)
                self.is_connected = False
                time.sleep(1)

task_meta_schema = {
    'task': {
        'type': 'dict',
        'schema': {
            '_id': { 'type': 'string' },
            'name': { 'type': 'string' },
            'type': { 'type': 'string' },
            'size': { 'type': 'string' },
            'jobId': { 'type': 'string' },
            'projectId': { 'type': 'string' },
        }
    },
    'job': {
        'type': 'dict',
        'schema': {
            'workflow': {
                'type': 'dict',
                'schema': {
                    'type': { 'type': 'string', }
                }
            }
        }
    }
}

class GroupWorker(Worker):
    def _validate_task_meta(self, meta):
        v = Validator(task_meta_schema)
        v.allow_unknown = True
        if v.validate(meta):
            if meta['job']['workflow']['type'] != 'GROUP':
                raise Exception('Expected group job but got ' + meta['job']['workflow']['type'])
            return True
        else:
            raise Exception(str(v.errors))

class SingleWorker(Worker):
    def _validate_task_meta(self, meta):
        v = Validator(task_meta_schema)
        v.allow_unknown = True
        if v.validate(meta):
            if meta['job']['workflow']['type'] != 'SINGLE_FILE':
                raise Exception('Expected group job but got ' + meta['job']['workflow']['type'])
            return True
        else:
            raise Exception(str(v.errors))

# A couple of helpful decorators
def task_handler(function):
    SingleWorker(function).run()
def group_task_handler(function):
    GroupWorker(function).run()
