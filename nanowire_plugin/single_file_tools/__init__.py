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
import requests
import sys
import time
import traceback

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

    def run(self):
        logging.info("Handling tasks for {} as {}".format(self.plugin_id, self.plugin_instance))
        while True:
            request_url = "{}/v1/tasks/?pluginId={}&pluginInstance={}".format(
                self.controller_base_uri,
                self.plugin_id,
                self.plugin_instance
                )
            response = None
            try:
                response = requests.get(request_url)
                if not self.is_connected:
                    logger.info("Connected to controller at {}".format(self.controller_base_uri))
                    self.is_connected = True
            except Exception as e:
                logger.info("Failed to connect to controller: " + str(e))
                if self.is_connected:
                    self.is_connected = False
                time.sleep(1)
                continue

            if response.status_code == 200:
                payload = response.json()
                meta = payload['metadata']
                jsonld = payload['jsonld']
                url = payload.get('url', None)

                if meta['job']['workflow']['type'] == 'GROUP':
                    logger.critical("Request for single task returned a group task")
                    self.report_task_failure(meta['task']['_id'], 'received a group job for some reason')
                    return

                try:
                    result = self.task_handler(meta, jsonld, url)
                    self.report_task_success(meta['task']['_id'], result)
                except Exception as exp:
                    self.report_task_failure(meta['task']['_id'], str(exp))

            elif response.status_code == 404:
                time.sleep(1)


def task_handler(function):
    Worker(function).run()
