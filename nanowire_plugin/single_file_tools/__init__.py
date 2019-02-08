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
import urllib.request

from nanowire_plugin import report_task_success, report_task_failure

#set up the logger globally
logger = logging.getLogger("nanowire-plugin")

class Worker(object):
    task_handler = None
    controller_base_uri = None
    plugin_id = None
    pod_name = None


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
        while True:
            request_url = "{}/v1/tasks/?pluginId={}&pluginInstance={}".format(
                self.controller_base_uri,
                self.plugin_id,
                self.plugin_instance
                )
            response = requests.get(request_url)

            if response.status_code == 200:
                payload = response.json()
                meta = payload['metadata']
                jsonld = payload['jsonld']
                url = payload.get('url', None)

                if meta['job']['workflow']['type'] == 'GROUP':
                    logger.critical("Request for single task returned a group task")
                    report_task_failure(meta, 'received a group job for some reason')
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
