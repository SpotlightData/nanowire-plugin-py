# -*- coding: utf-8 -*-
"""
Created on Wed Oct 25 10:11:44 2017

@author: stuart
"""

#nanowire-plugin unit tests
import os
import copy
import nanowire_plugin as nwp
import unittest
import sys


import time
import json

from io import TextIOWrapper, BytesIO

import logging
#programs for setting up background servers to simulate nanowire
import urllib
import requests
import socket
import io

from minio import Minio
import pika

from threading import Thread

import traceback


if sys.version_info.major == 3:
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from unittest.mock import MagicMock
    from unittest.mock import patch
    import queue as Queue
elif sys.version_info.major == 2:
    import BaseHTTPServer
    import mock
    from mock import MagicMock
    from mock import patch
    import Queue
else:
    import http.server

import log_tools as lt

global logger
logger = lt.setup_logging()

###############################################
### test functions called by the unit tests ###
###############################################

#a valid function which can be bound to
def ok_test_function(nmo, jsonld, url):
            
    jsonld = {}
    jsonld["jsonld"] = {}
    jsonld["jsonld"]["text"] = "this is some text"
    
    return jsonld

#a test function which has the wrong input names
def bad_test_function_wrong_args(notnmo, jsonld, url):
    
    jsonld = {}
    jsonld["jsonld"] = {}
    jsonld["jsonld"]["text"] = "This is some text"
    
    return jsonld
    
#a test function with only two input arguments
def bad_test_function_wrong_no_args(notnmo, jsonld):
    
    jsonld = {}
    jsonld["jsonld"] = {}
    jsonld["jsonld"]["text"] = "This is some text"
    
    return jsonld
    
def simulated_next_plugin(ch, method, props, body):
    
    
    logger = logging.getLogger("nanowire-plugin")
    logger.setLevel(logging.DEBUG)

    
    data = body.decode("utf-8")
    #have to convert to unicode for python2 because python2 is difficult
    print(data)


class sim_queues():
    
    def __init__(self):
        
        self.queue_list = {}

    def sim_basic_publish(self, exchange, routing_key, body, properties=""):
        
        self.queue_list[routing_key].put(body)
        
    def sim_basic_consume(self, on_request, queue, no_ack=False):
        
        if queue in self.queue_list.keys():
            
            body = self.queue_list[queue].get(block=True)
            
        else:
            body = ""
            print("Tried to access %s queue but could not find it"%queue)
            
        return body
        
    def sim_queue_declare(self, name, durable):
        
        if name not in self.queue_list.keys():
            
            self.queue_list[name] = Queue.LifoQueue()
            
######################################
### Beginning of unit test classes ###
######################################

#test the validate payload function
class test_validate_nmo(unittest.TestCase):
    
    
    bad_inputs = [5, "example string"]    
    
    def testInteger(self):
        
        
        with self.assertRaises(Exception) as context:
            nwp.validate_payload(5)
            
        self.assertTrue("payload is a" in str(context.exception))
        
    def testString(self):
        
        with self.assertRaises(Exception) as context:
            nwp.validate_payload("example string")
            

        self.assertTrue("payload is a" in str(context.exception))

    def testEmptyDict(self):
        
        
        with self.assertRaises(Exception) as context:
            nwp.validate_payload({})
            

        self.assertTrue("No nmo in payload" in str(context.exception))
        
        
    def testbadnmo(self):
        
        bad_payload = {}
        bad_payload["nmo"] = {}
        
        with self.assertRaises(Exception) as context:
            nwp.validate_payload(bad_payload)

        self.assertTrue("No job in nmo" in str(context.exception))
        
        
    def testbadnmojob(self):
        
        bad_payload = {}
        bad_payload["jsonld"] = None
        bad_payload["url"] = None
        bad_payload["nmo"] = {}
        bad_payload["nmo"]["task"] = "test_task"
        
        with self.assertRaises(Exception) as context:
            nwp.validate_payload(bad_payload)

        self.assertTrue("No job in nmo" in str(context.exception))
        
    def testbadnmotask(self):
        
        bad_payload = {}
        bad_payload["jsonld"] = None
        bad_payload["url"] = None
        bad_payload["nmo"] = {}
        bad_payload["nmo"]["job"] = "test_job"
        
        with self.assertRaises(Exception) as context:
            nwp.validate_payload(bad_payload)


        self.assertTrue("No task in nmo" in str(context.exception))
        
        
        
    def testNojsonld(self):
                
        bad_payload = {}
        bad_payload["url"] = None
        bad_payload["nmo"] = {}
        bad_payload["nmo"]["job"] = "test_job"
        bad_payload["nmo"]["task"] = "test_task"
        
        with self.assertRaises(Exception) as context:
            nwp.validate_payload(bad_payload)

        self.assertTrue("No jsonld in payload" in str(context.exception))


class testNextPlugin(unittest.TestCase):
    
    
    #result = nwp.get_next_plugin(name, workflow)
    
    def testbadpluginname(self):
        
        name = 5
        
        self.good_workflow = [{
                    'env': {
                        'DEBUG': '1'
                    },
                    'config': {
                        'memory': '100M',
                        'cpu': '10m',
                        'env': {
                            'DEBUG': '0'
                        },
                        'cmd': ['python', 'image_classifier_main.py'],
                        'outputs': ['jsonld'],
                        'inputs': ['jsonld'],
                        'image': 'docker.spotlightdata.co.uk/plugins/worker_basic_image_classifier',
                        'description': 'Produces google tensorflow image classifications of images.',
                        'email': 'stuart@spotlightdata.co.uk',
                        'author': 'stuart',
                        'name': 'basic-image-classifier'
                    },
                    'children': ['image-ocr'],
                    'parents': ['input'],
                    'id': 'basic-image-classifier'
                }, 
                {
                    'env': {
                        'STORAGE_QUEUE': 'node-store',
                        'DEBUG': '1'
                    },
                    'config': {
                        'memory': '100M',
                        'cpu': '10m',
                        'env': {
                            'STORAGE_QUEUE': 'node-store',
                            'DEBUG': '0'
                        },
                        'cmd': ['/main'],
                        'outputs': [],
                        'inputs': ['jsonld'],
                        'image': 'docker.spotlightdata.co.uk/plugins/worker_node_store:1.1.0',
                        'description': '',
                        'email': 'stuart@spotlightdata.co.uk',
                        'author': 'stuart',
                        'name': 'node-store'
                    },
                    'children': [],
                    'parents': ['basic-image-classifier'],
                    'id': 'worker-node-store'
                }]

        
        with self.assertRaises(Exception) as context:
            nwp.get_next_plugin(name, self.good_workflow)


        self.assertTrue("Plugin name must be a string" in str(context.exception))
        
        
    def testgood(self):
            
            self.name = "basic-image-classifier"
            
            self.good_workflow = [{
                        'env': {
                            'DEBUG': '1'
                        },
                        'config': {
                            'memory': '100M',
                            'cpu': '10m',
                            'env': {
                                'DEBUG': '0'
                            },
                            'cmd': ['python', 'image_classifier_main.py'],
                            'outputs': ['jsonld'],
                            'inputs': ['jsonld'],
                            'image': 'docker.spotlightdata.co.uk/plugins/worker_basic_image_classifier',
                            'description': 'Produces google tensorflow image classifications of images.',
                            'email': 'stuart@spotlightdata.co.uk',
                            'author': 'stuart',
                            'name': 'basic-image-classifier'
                        },
                        'children': ['image-ocr'],
                        'parents': ['input'],
                        'id': 'basic-image-classifier'
                    }, 
                    {
                        'env': {
                            'STORAGE_QUEUE': 'node-store',
                            'DEBUG': '1'
                        },
                        'config': {
                            'memory': '100M',
                            'cpu': '10m',
                            'env': {
                                'STORAGE_QUEUE': 'node-store',
                                'DEBUG': '0'
                            },
                            'cmd': ['/main'],
                            'outputs': [],
                            'inputs': ['jsonld'],
                            'image': 'docker.spotlightdata.co.uk/plugins/worker_node_store:1.1.0',
                            'description': '',
                            'email': 'stuart@spotlightdata.co.uk',
                            'author': 'stuart',
                            'name': 'node-store'
                        },
                        'children': [],
                        'parents': ['basic-image-classifier'],
                        'id': 'worker-node-store'
                    }]
    

            result = nwp.get_next_plugin(self.name, self.good_workflow)

    
            self.assertTrue("node-store" in str(result))
            
    def testNoWorkflow(self):
        
        self.name = "basic-image-classifier"
        self.workflow = None
        
        with self.assertRaises(Exception) as context:
            nwp.get_next_plugin(self.name, self.workflow)
            

        self.assertTrue("Workflow must be a list" in str(context.exception))
    
    def testEmptyWorkflow(self):
        
        self.name = "basic-image-classifier"
        self.workflow = []
        

        result = nwp.get_next_plugin(self.name, self.workflow)

        self.assertTrue(result==None)



#test the get next plugin function
class testGet_this_plugin(unittest.TestCase):
    
    
    
    def test_no_plugin(self):
        
        self.name = None
        
        self.good_workflow = [{
                    'env': {
                        'DEBUG': '1'
                    },
                    'config': {
                        'memory': '100M',
                        'cpu': '10m',
                        'env': {
                            'DEBUG': '0'
                        },
                        'cmd': ['python', 'image_classifier_main.py'],
                        'outputs': ['jsonld'],
                        'inputs': ['jsonld'],
                        'image': 'docker.spotlightdata.co.uk/plugins/worker_basic_image_classifier',
                        'description': 'Produces google tensorflow image classifications of images.',
                        'email': 'stuart@spotlightdata.co.uk',
                        'author': 'stuart',
                        'name': 'basic-image-classifier'
                    },
                    'children': ['image-ocr'],
                    'parents': ['input'],
                    'id': 'basic-image-classifier'
                }, 
                {
                    'env': {
                        'STORAGE_QUEUE': 'node-store',
                        'DEBUG': '1'
                    },
                    'config': {
                        'memory': '100M',
                        'cpu': '10m',
                        'env': {
                            'STORAGE_QUEUE': 'node-store',
                            'DEBUG': '0'
                        },
                        'cmd': ['/main'],
                        'outputs': [],
                        'inputs': ['jsonld'],
                        'image': 'docker.spotlightdata.co.uk/plugins/worker_node_store:1.1.0',
                        'description': '',
                        'email': 'stuart@spotlightdata.co.uk',
                        'author': 'stuart',
                        'name': 'node-store'
                    },
                    'children': [],
                    'parents': ['basic-image-classifier'],
                    'id': 'worker-node-store'
                }]

        
        
        with self.assertRaises(Exception) as context:
            nwp.get_this_plugin(self.name, self.good_workflow)
            

        self.assertTrue("Plugin name must be a string" in str(context.exception))
        
        
    def test_no_workflow(self):
        
        self.name = "basic-image-classifier"
        self.workflow = None
        
        with self.assertRaises(Exception) as context:
            nwp.get_this_plugin(self.name, self.workflow)
            
        self.assertTrue("Workflow must be a list" in str(context.exception))
    
    
    def test_empty_workflow(self):
        
        self.name = "basic-image-classifier"
        self.workflow = []
        
        with self.assertRaises(Exception) as context:
            nwp.get_this_plugin(self.name, self.workflow)
            
        self.assertTrue("Workflow is empty, something is wrong" in str(context.exception))
        
    def test_wrong_plugin_name(self):
        
        self.name = "oops"
        self.good_workflow = [{
                    'env': {
                        'DEBUG': '1'
                    },
                    'config': {
                        'memory': '100M',
                        'cpu': '10m',
                        'env': {
                            'DEBUG': '0'
                        },
                        'cmd': ['python', 'image_classifier_main.py'],
                        'outputs': ['jsonld'],
                        'inputs': ['jsonld'],
                        'image': 'docker.spotlightdata.co.uk/plugins/worker_basic_image_classifier',
                        'description': 'Produces google tensorflow image classifications of images.',
                        'email': 'stuart@spotlightdata.co.uk',
                        'author': 'stuart',
                        'name': 'basic-image-classifier'
                    },
                    'children': ['image-ocr'],
                    'parents': ['input'],
                    'id': 'basic-image-classifier'
                }, 
                {
                    'env': {
                        'STORAGE_QUEUE': 'node-store',
                        'DEBUG': '1'
                    },
                    'config': {
                        'memory': '100M',
                        'cpu': '10m',
                        'env': {
                            'STORAGE_QUEUE': 'node-store',
                            'DEBUG': '0'
                        },
                        'cmd': ['/main'],
                        'outputs': [],
                        'inputs': ['jsonld'],
                        'image': 'docker.spotlightdata.co.uk/plugins/worker_node_store:1.1.0',
                        'description': '',
                        'email': 'stuart@spotlightdata.co.uk',
                        'author': 'stuart',
                        'name': 'node-store'
                    },
                    'children': [],
                    'parents': ['basic-image-classifier'],
                    'id': 'worker-node-store'
                }]

        result = nwp.get_this_plugin(self.name, self.good_workflow)

        self.assertTrue(result == -1)


#unit tests for setting the monitor
class test_SetMonitor(unittest.TestCase):
    
    
    def test_send_no_error(self):
        
        monitor_url = "http://localhost:" + str(mock_monitor.mock_server_port)
        
        job_id = "1"
        
        task_id = "1"
        
        name = "example_plugin"

        #this should be a perfectly valid input
        nwp.set_status(monitor_url, job_id, task_id, name)


            
    def test_send_with_error(self):
        
        monitor_url = "http://localhost:" + str(mock_monitor.mock_server_port)
        
        job_id = "1"
        
        task_id = "1"
        
        name = "example_plugin"


        #this should be a perfectly valid input
        nwp.set_status(monitor_url, job_id, task_id, name, error="there is a problem")



    def test_send_bad_url(self):
        
        monitor_url = "not a url"
        
        job_id = "1"
        
        task_id = "1"
        
        name = "example_plugin"

            
            
        with self.assertRaises(Exception) as context:
            nwp.set_status(monitor_url, job_id, task_id, name)
            
        self.assertTrue("unknown url type:" in str(context.exception))
        
        
    def test_no_url(self):
        
        monitor_url = None
        
        job_id = "1"
        
        task_id = "1"
        
        name = "example_plugin"

            
            
        with self.assertRaises(Exception) as context:
            nwp.set_status(monitor_url, job_id, task_id, name)
            
        self.assertTrue("URL should be a string it is" in str(context.exception))
        

    def test_send_no_job(self):
        
        monitor_url = "http://localhost:" + str(mock_monitor.mock_server_port)
        
        job_id = None
        
        task_id = "1"
        
        name = "example_plugin"

            
            
        with self.assertRaises(Exception) as context:
            nwp.set_status(monitor_url, job_id, task_id, name)
            
        self.assertTrue("job_id should be a string, it is " in str(context.exception))
        
        
    
    def test_send_no_taskid(self):
        
        monitor_url = "http://localhost:" + str(mock_monitor.mock_server_port)
        
        job_id = "1"
        
        task_id = None
        
        name = "example_plugin"

            
            
        with self.assertRaises(Exception) as context:
            nwp.set_status(monitor_url, job_id, task_id, name)
            
        self.assertTrue("task_id should be a string, it is " in str(context.exception))


    def test_send_no_name(self):
        
        monitor_url = "http://localhost:" + str(mock_monitor.mock_server_port)
        
        job_id = "1"
        
        task_id = "1"
        
        name = None

            
        with self.assertRaises(Exception) as context:
            nwp.set_status(monitor_url, job_id, task_id, name)
            
        self.assertTrue("plugin name should be a string, it is " in str(context.exception))



#unit tests for the no_request class
class test_on_request_class(unittest.TestCase):
    
    
    def test_no_name(self):
        
        name = None
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)

        monitor_url = os.environ["MONITOR_URL"]

        output_channel = MagicMock()
        output_channel.is_open = True

        with self.assertRaises(Exception) as context:
            nwp.on_request_class(ok_test_function, name, minio_client, output_channel, monitor_url)
            
        self.assertTrue("plugin name should be a string, it is " in str(context.exception))
        
        
        
    def test_bad_function_input_names_on_request(self):
        
        name = "example name"
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)

        monitor_url = os.environ["MONITOR_URL"]

        output_channel = MagicMock()
        output_channel.is_open = True

        with self.assertRaises(Exception) as context:
            nwp.on_request_class(bad_test_function_wrong_args, name, minio_client, output_channel, monitor_url)
            
        self.assertTrue("Bound function must use argument names: ['nmo', 'jsonld', 'url']. You have used" in str(context.exception))


    def test_bad_function_wrong_args_number(self):

        name = "example name"
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)

        monitor_url = os.environ["MONITOR_URL"]
        
        connection = MagicMock()
        connection.channel = MagicMock()
        output_channel = connection.channel()
        output_channel.is_open = True        
        

        with self.assertRaises(Exception) as context:
            nwp.on_request_class(bad_test_function_wrong_no_args, name, minio_client, output_channel, monitor_url)

        self.assertTrue("Bound function must take 3 arguments: nmo, jsonld and url" in str(context.exception))



    def test_bad_monitor_url(self):
        
        name = "example name"
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)

        monitor_url = None

        connection = MagicMock()
        connection.channel = MagicMock()
        output_channel = connection.channel()
        output_channel.is_open = True 

        with self.assertRaises(Exception) as context:
            nwp.on_request_class(ok_test_function, name, minio_client, output_channel, monitor_url)


        self.assertTrue("monitor_url should be a string, it is actually" in str(context.exception))


    def test_bad_output_channel(self):
        
        name = "example name"
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)

        monitor_url = os.environ["MONITOR_URL"]

        #set up the channels        
        output_channel = None
        

        with self.assertRaises(Exception) as context:
            nwp.on_request_class(ok_test_function, name, minio_client, output_channel, monitor_url)


        self.assertTrue("output channel should be a pika blocking connection channel it is actually " in str(context.exception))
    
    
    
    def test_closed_output_channel_send_test(self):
        
        name = "example name"
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)

        monitor_url = os.environ["MONITOR_URL"]
        
        output_channel = MagicMock()
        output_channel.is_open = False

        with self.assertRaises(Exception) as context:
            nwp.on_request_class(ok_test_function, name, minio_client, output_channel, monitor_url)

        self.assertTrue("Output channel is closed" in str(context.exception))
        
        
    def test_good_result_on_request_class(self):
        
        name = "example name"
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)

        monitor_url = os.environ["MONITOR_URL"]

        
        connection = MagicMock()
        connection.channel = MagicMock()
        output_channel = connection.channel()
        
        test = nwp.on_request_class(ok_test_function, name, minio_client, output_channel, monitor_url)
        
        #just test that the on_request class is a valid class
        if sys.version_info.major == 2:
            self.assertTrue(str(type(test))=="<type 'instance'>")
        else:
            self.assertTrue(str(type(test))=="<class 'nanowire_plugin.on_request_class'>")
        
        #check that the variables have been set right
        self.assertTrue("ok_test_function" in str(test.function))
        
        self.assertTrue("minio.api.Minio" in str(test.minio_client))
        
        self.assertTrue(monitor_url == test.monitor_url)
        
        self.assertTrue(name == test.name)
        
        self.assertTrue("on_request" in str(test.on_request))
        
        self.assertTrue(output_channel == output_channel)
    

class test_clean_function_output(unittest.TestCase):
    
    
    def test_no_payload_or_result(self):
        

        result = None        
        
        payload = None        
                
        with self.assertRaises(Exception) as context:
            nwp.clean_function_output(result, payload)


        self.assertTrue("An empty payload has been receved" in str(context.exception))
    
        
        
    def test_no_payload_no_jsonld_result(self):
        
        result = {}
        result["@context"] = "http://schema.org/"
        result["@type"] = "ImageObject"
        result["@graph"] = []
        
        payload = None
        
        target = {}
        target = {}
        target["@context"] = "http://schema.org/"
        target["@type"] = "ImageObject"
        target["@graph"] = []
        

        with self.assertRaises(Exception) as context:
            nwp.clean_function_output(result, payload)


        self.assertTrue("An empty payload has been receved" in str(context.exception))
    
        
        
        
    def test_no_payload_good_json(self):
        
        
        jsonld = {}
        jsonld["@context"] = "http://schema.org/"
        jsonld["@type"] = "ImageObject"
        jsonld["@graph"] = []
        
        payload = None
        
        
        with self.assertRaises(Exception) as context:
            nwp.clean_function_output(jsonld, payload)


        self.assertTrue("An empty payload has been receved" in str(context.exception))
    
        
        
        
    def test_no_jsonld(self):
        
                    
                    
        nmo = {
            "schema_version": "1.0.0",
            "job": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "user_id": "u-00000000-0000-0000-0000-000000000000",
                "project_id": "p-00000000-0000-0000-0000-000000000000",
                "job_id": "j-00000000-0000-0000-0000-000000000000",
                "job_run": 0,
                "priority": 500,
                "workflow": [
                    {
                        "id": "worker-extract-47a6",
                        "parents": [
                            "input"
                        ],
                        "children": [
                            "worker-spacy-b31e"
                        ],
                        "config": {
                            "name": "Extract",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_extract:2.7.5",
                            "cmd": [
                                "/main"
                            ],
                            "inputs": [
                                "source",
                                "nmo"
                            ],
                            "outputs": [
                                "plugin",
                                "text"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0"
                            }
                        },
                        "env": {
                            "DEBUG": "1"
                        },
                        "lines_formatted": False
                    },
                    {
                        "id": "worker-spacy-b31e",
                        "parents": [
                            "worker-extract-47a6"
                        ],
                        "children": [],
                        "config": {
                            "name": "Spacy",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_spacy:1.0.0",
                            "cmd": [
                                "python3",
                                "main.py"
                            ],
                            "inputs": [
                                "text"
                            ],
                            "outputs": [
                                "plugin"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0",
                                "SPAAS_HOST": "http://spacy.spotlightdata.co.uk"
                            }
                        },
                        "env": {
                            "DEBUG": "1",
                            "SPAAS_HOST": "spacy.default"
                        },
                        "lines_formatted": False
                    }
                ],
                "misc": {
                    "what": "job meta should be the same for all tasks associated with a job, this field is for job-level decision making information such as the parallelism level etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "task": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "task_id": "t-00000000-0000-0000-0000-000000000000",
                "parent": "t-10000000-0000-0000-0000-000000000000",
                "misc": {
                    "what": "metadata for this task in the context of the system, nothing to do with the source file but possibly directives for how to handle this task such as some special kind of analysis etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "source": {
                "name": "test_image_1",
                "misc": {
                    "what": "information about the original source file, creation date, associated users, onedrive stuff, gdrive stuff, etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            }
        }
        
        payload = {}
        payload["nmo"] = nmo
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []
        
        jsonld = None        
        
        out = nwp.clean_function_output(jsonld, payload)
        
        
        target = {}
        target = payload["jsonld"]
        
        
        self.assertTrue(target==out)
        
        
    def test_everything_fine(self):
    

        nmo = {
            "schema_version": "1.0.0",
            "job": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "user_id": "u-00000000-0000-0000-0000-000000000000",
                "project_id": "p-00000000-0000-0000-0000-000000000000",
                "job_id": "j-00000000-0000-0000-0000-000000000000",
                "job_run": 0,
                "priority": 500,
                "workflow": [
                    {
                        "id": "worker-extract-47a6",
                        "parents": [
                            "input"
                        ],
                        "children": [
                            "worker-spacy-b31e"
                        ],
                        "config": {
                            "name": "Extract",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_extract:2.7.5",
                            "cmd": [
                                "/main"
                            ],
                            "inputs": [
                                "source",
                                "nmo"
                            ],
                            "outputs": [
                                "plugin",
                                "text"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0"
                            }
                        },
                        "env": {
                            "DEBUG": "1"
                        },
                        "lines_formatted": False
                    },
                    {
                        "id": "worker-spacy-b31e",
                        "parents": [
                            "worker-extract-47a6"
                        ],
                        "children": [],
                        "config": {
                            "name": "Spacy",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_spacy:1.0.0",
                            "cmd": [
                                "python3",
                                "main.py"
                            ],
                            "inputs": [
                                "text"
                            ],
                            "outputs": [
                                "plugin"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0",
                                "SPAAS_HOST": "http://spacy.spotlightdata.co.uk"
                            }
                        },
                        "env": {
                            "DEBUG": "1",
                            "SPAAS_HOST": "spacy.default"
                        },
                        "lines_formatted": False
                    }
                ],
                "misc": {
                    "what": "job meta should be the same for all tasks associated with a job, this field is for job-level decision making information such as the parallelism level etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "task": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "task_id": "t-00000000-0000-0000-0000-000000000000",
                "parent": "t-10000000-0000-0000-0000-000000000000",
                "misc": {
                    "what": "metadata for this task in the context of the system, nothing to do with the source file but possibly directives for how to handle this task such as some special kind of analysis etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "source": {
                "name": "test_image_1",
                "misc": {
                    "what": "information about the original source file, creation date, associated users, onedrive stuff, gdrive stuff, etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            }
        }
        
        payload = {}
        payload["nmo"] = nmo
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []
        
        jsonld = {}
        jsonld["@context"] = "http://schema.org/"
        jsonld["@type"] = "ImageObject"
        jsonld["@graph"] = []  
        
        out = nwp.clean_function_output(jsonld, payload)
        
        
        self.assertTrue(jsonld==out)



#figuring out how to simulate send to next plugin
#send_to_next_plugin(next_plugin, payload, output_channel)
class test_sendToNextPlugin(unittest.TestCase):
    
    
    #example of the pass to next plugin system working well
    def test_pass_next_plugin(self):
        
        
        #Set up the example payload
        nmo = {
            "schema_version": "1.0.0",
            "job": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "user_id": "u-00000000-0000-0000-0000-000000000000",
                "project_id": "p-00000000-0000-0000-0000-000000000000",
                "job_id": "j-00000000-0000-0000-0000-000000000000",
                "job_run": 0,
                "priority": 500,
                "workflow": [
                    {
                        "id": "worker-extract-47a6",
                        "parents": [
                            "input"
                        ],
                        "children": [
                            "worker-spacy-b31e"
                        ],
                        "config": {
                            "name": "Extract",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_extract:2.7.5",
                            "cmd": [
                                "/main"
                            ],
                            "inputs": [
                                "source",
                                "nmo"
                            ],
                            "outputs": [
                                "plugin",
                                "text"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0"
                            }
                        },
                        "env": {
                            "DEBUG": "1"
                        },
                        "lines_formatted": False
                    },
                    {
                        "id": "worker-spacy-b31e",
                        "parents": [
                            "worker-extract-47a6"
                        ],
                        "children": [],
                        "config": {
                            "name": "Spacy",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_spacy:1.0.0",
                            "cmd": [
                                "python3",
                                "main.py"
                            ],
                            "inputs": [
                                "text"
                            ],
                            "outputs": [
                                "plugin"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0",
                                "SPAAS_HOST": "http://spacy.spotlightdata.co.uk"
                            }
                        },
                        "env": {
                            "DEBUG": "1",
                            "SPAAS_HOST": "spacy.default"
                        },
                        "lines_formatted": False
                    }
                ],
                "misc": {
                    "what": "job meta should be the same for all tasks associated with a job, this field is for job-level decision making information such as the parallelism level etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "task": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "task_id": "t-00000000-0000-0000-0000-000000000000",
                "parent": "t-10000000-0000-0000-0000-000000000000",
                "misc": {
                    "what": "metadata for this task in the context of the system, nothing to do with the source file but possibly directives for how to handle this task such as some special kind of analysis etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "source": {
                "name": "test_image_1",
                "misc": {
                    "what": "information about the original source file, creation date, associated users, onedrive stuff, gdrive stuff, etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            }
        }
        
        payload = {}
        payload["nmo"] = nmo
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []        
        
        #set up the communications channel as a magic mock
        connection = MagicMock()
        connection.channel = MagicMock()
        output_channel = connection.channel()
        output_channel.is_open = True
        
        
        #we're going to simulate the rabbitmq functions using pythons queue library
        queuer = sim_queues()
    
        output_channel.basic_publish = queuer.sim_basic_publish
        output_channel.basic_consume = queuer.sim_basic_consume
        output_channel.queue_declare = queuer.sim_queue_declare
        
        #set up the name of the example next plugin
        next_plugin = "example_next_plugin1"
        
        #this is the function that we're here to actually test
        nwp.send_to_next_plugin(next_plugin, payload, output_channel)

        #check the body has been added to the queue
        test = output_channel.basic_consume(MagicMock(), next_plugin, False)
        
        #Test the body matches what we intended to save
        self.assertTrue(payload == json.loads(test))

    
    def test_no_payload(self):
        
        payload = None
        
        next_plugin = "example_next_plugin2"
            
        connection = MagicMock()
        MagicMock.channel = MagicMock()
        output_channel = connection.channel()
        output_channel.is_open = True
        
        with self.assertRaises(Exception) as context:
            nwp.send_to_next_plugin(next_plugin, payload, output_channel)
            

            
        self.assertTrue("payload should be a dictionary it is in fact:" in str(context.exception))        
        
        #no need to mock the next plugin here as it should just fail
    def test_bad_next_plugin(self):
        
        #Set up the example payload
        nmo = {
            "schema_version": "1.0.0",
            "job": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "user_id": "u-00000000-0000-0000-0000-000000000000",
                "project_id": "p-00000000-0000-0000-0000-000000000000",
                "job_id": "j-00000000-0000-0000-0000-000000000000",
                "job_run": 0,
                "priority": 500,
                "workflow": [
                    {
                        "id": "worker-extract-47a6",
                        "parents": [
                            "input"
                        ],
                        "children": [
                            "worker-spacy-b31e"
                        ],
                        "config": {
                            "name": "Extract",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_extract:2.7.5",
                            "cmd": [
                                "/main"
                            ],
                            "inputs": [
                                "source",
                                "nmo"
                            ],
                            "outputs": [
                                "plugin",
                                "text"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0"
                            }
                        },
                        "env": {
                            "DEBUG": "1"
                        },
                        "lines_formatted": False
                    },
                    {
                        "id": "worker-spacy-b31e",
                        "parents": [
                            "worker-extract-47a6"
                        ],
                        "children": [],
                        "config": {
                            "name": "Spacy",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_spacy:1.0.0",
                            "cmd": [
                                "python3",
                                "main.py"
                            ],
                            "inputs": [
                                "text"
                            ],
                            "outputs": [
                                "plugin"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0",
                                "SPAAS_HOST": "http://spacy.spotlightdata.co.uk"
                            }
                        },
                        "env": {
                            "DEBUG": "1",
                            "SPAAS_HOST": "spacy.default"
                        },
                        "lines_formatted": False
                    }
                ],
                "misc": {
                    "what": "job meta should be the same for all tasks associated with a job, this field is for job-level decision making information such as the parallelism level etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "task": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "task_id": "t-00000000-0000-0000-0000-000000000000",
                "parent": "t-10000000-0000-0000-0000-000000000000",
                "misc": {
                    "what": "metadata for this task in the context of the system, nothing to do with the source file but possibly directives for how to handle this task such as some special kind of analysis etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "source": {
                "name": "test_image_1",
                "misc": {
                    "what": "information about the original source file, creation date, associated users, onedrive stuff, gdrive stuff, etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            }
        }
        
        payload = {}
        payload["nmo"] = nmo
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []  
        
        next_plugin = 11
    
        connection = MagicMock()
        connection.channel = MagicMock()
        output_channel = connection.channel()
        output_channel.is_open = True
        
        with self.assertRaises(Exception) as context:
            nwp.send_to_next_plugin(next_plugin, payload, output_channel)
            

            
        self.assertTrue("Next plugin should be a string if present or None if no next plugin. It is actually" in str(context.exception))   
        
        
    def test_no_next_plugin(self):

                
        #Set up the example payload
        nmo = {
            "schema_version": "1.0.0",
            "job": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "user_id": "u-00000000-0000-0000-0000-000000000000",
                "project_id": "p-00000000-0000-0000-0000-000000000000",
                "job_id": "j-00000000-0000-0000-0000-000000000000",
                "job_run": 0,
                "priority": 500,
                "workflow": [
                    {
                        "id": "worker-extract-47a6",
                        "parents": [
                            "input"
                        ],
                        "children": [
                            "worker-spacy-b31e"
                        ],
                        "config": {
                            "name": "Extract",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_extract:2.7.5",
                            "cmd": [
                                "/main"
                            ],
                            "inputs": [
                                "source",
                                "nmo"
                            ],
                            "outputs": [
                                "plugin",
                                "text"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0"
                            }
                        },
                        "env": {
                            "DEBUG": "1"
                        },
                        "lines_formatted": False
                    },
                    {
                        "id": "worker-spacy-b31e",
                        "parents": [
                            "worker-extract-47a6"
                        ],
                        "children": [],
                        "config": {
                            "name": "Spacy",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_spacy:1.0.0",
                            "cmd": [
                                "python3",
                                "main.py"
                            ],
                            "inputs": [
                                "text"
                            ],
                            "outputs": [
                                "plugin"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0",
                                "SPAAS_HOST": "http://spacy.spotlightdata.co.uk"
                            }
                        },
                        "env": {
                            "DEBUG": "1",
                            "SPAAS_HOST": "spacy.default"
                        },
                        "lines_formatted": False
                    }
                ],
                "misc": {
                    "what": "job meta should be the same for all tasks associated with a job, this field is for job-level decision making information such as the parallelism level etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "task": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "task_id": "t-00000000-0000-0000-0000-000000000000",
                "parent": "t-10000000-0000-0000-0000-000000000000",
                "misc": {
                    "what": "metadata for this task in the context of the system, nothing to do with the source file but possibly directives for how to handle this task such as some special kind of analysis etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "source": {
                "name": "test_image_1",
                "misc": {
                    "what": "information about the original source file, creation date, associated users, onedrive stuff, gdrive stuff, etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            }
        }
        
        payload = {}
        payload["nmo"] = nmo
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []
        
        
        #set up the communications channel as a magic mock
        connection = MagicMock()
        connection.channel = MagicMock()
        output_channel = connection.channel()
        output_channel.is_open = True
        
        
        #we're going to simulate the rabbitmq functions using pythons queue library
        queuer = sim_queues()
        output_channel.basic_publish = queuer.sim_basic_publish
        output_channel.basic_consume = queuer.sim_basic_consume
        output_channel.queue_declare = queuer.sim_queue_declare
        
        #This is an example where there is no next plugin
        next_plugin = None
        
        nwp.send_to_next_plugin(next_plugin, payload, output_channel)
        
        #This basically just needs to pass as there should be no output but a log
        
        
    def test_closed_output_channel(self):
           
        #Set up the example payload
        nmo = {
            "schema_version": "1.0.0",
            "job": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "user_id": "u-00000000-0000-0000-0000-000000000000",
                "project_id": "p-00000000-0000-0000-0000-000000000000",
                "job_id": "j-00000000-0000-0000-0000-000000000000",
                "job_run": 0,
                "priority": 500,
                "workflow": [
                    {
                        "id": "worker-extract-47a6",
                        "parents": [
                            "input"
                        ],
                        "children": [
                            "worker-spacy-b31e"
                        ],
                        "config": {
                            "name": "Extract",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_extract:2.7.5",
                            "cmd": [
                                "/main"
                            ],
                            "inputs": [
                                "source",
                                "nmo"
                            ],
                            "outputs": [
                                "plugin",
                                "text"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0"
                            }
                        },
                        "env": {
                            "DEBUG": "1"
                        },
                        "lines_formatted": False
                    },
                    {
                        "id": "worker-spacy-b31e",
                        "parents": [
                            "worker-extract-47a6"
                        ],
                        "children": [],
                        "config": {
                            "name": "Spacy",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_spacy:1.0.0",
                            "cmd": [
                                "python3",
                                "main.py"
                            ],
                            "inputs": [
                                "text"
                            ],
                            "outputs": [
                                "plugin"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0",
                                "SPAAS_HOST": "http://spacy.spotlightdata.co.uk"
                            }
                        },
                        "env": {
                            "DEBUG": "1",
                            "SPAAS_HOST": "spacy.default"
                        },
                        "lines_formatted": False
                    }
                ],
                "misc": {
                    "what": "job meta should be the same for all tasks associated with a job, this field is for job-level decision making information such as the parallelism level etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "task": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "task_id": "t-00000000-0000-0000-0000-000000000000",
                "parent": "t-10000000-0000-0000-0000-000000000000",
                "misc": {
                    "what": "metadata for this task in the context of the system, nothing to do with the source file but possibly directives for how to handle this task such as some special kind of analysis etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "source": {
                "name": "test_image_1",
                "misc": {
                    "what": "information about the original source file, creation date, associated users, onedrive stuff, gdrive stuff, etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            }
        }
        
        payload = {}
        payload["nmo"] = nmo
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []  
        
        next_plugin = "example_next_plugin3"
        
        connection = MagicMock()
        connection.channel = MagicMock()
        output_channel = connection.channel()
        output_channel = MagicMock()
        output_channel.is_open = False
        
        with self.assertRaises(Exception) as context:
            nwp.send_to_next_plugin(next_plugin, payload, output_channel)
            
        self.assertTrue("Output channel is closed" in str(context.exception))   
    
        
    def test_non_pika_channel(self):
           
        #Set up the example payload
        nmo = {
            "schema_version": "1.0.0",
            "job": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "user_id": "u-00000000-0000-0000-0000-000000000000",
                "project_id": "p-00000000-0000-0000-0000-000000000000",
                "job_id": "j-00000000-0000-0000-0000-000000000000",
                "job_run": 0,
                "priority": 500,
                "workflow": [
                    {
                        "id": "worker-extract-47a6",
                        "parents": [
                            "input"
                        ],
                        "children": [
                            "worker-spacy-b31e"
                        ],
                        "config": {
                            "name": "Extract",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_extract:2.7.5",
                            "cmd": [
                                "/main"
                            ],
                            "inputs": [
                                "source",
                                "nmo"
                            ],
                            "outputs": [
                                "plugin",
                                "text"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0"
                            }
                        },
                        "env": {
                            "DEBUG": "1"
                        },
                        "lines_formatted": False
                    },
                    {
                        "id": "worker-spacy-b31e",
                        "parents": [
                            "worker-extract-47a6"
                        ],
                        "children": [],
                        "config": {
                            "name": "Spacy",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_spacy:1.0.0",
                            "cmd": [
                                "python3",
                                "main.py"
                            ],
                            "inputs": [
                                "text"
                            ],
                            "outputs": [
                                "plugin"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0",
                                "SPAAS_HOST": "http://spacy.spotlightdata.co.uk"
                            }
                        },
                        "env": {
                            "DEBUG": "1",
                            "SPAAS_HOST": "spacy.default"
                        },
                        "lines_formatted": False
                    }
                ],
                "misc": {
                    "what": "job meta should be the same for all tasks associated with a job, this field is for job-level decision making information such as the parallelism level etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "task": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "task_id": "t-00000000-0000-0000-0000-000000000000",
                "parent": "t-10000000-0000-0000-0000-000000000000",
                "misc": {
                    "what": "metadata for this task in the context of the system, nothing to do with the source file but possibly directives for how to handle this task such as some special kind of analysis etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "source": {
                "name": "test_image_1",
                "misc": {
                    "what": "information about the original source file, creation date, associated users, onedrive stuff, gdrive stuff, etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            }
        }
        
        payload = {}
        payload["nmo"] = nmo
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []  
        
        next_plugin = "example_next_plugin3"
        

        output_channel = None
        
        
        with self.assertRaises(Exception) as context:
            nwp.send_to_next_plugin(next_plugin, payload, output_channel)
            

            
        self.assertTrue("output channel should be a pika blocking connection channel it is actually" in str(context.exception))   
    
    def test_bad_payload(self):
           
        #Set up the example payload
        
        payload = {}
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []  
        
        next_plugin = "example_next_plugin4"
        
        connection = MagicMock()
        connection.channel = MagicMock()
        output_channel = connection.channel()
        
        
        with self.assertRaises(Exception) as context:
            nwp.send_to_next_plugin(next_plugin, payload, output_channel)
            
        self.assertTrue("nmo is critical to payload however is missing, payload is currently" in str(context.exception))   
        
        
        

#test the inform_monitor function
class test_inform_monitor(unittest.TestCase):
    
    def test_fine(self):
        
        name = "example_plugin"
        #Set up the example payload
        nmo = {
            "schema_version": "1.0.0",
            "job": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "user_id": "u-00000000-0000-0000-0000-000000000000",
                "project_id": "p-00000000-0000-0000-0000-000000000000",
                "job_id": "j-00000000-0000-0000-0000-000000000000",
                "job_run": 0,
                "priority": 500,
                "workflow": [
                    {
                        "id": "worker-extract-47a6",
                        "parents": [
                            "input"
                        ],
                        "children": [
                            "worker-spacy-b31e"
                        ],
                        "config": {
                            "name": "example_plugin",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_extract:2.7.5",
                            "cmd": [
                                "/main"
                            ],
                            "inputs": [
                                "source",
                                "nmo"
                            ],
                            "outputs": [
                                "plugin",
                                "text"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0"
                            }
                        },
                        "env": {
                            "DEBUG": "1"
                        },
                        "lines_formatted": False
                    },
                    {
                        "id": "example_plugin",
                        "parents": [
                            "worker-extract-47a6"
                        ],
                        "children": [],
                        "config": {
                            "name": "Spacy",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_spacy:1.0.0",
                            "cmd": [
                                "python3",
                                "main.py"
                            ],
                            "inputs": [
                                "text"
                            ],
                            "outputs": [
                                "plugin"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0",
                                "SPAAS_HOST": "http://spacy.spotlightdata.co.uk"
                            }
                        },
                        "env": {
                            "DEBUG": "1",
                            "SPAAS_HOST": "spacy.default"
                        },
                        "lines_formatted": False
                    }
                ],
                "misc": {
                    "what": "job meta should be the same for all tasks associated with a job, this field is for job-level decision making information such as the parallelism level etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "task": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "task_id": "t-00000000-0000-0000-0000-000000000000",
                "parent": "t-10000000-0000-0000-0000-000000000000",
                "misc": {
                    "what": "metadata for this task in the context of the system, nothing to do with the source file but possibly directives for how to handle this task such as some special kind of analysis etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "source": {
                "name": "test_image_1",
                "misc": {
                    "what": "information about the original source file, creation date, associated users, onedrive stuff, gdrive stuff, etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            }
        }
        
        payload = {}
        payload["nmo"] = nmo
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []  
                
        
        
        version = "1.0.0"
        
        
        monitor_url = "http://localhost:" + str(mock_monitor.mock_server_port)
        
        #set up the minio client
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)
        
        
        #print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")        
        
        minio_client.set_app_info(name, version)
        
        #minio_client.stat_object(payload["nmo"]["job"]["job_id"], path)
        
        if sys.version_info.major == 3:
            minio_client.stat_object = MagicMock(True)
            minio_client.presigned_get_object = MagicMock(return_value="http://example_url.com")
            
            
        elif sys.version_info.major == 2:
            minio_client.stat_object = mock.Mock()
            minio_client.stat_object.return_value = True
            
            
            minio_client.presigned_get_object = mock.Mock()
            minio_client.presigned_get_object.return_value = "http://example_url.com"
            
        else:
            minio_client.stat_object = mock.Mock()
            minio_client.stat_object.return_value = True
            
            
            minio_client.presigned_get_object = mock.Mock()
            minio_client.presigned_get_object.return_value = "http://example_url.com"
            


        out = nwp.inform_monitor(payload, name, monitor_url, minio_client)

        
        self.assertTrue(out==['Spacy', 'http://example_url.com'])
    
    def test_no_payload(self):
        
        payload = None
        
        name = "example_plugin"
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)
             
             
             
        version = "1.0.0"
        minio_client.set_app_info(name, version)        
        
        
        monitor_url = "http://localhost:" + str(mock_monitor.mock_server_port)
        

        with self.assertRaises(Exception) as context:
            nwp.inform_monitor(payload, name, monitor_url, minio_client)
            
        self.assertTrue("Payload should be a dictionary, it is actually: " in str(context.exception))   
        
    
    def test_bad_payload(self):
        
        payload = {}
        
        name = "example_plugin"
        
        minio_client = Minio(
        os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
        access_key=os.environ["MINIO_ACCESS"],
        secret_key=os.environ["MINIO_SECRET"],
        secure=True if os.environ["MINIO_SCHEME"] == "https" else False)

        version = "1.0.0"
        minio_client.set_app_info(name, version)        
        
        
        monitor_url = "http://localhost:" + str(mock_monitor.mock_server_port)

        with self.assertRaises(Exception) as context:
            nwp.inform_monitor(payload, name, monitor_url, minio_client)
            
        #check we hit the problems covered in validate payload
        self.assertTrue("No nmo in payload" in str(context.exception))
        
        
    def test_bad_name(self):
        
        
        name = None
        #Set up the example payload
        nmo = {
            "schema_version": "1.0.0",
            "job": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "user_id": "u-00000000-0000-0000-0000-000000000000",
                "project_id": "p-00000000-0000-0000-0000-000000000000",
                "job_id": "j-00000000-0000-0000-0000-000000000000",
                "job_run": 0,
                "priority": 500,
                "workflow": [
                    {
                        "id": "worker-extract-47a6",
                        "parents": [
                            "input"
                        ],
                        "children": [
                            "worker-spacy-b31e"
                        ],
                        "config": {
                            "name": "example_plugin",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_extract:2.7.5",
                            "cmd": [
                                "/main"
                            ],
                            "inputs": [
                                "source",
                                "nmo"
                            ],
                            "outputs": [
                                "plugin",
                                "text"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0"
                            }
                        },
                        "env": {
                            "DEBUG": "1"
                        },
                        "lines_formatted": False
                    },
                    {
                        "id": "example_plugin",
                        "parents": [
                            "worker-extract-47a6"
                        ],
                        "children": [],
                        "config": {
                            "name": "Spacy",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_spacy:1.0.0",
                            "cmd": [
                                "python3",
                                "main.py"
                            ],
                            "inputs": [
                                "text"
                            ],
                            "outputs": [
                                "plugin"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0",
                                "SPAAS_HOST": "http://spacy.spotlightdata.co.uk"
                            }
                        },
                        "env": {
                            "DEBUG": "1",
                            "SPAAS_HOST": "spacy.default"
                        },
                        "lines_formatted": False
                    }
                ],
                "misc": {
                    "what": "job meta should be the same for all tasks associated with a job, this field is for job-level decision making information such as the parallelism level etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "task": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "task_id": "t-00000000-0000-0000-0000-000000000000",
                "parent": "t-10000000-0000-0000-0000-000000000000",
                "misc": {
                    "what": "metadata for this task in the context of the system, nothing to do with the source file but possibly directives for how to handle this task such as some special kind of analysis etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "source": {
                "name": "test_image_1",
                "misc": {
                    "what": "information about the original source file, creation date, associated users, onedrive stuff, gdrive stuff, etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            }
        }
        
        payload = {}
        payload["nmo"] = nmo
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []  
                
        
        
        version = "1.0.0"
        
        
        monitor_url = "http://localhost:" + str(mock_monitor.mock_server_port)
        
        #set up the minio client
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)
        
        
      
        minio_name = "test"
        minio_client.set_app_info(minio_name, version)
        

        if sys.version_info.major == 3:
            minio_client.stat_object = MagicMock(True)
            minio_client.presigned_get_object = MagicMock(return_value="http://example_url.com")
            
            
        elif sys.version_info.major == 2:
            minio_client.stat_object = mock.Mock()
            minio_client.stat_object.return_value = True
            
            
            minio_client.presigned_get_object = mock.Mock()
            minio_client.presigned_get_object.return_value = "http://example_url.com"
            
        else:
            minio_client.stat_object = mock.Mock()
            minio_client.stat_object.return_value = True
            
            
            minio_client.presigned_get_object = mock.Mock()
            minio_client.presigned_get_object.return_value = "http://example_url.com"
        
        with self.assertRaises(Exception) as context:
            nwp.inform_monitor(payload, name, monitor_url, minio_client)
        
        #check we hit the problems covered in validate payload
        self.assertTrue("plugin name should be a string, it is actually:" in str(context.exception))
        
        
    def test_bad_monitor_url(self):
        
        name = "example_plugin"
        #Set up the example payload
        nmo = {
            "schema_version": "1.0.0",
            "job": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "user_id": "u-00000000-0000-0000-0000-000000000000",
                "project_id": "p-00000000-0000-0000-0000-000000000000",
                "job_id": "j-00000000-0000-0000-0000-000000000000",
                "job_run": 0,
                "priority": 500,
                "workflow": [
                    {
                        "id": "worker-extract-47a6",
                        "parents": [
                            "input"
                        ],
                        "children": [
                            "worker-spacy-b31e"
                        ],
                        "config": {
                            "name": "example_plugin",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_extract:2.7.5",
                            "cmd": [
                                "/main"
                            ],
                            "inputs": [
                                "source",
                                "nmo"
                            ],
                            "outputs": [
                                "plugin",
                                "text"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0"
                            }
                        },
                        "env": {
                            "DEBUG": "1"
                        },
                        "lines_formatted": False
                    },
                    {
                        "id": "example_plugin",
                        "parents": [
                            "worker-extract-47a6"
                        ],
                        "children": [],
                        "config": {
                            "name": "Spacy",
                            "author": "Barnaby Keene",
                            "email": "barnaby@spotlightdata.co.uk",
                            "image": "nanowire/worker_spacy:1.0.0",
                            "cmd": [
                                "python3",
                                "main.py"
                            ],
                            "inputs": [
                                "text"
                            ],
                            "outputs": [
                                "plugin"
                            ],
                            "cpu": "10m",
                            "memory": "50M",
                            "lines_formatted_support": True,
                            "env": {
                                "DEBUG": "0",
                                "SPAAS_HOST": "http://spacy.spotlightdata.co.uk"
                            }
                        },
                        "env": {
                            "DEBUG": "1",
                            "SPAAS_HOST": "spacy.default"
                        },
                        "lines_formatted": False
                    }
                ],
                "misc": {
                    "what": "job meta should be the same for all tasks associated with a job, this field is for job-level decision making information such as the parallelism level etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "task": {
                "created_at": "2017-07-24T11:05:55,801624028+01:00",
                "created_by": "n-00000000-0000-0000-0000-000000000000",
                "task_id": "t-00000000-0000-0000-0000-000000000000",
                "parent": "t-10000000-0000-0000-0000-000000000000",
                "misc": {
                    "what": "metadata for this task in the context of the system, nothing to do with the source file but possibly directives for how to handle this task such as some special kind of analysis etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            },
            "source": {
                "name": "test_image_1",
                "misc": {
                    "what": "information about the original source file, creation date, associated users, onedrive stuff, gdrive stuff, etc.",
                    "aribtrary": "data",
                    "any": {
                        "types": "allowed"
                    }
                }
            }
        }
        
        payload = {}
        payload["nmo"] = nmo
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []  
 
        version = "1.0.0"

        monitor_url = None
        
        #set up the minio client
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)
        

        minio_client.set_app_info(name, version)
        
        if sys.version_info.major == 3:
            minio_client.stat_object = MagicMock(True)
            minio_client.presigned_get_object = MagicMock(return_value="http://example_url.com")
            
            
        elif sys.version_info.major == 2:
            minio_client.stat_object = mock.Mock()
            minio_client.stat_object.return_value = True
            
            
            minio_client.presigned_get_object = mock.Mock()
            minio_client.presigned_get_object.return_value = "http://example_url.com"
            
        else:
            minio_client.stat_object = mock.Mock()
            minio_client.stat_object.return_value = True
            
            
            minio_client.presigned_get_object = mock.Mock()
            minio_client.presigned_get_object.return_value = "http://example_url.com"

        with self.assertRaises(Exception) as context:
            nwp.inform_monitor(payload, name, monitor_url, minio_client)

        #check we hit the problems covered in validate payload
        self.assertTrue("Monitor url should be a string, it is actually:" in str(context.exception))
        

class unit_test_on_request_function(unittest.TestCase):

    def test_pass_request_function(self):
        
        
        name = "example_on_request_plugin_name1"
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)
    
        monitor_url = os.environ["MONITOR_URL"]
    
        output_channel = MagicMock()
        output_channel.is_open = True

        exampleOn_request = nwp.on_request_class(ok_test_function, name, minio_client, output_channel, monitor_url)
        
        ch = MagicMock()
        ch.is_open = True
        
        method = pika.spec.Basic.Deliver(delivery_tag=1, routing_key=name)
        
        props = ""
        
        payload = {}
        payload["jsonld"] = None
        payload["nmo"] = {}
        payload["nmo"]["job"] = {}
        payload["nmo"]["job"]["job_id"] = "example-job-id"
        payload["nmo"]["task"] = {}
        payload["nmo"]["task"]["task_id"] = "example-taskid"
        payload["nmo"]["source"] = {}
        payload["nmo"]["source"]["name"] = "example_on_request_plugin_name1"
        
        eg_workflow = [{
                'env': {
                    'DEBUG': '1'
                },
                'config': {
                    'memory': '100M',
                    'cpu': '10m',
                    'env': {
                        'DEBUG': '0'
                    },
                    'cmd': ['python', 'image_classifier_main.py'],
                    'outputs': ['jsonld'],
                    'inputs': ['jsonld'],
                    'image': 'docker.spotlightdata.co.uk/plugins/worker_basic_image_classifier',
                    'description': 'Produces google tensorflow image classifications of images.',
                    'email': 'stuart@spotlightdata.co.uk',
                    'author': 'stuart',
                    'name': 'example_on_request_plugin_name1'
                },
                'children': ['image-ocr'],
                'parents': ['input'],
                'id': 'example_on_request_plugin_name1'
            }, 
            {
                'env': {
                    'STORAGE_QUEUE': 'node-store',
                    'DEBUG': '1'
                },
                'config': {
                    'memory': '100M',
                    'cpu': '10m',
                    'env': {
                        'STORAGE_QUEUE': 'node-store',
                        'DEBUG': '0'
                    },
                    'cmd': ['/main'],
                    'outputs': [],
                    'inputs': ['jsonld'],
                    'image': 'docker.spotlightdata.co.uk/plugins/worker_node_store:1.1.0',
                    'description': '',
                    'email': 'stuart@spotlightdata.co.uk',
                    'author': 'stuart',
                    'name': 'node-store'
                },
                'children': [],
                'parents': ['example__on_request_plugin_name1r'],
                'id': 'worker-node-store'
            }]        
        
        
        payload["nmo"]["job"]["workflow"] = eg_workflow
        
        body = json.dumps(payload).encode("utf-8")


        status = exampleOn_request.on_request(ch, method, props, body)

        self.assertTrue(status==None)
    
    @patch('nanowire_plugin.set_status')
    def test_bad_body(self, MagicMock):
        
        
        name = "example_on_request_plugin_name1"
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)
    
        monitor_url = os.environ["MONITOR_URL"]
        
        connection = MagicMock()
        connection.channel = MagicMock()
        output_channel = connection.channel()
        
        
    
        exampleOn_request = nwp.on_request_class(ok_test_function, name, minio_client, output_channel, monitor_url)
        
        connection = MagicMock()
        ch = MagicMock()
        
        method = MagicMock()
        
        props = ""
        
        body = "Oh noes, this isn't a json!!!".encode("utf-8")
        
        
        with self.assertRaises(Exception) as context:
        #print("mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm")
            exampleOn_request.on_request(ch, method, props, body)
        
        #check we got the right exception
        self.assertTrue("Problem with payload, payload should be json serializeable. Payload is " in str(context.exception))
        
        
    def test_closed_connection(self):
        
        
        name = "example_on_request_plugin_name1"
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)
    
        monitor_url = os.environ["MONITOR_URL"]
    
        connection = MagicMock()
        connection.channel = MagicMock()
        output_channel = connection.channel()
        output_channel.is_open = True
        
        
    
        exampleOn_request = nwp.on_request_class(ok_test_function, name, minio_client, output_channel, monitor_url)
        
        connection = MagicMock()
        ch = MagicMock()
        ch.is_open = False
        method = ""
        
        props = ""
        
        payload = {}
        payload["jsonld"] = None
        payload["nmo"] = {}
        payload["nmo"]["job"] = {}
        payload["nmo"]["job"]["job_id"] = "example-job-id"
        payload["nmo"]["task"] = {}
        payload["nmo"]["task"]["task_id"] = "example-taskid"
        payload["nmo"]["source"] = {}
        payload["nmo"]["source"]["name"] = "example_on_request_plugin_name1"
        
        eg_workflow = [{
                'env': {
                    'DEBUG': '1'
                },
                'config': {
                    'memory': '100M',
                    'cpu': '10m',
                    'env': {
                        'DEBUG': '0'
                    },
                    'cmd': ['python', 'image_classifier_main.py'],
                    'outputs': ['jsonld'],
                    'inputs': ['jsonld'],
                    'image': 'docker.spotlightdata.co.uk/plugins/worker_basic_image_classifier',
                    'description': 'Produces google tensorflow image classifications of images.',
                    'email': 'stuart@spotlightdata.co.uk',
                    'author': 'stuart',
                    'name': 'example_on_request_plugin_name1'
                },
                'children': ['image-ocr'],
                'parents': ['input'],
                'id': 'example_on_request_plugin_name1'
            }, 
            {
                'env': {
                    'STORAGE_QUEUE': 'node-store',
                    'DEBUG': '1'
                },
                'config': {
                    'memory': '100M',
                    'cpu': '10m',
                    'env': {
                        'STORAGE_QUEUE': 'node-store',
                        'DEBUG': '0'
                    },
                    'cmd': ['/main'],
                    'outputs': [],
                    'inputs': ['jsonld'],
                    'image': 'docker.spotlightdata.co.uk/plugins/worker_node_store:1.1.0',
                    'description': '',
                    'email': 'stuart@spotlightdata.co.uk',
                    'author': 'stuart',
                    'name': 'node-store'
                },
                'children': [],
                'parents': ['example__on_request_plugin_name1r'],
                'id': 'worker-node-store'
            }]        
        
        
        payload["nmo"]["job"]["workflow"] = eg_workflow
        
        body = json.dumps(payload).encode("utf-8")

        
        with self.assertRaises(Exception) as context:
            exampleOn_request.on_request(ch, method, props, body)
            
        #check we got the correct exception
        self.assertTrue("Input channel is closed" in str(context.exception))
    
    
    def test_no_body(self):

        name = "example_on_request_plugin_name1"
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)
    
        monitor_url = os.environ["MONITOR_URL"]
        
        output_channel = MagicMock()
        #output_channel
        exampleOn_request = nwp.on_request_class(ok_test_function, name, minio_client, output_channel, monitor_url)
        
        #connection = pika.BlockingConnection(parameters)
        #ch = connection.channel()
       # method = pika.spec.Basic.Deliver(delivery_tag=1, routing_key=name)
        method = MagicMock()
        ch = MagicMock()
        props = ""
        
        payload = {}
        payload["jsonld"] = None
        payload["nmo"] = {}
        payload["nmo"]["job"] = {}
        payload["nmo"]["job"]["job_id"] = "example-job-id"
        payload["nmo"]["task"] = {}
        payload["nmo"]["task"]["task_id"] = "example-taskid"
        payload["nmo"]["source"] = {}
        payload["nmo"]["source"]["name"] = "example_on_request_plugin_name1"
        
        eg_workflow = [{
                'env': {
                    'DEBUG': '1'
                },
                'config': {
                    'memory': '100M',
                    'cpu': '10m',
                    'env': {
                        'DEBUG': '0'
                    },
                    'cmd': ['python', 'image_classifier_main.py'],
                    'outputs': ['jsonld'],
                    'inputs': ['jsonld'],
                    'image': 'docker.spotlightdata.co.uk/plugins/worker_basic_image_classifier',
                    'description': 'Produces google tensorflow image classifications of images.',
                    'email': 'stuart@spotlightdata.co.uk',
                    'author': 'stuart',
                    'name': 'example_on_request_plugin_name1'
                },
                'children': ['image-ocr'],
                'parents': ['input'],
                'id': 'example_on_request_plugin_name1'
            }, 
            {
                'env': {
                    'STORAGE_QUEUE': 'node-store',
                    'DEBUG': '1'
                },
                'config': {
                    'memory': '100M',
                    'cpu': '10m',
                    'env': {
                        'STORAGE_QUEUE': 'node-store',
                        'DEBUG': '0'
                    },
                    'cmd': ['/main'],
                    'outputs': [],
                    'inputs': ['jsonld'],
                    'image': 'docker.spotlightdata.co.uk/plugins/worker_node_store:1.1.0',
                    'description': '',
                    'email': 'stuart@spotlightdata.co.uk',
                    'author': 'stuart',
                    'name': 'node-store'
                },
                'children': [],
                'parents': ['example__on_request_plugin_name1r'],
                'id': 'worker-node-store'
            }]        
        
        
        payload["nmo"]["job"]["workflow"] = eg_workflow
        
        body = None

        with self.assertRaises(Exception) as context:
            exampleOn_request.on_request(ch, method, props, body)

        #check we got the desired exception
        self.assertTrue("The body data should be a byte stream, it is actually " in str(context.exception))



#send(name, payload, input_channel, output_channel, method, properties, minio_client, monitor_url, function)
class test_send(unittest.TestCase):
    
    def test_bad_name(self):
        
        #Set up the example payload
        payload = {}
        payload["nmo"] = {}
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []  
        
        name = None
        
        properties = pika.BasicProperties(delivery_mode=2)
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)
    
        monitor_url = os.environ["MONITOR_URL"]
        
        input_channel = MagicMock()
        input_channel.is_open = True
        output_channel = MagicMock()
        output_channel.is_open = True
        
        method = pika.spec.Basic.Deliver(delivery_tag=1, routing_key=name)
        
        with self.assertRaises(Exception) as context:
            nwp.send(name, payload, input_channel, output_channel, method, properties, minio_client, monitor_url, ok_test_function)

        self.assertTrue("plugin name passed to send should be a string, it is actually " in str(context.exception))   
    
    def test_bad_input_channel(self):
        #Set up the example payload
        nmo = {}
        payload = {}
        payload["nmo"] = nmo
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []  
        
        
        name = "example_plugin"
        
        properties = pika.BasicProperties(delivery_mode=2)
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)
    
        monitor_url = os.environ["MONITOR_URL"]
        
        input_channel = None      
        output_channel = MagicMock()
        output_channel.is_open = True
        
        method = pika.spec.Basic.Deliver(delivery_tag=1, routing_key=name)
        
        with self.assertRaises(Exception) as context:
            nwp.send(name, payload, input_channel, output_channel, method, properties, minio_client, monitor_url, ok_test_function)
    

        self.assertTrue("Input channel should be a pika blocking connection channel it is actually" in str(context.exception))  
           

    def test_bad_output_channel_on_send(self):
        #Set up the example payload
        nmo = {}
        payload = {}
        payload["nmo"] = nmo
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []  
        
        
        name = "example_plugin"
        
        properties = pika.BasicProperties(delivery_mode=2)
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)
    
        monitor_url = os.environ["MONITOR_URL"]
        
        input_channel = MagicMock()
        input_channel.is_open = True
        output_channel = None 
        
        method = pika.spec.Basic.Deliver(delivery_tag=1, routing_key=name)
        
        with self.assertRaises(Exception) as context:
            nwp.send(name, payload, input_channel, output_channel, method, properties, minio_client, monitor_url, ok_test_function)
            
    
        self.assertTrue("Output channel should be a pika blocking connection channel it is actually" in str(context.exception))  
    
    def test_closed_input_channel_on_send(self):
        #Set up the example payload
        nmo = {}
        payload = {}
        payload["nmo"] = nmo
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []  
        
        
        name = "example_plugin"
        
        properties = pika.BasicProperties(delivery_mode=2)
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)
    
        monitor_url = os.environ["MONITOR_URL"]

        input_channel = MagicMock()
        input_channel.is_open = False
        output_channel = MagicMock()
        output_channel.is_open = True
        

        
        method = pika.spec.Basic.Deliver(delivery_tag=1, routing_key=name)
        
        with self.assertRaises(Exception) as context:
            nwp.send(name, payload, input_channel, output_channel, method, properties, minio_client, monitor_url, ok_test_function)
            
        self.assertTrue("Input channel is closed" in str(context.exception))  
    
    def test_closed_output_channel_next_plugin(self):
        #Set up the example payload
        nmo = {}
        payload = {}
        payload["nmo"] = nmo
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []  
        
        
        name = "example_plugin"
        
        properties = pika.BasicProperties(delivery_mode=2)
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)
    
        monitor_url = os.environ["MONITOR_URL"]
        
        connection = MagicMock()
        connection.channel = MagicMock()
        input_channel = connection.channel()     
        input_channel.is_open = True
        output_channel = MagicMock()
        output_channel.is_open = False
        
        output_channel.close()
        
        method = pika.spec.Basic.Deliver(delivery_tag=1, routing_key=name)
        

        with self.assertRaises(Exception) as context:
            nwp.send(name, payload, input_channel, output_channel, method, properties, minio_client, monitor_url, ok_test_function)     
        
        self.assertTrue("Output channel is closed" in str(context.exception))

    def test_bad_method(self):
        
        #Set up the example payload
        nmo = {}
        payload = {}
        payload["nmo"] = nmo
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []  
        
        
        name = "example_plugin"
        
        properties = pika.BasicProperties(delivery_mode=2)
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)
    
        monitor_url = os.environ["MONITOR_URL"]
        
        connection = MagicMock()
        connection.channel = MagicMock()
        input_channel = connection.channel()
        output_channel = connection.channel()
        output_channel.is_open = True 
        
        method = 4
        
        with self.assertRaises(Exception) as context:
            nwp.send(name, payload, input_channel, output_channel, method, properties, minio_client, monitor_url, ok_test_function)
            
        self.assertTrue("Method needs to be a pika method, it is actually:" in str(context.exception))
        


    def test_fine_pass_send(self):
        
        eg_workflow = [{
                        'env': {
                            'DEBUG': '1'
                        },
                        'config': {
                            'memory': '100M',
                            'cpu': '10m',
                            'env': {
                                'DEBUG': '0'
                            },
                            'cmd': ['python', 'image_classifier_main.py'],
                            'outputs': ['jsonld'],
                            'inputs': ['jsonld'],
                            'image': 'docker.spotlightdata.co.uk/plugins/worker_basic_image_classifier',
                            'description': 'Produces google tensorflow image classifications of images.',
                            'email': 'stuart@spotlightdata.co.uk',
                            'author': 'stuart',
                            'name': 'test_send_example_plugin'
                        },
                        'children': ['node-store'],
                        'parents': ['input'],
                        'id': 'test_send_example_plugin'
                    }, 
                    {
                        'env': {
                            'STORAGE_QUEUE': 'node-store',
                            'DEBUG': '1'
                        },
                        'config': {
                            'memory': '100M',
                            'cpu': '10m',
                            'env': {
                                'STORAGE_QUEUE': 'node-store',
                                'DEBUG': '0'
                            },
                            'cmd': ['/main'],
                            'outputs': [],
                            'inputs': ['jsonld'],
                            'image': 'docker.spotlightdata.co.uk/plugins/worker_node_store:1.1.0',
                            'description': '',
                            'email': 'stuart@spotlightdata.co.uk',
                            'author': 'stuart',
                            'name': 'node-store'
                        },
                        'children': [],
                        'parents': ['basic-image-classifier'],
                        'id': 'worker-node-store'
                    }]
                    

        nmo = {}
        nmo["job"] = {}
        nmo["job"]["job_id"] = "example-job-id-pass-send"
        nmo["task"] = {}
        nmo["task"]["task_id"] = "example-taskid-pass-send"
        nmo["source"] = {}
        nmo["source"]["name"] = "example_on_request_plugin_name1"                    
        nmo["job"]["workflow"] = eg_workflow
                    
        #Set up the example payload
        payload = {}
        payload["nmo"] = nmo
        payload["jsonld"] = {}
        payload["jsonld"]["@context"] = "http://schema.org/"
        payload["jsonld"]["@type"] = "ImageObject"
        payload["jsonld"]["@graph"] = []  
        
        
        name = "test_send_example_plugin"
        
        properties = pika.BasicProperties(delivery_mode=2)
        
        minio_client = Minio(
            os.environ["MINIO_HOST"] + ":" + os.environ["MINIO_PORT"],
            access_key=os.environ["MINIO_ACCESS"],
            secret_key=os.environ["MINIO_SECRET"],
            secure=True if os.environ["MINIO_SCHEME"] == "https" else False)
    
        monitor_url = "http://localhost:" + str(mock_monitor.mock_server_port)
        
        connection = MagicMock()
        connection.channel = MagicMock()
        input_channel = connection.channel()     
        output_channel = connection.channel() 
        
        queuer = sim_queues()
        input_channel.basic_publish = queuer.sim_basic_publish
        input_channel.basic_consume = queuer.sim_basic_consume
        input_channel.queue_declare = queuer.sim_queue_declare
        
        output_channel.basic_publish = queuer.sim_basic_publish
        output_channel.basic_consume = queuer.sim_basic_consume
        output_channel.queue_declare = queuer.sim_queue_declare

        
        method = pika.spec.Basic.Deliver(delivery_tag=1, routing_key=name)
        
        
        result = nwp.send(name, payload, input_channel, output_channel, method, properties, minio_client, monitor_url, ok_test_function)
        
        self.assertTrue(result == {'task_id': 'example-taskid-pass-send', 'job_id': 'example-job-id-pass-send'})

        
        #should check that the result has been added to the queue
        
        sent = queuer.queue_list["node-store"].get()

        self.assertTrue(sent==json.dumps(payload))


class mocked_channel():
    
    def __init__(self, queue_list):
        
        self.queue_list = queue_list
        self.is_open = True
    
    def basic_publish(self, exchange, routing_key, body, properties=""):
        
        self.queue_list[routing_key].put(body)
        
    def basic_consume(self, on_request, queue, no_ack=False):
        
        if queue in self.queue_list.keys():
            
            body = ""
            #Heres where the API would set up the conditions to grab stuff       
            
        else:
            body = ""
            raise Exception("Tried to access %s queue but could not find it"%queue)
            
        return body
        
    def queue_declare(self, name, durable):
        
        if name not in self.queue_list.keys():
            
            self.queue_list[name] = Queue.LifoQueue()
            
    def basic_qos(self, prefetch_count):
        
        self.prefetch = prefetch_count

    #just finish since this is a function of pika and can be seen as an API call. When consuming this is the function
    #that does the actuall consuming. Basic_consume just sets everything up for this that essentially sets up a server
    #to sit there and consume. This has to just pass since we are just testing the non-API stuff here and mock is not
    #thread safe       
    def start_consuming(self):
        
        return None
        
    def confirm_delivery(self):
        time.sleep(0.1)
        


class mocked_connection_builder():
    
    def __init__(self):
        
        self.queue_list = {}
        self.is_open = True
    
    def channel(self):
        
        return mocked_channel(self.queue_list)
        


def sleeper1():
    time.sleep(1)


class test_bind(unittest.TestCase):

    @patch('pika.BlockingConnection')
    def test_good_bind(self, mock_function):
    
        print("testing good bind function call")
        
    
        name = "passing_bind_function"
        #with patch("pika.BlockingConnection", return_value=mocked_connection_builder()):
        
        mock_function.return_value = mocked_connection_builder()
        
        print(pika.BlockingConnection("test"))
        
        print("Entering thread")
        #set up the bind function which will have the data sent to it by our mock rabbit mq server
        #mock_bind_thread = Thread(target=nwp.bind, args=(ok_test_function, name))
        #mock_bind_thread.setDaemon(True)
        #mock_bind_thread.start()
        
        nwp.bind(ok_test_function, name)        
        
        connection = MagicMock()
        connection.channel = MagicMock
        channel = connection.channel()
        
        
        queuer = sim_queues()
        channel.basic_publish = queuer.sim_basic_publish
        channel.basic_consume = queuer.sim_basic_consume
        channel.queue_declare = queuer.sim_queue_declare
        channel.confirm_delivery = sleeper1
        
    def test_bad_function_input_names(self):
        
        name = "example_name_bad_function_inputs_bind"
        
        with self.assertRaises(Exception) as context:
            nwp.bind(bad_test_function_wrong_args, name)
            
        self.assertTrue("Bound function must use argument names: [nmo, jsonld, url]. You have used" in str(context.exception))
    
    def test_bad_function_input_no(self):
        
        name = "example_no_bad_function_inputs_bind"
        
        with self.assertRaises(Exception) as context:
            nwp.bind(bad_test_function_wrong_no_args, name)
            
            #lt.log_debug(logger, str(context.exception))

        self.assertTrue("Bound function must use argument names: [nmo, jsonld, url]. You have used" in str(context.exception))
        
        
    def test_bad_name(self):
        
        name = "example_num_bad_function_inputs_bind"
        
        with self.assertRaises(Exception) as context:
            nwp.bind(bad_test_function_wrong_args, name)
        
        self.assertTrue("Bound function must use argument names: [nmo, jsonld, url]. You have used" in str(context.exception))
        
        
class test_heartrunner(unittest.TestCase):
    
    def test_pacemaker(self):
        
        connection = MagicMock()
        connection.is_open = True
        
        nwp.heart_runner(connection)
        
    def test_bad_connection(self):
        
        connection = 5
        
        with self.assertRaises(Exception) as context:
            nwp.heart_runner(connection)
            
        self.assertTrue("Heartbeat runner requires a connection to rabbitmq as connection, actually has" in str(context.exception))
    
    def test_closed_connection(self):
        
        connection = MagicMock()
        connection.is_open = False
        
        with self.assertRaises(Exception) as context:
            nwp.heart_runner(connection)
            
        self.assertTrue("Heart runner's connection to rabbitmq should be open, is actually closed" in str(context.exception))

    def test_auto_timeout(self):
        
        def sleeper():
            time.sleep(11)
        
        connection=MagicMock()
        connection.is_open = True
        connection.process_data_events = sleeper
        
        with self.assertRaises(Exception) as context:
            
            runner = nwp.heart_runner(connection)
            
            runner._process_data_events()
        
        self.assertTrue("Pacemaker has timed out" in str(context.exception))


###############################
### End of test definitions ###
###############################
    

#####################################
### Mock versions of the pipeline ###
#####################################


#####################################
### Mocking up the monitor server ###
#####################################


#find a free port to host from
def get_free_port():
    
    s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    [address, port] = s.getsockname()
    s.close()
        
    return port



#Mock version of the monitor
if sys.version_info.major == 3:
    class MockServerRequestHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            
            #print("There is a get")
    
            # Process an HTTP GET request and return a response with an HTTP 200 status.
            self.protocol_version='HTTP/1.1'
            self.send_response(200, 'OK')
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(bytes("This is a string\n", 'UTF-8'))
            
    
        def do_POST(self):
            
            #print("There is indeed a post")
            
            
            self.protocol_version='HTTP/1.1'
            self.send_response(200, 'OK')
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            #testing_posts = self.rfile.read()
    
    
    class Start_Mock_Monitor_server():
        
        def __init__(self):
            
            self.mock_server_port = get_free_port()
            self.mock_server = HTTPServer(('localhost', self.mock_server_port), MockServerRequestHandler)
            
            #start running the mock server in a seperate thread
            self.mock_server_thread = Thread(target=self.mock_server.serve_forever)
            self.mock_server_thread.setDaemon(True)
            self.mock_server_thread.start()


#python 2 version of the mock monitor
elif sys.version_info.major == 2:
    class MockServerRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
        def do_GET(self):
            
            #print("There is a get")
    
            # Process an HTTP GET request and return a response with an HTTP 200 status.
            self.protocol_version='HTTP/1.1'
            self.send_response(200, 'OK')
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(bytes("This is a string\n", 'UTF-8'))
            
    
        def do_POST(self):
            
            #print("There is indeed a post")
            
            
            self.protocol_version='HTTP/1.1'
            self.send_response(200, 'OK')
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            #testing_posts = self.rfile.read()

    
    
    class Start_Mock_Monitor_server():
        
        def __init__(self):
            
            self.mock_server_port = get_free_port()
            self.mock_server = BaseHTTPServer.HTTPServer(('localhost', self.mock_server_port), MockServerRequestHandler)
            
            #start running the mock server in a seperate thread
            self.mock_server_thread = Thread(target=self.mock_server.serve_forever)
            self.mock_server_thread.setDaemon(True)
            self.mock_server_thread.start()



############################
### Mocking up rabbit mq ###
############################


#set some enviromental variables
os.environ["MINIO_SCHEME"] = "https"
os.environ["MINIO_HOST"] = "minio.spotlightdata.co.uk"
os.environ["MINIO_PORT"] = "443"
os.environ["MINIO_ACCESS"] = "S4752LWGKTZ5F96J"
os.environ["MINIO_SECRET"] = "M7U6PBYQJW2CJE6PGWWBHZ73TVQSXCGX"
os.environ["AMQP_USER"] = "guest"
os.environ["AMQP_PASS"] = "guest"
os.environ["AMQP_HOST"] = "localhost"
os.environ["AMQP_PORT"] = "5672"
os.environ["MONITOR_URL"] = "http://fake-url"




#q = queue.LifoQueue()

#simulate the monitor
mock_monitor = Start_Mock_Monitor_server()

#perform the unit tests
unittest.main()