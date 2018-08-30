# -*- coding: utf-8 -*-
"""
Created on Fri Mar 23 11:33:24 2018

@author: stuart
"""

#single file processing tools for nanowire-plugin

import os
from minio import Minio
import time
import traceback
import logging
import json
from os import environ
import inspect

import requests

#from ssl import PROTOCOL_TLSv1_2



import sys
#import hashlib
import datetime

#import urllib.request

from nanowire_plugin import send

#from minio.error import AccessDenied


###############################################################################
### Here the tools that could be exported to single file plugin can be sent ###
###############################################################################
#!!!!!!!!!!!!!!!!!!!!!!!!

#set up the logger globally
logger = logging.getLogger("nanowire-plugin")


#logging.basicConfig(level=10, stream=sys.stdout)

class Worker(object):
    def __init__(self, function, name, debug_mode, monitor_url, minio_client):
        
        
        self.name = name
        self.function = function
        self.minio_client = minio_client
        self.monitor_url = monitor_url
        self.debug_mode = debug_mode

        logging.debug("ESTABLISHED WORKER")

    def run(self):
        while True:
            
            message = requests.get(os.environ['CONTROLLER_BASE_URI'] + '/v1/tasks/?pluginId=' + os.environ['PLUGIN_ID'] + '&pluginInstance=' + os.environ['POD_NAME'])
            
            #print("-------------------")
            #print(message.status_code)
            #print(dir(message))
            #print(message.text)
            #print("-------------------")
            code = message.status_code
            
            if code == 200:
                
                payload = json.loads(message.text)
                
                meta = payload['metadata']
                jsonld = payload['jsonld']
                try:
                    url = meta['task']['metadata']['cacheURL']
                except:
                    url = None
                    
                if meta['job']['workflow']['type'] == 'GROUP':
                    logger.warning("SINGLE FILE PLUGIN TOOL WAS SENT A GROUP JOB")
                    
                else:
                    #try to run our function
                    try:
                        #result = self.function(nmo, jsonld, url)
                        result = run_function(self.function, meta, jsonld, url)
                        
                    except Exception as exp:
                        if self.debug_mode > 0:
                            result = str(traceback.format_exc())
                            logger.info("THERE WAS A PROBLEM RUNNING THE MAIN FUNCTION: %s"%result)
                        else:
                            result = str(exp)
                            logger.info("THERE WAS A PROBLEM RUNNING THE MAIN FUNCTION: %s"%result)
                            

                    job_stats = send(meta, result, self.minio_client, self.debug_mode)
                    
                    
                    if self.debug_mode >= 1:
                        logger.info(job_stats)
                            
                    logger.info("FINISHED RUNNING USER CODE AT %s"%str(datetime.datetime.now()))
                    logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                    
                            
                
            elif code == 404:
                time.sleep(1)


def bind(function, name, debug_mode=0):
    """binds a function to the input message queue"""
    
    if not isinstance(name, str):
        raise Exception("plugin name should be a string, it is actually %s"%name)
    
    #set up the logging
    logger.setLevel(logging.DEBUG)
    
    if debug_mode > 0:
        #write to screen to ensure logging is working ok
        #print "Initialising nanowire lib, this is a print"
        logger.info("Running on %s"%sys.platform)
    
    logger.info("initialising plugin: %s"%name)
    
    
    #set up the minio client. Do this before the AMPQ stuff
    
    if 'MINIO_REGION' in environ.keys():
        minio_client = Minio(
            environ["MINIO_HOST"] + ":" + environ["MINIO_PORT"],
            access_key=environ["MINIO_ACCESS"],
            secret_key=environ["MINIO_SECRET"],
            region = environ['MINIO_REGION'],
            secure=True if environ["MINIO_SCHEME"] == "https" else False)
    else:
        minio_client = Minio(
            environ["MINIO_HOST"] + ":" + environ["MINIO_PORT"],
            access_key=environ["MINIO_ACCESS"],
            secret_key=environ["MINIO_SECRET"],
            region=None,
            secure=True if environ["MINIO_SCHEME"] == "https" else False)
        
    '''
    #use a boto client to try and avoid some serious bugs with minio
    session = boto3.session.Session()
    
    minio_client = session.client('s3', region_name='ams3', endpoint_url=environ['MINIO_HOST']+":"+environ['MINIO_PORT'],
                                  aws_access_key_id=environ['MINIO_ACCESS'],
                                    aws_secret_access_key=environ['MINIO_SECRET'],
                                    secure=True if environ['MINIO_SCHEME']=='https' else False)    
    '''
    minio_client.set_app_info(name, '1.0.0')

    monitor_url = environ["MONITOR_URL"]

    logger.info("initialised nanowire lib", extra={
        "monitor_url": monitor_url,
        "minio": environ["MINIO_HOST"],
        "rabbit": environ["AMQP_HOST"]
    })
    
    logger.info("monitor_url: %s"%monitor_url)
    logger.info("minio: %s"%environ["MINIO_HOST"])
    logger.info("rabbit: %s"%environ["AMQP_HOST"])


    logger.info("consuming from %s"%name)

    #this is only commented out since I'm trying to find the source of these terrible errors
    
    worker = Worker(function, name, debug_mode, monitor_url, minio_client)
    worker.run()
    logger.warning("PAST THE RUN FUNCTION, SOMETHING HAS GONE VERY WRONG")
        
        
        

def validate_single_file_function(function):
    
    if sys.version_info.major == 3:
        
        arguments = list(inspect.signature(function).parameters)
      
    elif sys.version_info.major == 2:
        
        arguments = inspect.getargspec(function)[0]
        
    allowed = ['self', 'jsonld', 'nmo', 'url']
    
    arg_dict = set()
    
    for arg in arguments:
        
        if arg not in arg_dict:
            arg_dict.add(arg)
        else:
            raise Exception("ARGUMENTS MAY NOT BE REPEATED")
            
        if arg not in allowed:
            raise Exception("FUNCTION MAY ONLY USE ALLOWED ARGUMENTS, ALLOWED ARGUMENTS ARE: jsonld, nmo, url, YOU HAVE USED THE ARGUMENT %s"%arg)
    
    if 'jsonld' not in arguments:
        
        raise Exception("FUNCTION MUST TAKE jsonld AS AN ARGUMENT")
        

def run_function(function, nmo, jsonld, url):


    if sys.version_info.major == 3:
      
        arguments = inspect.signature(function).parameters
        
    elif sys.version_info.major == 2:
        
        arguments = inspect.getargspec(function)[0]
    
    arguments = inspect.getargspec(function)[0]
    
    #3 argument variations
    if arguments == ['nmo', 'jsonld', 'url'] or arguments == ['self', 'nmo', 'jsonld', 'url']:
        
        result = function(nmo, jsonld, url)
        
        return result
        
    elif arguments == ['nmo', 'url', 'jsonld'] or arguments == ['self', 'nmo', 'url', 'jsonld']:
        
        result = function(nmo, url, jsonld)
        
        return result
        
    elif arguments == ['jsonld', 'nmo', 'url'] or arguments == ['self', 'jsonld', 'nmo', 'url']:
        
        result = function(jsonld, nmo, url)
        
        return result
        
    elif arguments == ['jsonld', 'url', 'nmo'] or arguments == ['self', 'jsonld', 'url', 'nmo']:
        
        result = function(jsonld, url, nmo)
        
        return result
        
    elif arguments == ['url', 'nmo', 'jsonld'] or arguments == ['self', 'url', 'nmo', 'jsonld']:
        
        result = function(url, nmo, jsonld)
        
        return result
        
    elif arguments == ['url', 'jsonld', 'nmo'] or arguments == ['self', 'url', 'jsonld', 'nmo']:
        
        result = function(url, jsonld, nmo)
        
        return result
        
        
    #2 argument variations
    elif arguments == ['url', 'jsonld'] or arguments == ['self', 'url', 'jsonld']:
        
        result = function(url, jsonld)
        
        return result

    elif arguments == ['jsonld', 'url'] or arguments == ['self', 'jsonld', 'url']:
        
        result = function(jsonld, url)
        
        return result

    elif arguments == ['nmo', 'jsonld'] or arguments == ['self', 'nmo', 'jsonld']:
        
        result = function(nmo, jsonld)
        
        return result
        
    elif arguments == ['jsonld', 'nmo'] or arguments == ['self', 'jsonld', 'nmo']:
        
        result = function(jsonld, nmo)
        
        return result
        
    #1 argument variations
    elif arguments == ['jsonld'] or arguments == ['self', 'jsonld']:
        
        result = function(jsonld)
        
        return result


    else:
        
        raise Exception("FUNCTION MUST ACCEPT VALID ARGUMENTS, CURRENT ARGUMENTS ARE %s"%str(arguments))
