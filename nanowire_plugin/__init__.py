# -*- coding: utf-8 -*-
"""
Created on Wed Oct 25 11:30:46 2017

@author: stuart
"""

#!/usr/bin/env python3
"""
Provides a `bind` function to plugins so they can simply bind a function to a queue.
"""

import traceback
import logging
import json
from os import environ
from os.path import join

import uuid

import datetime

#from ssl import PROTOCOL_TLSv1_2

#from minio import Minio
import os
import time
import sys

#import the relavant version of urllib depending on the version of python we are
if sys.version_info.major == 3:
    import urllib
elif sys.version_info.major == 2:
    import urllib
    import urllib2
else:
    import urllib
    
    
import requests

#import hashlib

#set up the logger globally
logger = logging.getLogger("nanowire-plugin")



##################################################################
### These tools are used by both group and single file plugins ###
##################################################################

def validate_payload(payload):
    """ensures payload includes the required metadata and this plugin is in there"""

    if not isinstance(payload, dict):
        raise Exception("payload is a %s, not a dictionary"%type(payload))

    if "nmo" not in payload:
        raise Exception("No nmo in payload")

    if "job" not in payload["nmo"]:
        raise Exception("No job in nmo \nnmo is %s"%payload["nmo"])

    if "task" not in payload["nmo"]:
        raise Exception("No task in nmo \nnmo is %s"%payload["nmo"])
    
    try:
        isGroup = payload['nmo']['source']['misc']['isGroup']    
    except:
        isGroup = False
        
        
    if "jsonld" not in payload and not isGroup:
        raise Exception("No jsonld in payload \nPayload is:- %s"%payload)
        



#Send the data back to the controler
def send(metadata, output, minio_client, debug_mode):
    """unwraps a message and calls the user function"""   
    
    #log some info about what the send function has been given
    logger.info("LETTING MONITOR KNOW PROCESSING HAS BEEN DONE")
        
        
    if isinstance(output, str):
        err = output
        output = {}
    elif isinstance(output, dict):
        err = None
    elif output == None:
        output = {}
        err = "NO OUTPUT WAS RETURNED"
    
    #send the info from this plugin to the next one in the pipeline
    send_result(output, metadata, err)
    
   

def send_result(output, metadata, error):
    
    logger.info("RUNNING SEND TO NEXT PLUGIN")

    #check if we're dealing with a group job. If it is a group job
    #any additional metadata created will be stored in what is erroneously
    #labeled as jsonld to make life easier
    if metadata['job']['workflow']['type'] == 'GROUP':
        
        #Group job went without a hitch
        if error == None:
            
            logger.debug("NO ERRORS DETECTED RETURNING FINE JSONLD")
            
            send_json = {
                  "pluginInstance": os.environ["POD_NAME"],
                  "status": "success",
                  "additionalMetadata": output
                }
        #this is the instance of we had an error when calculating the group
        #job
        else:
            
            logger.debug("ERROR WAS DETECTED")
            logger.debug(error)
            
            send_json = {
                  "pluginInstance": os.environ["POD_NAME"],
                  "status": "failure",
                  'error':error,
                  "additionalMetadata": output
                }
                
            #logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~")
            #logger.debug(json.dumps(send_json))
        
    #if we don't have a group job we must have a single file job.
    else:
        
        if error == None:
            
            send_json = {'pluginInstance':os.environ['POD_NAME'],
                         'status':'success',
                         'jsonld':output}
                         
        else:
            #logger.warning(error)
            #logger.warning("++++++++++++++++++++++")
            send_json = {'pluginInstance':os.environ['POD_NAME'],
             'status':'failure',
             'error':error,
             'jsonld':output}
             
    #This bit sends the result to the controller. It is the most likely
    #source of trouble
    trying = True
    tries = 0
    backoff = 1
    while trying:
        try:
            r = requests.put(os.environ['CONTROLLER_BASE_URI'] + '/v1/tasks/' + metadata['task']['_id'],
                             data=json.dumps(send_json), headers={'content-type':'application/json'})
            trying = False
        except:
            tries += 1
            time.sleep(backoff)
            backoff *= 2
            if tries >= 5:
                r = requests.put(os.environ['CONTROLLER_BASE_URI'] + '/v1/tasks/' + metadata['task']['_id'],
                 data=json.dumps(send_json), headers={'content-type':'application/json'})

    if r.status_code != 200:
        logger.warning(str(dir(r)))
        logger.warning(str(r.reason))
        logger.warning(str(r.text))
        logger.warning("-----------")
        logger.warning(str(output))
        logger.warning(str(type(output)))
        logger.warning("---------")
        logger.warning("PUT REQUEST STATUS IS %s SOMETHING HAS GONE VERY WRONG"%str(r.status_code))

  
    
def grab_file(url):
    
    if not os.path.exists('/tmp'):
        os.mkdir('/tmp')
    try:
        filename = '/tmp/document'
        urllib.request.urlretrieve(url, filename)
        return filename
    except:
        
        return "COULD NOT GRAB DOCUMENT, URL MAY HAVE EXPIRED"
        


#EXPEREMENTAL SECTION
def create_task(nmo, new_jsonld):
    
    job_id = nmo["job"]["job_id"],
    
    monitor_url = environ["MONITOR_URL"]
    
    task_dict = {}
    task_dict['_id'] = str(uuid.uuid4())
    task_dict["name"] = "Extracted data file"
    task_dict["type"] = "TestAction"
    task_dict["jobId"] = job_id
    
    upload_dict = {}
    upload_dict["type"] = "Example"
    upload_dict["meta"] = new_jsonld
    upload_dict["credentials"] = {"accessToken":environ["AMQP_USER"],
                                "accessTokenSecret":environ["AMQP_PASS"],
                                "consumerKey":environ["AMQP_USER"],
                                "consumerSecret":environ["AMQP_PASS"]}
    payload=json.dumps({
        "task":task_dict,
        "upload":upload_dict})
        
    #if we're working with python3
    if sys.version_info.major == 3:
        
        #if debug_status >= 2:
        #    logger.info("Running in python 3")
        
        request_url = monitor_url + "/v1/tasks"
        #urllib.parse.urljoin(monitor_url,"/v4/tasks/%s/positions"%task_id)
        
        req = urllib.request.Request(request_url,
            payload.encode(),
            headers={
                "Content-Type": "application/json"
            })
            
        result = urllib.request.urlopen(req)
        
        #logger.info("Task creation result was")
        #logger.info(str(result))

    
    #if we're working with python2
    elif sys.version_info.major == 2:
        
        #if debug_status >= 2:
        #    logger.info("Running in python 2")
        
        #there's no urljoin command in python2
        request_url = monitor_url + "/v1/tasks"       
        
        req = urllib2.Request(request_url,
            payload.encode(),
            headers={
                "Content-Type": "application/json"
            })
            
        result = urllib2.urlopen(req)
        
        logger.info("Task creation result was")
        logger.info(str(result))
    
    #if we're not in python2 or python3
    else:
        
        logger.warning("Running in an unknown version of python:- %s"%str(sys.version_info))