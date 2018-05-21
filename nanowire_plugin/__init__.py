# -*- coding: utf-8 -*-
"""
Created on Wed Oct 25 11:30:46 2017

@author: stuart
"""

#!/usr/bin/env python3
"""
Provides a `bind` function to plugins so they can simply bind a function to a queue.
"""
#from kombu import Connection, Exchange, Queue, Producer, pools
from kombu import Producer
#from kombu.mixins import ConsumerMixin
import traceback
import logging
import json
from os import environ
from os.path import join

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
    
try:
    from Queue import Queue as qq
except ImportError:
    from queue import Queue as qq

#from threading import Thread

try:
    import thread
except:
    import _thread as thread


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
        


def get_this_plugin(this_plugin, workflow):
    """ensures the current plugin is present in the workflow"""
    
    #perform type checking
    if not isinstance(this_plugin, str):
        raise Exception("Plugin name must be a string")
    
    if not isinstance(workflow, list):
        raise Exception("Workflow must be a list")
        
    if len(workflow) == 0:
        raise Exception("Workflow is empty, something is wrong")
    
    
    #logger.info("this_plugin: %s"%this_plugin)
    
    for i, workpipe in enumerate(workflow):
        if workpipe["config"]["name"] == this_plugin:
            return i
    return -1


def get_next_plugin(this_plugin, workflow):
    """returns the next plugin in the sequence"""
    
    if not isinstance(this_plugin, str):
        raise Exception("Plugin name must be a string \nIt is a %s"%type(this_plugin))
        
    if not isinstance(workflow, list):
        raise Exception("Workflow must be a list")
    
    
    found = False
    for workpipe in workflow:
        if not found:
            if workpipe["config"]["name"] == this_plugin:
                found = True
        else:
            return workpipe["config"]["name"]

    return None


def set_status(monitor_url, job_id, task_id, name, error=0):
    
    """sends a POST request to the monitor to notify it of task position"""
    
    if not isinstance(monitor_url, str):
        raise Exception("URL should be a string it is %s, a %s"%(str(monitor_url), str(type(monitor_url))))
    
    if sys.version_info.major == 3:    
    
        if not isinstance(job_id, str):
            raise Exception("job_id should be a string, it is %s, a %s"%(str(job_id), str(type(job_id))))
        
        if not isinstance(task_id, str):
            raise Exception("task_id should be a string, it is %s, a %s"%(str(task_id), str(type(task_id))))
        
    elif sys.version_info.major == 2:
        
        if not isinstance(job_id, unicode):
            raise Exception("job_id should be in unicode in python2, it is %s, a %s"%(str(job_id), str(type(job_id))))
        
        if not isinstance(task_id, unicode):
            raise Exception("task_id should be in unicode in python2, it is %s, a %s"%(str(task_id), str(type(task_id))))
        
    if not isinstance(name, str):
        raise Exception("plugin name should be a string, it is %s, a %s"%(str(name), str(type(name))))
        

    if error != 0:
        payload=json.dumps({
            "t": int(time.time() * 1000 * 1000),
            "p": name,
            "jobId": job_id, 
            "e": error})
            
    else:
        payload=json.dumps({
            "t": int(time.time() * 1000 * 1000),
            "p": name,
            "jobId": job_id})
    try:
        #if we're working with python3
        if sys.version_info.major == 3:
            
            logger.info("Running in python 3")
            
            request_url = urllib.parse.urljoin(monitor_url,"/v4/tasks/%s/positions"%task_id)
            
            req = urllib.request.Request(request_url,
                payload.encode(),
                headers={
                    "Content-Type": "application/json"
                })
                
            urllib.request.urlopen(req)
    
        
        #if we're working with python2
        elif sys.version_info.major == 2:
            
            logger.info("Running in python 2")
            
            #there's no urljoin command in python2
            request_url = monitor_url + "/v4/tasks/%s/positions"%task_id       
            
            req = urllib2.Request(request_url,
                payload.encode(),
                headers={
                    "Content-Type": "application/json"
                })
                
            urllib2.urlopen(req)
        
        #if we're not in python2 or python3
        else:
            
            logger.warning("Running in an unknown version of python:- %s"%str(sys.version_info))
    except:
        logger.warning("COULD NOT CONNECT TO MONITOR")

#Rewrite send for the celery library
def send(name, payload, output, connection, out_channel, minio_client, monitor_url, message, debug_mode):
    """unwraps a message and calls the user function"""   
    
    #check the plugin name
    if not isinstance(name, str):
        raise Exception("plugin name passed to send should be a string, it is actually %s"%name)    

    #check the payload
    validate_payload(payload)
    
    #log some info about what the send function has been given
    logger.info("LETTING MONITOR KNOW PROCESSING HAS BEEN DONE")
    
    next_plugin = inform_monitor(payload, name, monitor_url, minio_client, debug_mode)


    #python2 has a nasty habit of converting things to unicode so this forces that behaviour out
    if str(type(next_plugin)) == "<type 'unicode'>":
        next_plugin = str(next_plugin)
        
        
    if isinstance(output, str):
        err = output
    elif isinstance(output, dict):
        err = 0
    elif output == None:
        err = "NO OUTPUT WAS RETURNED"
    
        
    #this log is for debug but makes the logs messy when left in production code
    #logger.info("Result is:- %s"%str(result))

    #now set the payload jsonld to be the plugin output, after ensuring that the output is
    # in EXACTLY the right format
    
    out_jsonld = clean_function_output(output, payload)
    
    #logger.info("Out json is")
    #logger.info(out_jsonld)
    #logger.info("======================")
       
    try:
        group = payload['nmo']['source']['misc']['isGroup']
        #logger.info("Its a group plugin")
    except:
        #logger.info("Its not a group plugin")
        group = False
        
    if not group:

        if out_jsonld != None:
            try:
                payload["jsonld"] = out_jsonld
            except:
                logger.warning("could not set payload")
    
    if isinstance(output, dict):
        if 'nmo' in output.keys():    
        
            if payload['nmo'] != output['nmo']:
                
                logger.info("mutating nmo")
                payload['nmo'] = output['nmo']

    logger.info("finished running user code on %s at %s"%(payload["nmo"]["source"]["name"], str(datetime.datetime.now())))
    
    if debug_mode > 1:
        logger.warning("SENDING:-")
        logger.warning(json.dumps(payload))
    
    #send the info from this plugin to the next one in the pipeline
    sent_success = send_to_next_plugin(next_plugin, payload, connection, out_channel, message)
    
    
    if sent_success:
        #set status afer we've sent the message in case the publisher gets disconnected
        try:
            logger.info("INFORMING THE MONITOR")
            set_status(monitor_url,
                       payload["nmo"]["job"]["job_id"],
                       payload["nmo"]["task"]["task_id"],
                       name, error=err)
        except Exception as exp:
            logger.warning("failed to set status")
            logger.warning("exception: %s"%str(exp))
            logger.warning("job_id: %s"%payload["nmo"]["job"]["job_id"])
            logger.warning("task_id: %s"%payload["nmo"]["task"]["task_id"])
        
        #Let the frontend know that we're done
        #input_channel.basic_ack(method.delivery_tag)
    

    return {
        "job_id": payload["nmo"]["job"]["job_id"],
        "task_id": payload["nmo"]["task"]["task_id"]
    }

def get_url(payload, minio_cl):
    
    if not isinstance(payload, dict):
        raise Exception("The payload should be a dictionary, is actually: %s, a %s"%(str(payload), str(type(payload))))
    #create the path to the target in minio
    path = join(
        payload['nmo']['job']['job_id'],
        payload["nmo"]["task"]["task_id"],
        "input",
        "source",
        payload["nmo"]["source"]["name"])
        
    status = 'PASS'
    #set the url of the file being examined
    try:
        #Check if we're using the MINIO_BUCKET enviromental varable. LEGACY
        if 'MINIO_BUCKET' in os.environ.keys():
            
            #if inMinio is set in the nmo
            if 'inMinio' in payload['nmo']['source']['misc'].keys():
                
                if payload['nmo']['source']['misc']['inMinio']:
                    url = minio_cl.presigned_get_object(os.environ['MINIO_BUCKET'], path)
                else:
                    url = None
            #if we are using the old system where inMinio is not present. LEGACY
            else:
                minio_cl.stat_object(os.environ['MINIO_BUCKET'], path)
            
                url = minio_cl.presigned_get_object(os.environ['MINIO_BUCKET'], path)
                
        else:
            minio_cl.stat_object(payload['nmo']['job']['job_id'], path)
        
            url = minio_cl.presigned_get_object(payload['nmo']['job']['job_id'], path)
    #if we cant get the url from the monitor then we set it as None and nack the message
    except:
        result = traceback.format_exc()
        
        logger.warning("FAILED TO GET URL DUE TO: %s"%str(result))
        logger.warning("target is %s"%path)
        url = None
        status = 'FAIL'
    
    return [status, url]

def inform_monitor(payload, name, monitor_url, minio_client, debug_mode):
    
    if not isinstance(payload, dict):
        raise Exception("Payload should be a dictionary, it is actually: %s"%payload)
        
    if not isinstance(monitor_url, str):
        raise Exception("Monitor url should be a string, it is actually: %s"%monitor_url)
        
    if not isinstance(name, str):
        raise Exception("plugin name should be a string, it is actually: %s"%name)
        
    #set the system enviroment properties
    sys_env = [
        "AMQP_HOST",
        "AMQP_PORT",
        "AMQP_USER",
        "AMQP_PASS",
        "MINIO_HOST",
        "MINIO_PORT",
        "MINIO_ACCESS",
        "MINIO_SECRET",
        "MINIO_SCHEME",
        "MONITOR_URL"
    ]
    
    #this is effectivly type checking for the payload
    validate_payload(payload)
    
    #get the postion of the plugin in the pipeline
    plugin_no = get_this_plugin(name, payload["nmo"]["job"]["workflow"])
    
    logger.info("Plugin name: %s"%name)
    
    if plugin_no == -1:
        raise Exception(
            "declared plugin name does not match workflow \njob_id: %s\ntask_id: %s"%(
            payload["nmo"]["job"]["job_id"],
            payload["nmo"]["task"]["task_id"]))
    
    if debug_mode >= 1:    
        logger.info("Plugin number %s in pipeline"%plugin_no)
        logger.info("monitor url is: %s"%monitor_url)
        logger.info("filename is %s"%payload["nmo"]["source"]["name"])    
    #Inform the monitor as to where we are. If we can't then list a series of
    #warnings
    
    #if this is the final plugin in the process send a log stating as such
    next_plugin = get_next_plugin(name, payload["nmo"]["job"]["workflow"])
    if next_plugin is None:
        logger.info("this is the final plugin: %s"%payload["nmo"]["job"]["job_id"])

    # calls the user function to mutate the JSON-LD data
    if "env" in payload["nmo"]["job"]["workflow"][plugin_no]:
        
        for ename in payload["nmo"]["job"]["workflow"][plugin_no]["env"].keys():
            evalue = payload["nmo"]["job"]["workflow"][plugin_no]["env"][ename]
            logger.info(ename + "  " + str(evalue))
            
            if ename in sys_env:
                logger.error("attempt to set plugin env var")
                continue

            environ[ename] = evalue
            
            
    return next_plugin
   

def send_to_next_plugin(next_plugin, payload, conn, out_channel, message):
    
    if not isinstance(next_plugin, str) and not next_plugin==None:
        raise Exception("Next plugin should be a string if present or None if no next plugin. It is actually %s, %s"%(next_plugin, str(type(next_plugin))))

    if not isinstance(payload, dict):
        raise Exception("payload should be a dictionary it is in fact: %s, %s"%(payload, str(type(payload))))
        
    if "nmo" not in payload:
        raise Exception("nmo is critical to payload however is missing, payload is currently %s"%payload)
    logger.info("RUNNING SEND TO NEXT PLUGIN")
    if next_plugin != None:
        
        logger.info("CREATING PAYLOAD STRING")
        send_payload = json.dumps(payload)
        
        #logger.info("CREATE A CHANNEL TO SEND THE DATA THROUGH")
        
        #set up the producer to send messages to the next plugin
        sent_well = False
        #use the with argument to avoid creating too many channels and causing a hang
        trying_to_publish = True
        retry_quota = 30
        times_tried = 0
        while trying_to_publish:
            try:
                
                logger.info("SET UP THE PRODUCER")
                producer = Producer(out_channel)
                
                #ensure the connection
                logger.info("CONNECTION IS UP:- %s"%str(conn.connected))
                #ensure the out_channel
                logger.info("OUT CHANNEL IS UP:- %s"%str(out_channel.is_open))
                
                logger.info("Trying to publish result to %s"%next_plugin)
                #logger.info(type(send_payload))
                producer.publish(send_payload, exchange='', routing_key=next_plugin, retry=True, 
                                 retry_policy={'interval_start':1,
                                               'interval_step':2,
                                               'interval_max':10,
                                               'max_retries':30})
                
                logger.info("Output was published for %s"%payload["nmo"]["source"]["name"])
                
                #logger.info("Acking message")
                message.ack()
                sent_well = True
                
                trying_to_publish = False
    
            except:
                #if we can't publish we A) need to know why and B) need to kill everything
                logger.warning("---")
                logger.warning(traceback.format_exc())
                logger.warning("==================================")
                times_tried += 1 
                #logger.warning(send_payload)
                #logger.warning("$$$$$$$$$$$$$$$$$$$$$")
                #logger.warning(str(type(send_payload)))
                #logger.warning("------------------------------------------------------")
                if times_tried >= retry_quota:
                    raise Exception("KILL MAIN THREAD")
                #thread.interrupt_main()
            
    else:
        logger.warning("There is no next plugin, if this is not a storage plugin you may loose analysis data")
        message.ack()
        sent_well = True
        
    return sent_well
    
    
def grab_file(url):
    
    if not os.path.exists('/tmp'):
        os.mkdir('/tmp')
    try:
        filename = '/tmp/document'
        urllib.request.urlretrieve(url, filename)
        return filename
    except:
        
        return "COULD NOT GRAB DOCUMENT, URL MAY HAVE EXPIRED"
        
        
        

def clean_function_output(result, payload):
    
    if payload == None:
        
        raise Exception("An empty payload has been receved")
    
    #if the system cannot grab the group tarball I need to report an error to the user
    #indicating that things have gone wrong somewhere
    if result == "GROUP TARBALL IS MISSING":
        return None
    
    try:
        group = payload['nmo']['source']['misc']['isGroup']
    except:
        group = False
    
    #if the plugin has not produced a dictionary then we look to replace it with
    #something more sensible
    if not isinstance(result, dict):
        logger.error("Return value from clean function output is not a dictionary it is:- %s, a %s"%(str(result), type(result)))
        
        if not isinstance(payload, dict):
            logger.error("Payload is not a dictionary, it is %s, a %s"%(str(payload), type(payload)))
            
            if "jsonld" in payload and not group:
                
                if isinstance(payload["jsonld"], dict):
                    result = payload["jsonld"]
                #result is none and the jsonld in the payload is empty
                else:
                    result = None
            
            #result is none and payload is faulty, no jsonld key. Not even pointing to a none
            else:
                result = None
                
        #the result is not a dictionary, nor is the payload. Something has gone very wrong but we can still return a None
        else:
            result = None
            
    else:
        result = None
        
    #check to see that result is not an empty field. If result is None everything
    #goes wrong
    if isinstance(result, dict):
        #if the result has jsonld as its top level then make it not so i.e
        #result = {"jsonld": {blah blah blah in jsonld format}} becomes=>
        #result = {blah blah blah in jsonld format}
        if "jsonld" in result.keys():
            result = result["jsonld"]
        else:
            result = result

    else:
        result = None
        
    if result != None:
        return result
    else:
        logger.info("returning initial payload")
        
        if 'jsonld' in payload.keys():
            return payload["jsonld"]
        else:
            return None

