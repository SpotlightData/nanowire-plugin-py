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

#from ssl import PROTOCOL_TLSv1_2

import time
import sys
import pika

#import the relavant version of urllib depending on the version of python we are
if sys.version_info.major == 3:
    import urllib
elif sys.version_info.major == 2:
    import urllib
    import urllib2
else:
    import urllib

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
    
    
    logger.info("this_plugin: %s"%this_plugin)
    
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



def send(name, payload, output, input_channel, output_channel, method, minio_client, monitor_url):
    """unwraps a message and calls the user function"""   
    
    #check the plugin name
    if not isinstance(name, str):
        raise Exception("plugin name passed to send should be a string, it is actually %s"%name)    

    
    #check that the input channel is indeed a pika channel
    if not str(type(input_channel)) == "<class 'pika.adapters.blocking_connection.BlockingChannel'>"  and "mock" not in str(input_channel).lower():
            raise Exception("Input channel should be a pika blocking connection channel it is actually %s"%output_channel)
        
    #check the input channel is open
    if not input_channel.is_open:
        raise Exception("Input channel is closed") 
    
    #check that the output channel is indeed a pika channel
    if not str(type(output_channel)) == "<class 'pika.adapters.blocking_connection.BlockingChannel'>" and "mock" not in str(output_channel).lower():
            raise Exception("Output channel should be a pika blocking connection channel it is actually %s"%output_channel)
        
    #check the output channel is open
    if not output_channel.is_open:
        raise Exception("Output channel is closed")
        
        
    if sys.version_info.major == 3:
        if str(type(method)) != "<class 'pika.spec.Basic.Deliver'>" and "mock" not in str(type(method)):
            raise Exception("Method needs to be a pika method, it is actually: %s"%str(type(method)))
            
    elif sys.version_info.major == 2:
        if str(type(method)) != "<class 'pika.spec.Deliver'>" and "mock" not in str(type(method)):
            raise Exception("Method needs to be a pika method, it is actually: %s"%str(type(method)))

    #check the payload
    validate_payload(payload)
    
    #log some info about what the send function has been given
    logger.info("consumed message")
    logger.info("channel %s"%input_channel)
    logger.info("method %s"%method)
    
    next_plugin = inform_monitor(payload, name, monitor_url, minio_client)


    #python2 has a nasty habit of converting things to unicode so this forces that behaviour out
    if str(type(next_plugin)) == "<type 'unicode'>":
        next_plugin = str(next_plugin)
        
        
    if isinstance(output, str):
        err = output
    elif isinstance(output, dict):
        err = 0
    elif output == None:
        err = "NO OUTPUT WAS RETURNED"

       
    try:
        set_status(monitor_url,
                   payload["nmo"]["job"]["job_id"],
                   payload["nmo"]["task"]["task_id"],
                   name, error=err)
    except Exception as exp:
        logger.warning("failed to set status")
        logger.warning("exception: %s"%str(exp))
        logger.warning("job_id: %s"%payload["nmo"]["job"]["job_id"])
        logger.warning("task_id: %s"%payload["nmo"]["task"]["task_id"])
        
    #this log is for debug but makes the logs messy when left in production code
    #logger.info("Result is:- %s"%str(result))

    #now set the payload jsonld to be the plugin output, after ensuring that the output is
    # in EXACTLY the right format
    
    out_jsonld = clean_function_output(output, payload)

    if out_jsonld != None:
        payload["jsonld"] = out_jsonld
        
    if payload['nmo'] != output['nmo']:
        
        #logger.info("Input")
        #logger.info(json.dumps(payload))
        #logger.info("output")
        #logger.info(json.dumps(output))
        
        payload['nmo'] = output['nmo']

    logger.info("finished running user code on %s"%payload["nmo"]["source"]["name"])
    
    #send the info from this plugin to the next one in the pipeline
    send_to_next_plugin(next_plugin, payload, output_channel)
    
    #Let the frontend know that we're done
    input_channel.basic_ack(method.delivery_tag)

    return {
        "job_id": payload["nmo"]["job"]["job_id"],
        "task_id": payload["nmo"]["task"]["task_id"]
    }


def get_url(payload, minio_cl):
    
    if not isinstance(payload, dict):
        raise Exception("The payload should be a dictionary, is actually: %s, a %s"%(str(payload), str(type(payload))))
    #create the path to the target in minio
    path = join(
        payload["nmo"]["task"]["task_id"],
        "input",
        "source",
        payload["nmo"]["source"]["name"])
        
    #set the url of the file being examined
    try:
        minio_cl.stat_object(payload["nmo"]["job"]["job_id"], path)
        
        url = minio_cl.presigned_get_object(payload["nmo"]["job"]["job_id"], path)
    #if we cant get the url from the monitor then we set it as None
    except:
        result = traceback.format_exc()
        
        logger.warning("FALIED TO GET URL DUE TO: %s"%str(result))
        url = None
    
    return url

def inform_monitor(payload, name, monitor_url, minio_client):
    
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
    
    
def send_to_next_plugin(next_plugin, payload, output_channel):
    
    if not isinstance(next_plugin, str) and not next_plugin==None:
        raise Exception("Next plugin should be a string if present or None if no next plugin. It is actually %s, %s"%(next_plugin, str(type(next_plugin))))

    if not isinstance(payload, dict):
        raise Exception("payload should be a dictionary it is in fact: %s, %s"%(payload, str(type(payload))))
        
    if "nmo" not in payload:
        raise Exception("nmo is critical to payload however is missing, payload is currently %s"%payload)
    
    #check that the output channel is indeed a pika channel
    if not str(type(output_channel)) == "<class 'pika.adapters.blocking_connection.BlockingChannel'>" and "mock" not in str(output_channel).lower():
            raise Exception("output channel should be a pika blocking connection channel it is actually %s"%output_channel)
        
    #check the output channel is open
    if not output_channel.is_open:
        raise Exception("Output channel is closed") 

    if next_plugin != None:
            
        #declare a queue for outputing the results to the next plugin
        output_channel.queue_declare(
            next_plugin,
            durable=True
            )
        
        
        #send the result from this plugin to the next plugin in the pipeline
        send_result = output_channel.basic_publish("", next_plugin, json.dumps(payload), pika.BasicProperties(content_type='text/plain', delivery_mode=2))

        #if the result sent ok then log that everything should be fine
        if send_result:
                logger.info("Output was published for %s"%payload["nmo"]["source"]["name"])
        else:
            logger.warning("Output was not published for %s"%payload["nmo"]["source"]["name"])
            logger.warning("next plugin: %s"%next_plugin)

    else:
        logger.warning("There is no next plugin, if this is not a storage plugin you may loose analysis data")



def clean_function_output(result, payload):
    
    if payload == None:
        
        raise Exception("An empty payload has been receved")
        
    #if the plugin has not produced a dictionary then we look to replace it with
    #something more sensible
    if not isinstance(result, dict):
        logger.error("Return value from clean function output is not a dictionary it is:- %s, a %s"%(str(result), type(result)))
        
        if not isinstance(payload, dict):
            logger.error("Payload is not a dictionary, it is %s, a %s"%(str(payload), type(payload)))
            
            if "jsonld" in payload:
                
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
        
    #check to see that result is not an empty field. If result is None everything
    #goes wrong
    if isinstance(result, dict):
        #if the result has jsonld as its top level then make it not so i.e
        #result = {"jsonld": {blah blah blah in jsonld format}} becomes=>
        #result = {blah blah blah in jsonld format}
        if "jsonld" in result:
            result = result["jsonld"]
        else:
            result = result

    else:
        result = None
        
    if result != None:
        return result
    else:
        return payload["jsonld"]

from nanowire_plugin import group_tools
from nanowire_plugin import single_file_tools