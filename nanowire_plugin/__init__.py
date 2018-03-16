# -*- coding: utf-8 -*-
"""
Created on Wed Oct 25 11:30:46 2017

@author: stuart
"""

#!/usr/bin/env python3
"""
Provides a `bind` function to plugins so they can simply bind a function to a queue.
"""
import os
import tarfile
import traceback
import logging
import json
from os import environ
from os.path import join
import inspect

import threading

import time
import sys
import pika
from minio import Minio
import datetime
import shutil
import psutil

#from minio.error import AccessDenied

#import the relavant version of urllib depending on the version of python we are
if sys.version_info.major == 3:
    import urllib
    from queue import Queue
elif sys.version_info.major == 2:
    import urllib
    import urllib2
    from Queue import Queue
else:
    import urllib

#set up the logger globally
logger = logging.getLogger("nanowire-plugin")



#create a class so we can feed things into the on_request function
class on_request_class():
    
    def __init__(self, connection, function, name, minio_client, output_channel, monitor_url):
        
        #check to see if the input function has the correct number of arguments. This changes depending on whether we're working
        #in python2 or python3 because apparantly unit testing is super important and my time isn't

        #check the function we're working with is valid        
        validate_single_file_function(function)        
        
        #setting up the class type checking
        if not isinstance(name, str):
            raise Exception("plugin name should be a string, it is actually %s"%name)
            
        #setting up the class type checking
        if not isinstance(monitor_url, str):
            raise Exception("monitor_url should be a string, it is actually %s"%monitor_url)
        
        if not str(type(output_channel)) == "<class 'pika.adapters.blocking_connection.BlockingChannel'>" and "mock" not in str(output_channel).lower():
            raise Exception("output channel should be a pika blocking connection channel it is actually %s"%output_channel)
        
        if not output_channel.is_open:
            raise Exception("Output channel is closed")
            
        #check the connection
            
        if not connection.is_open:
            raise Exception("Connection to rabbitmq is closed")
            
            
        #check the input queue is a pika queue, or a mock of one
            


        
        self.name = name
        self.connection = connection
        self.function = function
        self.minio_client = minio_client
        self.monitor_url = monitor_url
        self.output_channel = output_channel
        self.process_queue = Queue()

    def on_request(self, ch, method, props, body):
        
        #check the channel is open
        if not ch.is_open:
            raise Exception("Input channel is closed")

        #check the body is a byte string
        if not isinstance(body, bytes):
            raise Exception("The body data should be a byte stream, it is actually %s, %s"%(body, type(body)))

        
        #set up logging inside the server functions
        logger.setLevel(logging.DEBUG)
        
        data = body.decode("utf-8")

        if data == None:
            
            logger.info("Empty input")
            
        else:

            #try to load the payload into a dictionary
            try:
                self.payload = json.loads(data)
            except:
                set_status(self.monitor_url, "Unknown", "Unknown", self.name, error="Message passed to %s is incomplete")
                #remove the bad file from the queue
                ch.basic_ack(method.delivery_tag)
                logger.error("The end of the file may have been cut off by rabbitMQ, last 10 characters are: %s"%data[0:10])
                raise Exception("Problem with payload, payload should be json serializeable. Payload is %s"%data)
                
            #check that the payload is valid. If not this function returns the errors that tell the user why it's not
            #valid
            validate_payload(self.payload)
            

        #handle the function call here!!!
        proc_thread = threading.Thread(target=self.run_processing_thread)
        proc_thread.setDaemon(True)
        proc_thread.start()
        
        processing = True
        
        pacemaker_pluserate = 10        
        
        #set up t=0 for the heartbeats
        beat_time = time.time()
        
        #wait here for the process to finish
        while processing:
            
            time_since_last_heartbeat = time.time() - beat_time
            #perform the heartbeat every n seconds
            if time_since_last_heartbeat >= pacemaker_pluserate:
                self.connection.process_data_events()
                
                #perform a memory check here
                self.check_memory(proc_thread, ch, method)
                
                #reset the timer on the pacemaker
                beat_time = time.time()
                time_since_last_heartbeat = 0
            
            messages = self.process_queue.qsize()
            
            if messages == 1:
                try:
                    output = self.process_queue.get_nowait()
                except:
                    output = 'Result did not get put onto the processing queue'

                processing = False
            
            elif messages > 1:
                raise Exception("Something has gone wrong, there are multiple messages on the queue: %s"%str(self.process_queue.queue))
        
        
        #run the send command with a 2 minute timeout
        send(self.name, self.payload, output, ch, self.output_channel, method, self.minio_client, self.monitor_url)
        #returned = send(self.name, payload, ch, self.output_channel, method, props, self.minio_client, self.monitor_url, self.function)
        

        logger.info("Finished running user code at %s"%str(datetime.datetime.now()))
        logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")


    def run_processing_thread(self):
        
        logger.info("RUNNING PROCESSING THREAD")
        
        nmo = self.payload['nmo']
        jsonld = self.payload['jsonld']
        #pull the url from minio
        url = get_url(self.payload, self.minio_client)
        #************** There needs to be some way of getting the url before we hit this
        
        try:
            #result = self.function(nmo, jsonld, url)
            result = run_function(self.function, nmo, jsonld, url)
        except:
            result = traceback.format_exc()
        
        self.process_queue.put_nowait(result)
        
        
    def check_memory(self, processing_thread_handle, ch, method):
        #perform a memory check here too
        usage = psutil.virtual_memory().percent
        if usage > 95:
            limit = psutil.virtual_memory().total >> 20
            #We are using too much memory and must kill the pod telling
            #the user to send smaller files

            #step 1 is to terminate the running thread
            processing_thread_handle.terminate()
            
            #now construct an error message to send to the monitor
            error = "%s is using too much memory, limit is %s Mb and you have used %s%% of avalible memory"%(self.name, str(limit), str(usage))
            #send the error message to the monitor
            inform_monitor(self.payload, self.name, self.monitor_url, self.minio_client, error)

            #send the payload to the next plugin without my blessing
            send(self.name, self.payload, self.payload, ch, self.output_channel, method, self.minio_client, self.monitor_url)
            #finally kill the pod to restart it
            sys.exit()
        
        
        

def bind(function, name, version="1.0.0", pulserate=25):
    """binds a function to the input message queue"""
    
    if not isinstance(name, str):
        raise Exception("plugin name should be a string, it is actually %s"%name)
    
    #set up the logging
    logger.setLevel(logging.DEBUG)
    
    logger.info("Running with pika version %s"%str(pika.__version__))

    #write to screen to ensure logging is working ok
    #print "Initialising nanowire lib, this is a print"
    logger.info("initialising nanowire lib")
    
    logger.info("initialising plugin: %s"%name)

    #set the parameters for pika
    parameters = pika.ConnectionParameters(
        host=environ["AMQP_HOST"],
        port=int(environ["AMQP_PORT"]),
        credentials=pika.PlainCredentials(environ["AMQP_USER"], environ["AMQP_PASS"]),
        heartbeat=pulserate,
        socket_timeout=10,
        connection_attempts=1,
        retry_delay = 5,
        blocked_connection_timeout=120)

    #set up pika connection channels between rabbitmq and python
    connection = pika.BlockingConnection(parameters)
    
    #add something to stop the connection hanging when it's supposed to be grabbing. This does not work
    input_channel = connection.channel()
    output_channel = connection.channel()
    
    #The confirm delivery on the input channel is an attempt to fix the hanging problem. IT MIGHT NOT WORK!!!
    input_channel.confirm_delivery()
    output_channel.confirm_delivery()

    #set up the minio client
    minio_client = Minio(
        environ["MINIO_HOST"] + ":" + environ["MINIO_PORT"],
        access_key=environ["MINIO_ACCESS"],
        secret_key=environ["MINIO_SECRET"],
        secure=True if environ["MINIO_SCHEME"] == "https" else False)
        
    minio_client.set_app_info(name, version)

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

    input_queue = input_channel.queue_declare(name, durable=True)
    
    #all the stuff that needs to be passed into the callback function is stored
    #in this object so that it can be easily passed through
    requester = on_request_class(connection, function, name, minio_client, output_channel, monitor_url)
    
    #set the queue length to one
    input_channel.basic_qos(prefetch_count=1)    
    
    #set up the function for running the users code on the input message
    input_channel.basic_consume(requester.on_request, queue=name, no_ack=False)
    
    
    #thread = threading.Thread(target=requester.countdown_timer.begin_countdown)
    #thread.setDaemon(True)
    #thread.start()
    
    #print("Created basic consumer")
    logger.info("Created basic consumer")
    #print("ENTERING THE FUNCTION")

        
    input_channel.start_consuming()
    
    logger.info("Past start consuming, not sure whats going on...")


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
        

def validate_group_function(function):
    
    if sys.version_info.major == 3:
        
        arguments = list(inspect.signature(function).parameters)
      
    elif sys.version_info.major == 2:
        
        arguments = inspect.getargspec(function)[0]
        
    allowed = ['self', 'nmo', 'reader', 'writer']
    
    arg_dict = set()
    
    for arg in arguments:
        
        if arg not in arg_dict:
            arg_dict.add(arg)
        else:
            raise Exception("ARGUMENTS MAY NOT BE REPEATED")
            
        if arg not in allowed:
            raise Exception("FUNCTION MAY ONLY USE ALLOWED ARGUMENTS, ALLOWED ARGUMENTS ARE: reader, writer, nmo YOU HAVE USED THE ARGUMENT %s"%arg)
    
    if 'reader' not in arguments:
        raise Exception("GROUP ANALYSIS FUNCTION MUST TAKE reader AS AN ARGUMENT. THIS IS A CLASS FOR READING DATA")
        
    if 'writer' not in arguments:
        raise Exception("GROUP ANALYSIS FUNCTION MUST TAKE writer AS AN ARGUMENT. THIS IS A CLASS FOR WRITING RESULTS")
    
    

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
    
    next_plugin= inform_monitor(payload, name, monitor_url, minio_client)


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
                   name + ".consumed", error=err)
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
    except Exception as exp:
        
        logger.warning("FALIED TO GET URL DUE TO: %s"%str(exp))
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


############################
### Group specific tools ###
############################



#check the nmo to see if we're working with groups
def check_for_group(nmo):
    
    if not isinstance(nmo, dict):
        raise Exception('nmo should be a dictionary, is actually: %s, a %s'%(str(nmo), str(type(nmo))))
    
    try:
        result = nmo['source']['misc']['isGroup']
    except:
        result = False
        
    return result

#download a tarball from a url
def pull_tarball_url(nmo):
    
    
    if not isinstance(nmo, dict):
        raise Exception('nmo should be a dictionary, is actually: %s, a %s'%(str(nmo), str(type(nmo))))
    
    try:
        url = nmo['source']['misc']['cacheURL']
    except:
        url = None
        
    return url


#This function handles all the tarball stuff
def pull_and_extract_tarball(tar_url, cache_folder_name):
    
    logger.info("DRAWING TARBALL FROM URL")
    
    if not isinstance(cache_folder_name, str):
        raise Exception("The cache folder should be a creatable path, is actually: %s, a %s"%(str(cache_folder_name), str(type(cache_folder_name))))
    
    try:
        #different libraries for interacting with urls in python 2 and python 3
        if sys.version_info.major >= 3:
            file_tmp = urllib.request.urlretrieve(tar_url, filename=None)[0]
        else:
            file_tmp = urllib.urlretrieve(tar_url, filename=None)[0]
        
    except Exception as exp:
        raise Exception("COULD NOT FIND TARBALL AT: %s, due to %s"%(tar_url, str(exp)))
    
    #base_name = os.path.basename(tar_url)
    

    #except Exception as e:
    #    logger.info("COULD NOT PULL TARBALL BECAUSE: %s"%str(e))
    
    tar = tarfile.open(file_tmp)
    
    logger.info("EXTRACTING TARBALL")
    tar.extractall(cache_folder_name)
    

def read_jsonld(filename):
    
    if not isinstance(filename, str):
        
        raise Exception("Filename must be a string, is actually %s, a %s"%(str(filename), str(type(filename))))
    
    f = open(filename, 'r')
    raw = f.read()
    f.close()
    
    return json.loads(raw)


class writer():
    
    def __init__(self, nmo):
        
        if not isinstance(nmo, dict):
            raise Exception("nmo should be a dictionary, is actually: %s, a %s"%(str(nmo), str(type(nmo))))
        
        self.nmo = nmo
        self.out_folder = '/output'
        self.output_filename = 'results.json'
        self.out_file = os.path.join(self.out_folder, self.output_filename)
        self.initialise_output_file()
        
    def initialise_output_file(self):
    
        if not os.path.isdir(self.out_folder):
            os.mkdir(self.out_folder)
    
        logger.info("Creating output file")
        

        f = open(os.path.join(self.out_folder, self.output_filename), "w")
        f.write('')
        f.close()
            
    
    def add_group_jsonld(self, group_jsonld):
    
        if group_jsonld != {}:
            #the group jsonld needs to be labeled with a line thats a json formatted like this
            group_line_dict = {}
            group_line_dict['update'] = {}
            group_line_dict['update']['_id'] = self.nmo['task']['task_id']
            group_line_dict['update']['_type'] = 'groupResults'
            group_line_dict['update']['_index'] = 'group'
            
            
            group_store_dict = {}
            group_store_dict['doc_as_upsert'] = True
            group_store_dict['doc'] = {}
            group_store_dict['doc']['meta'] = {}
            group_store_dict['doc']['meta']['userId'] = self.nmo['job']['user_id']
            group_store_dict['doc']['meta']['projectId'] = self.nmo['job']['project_id']
            group_store_dict['doc']['meta']['taskId'] = self.nmo['task']['task_id']
            group_store_dict['doc']['meta']['storedAt'] = datetime.datetime.utcnow().isoformat()
            group_store_dict['doc']['jsonLD'] = group_jsonld
            
            
            with open(self.out_file, "r+") as f:
                
                lines = f.readlines()
                
                f.seek(0)
               
                f.write(json.dumps(group_line_dict) + '\n')
        
                f.write(json.dumps(group_store_dict) + '\n')
    
                f.writelines(lines)
    
    def append_task(self, single_file):
        
        if sys.version_info.major >= 3:
            if 'nanowire_plugin.single_file' not in str(type(single_file)):
                raise Exception("You can only write a nanowire plugin single_file object to the output using the append task command. You have tried to send an invalid %s object"%str(type(single_file)))
        else:
            if 'nanowire_plugin.single_file' not in str(single_file):
                raise Exception("You can only write a nanowire plugin single_file object to the output using the append task command. You have tried to send an invalid %s object"%str(type(single_file)))
 
        #we only save a single file result if there have been changes
        if single_file.change_dict != {}:    
        
            #Each single file jsonld needs to be labeled like this
            task_line = {}
            task_line['update'] = {}
            #label the jsonld
            task_line['update']['_id'] = single_file.filename.split("/")[-1] + ':' + self.nmo['task']['task_id']
            task_line['update']['_type'] = 'taskResults'
            task_line['update']['_index'] = 'group'
            task_line['update']['_parent'] = self.nmo['task']['task_id']
            
            #This is how we store the individual jsonlds
            task_store_line = {}
            task_store_line['doc'] = single_file.change_dict
            task_store_line['doc_as_upsert'] = True
        
            with open(self.out_file, 'a') as f:
                
                f.write(json.dumps(task_line) + '\n')
                f.write(json.dumps(task_store_line) + '\n')

    def amplify_nmo(self, adding_dict):

        banned = ['isGroup', 'cacheURL']
    
        for key, value in adding_dict.items():
            
            if key not in banned:
                
                self.nmo['source']['misc'][key] = value
                

class reader():
    
    def __init__(self):
        self.file_cache = '/cache/jsonlds'
        self.files = os.listdir(self.file_cache)
    #a function to create a generator to pull data
    def file_generator(self):
        
        for file_dat in self.files:
            
            filename = os.path.join(self.file_cache, file_dat)
            yield single_file(filename)

class Minio_tool():
    
    def __init__(self, minio_client):
        
        if "minio.api.Minio" not in str(type(minio_client)):
            
            raise Exception("Minio_tool requires a minio client to initialise, has actually been given: %s, a %s"%(str(minio_client), str(type(minio_client))))
        
        self.minioClient = minio_client
        
        
    def send_file(self, filename, nmo, plugin_name):
        
        
        try:        
            bucket_name = nmo['job']['job_id']
            task_id = nmo['task']['task_id']
            
        except:
            raise Exception("Key information missing from nmo either job_id or task_id. nmo is: %s"%json.dumps(nmo))
        
        logger.info("ENSURING EXISTANCE OF BUCKET: %s"%bucket_name)
        #first check the bucket we want to save to exists
        if not self.minioClient.bucket_exists(bucket_name):
            
            self.minioClient.make_bucket(bucket_name)
            
        save_name = '%s/group/%s.bin'%(task_id, plugin_name)
        
        
        logger.info("READING SAVE FILE")
        if not os.path.exists(filename):
            raise Exception("Tried to send non-existant file: %s"%filename)
        
        file_stat = os.stat(filename)
        file_data = open(filename, 'rb')
        
        logger.info("PUTTING OBJECT")
        self.minioClient.put_object(bucket_name, save_name, file_data, file_stat.st_size)
        
        logger.info("CLEANING UP")
        self.clean_up_after_sending()
        
        #add to the payload locations
        
        if 'source' in nmo.keys():
            p1 = True
            if 'misc' in nmo['source'].keys():
                p1 = True
                
            else:
                p1 = False
        else:
            p1 =  False
            
        if not p1:
            raise Exception("Misc field missing from nmo. nmo is: %s"%json.dumps(nmo))
        
        try:
            nmo['source']['misc']['storePayloads'].append(save_name)
        except:
            nmo['source']['misc']['storePayloads'] = [save_name]
        
        return nmo
        
        
    def clean_up_after_sending(self):
        
        if os.path.exists('/cache'):
            shutil.rmtree('/cache')
            
        if os.path.exists('/output'):
            shutil.rmtree('/output')
        

#use this class to make sure all the data from each file stays together
class single_file():
    
    def __init__(self, filename):
        
        file_cache = '/cache' 
        
        if not os.path.exists(os.path.join(file_cache, filename)):
            raise Exception("File to be loaded does not exist: %s"%filename)

        self.filename = filename
        self.jsonld = read_jsonld(os.path.join(file_cache, filename))
        self.change_dict = {}
        
        
def run_group_function(function , read_tool, write_tool, nmo):
    
    arguments = inspect.getargspec(function)[0]
    
    #3 argument calls
    if arguments == ['reader', 'writer', 'nmo'] or arguments == ['self', 'reader', 'writer', 'nmo']:
        
        function(read_tool, write_tool, nmo)
        
        return write_tool.nmo
        
    if arguments == ['reader', 'nmo', 'writer'] or arguments == ['self', 'reader', 'nmo', 'writer']:
        
        function(read_tool, nmo, write_tool)
        
        return write_tool.nmo
        
    elif arguments == ['nmo', 'reader', 'writer'] or arguments == ['self', 'nmo', 'reader', 'writer']:
        
        function(nmo, read_tool, write_tool)
        
        return write_tool.nmo
        
    elif arguments == ['nmo', 'writer', 'reader'] or arguments == ['self', 'nmo', 'writer', 'reader']:
        
        function(nmo, write_tool, read_tool)
        
        return write_tool.nmo
        
    elif arguments == ['writer', 'reader', 'nmo'] or arguments == ['self', 'writer', 'reader', 'nmo']:
        
        function(write_tool, read_tool, nmo)
        
        return write_tool.nmo
        
    elif arguments == ['writer', 'nmo', 'reader'] or arguments == ['self', 'writer', 'nmo', 'reader']:
        
        function(write_tool, nmo, read_tool)
        
        return write_tool.nmo
    
    
    #2 arguments calls
    elif arguments == ['reader', 'writer'] or arguments == ['self', 'reader', 'writer']:
        
        function(read_tool, write_tool)
        
        return write_tool.nmo
        
    elif arguments == ['writer', 'reader'] or arguments == ['self', 'writer', 'reader']:
        
        function(write_tool, read_tool)
        
        return write_tool.nmo
        

    else:
        
        raise Exception("FUNCTION MUST ACCEPT VALID ARGUMENTS, CURRENT ARGUMENTS ARE %s"%str(arguments))



def group_bind(function, name, version="1.0.0", pulserate=25):
    """binds a function to the input message queue"""
    
    #time.sleep(120)
    
    if not isinstance(name, str):
        raise Exception("plugin name should be a string, it is actually %s"%name)
        
    if not isinstance(pulserate, int):
        raise Exception("Pulserate should be an intiger, is actually %s, a %s"%(str(pulserate), str(type(pulserate))))

    #set up the logging
    logger.setLevel(logging.DEBUG)
    
    logger.info("Running with pika version %s"%str(pika.__version__))

    #write to screen to ensure logging is working ok
    #print "Initialising nanowire lib, this is a print"
    logger.info("initialising nanowire lib")
    
    logger.info("initialising plugin: %s"%name)

    #set the parameters for pika
    parameters = pika.ConnectionParameters(
        host=environ["AMQP_HOST"],
        port=int(environ["AMQP_PORT"]),
        credentials=pika.PlainCredentials(environ["AMQP_USER"], environ["AMQP_PASS"]),
        heartbeat=pulserate,
        socket_timeout=10,
        connection_attempts=1,
        retry_delay = 5,
        blocked_connection_timeout=120)

    #set up pika connection channels between rabbitmq and python
    connection = pika.BlockingConnection(parameters)
    
    #add something to stop the connection hanging when it's supposed to be grabbing. This does not work
    input_channel = connection.channel()
    output_channel = connection.channel()
    
    #The confirm delivery on the input channel is an attempt to fix the hanging problem. IT MIGHT NOT WORK!!!
    input_channel.confirm_delivery()
    output_channel.confirm_delivery()

    #set up the minio client
    minio_client = Minio(
        environ["MINIO_HOST"] + ":" + environ["MINIO_PORT"],
        access_key=environ["MINIO_ACCESS"],
        secret_key=environ["MINIO_SECRET"],
        secure=True if environ["MINIO_SCHEME"] == "https" else False)
        
    minio_client.set_app_info(name, version)

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

    input_queue = input_channel.queue_declare(name, durable=True)
    
    #all the stuff that needs to be passed into the callback function is stored
    #in this object so that it can be easily passed through
    group_requester = group_on_request_class(connection, function, name, minio_client, output_channel, monitor_url)
    
    #set the queue length to one
    input_channel.basic_qos(prefetch_count=1)    
    
    #set up the function for running the users code on the input message
    input_channel.basic_consume(group_requester.on_request, queue=name, no_ack=False)
    
    #print("Created basic consumer")
    logger.info("Created basic consumer")
    #print("ENTERING THE FUNCTION")
    
    #start the countdown to make sure the first consume does not hang        
    input_channel.start_consuming()
    
    logger.info("Past start consuming, not sure whats going on...")




#create a class so we can feed things into the on_request function
class group_on_request_class():
    
    def __init__(self, connection, function, name, minio_client, output_channel, monitor_url):
        
        #check to see if the input function has the correct number of arguments. This changes depending on whether we're working
        #in python2 or python3 because apparantly unit testing is super important and my time isn't

        #check the function we're going to work with            
        validate_group_function(function)            
            
        #setting up the class type checking
        if not isinstance(name, str):
            raise Exception("plugin name should be a string, it is actually %s"%name)
            
        #setting up the class type checking
        if not isinstance(monitor_url, str):
            raise Exception("monitor_url should be a string, it is actually %s"%monitor_url)
        
        if not str(type(output_channel)) == "<class 'pika.adapters.blocking_connection.BlockingChannel'>" and "mock" not in str(output_channel).lower():
            raise Exception("output channel should be a pika blocking connection channel it is actually %s"%output_channel)
        
        if not output_channel.is_open:
            raise Exception("Output channel is closed")
       
        self.name = name
        self.connection = connection
        self.function = function
        self.minio_client = minio_client
        self.monitor_url = monitor_url
        self.output_channel = output_channel

        self.process_queue = Queue()

    def on_request(self, ch, method, props, body):

        #check the channel is open
        if not ch.is_open:
            raise Exception("Input channel is closed")

        #check the body is a byte string
        if not isinstance(body, bytes):
            raise Exception("The body data should be a byte stream, it is actually %s, %s"%(body, type(body)))

        #set up logging inside the server functions
        logger.setLevel(logging.DEBUG)
        
        data = body.decode("utf-8")

        if data == None:
            
            logger.info("Empty input")
            
        else:

            #try to load the payload into a dictionary
            try:
                self.payload = json.loads(data)
            except:
                set_status(self.monitor_url, "Unknown", "Unknown", self.name, error="Message passed to %s is incomplete")
                #remove the bad file from the queue
                ch.basic_ack(method.delivery_tag)
                logger.error("The end of the file may have been cut off by rabbitMQ, last 10 characters are: %s"%data[0:10])
                raise Exception("Problem with payload, payload should be json serializeable. Payload is %s"%data)
                
            #check that the payload is valid. If not this function returns the errors that tell the user why it's not
            #valid
            validate_payload(self.payload)
            

        #handle the function call here!!!
        proc_thread = threading.Thread(target=self.run_processing_thread)
        proc_thread.setDaemon(True)
        proc_thread.start()
        
        processing = True
        
        pacemaker_pluserate = 10        
        
        #set up t=0 for the heartbeats
        beat_time = time.time()
        
        #wait here for the process to finish
        while processing:
            
            time_since_last_heartbeat = time.time() - beat_time
            #perform the heartbeat every n seconds
            if time_since_last_heartbeat >= pacemaker_pluserate:
                self.connection.process_data_events()
                
                #check the memory situation
                self.check_memory(self, proc_thread, ch, method)
                
                #reset the heartbeat counter
                beat_time = time.time()
                time_since_last_heartbeat = 0
            
            messages = self.process_queue.qsize()
            
            if messages == 1:
                try:
                    output = self.process_queue.get_nowait()
                except:
                    output = 'Result did not get put onto the processing queue'

                processing = False
            
            elif messages > 1:
                raise Exception("Something has gone wrong, there are multiple messages on the queue: %s"%str(self.process_queue.queue))
        
        
        #run the send command with a 2 minute timeout
        send(self.name, self.payload, output, ch, self.output_channel, method, self.minio_client, self.monitor_url)
        #returned = send(self.name, payload, ch, self.output_channel, method, props, self.minio_client, self.monitor_url, self.function)
        

        logger.info("Finished running user code at %s"%str(datetime.datetime.now()))
        logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")


    def run_processing_thread(self):
        
        logger.info("RUNNING GROUP PROCESSING THREAD")
        result = ''
        #logger.info("IN PAYLOAD IS:-")
        #logger.info(json.dumps(self.payload))
        #logger.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        
        nmo = self.payload['nmo']
        #pull the url from minio
        #************** There needs to be some way of getting the url before we hit this
        
        tar_url = pull_tarball_url(nmo)
        
        #logger.info("FOUND TARBALL URL:- %s"%tar_url)        
        
        if tar_url != None:
        
            pull_and_extract_tarball(tar_url, '/cache')

            read_tool = reader()

            write_tool = writer(nmo)
            
            #logger.info("CREATED THE REQUIRED INFASTRUCTURE")
            #logger.info(os.listdir("/cache"))
            #logger.info(os.listdir("/cache/jsonlds"))
            #logger.info(os.listdir("/output"))
            
            try:
                logger.info("STARTING THE MAIN FUNCTION")
                nmo = run_group_function(self.function , read_tool, write_tool, nmo)
                
                
                
            except:
                result = traceback.format_exc()
                logger.info("THERE WAS A PROBLEM RUNNING THE MAIN FUNCTION: %s"%str(result))
                
        else:
            result = "GROUP TARBALL IS MISSING"

        #send the result to minio and close everything down
        minio_sender = Minio_tool(self.minio_client)
        
        
        if result != "GROUP TARBALL IS MISSING":
            try:
                nmo = minio_sender.send_file('/output/results.json', nmo, self.name)
                result = {'nmo':nmo}

                
            except Exception as exp:
                logger.info("FAILED TO SEND RESULT: %s"%str(exp))
                result = exp
            
        #logger.info("RETURNED PAYLOAD:-")
        #logger.info(json.dumps(result))
        #logger.info("************************************")
        
        #put our result onto the queue so that it can be sent through the system
        self.process_queue.put_nowait(result)
        
    def check_memory(self, processing_thread_handle, ch, method):
        #perform a memory check here too
        usage = psutil.virtual_memory().percent
        if usage > 95:
            limit = psutil.virtual_memory().total >> 20
            #We are using too much memory and must kill the pod telling
            #the user to send smaller files

            #step 1 is to terminate the running thread
            processing_thread_handle.terminate()
            
            #now construct an error message to send to the monitor
            error = "%s is using too much memory, limit is %s Mb and you have used %s%% of avalible memory"%(self.name, str(limit), str(usage))
            #send the error message to the monitor
            inform_monitor(self.payload, self.name, self.monitor_url, self.minio_client, error)

            #send the payload to the next plugin without my blessing
            send(self.name, self.payload, self.payload, ch, self.output_channel, method, self.minio_client, self.monitor_url)
            #finally kill the pod to restart it
            sys.exit()