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
import inspect

import threading

import time
import sys
import pika
from minio import Minio
import datetime
from multiprocessing import Process

from queue import Queue
#from minio.error import AccessDenied

#import the relavant version of urllib depending on the version of python we are
if sys.version_info.major == 3:
    import urllib
    import _thread as thread
elif sys.version_info.major == 2:
    import urllib2
    import thread
else:
    import urllib

#set up the logger globally
logger = logging.getLogger("nanowire-plugin")

#trying another wacky plan to try and fix the hanging problem

#create a class so we can feed things into the on_request function
class on_request_class():
    
    def __init__(self, connection, function, name, minio_client, output_channel, monitor_url, input_queue):
        
        #check to see if the input function has the correct number of arguments. This changes depending on whether we're working
        #in python2 or python3 because apparantly unit testing is super important and my time isn't
        if sys.version_info.major == 3:
            if len(list(inspect.signature(function).parameters)) != 3 and list(inspect.signature(function).parameters) != ['self', 'nmo', 'jsonld', 'url']:

                raise Exception("Bound function must take 3 arguments: nmo, jsonld and url")          
            
            if list(inspect.signature(function).parameters) != ['nmo', 'jsonld', 'url'] and list(inspect.signature(function).parameters) != ['self', 'nmo', 'jsonld', 'url']:
                raise Exception("Bound function must use argument names: ['nmo', 'jsonld', 'url']. You have used %s"%list(inspect.signature(function).parameters))     
            
        elif sys.version_info.major ==2:
            if len(inspect.getargspec(function)[0]) != 3 and inspect.getargspec(function)[0] != ['self', 'nmo', 'jsonld', 'url']:

                raise Exception("Bound function must take 3 arguments: nmo, jsonld and url")          
            
            if inspect.getargspec(function)[0] != ['nmo', 'jsonld', 'url'] and inspect.getargspec(function)[0] != ['self', 'nmo', 'jsonld', 'url']:
                raise Exception("Bound function must use argument names: [nmo, jsonld, url]. You have used %s"%inspect.getargspec(function)[0])     
            
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
        self.input_queue = input_queue
        #the alert queue is designed to check if the basic_consume is hanging
        self.alert_queue = Queue()
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
            result = self.function(nmo, jsonld, url)
            
        except:
            result = traceback.format_exc()
        
        self.process_queue.put_nowait(result)
        
        
        

def bind(function, name, version="1.0.0", pulserate=25):
    """binds a function to the input message queue"""
    
    #time.sleep(120)
    
    if not isinstance(name, str):
        raise Exception("plugin name should be a string, it is actually %s"%name)
    
    #check to see if the input function has the correct number of arguments
    if sys.version_info.major == 3:
      
        if list(inspect.signature(function).parameters) != ['nmo', 'jsonld', 'url'] and list(inspect.signature(function).parameters) != ['self', 'nmo', 'jsonld', 'url']:
            raise Exception("Bound function must use argument names: [nmo, jsonld, url]. You have used %s"%list(inspect.signature(function).parameters))     
        
    elif sys.version_info.major == 2:
        
        if inspect.getargspec(function)[0] != ['nmo', 'jsonld', 'url'] and inspect.getargspec(function)[0] != ['self', 'nmo', 'jsonld', 'url']:
            raise Exception("Bound function must use argument names: [nmo, jsonld, url]. You have used %s"%inspect.getargspec(function)[0])

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
    requester = on_request_class(connection, function, name, minio_client, output_channel, monitor_url, input_queue)
    
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
    '''
    logger.info("*****************************************")
    logger.info(dir(eg_queue))
    
    #This is the function that should let us know what we're looking at
    logger.info(eg_queue.method.message_count)
    logger.info("*****************************************")
    '''
    
    #start the countdown to make sure the first consume does not hang
    
        
    input_channel.start_consuming()
    
    logger.info("Past start consuming, not sure whats going on...")




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
                
    if "jsonld" not in payload:
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
        raise Exception("URL should be a string it is %s"%monitor_url)
    
    if not isinstance(job_id, str):
        raise Exception("job_id should be a string, it is %s"%job_id)
    
    if not isinstance(task_id, str):
        raise Exception("task_id should be a string, it is %s"%task_id)
    
    if not isinstance(name, str):
        raise Exception("plugin name should be a string, it is %s"%name)
    
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


def get_url(payload, minio_client):
    
    #create the path to the target in minio
    path = join(
        payload["nmo"]["task"]["task_id"],
        "input",
        "source",
        payload["nmo"]["source"]["name"])
    
    #set the url of the file being examined
    try:
        minio_client.stat_object(payload["nmo"]["job"]["job_id"], path)
        
        url = minio_client.presigned_get_object(payload["nmo"]["job"]["job_id"], path)
        
    #if we cant get the url from the monitor then we set it as None
    except:
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
        
        #test_result = output_channel.basic_get(queue=next_plugin, no_ack=True)        
        
        #if test_result != json.dumps(payload):
        #    logger.warning("Plugin has not published correct message, message should be:\n %s \n but has come out as: \n %s \n")
            

        #if the result sent ok then log that everything should be fine
        if send_result:
                logger.info("Output was published for %s"%payload["nmo"]["source"]["name"])
        else:
            logger.warning("Output was not published for %s"%payload["nmo"]["source"]["name"])
            logger.warning("next plugin: %s"%next_plugin)

    else:
        logger.warning("There is no next plugin, if this is not a storage plugin you may loose analysis data")


def return_value(ch, method, props, body):
    
    return body


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
