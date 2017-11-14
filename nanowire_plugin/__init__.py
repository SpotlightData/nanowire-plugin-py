# -*- coding: utf-8 -*-
"""
Created on Wed Oct 25 11:30:46 2017

@author: stuart
"""

#!/usr/bin/env python3
"""
Provides a `bind` function to plugins so they can simply bind a function to a queue.
"""

import logging
import json
from os import environ
from os.path import join
import inspect




import time
import sys
import pika
from minio import Minio
#from minio.error import AccessDenied

#import the relavant version of urllib depending on the version of python we are
if sys.version_info.major == 3:
    import urllib
elif sys.version_info.major == 2:
    import urllib2
else:
    import urllib

#set up the logger globally
logger = logging.getLogger("nanowire-plugin")




#create a class so we can feed things into the on_request function
class on_request_class():
    
    def __init__(self, function, name, minio_client, output_channel, monitor_url):
        
        
        #check to see if the input function has the correct number of arguments. This changes depending on whether we're working
        #in python2 or python3 because apparantly unit testing is super important and my time isn't
        if sys.version_info.major == 3:
            if len(list(inspect.signature(function).parameters)) != 3:
                raise Exception("Bound function must take 3 arguments: nmo, jsonld and url")          
            
            if list(inspect.signature(function).parameters) != ['nmo', 'jsonld', 'url']:
                raise Exception("Bound function must use argument names: [nmo, jsonld, url]. You have used %s"%list(inspect.signature(function).parameters))     
            
        elif sys.version_info.major ==2:
            if len(inspect.getargspec(function)[0]) != 3:
                raise Exception("Bound function must take 3 arguments: nmo, jsonld and url")          
            
            if inspect.getargspec(function)[0] != ['nmo', 'jsonld', 'url']:
                raise Exception("Bound function must use argument names: [nmo, jsonld, url]. You have used %s"%inspect.getargspec(function)[0])     
            
        
        
        #setting up the class type checking
        if not isinstance(name, str):
            raise Exception("plugin name should be a string, it is actually %s"%name)
            
        #setting up the class type checking
        if not isinstance(monitor_url, str):
            raise Exception("monitor_url should be a string, it is actually %s"%monitor_url)
        
        if not str(type(output_channel)) == "<class 'pika.adapters.blocking_connection.BlockingChannel'>":
            raise Exception("output channel should be a pika blocking connection channel it is actually %s"%output_channel)
            
        if not output_channel.is_open:
            raise Exception("Output channel is closed")
       
            
        
        self.name = name
        self.function = function
        self.minio_client = minio_client
        self.monitor_url = monitor_url
        self.output_channel = output_channel
        

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

        #logger.info("Receved data:-")
        #logger.info(data)

        
        if data == None:
            
            logger.info("Empty input")
            #print("Empty input")
            
        else:
            #logger.info(data)
            
            #try to load the payload into a dictionary
            try:
                payload = json.loads(data)
            except:
                raise Exception("Problem with payload, payload should be json serializeable. Payload is %s"%data)
                
            #check that the payload is valid. If not this function returns the errors that tell the user why it's not
            #valid                
            validate_payload(payload)
            
            #logger.info("Payload is:- ")
            #logger.info(payload)


        returned = send(self.name, payload, ch, self.output_channel, method, props, self.minio_client, self.monitor_url, self.function)
        
        
        logger.info("returned, %s"%returned)
        logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        #ch.basic_publish(exchange='',
        #                 routing_key=self.name,
        #                 properties=pika.BasicProperties(correlation_id=props.correlation_id), body=str(response))
                                                         
                                                     
    


def bind(function, name, version="1.0.0"):
    """binds a function to the input message queue"""
    
    if not isinstance(name, str):
        raise Exception("plugin name should be a string, it is actually %s"%name)
    
    #check to see if the input function has the correct number of arguments
    if sys.version_info.major == 3:
        if len(list(inspect.signature(function).parameters)) != 3:
            raise Exception("Bound function must take 3 arguments: nmo, jsonld and url")          
        
        if list(inspect.signature(function).parameters) != ['nmo', 'jsonld', 'url']:
            raise Exception("Bound function must use argument names: [nmo, jsonld, url]. You have used %s"%list(inspect.signature(function).parameters))     
        
    elif sys.version_info.major == 2:
        if len(inspect.getargspec(function)[0]) != 3:
            raise Exception("Bound function must take 3 arguments: nmo, jsonld and url")          
        
        if inspect.getargspec(function)[0] != ['nmo', 'jsonld', 'url']:
            raise Exception("Bound function must use argument names: [nmo, jsonld, url]. You have used %s"%inspect.getargspec(function)[0])     
        


    #set up the logging
    logger.setLevel(logging.DEBUG)

    
    #write to screen to ensure logging is working ok
    #print "Initialising nanowire lib, this is a print"
    logger.info("initialising nanowire lib")
    #print("Initailising nanowire-lib")

    #set the parameters for pika
    parameters = pika.ConnectionParameters(
        host=environ["AMQP_HOST"],
        port=int(environ["AMQP_PORT"]),
        credentials=pika.PlainCredentials(environ["AMQP_USER"], environ["AMQP_PASS"]),
        heartbeat=30)



    #set up pika connection channels between rabbitmq and python
    connection = pika.BlockingConnection(parameters)
    input_channel = connection.channel()
    output_channel = connection.channel()



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


        
    input_channel.queue_declare(name, durable=True)

    #all the stuff that needs to be passed into the callback function is stored
    #in this object so that it can be easily passed through
    requester = on_request_class(function, name, minio_client, output_channel, monitor_url)
    
    #set the queue length to one
    input_channel.basic_qos(prefetch_count=1)    
    
    input_channel.basic_consume(requester.on_request, queue=name, no_ack=False)
    
    #print("Created basic consumer")
    logger.info("Created basic consumer")
    #print("ENTERING THE FUNCTION")
        
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
    
    
    
    logger.info("getting this plugin info")
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



def send(name, payload, input_channel, output_channel, method, properties, minio_client, monitor_url, function):
    """unwraps a message and calls the user function"""
    
    #check the plugin name
    if not isinstance(name, str):
        raise Exception("plugin name passed to send should be a string, it is actually %s"%name)    

    
    #check that the input channel is indeed a pika channel
    if not str(type(input_channel)) == "<class 'pika.adapters.blocking_connection.BlockingChannel'>":
            raise Exception("Input channel should be a pika blocking connection channel it is actually %s"%output_channel)
        
    #check the input channel is open
    if not input_channel.is_open:
        raise Exception("Input channel is closed") 
    
    #check that the output channel is indeed a pika channel
    if not str(type(output_channel)) == "<class 'pika.adapters.blocking_connection.BlockingChannel'>":
            raise Exception("Output channel should be a pika blocking connection channel it is actually %s"%output_channel)
        
    #check the output channel is open
    if not output_channel.is_open:
        raise Exception("Output channel is closed")
        
        
    if sys.version_info.major == 3:
        if str(type(method)) != "<class 'pika.spec.Basic.Deliver'>":
            raise Exception("Method needs to be a pika method, it is actually: %s"%str(type(method)))
            
    elif sys.version_info.major == 2:
        if str(type(method)) != "<class 'pika.spec.Deliver'>":
            raise Exception("Method needs to be a pika method, it is actually: %s"%str(type(method)))

    #check the payload
    validate_payload(payload)
    
    
    #log some info about what the send function has been given
    logger.info("consumed message")
    logger.info("channel %s"%input_channel)
    logger.info("method %s"%method)
    logger.info("properties %s"%properties)
    
    [next_plugin, url] = inform_monitor(payload, name, monitor_url, minio_client)


    #python2 has a nasty habit of converting things to unicode so this forces that behaviour out
    if str(type(next_plugin)) == "<type 'unicode'>":
        next_plugin = str(next_plugin)


    try:
        #run the function that we're all here for
        result = function(payload["nmo"], payload["jsonld"], url)
        err = 0
    except Exception as e:
        result = None
        err = str(e)
        logger.info("Sending exception to monitor: %s"%str(e))
        
        
        
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
    payload["jsonld"] = clean_function_output(result, payload)

    logger.info("finished running user code on %s"%payload["nmo"]["source"]["name"])
    
    #send the info from this plugin to the next one in the pipeline
    send_to_next_plugin(next_plugin, payload, output_channel)
    
    #Let the frontend know that we're done
    input_channel.basic_ack(method.delivery_tag)

    return {
        "job_id": payload["nmo"]["job"]["job_id"],
        "task_id": payload["nmo"]["task"]["task_id"]
    }


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
    
    #logger.info("payload: %s"%payload)    
    
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
    except Exception as exp:
        url = None

        

    # calls the user function to mutate the JSON-LD data
    if "env" in payload["nmo"]["job"]["workflow"][plugin_no]:
        #logger.info("to split: %s"%payload["nmo"]["job"]["workflow"][plugin_no]["env"])
        
        
        for ename in payload["nmo"]["job"]["workflow"][plugin_no]["env"].keys():
            evalue = payload["nmo"]["job"]["workflow"][plugin_no]["env"][ename]
            logger.info(ename + "  " + str(evalue))
            
            if ename in sys_env:
                logger.error("attempt to set plugin env var")
                continue

            environ[ename] = evalue
            
            
    return [next_plugin, url]
    
    
def send_to_next_plugin(next_plugin, payload, output_channel):
    
    if not isinstance(next_plugin, str) and not next_plugin==None:
        raise Exception("Next plugin should be a string if present or None if no next plugin. It is actually %s, %s"%(next_plugin, str(type(next_plugin))))

    if not isinstance(payload, dict):
        raise Exception("payload should be a dictionary it is in fact: %s, %s"%(payload, str(type(payload))))
        
    if "nmo" not in payload:
        raise Exception("nmo is critical to payload however is missing, payload is currently %s"%payload)
    
    #check that the output channel is indeed a pika channel
    if not str(type(output_channel)) == "<class 'pika.adapters.blocking_connection.BlockingChannel'>":
            raise Exception("output channel should be a pika blocking connection channel it is actually %s"%output_channel)
        
    #check the output channel is open
    if not output_channel.is_open:
        raise Exception("Output channel is closed") 

    if next_plugin:
            
        #declare a queue for outputing the results to the next plugin
        output_channel.queue_declare(
            next_plugin,
            durable=True
            )
            
        #send the result from this plugin to the next plugin in the pipeline
        send_result = output_channel.basic_publish("", next_plugin, json.dumps(payload), pika.BasicProperties(delivery_mode=2))

        #if the result sent ok then log that everything should be fine
        if send_result:
                logger.info("Output was published for %s"%payload["nmo"]["source"]["name"])
        else:
            logger.warning("Output was not published for %s"%payload["nmo"]["source"]["name"])
            logger.warning("next plugin: %s"%next_plugin)

    else:
        logger.warning("There is no next plugin, if this is not a storage plugin you may loose analysis data")
        


def clean_function_output(result, payload):
    
        
    #if the plugin has not produced a dictionary then we look to replace it with
    #something more sensible
    if not isinstance(result, dict):
        logger.error("Return value is not a dictionary it is:- %s"%result)
        
        if isinstance(payload, dict):
            #logger.error("Payload is not a dictionary, it is %s"%payload)
            
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
        
        
    return result
