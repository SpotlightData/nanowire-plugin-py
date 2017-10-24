#!/usr/bin/env python3
"""
Provides a `bind` function to plugins so they can simply bind a function to a queue.
"""

import logging
import json
from os import environ
from os.path import join

import time
import sys
import pika
from minio import Minio
from minio.error import AccessDenied

#import the relavant version of urllib depending on the version of python we are
#working with
if sys.version_info.major == 3:
    import urllib
elif sys.version_info.major == 2:
    import urllib2
else:
    import urllib

#set up the logger globally
logger = logging.getLogger("nanowire-plugin")



class ProcessingError(Exception):
    """Exception raised for errors during processing.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message, job_id, task_id, extra):
        super(ProcessingError, self).__init__(message)
        self.message = message
        self.extra = extra or {}
        self.meta = {"job_id": job_id, "task_id": task_id}



#create a class so we can feed things into the on_request function
class on_request_class():
    
    def __init__(self, function, name, minio_client, output_channel, monitor_url):
        
        self.name = name
        self.function = function
        self.minio_client = minio_client
        self.monitor_url = monitor_url
        self.output_channel = output_channel
        

    def on_request(self, ch, method, props, body):
        
        #set up logging inside the server functions
        logger.setLevel(logging.DEBUG)
        #print("Inside the function")


        #logger.info("raw data is:- %s"%body)
        
        
        data = body.decode("utf-8")

        #logger.info("Receved data:-")
        #logger.info(data)
        #logger.info("=========================================")
        #print("Receved data:-")
        #print(data)
        
        if data == None:
            
            logger.info("Empty input")
            #print("Empty input")
            
        else:
            #logger.info(data)
            payload = json.loads(data)
            
            validate_payload(payload, self.name)
            
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
    
    #setup logger

    #set up the logging
    #logger = logging.getLogger("nanowire-plugin")
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
        heartbeat_interval=30)



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

    minio_client.list_buckets()
    


    monitor_url = environ["MONITOR_URL"]

    
    logger.info("initialised nanowire lib", extra={
        "monitor_url": monitor_url,
        "minio": environ["MINIO_HOST"],
        "rabbit": environ["AMQP_HOST"]
    })
    
    logger.info("monitor_url: %s"%monitor_url)
    logger.info("minio: %s"%environ["MINIO_HOST"])
    logger.info("rabbit: %s"%environ["AMQP_HOST"])


    logger.info("consuming from", extra={"queue": name})
    logger.info(name)


        
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




def validate_payload(payload, name):
    """ensures payload includes the required metadata and this plugin is in there"""

    if "nmo" not in payload:
        raise ProcessingError("no job in nmo")

    if "job" not in payload["nmo"]:
        raise ProcessingError("no job in nmo")

    if "task" not in payload["nmo"]:
        raise ProcessingError("no task in nmo")


def get_this_plugin(this_plugin, workflow, logger):
    """ensures the current plugin is present in the workflow"""
    logger.info("getting this plugin info")
    logger.info("this_plugin: %s"%this_plugin)
    #logger.info("workflow is: %s"%workflow)    
    
    for i, workpipe in enumerate(workflow):
        if workpipe["config"]["name"] == this_plugin:
            return i
    return -1


def get_next_plugin(this_plugin, workflow):
    """returns the next plugin in the sequence"""
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
        
        logger.info("Running in python2")
        
        request_url = urllib.parse.urljoin(monitor_url,"/v4/tasks/%s/positions"%task_id)
        
        req = urllib.request.Request(request_url,
            payload.encode(),
            headers={
                "Content-Type": "application/json"
            })
        urllib.request.urlopen(req)
    
    #if we're working with python2
    elif sys.version_info.major == 2:
        
        logger.info("Running in python2")
        
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
    #log some info about what the send function has been given
    logger.info("consumed message", extra={
        "chan": input_channel,
        "method": method,
        "properties": properties})
    
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

    validate_payload(payload, name)
    
    logger.info("payload: %s"%payload)    
    
    #get the postion of the plugin in the pipeline
    plugin_no = get_this_plugin(name, payload["nmo"]["job"]["workflow"], logger)
    
    logger.info("Plugin name: %s"%name)
    
    if plugin_no == -1:
        raise ProcessingError(
            "declared plugin name does not match workflow",
            job_id=payload["nmo"]["job"]["job_id"],
            task_id=payload["nmo"]["task"]["task_id"])
    logger.info("Plugin number %s in pipeline"%plugin_no)
    
    
    logger.info("monitor url is: %s"%monitor_url)
    logger.info("filename is %s"%payload["nmo"]["source"]["name"])    
    #I'm not 100% sure what this is doing
    try:
        set_status(monitor_url,
                   payload["nmo"]["job"]["job_id"],
                   payload["nmo"]["task"]["task_id"],
                   name + ".consumed")
    except Exception as exp:
        logger.warning("failed to set status")
        logger.warning("exception: %s"%str(exp))
        logger.warning("job_id: %s"%payload["nmo"]["job"]["job_id"])
        logger.warning("task_id: %s"%payload["nmo"]["task"]["task_id"])
                       
                       #extra={
            #"exception": str(exp),
            #"job_id": payload["nmo"]["job"]["job_id"],
            #"task_id": payload["nmo"]["task"]["task_id"]})

    next_plugin = get_next_plugin(name, payload["nmo"]["job"]["workflow"])
    if next_plugin is None:
        logger.info("this is the final plugin", extra={
            "job_id": payload["nmo"]["job"]["job_id"],
            "task_id": payload["nmo"]["task"]["task_id"]})

    #create the path to the target in minio
    path = join(
        payload["nmo"]["task"]["task_id"],
        "input",
        "source",
        payload["nmo"]["source"]["name"])

    #check the job id exists in minio
    if not minio_client.bucket_exists(payload["nmo"]["job"]["job_id"]):
        raise ProcessingError(
            "job_id does not have a bucket",
            job_id=payload["nmo"]["job"]["job_id"],
            task_id=payload["nmo"]["task"]["task_id"])

    url = minio_client.presigned_get_object(payload["nmo"]["job"]["job_id"], path)



    # calls the user function to mutate the JSON-LD data
    if "env" in payload["nmo"]["job"]["workflow"][plugin_no]:
        #logger.info("to split: %s"%payload["nmo"]["job"]["workflow"][plugin_no]["env"])
        
        
        for ename in payload["nmo"]["job"]["workflow"][plugin_no]["env"].keys():
            evalue = payload["nmo"]["job"]["workflow"][plugin_no]["env"][ename]
            logger.info(ename + "  " + str(evalue))
            
            if ename in sys_env:
                logger.error("attempt to set plugin env var", extra={
                    "name": ename,
                    "attempted_value": evalue})
                continue

            environ[ename] = evalue

    #run the function that we're all here for
    result = function(payload["nmo"], payload["jsonld"], url)

    logger.info("Result is:- %s"%str(result))
    
    # if there are issues, just use the input and carry on the pipeline
    
    #if the plugin has produced no output then set the output to be the same as the input
    if result is None:
        logger.error("return value is None")
        result = payload["jsonld"]
        #this has potential to set result to None if the input jsonld is none

    #check to see if the result is a dictionary
    if not isinstance(result, dict):
        logger.error("return value must be of type dict, not %s", type(result))
        result = payload["jsonld"]
        #this still leaves room for errors if jsonld is also type None
        
        
    #check to see that result is not an empty field. If result is None everything
    #goes wrong
    if result != None:
        #if the result has jsonld as its top level then make it not so i.e
        #result = {"jsonld": {blah blah blah in jsonld format}} becomes=>
        #result = {blah blah blah in jsonld format}
        if "jsonld" in result:
            result = result["jsonld"]
        else:
            result = result

    else:
        result = None

    #now set the payload jsonld to be the plugin output
    payload["jsonld"] = result

    logger.info("finished running user code on %s"%payload["nmo"]["source"]["name"], extra={
        "job_id": payload["nmo"]["job"]["job_id"],
        "task_id": payload["nmo"]["task"]["task_id"]})
    

    if next_plugin:
            
    
        output_channel.queue_declare(
            next_plugin,
            durable=True
            )
            
        #make a delivery confimation request
        #output_channel.confirm_delivery()
    
        send_result = output_channel.basic_publish("", next_plugin, json.dumps(payload), pika.BasicProperties(delivery_mode=2))

    
        if send_result:
            
                logger.info("Output was published for %s"%payload["nmo"]["source"]["name"])
                
                
        else:
            logger.warning("Output was not published for %s"%payload["nmo"]["source"]["name"])
            logger.warning("next plugin: %s"%next_plugin)

    else:
        logger.warning("There is no next plugin, this might be what's going wrong")
    
    #set up a connection to the output channel
    input_channel.basic_ack(method.delivery_tag)

    return {
        "job_id": payload["nmo"]["job"]["job_id"],
        "task_id": payload["nmo"]["task"]["task_id"]
    }
