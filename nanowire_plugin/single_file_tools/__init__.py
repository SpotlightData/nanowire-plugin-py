# -*- coding: utf-8 -*-
"""
Created on Fri Mar 23 11:33:24 2018

@author: stuart
"""

#single file processing tools for nanowire-plugin

from minio import Minio

import traceback
import logging
import json
from os import environ
import inspect

#from ssl import PROTOCOL_TLSv1_2

import ssl

#import threading
from multiprocessing import  Process, Queue


import time
import sys
import pika
import hashlib
import datetime

from nanowire_plugin import send, set_status, validate_payload, get_url

#from minio.error import AccessDenied


#set up the logger globally
logger = logging.getLogger("nanowire-plugin")



#The function that does the hashing
def hash_func(text):
    
    if not isinstance(text, str):
        raise Exception("The input to the hashing function should be a string, it is actually: %s, a %s"%(text, type(text)))
    
    hs = hashlib.sha1(text.encode('utf-8'))
    hs = hs.hexdigest()
    
    #lt.log_debug(logger, 'TEXT ENCODED', input_dict={"text":text, "hash":hs})  
    
    return hs
    
    
def clear_queue(q):
    
    try:
        while True:
            q.get_nowait()
    except:
        pass


#create a class so we can feed things into the on_request function
class on_request_class():
    
    def __init__(self, connection, function, name, minio_client, output_channel, monitor_url, debug_mode, set_timeout):
        
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
        self.timeout = set_timeout
        self.connection = connection
        self.function = function
        self.minio_client = minio_client
        self.monitor_url = monitor_url
        self.output_channel = output_channel
        self.process_queue = Queue()
        self.debug_mode = debug_mode
        self.confirm_queue = Queue()

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
                if sys.version_info.major == 3:
                    set_status(self.monitor_url, "Unknown", "Unknown", self.name, error="Message passed to %s is incomplete")
                else:
                    set_status(self.monitor_url, u"Unknown", u"Unknown", self.name, error="Message passed to %s is incomplete")
                #remove the bad file from the queue
                ch.basic_ack(method.delivery_tag)
                logger.error("The end of the file may have been cut off by rabbitMQ, last 10 characters are: %s"%data[0:10])
                raise Exception("Problem with payload, payload should be json serializeable. Payload is %s"%data)
                
            #check that the payload is valid. If not this function returns the errors that tell the user why it's not
            #valid
            validate_payload(self.payload)
            

        #handle the function call here!!!
        #proc_thread = threading.Thread(target=self.run_processing_thread)
        #proc_thread.setDaemon(True)
        #proc_thread.start()

        #handle the function call on a second thread
        p = Process(target=self.run_processing_thread, args=())
        p.start()
        
        processing = True
        
        pacemaker_pluserate = 10        
        
        #set up t=0 for the heartbeats
        beat_time = time.time()
        
        start_time = time.time()
        
        #wait here for the process to finish
        while processing:
            
            time_since_last_heartbeat = time.time() - beat_time
            #perform the heartbeat every n seconds
            if time_since_last_heartbeat >= pacemaker_pluserate:
                self.connection.process_data_events()
                
                #reset the timer on the pacemaker
                beat_time = time.time()
                time_since_last_heartbeat = 0
            
            if self.timeout > 0:
                time_taken = time.time() - start_time
                if time_taken > self.timeout:
                    #if we exceed the timeout we are going to want to kill the pod
                    raise Exception("The plugin has timed out, consider increasing timeout or using smaller files")
            
            
            messages = self.process_queue.qsize()
            
            if not self.process_queue.empty():
                if self.debug_mode >= 0:
                    logger.info("DATA FOUND ON QUEUE")
            
                if messages == 1:
                    try:
                        logger.info("TAKING DATA FROM QUEUE")
                        output = self.process_queue.get()
                        #put the sha-1 hash on the confirm queue
                        logger.info("GENERATING CONFIRM MESSAGE")
                        #confirm = hash_func(str(output))
                        logger.info("SENDING CONFIRM MESSAGE")
                        #self.confirm_queue.put_nowait(confirm)
                        processing = False
                    except:
                        logger.warning(self.process_queue.qsize())
                        logger.warning("=========================")
                        logger.warning(traceback.format_exc())
                        #'Result did not get put onto the processing queue'
                    
                elif messages > 1:
                    raise Exception("Something has gone wrong, there are %s multiple messages on the processing queue"%str(self.process_queue.qsize()))      
        
        #run the send command with a 2 minute timeout
        send(self.name, self.payload, output, ch, self.output_channel, method, self.minio_client, self.monitor_url, self.debug_mode)
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
            
        except Exception as exp:
            if self.debug_mode > 0:
                result = traceback.format_exc()
                logger.info("THERE WAS A PROBLEM RUNNING THE MAIN FUNCTION: %s"%str(result))
            else:
                result = exp
                logger.info("THERE WAS A PROBLEM RUNNING THE MAIN FUNCTION: %s"%str(result))
        
        if self.debug_mode > 0:
            logger.info("PUTTING DATA ON QUEUE")
        self.process_queue.put_nowait(result)
        '''
                
        missing_message_time = 5
        #confirm that the data has been read
        unreceved = True
        conf_msg = hash_func(str(result))
        handshake_t0 = time.time()
        while unreceved:
            
            n_messages = self.confirm_queue.qsize()
            
            if n_messages == 1:
                conf = self.confirm_queue.get_nowait()
                if conf == conf_msg:
                    #if the message is confirmed we need to clear all queues because everyting is happening as expected
                    if self.debug_mode > 1:
                        logger.info("DATA TRANSFER CONFIRMED")
                        
                        
                    clear_queue(self.confirm_queue)
                    clear_queue(self.process_queue)
                    unreceved = False
                    
                else:
                    #if the message does not match the message we sent we resend and try again
                    logger.warning("MESSAGE WAS CHANGED WHEN PASSING BETWEEN THREADS")
                    logger.warning(str(conf))
                    logger.warning(str(conf_msg))
                    logger.warning(str(result))
                    
                    clear_queue(self.confirm_queue)
                    clear_queue(self.process_queue)
                    #self.process_queue.put_nowait(result)
                    
                    
            time.sleep(0.1)
            
            if time.time() - handshake_t0 >= missing_message_time:
                logger.warning("MESSAGE DISAPPEARD FROM THE QUEUE WITHOUT CONFIRMING")
                logger.warning(self.confirm_queue.qsize())
                logger.warning(self.process_queue.qsize())
                clear_queue(self.confirm_queue)
                clear_queue(self.process_queue)
                logger.warning(self.confirm_queue.qsize())
                logger.warning(self.process_queue.qsize())
                self.process_queue.put(result)
                handshake_t0 = time.time()
        '''
        if self.debug_mode > 0:
            logger.info("FINISHED PROCESSING THREAD")
            
        
        

def bind(function, name, version="1.0.0", pulserate=25, debug_mode=0, set_timeout=0):
    """binds a function to the input message queue"""
    
    if not isinstance(name, str):
        raise Exception("plugin name should be a string, it is actually %s"%name)
    
    #set up the logging
    logger.setLevel(logging.DEBUG)
    
    if debug_mode > 0:
        
        logger.info("Running with pika version %s"%str(pika.__version__))
    
        #write to screen to ensure logging is working ok
        #print "Initialising nanowire lib, this is a print"
        logger.info("Running on %s"%sys.platform)
    
    logger.info("initialising plugin: %s"%name)

    #this is only commented out since I'm trying to find the source of these terrible errors
    
    try:
        if environ["AMQP_SECURE"] == "1":
            logger.info("Using ssl connection")
            
            if str(pika.__version__).split(".")[0] != '1':
                raise Exception("Pika version %s does not support ssl connections, you must use version 1.0.0 or above"%pika.__version__)
            
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            
            parameters = pika.ConnectionParameters(
                host=environ["AMQP_HOST"],
                port=int(environ["AMQP_PORT"]),
                credentials=pika.PlainCredentials(environ["AMQP_USER"], environ["AMQP_PASS"]),
                heartbeat=pulserate,
                socket_timeout=10,
                connection_attempts=1,
                retry_delay = 5,
                blocked_connection_timeout=120,
                ssl = pika.SSLOptions(context))
            
        else:
            logger.info("Not using ssl")
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
                
    except:
    
        logger.info("Not using ssl")
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
    
    '''
    #use a boto client to try and avoid some serious bugs with minio
    session = boto3.session.Session()
    
    minio_client = session.client('s3', region_name='ams3', endpoint_url=environ['MINIO_HOST']+":"+environ['MINIO_PORT'],
                                  aws_access_key_id=environ['MINIO_ACCESS'],
                                    aws_secret_access_key=environ['MINIO_SECRET'],
                                    secure=True if environ['MINIO_SCHEME']=='https' else False)    
    '''
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
    requester = on_request_class(connection, function, name, minio_client, output_channel, monitor_url, debug_mode, set_timeout)
    
    #set the queue length to one
    input_channel.basic_qos(prefetch_count=1)
    
    #find some things out about the way basic_consume has changed
    #print("---------------------------------")
    #print(inspect.getargspec(input_channel.basic_consume))
    #print(name)
    
    #set up the function for running the users code on the input message
    input_channel.basic_consume(name, requester.on_request, auto_ack=False)
    
    
    #thread = threading.Thread(target=requester.countdown_timer.begin_countdown)
    #thread.setDaemon(True)
    #thread.start()
    if debug_mode > 0:
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
