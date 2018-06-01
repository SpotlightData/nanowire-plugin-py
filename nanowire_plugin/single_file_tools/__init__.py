# -*- coding: utf-8 -*-
"""
Created on Fri Mar 23 11:33:24 2018

@author: stuart
"""

#single file processing tools for nanowire-plugin

from minio import Minio
import subprocess
import traceback
import logging
import json
from os import environ
import inspect

import kombu

#from ssl import PROTOCOL_TLSv1_2

try:
    from Queue import Queue as qq
except ImportError:
    from queue import Queue as qq


try:
    import thread
except:
    import _thread as thread



from threading import Thread
#import threading
#from kombu.mixins import ConsumerMixin
from kombu.mixins import ConsumerProducerMixin
#from kombu import Connection, Exchange, Queue
from kombu import Connection, Queue

import sys
import hashlib
import datetime

from nanowire_plugin import send, set_status, validate_payload, get_url

#from minio.error import AccessDenied


###############################################################################
### Here the tools that could be exported to single file plugin can be sent ###
###############################################################################
#!!!!!!!!!!!!!!!!!!!!!!!!

#set up the logger globally
logger = logging.getLogger("nanowire-plugin")


#logging.basicConfig(level=10, stream=sys.stdout)

class Worker(ConsumerProducerMixin):
    def __init__(self, connection, queues, function, name, minio_client, monitor_url, debug_mode):
        
        
        self.name = name
        self.function = function
        self.minio_client = minio_client
        self.monitor_url = monitor_url
        self.debug_mode = debug_mode
        self.connect_max_retries=5
        
        self.connection = connection
        self.queues = queues
        self.q = qq()
        self.out_channel = self.connection.channel()
        
        self.workThread = Thread(target=self.run_tasks)
        self.workThread.daemon = True
        self.workThread.start()
        
        logging.debug("ESTABLISHED WORKER")

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.queues,
                         callbacks=[self.on_message],
                         prefetch_count=1, no_ack=False)]

    def on_message(self, body, message):
        #logger.info("new message to internal queue")
        self.q.put((body, message))

    def run_tasks(self):
        while True:
            try:
                self.on_task(*self.q.get())
            except Exception as ex:
                #logging.error("FOUND WHERE THAT LINKS TO:-")
                logging.error(ex)
                logging.error("BROKEN OUT OF THE MAIN LOOP, RUNNING THREADING BREAKER")
                subprocess.call(['kill', '-2', '1'])
                #logging.error("RAN KILL COMMAND, NOW RUNNING KEYBOARD INTERUPT")
                thread.interrupt_main()
                #logging.error("RAN KEYBOARD INTERUPT")
                break

            except KeyboardInterrupt:
                break

    def on_task(self, body, message):
        logger.info("run task %s"%str(self.debug_mode))

        if self.debug_mode == 5:
            logger.info("BODY:-")
            logger.warning(body)
            logger.info("MESSAGE:-")
            logger.warning(message)
            logger.warning("---------------------------")
            
        try:
            data = body.decode("utf-8")
        except:
            data = body

        if data == None:
            
            logger.info("Empty input")
            
        else:
            #try to load the payload into a dictionary
            try:
                payload = json.loads(data)
                
                logger.info("PAYLOAD EXTRACTED")
                #logger.info(str(payload))
                #logger.info(type(payload))
            except:
                if sys.version_info.major == 3:
                    set_status(self.monitor_url, "Unknown", "Unknown", self.name, error="Message passed to %s is incomplete")
                else:
                    set_status(self.monitor_url, u"Unknown", u"Unknown", self.name, error="Message passed to %s is incomplete")
                #remove the bad file from the queue
                message.ack()
                logger.error("The end of the file may have been cut off by rabbitMQ, last 10 characters are: %s"%data[0:10])
                raise Exception("Problem with payload, payload should be json serializeable. Payload is %s"%data)
                
            #check that the payload is valid. If not this function returns the errors that tell the user why it's not
            #valid
            validate_payload(payload)
            

        nmo = payload['nmo']
        jsonld = payload['jsonld']
        #pull the url from minio
        [status, url] = get_url(payload, self.minio_client)
        #************** There needs to be some way of getting the url before we hit this
        
        if status == 'PASS':
        
            try:
                #result = self.function(nmo, jsonld, url)
                result = run_function(self.function, nmo, jsonld, url)
                
            except Exception as exp:
                if self.debug_mode > 0:
                    result = str(traceback.format_exc())
                    logger.info("THERE WAS A PROBLEM RUNNING THE MAIN FUNCTION: %s"%result)
                else:
                    result = str(exp)
                    logger.info("THERE WAS A PROBLEM RUNNING THE MAIN FUNCTION: %s"%result)
            
            job_stats = send(self.name, payload, result, self.connection, self.out_channel, self.minio_client, self.monitor_url, message, self.debug_mode)
    
            if self.debug_mode >=3:
                logger.info(str(job_stats))
    
            logger.info("FINISHED RUNNING USER CODE AT %s"%str(datetime.datetime.now()))
            logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
            
        else:
            message.reject(requeue=True)
        #message.ack()



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


def bind(function, name, version="1.0.0", pulserate=25, debug_mode=0, set_timeout=0):
    """binds a function to the input message queue"""
    
    if not isinstance(name, str):
        raise Exception("plugin name should be a string, it is actually %s"%name)
    
    #set up the logging
    logger.setLevel(logging.DEBUG)
    
    if debug_mode > 0:
        
        logger.info("Running with kombu version %s"%str(kombu.__version__))
    
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

    #this is only commented out since I'm trying to find the source of these terrible errors
    
    if environ.get("AMQP_SECURE") != None:
        if environ["AMQP_SECURE"] == "1":
            logger.info("Using ssl connection")

            rabbit_url = "amqp://%s:%s@%s:%s/"%(environ["AMQP_USER"], environ["AMQP_PASS"], environ["AMQP_HOST"], environ["AMQP_PORT"])
            
            queues = [Queue(name)]
            with Connection(rabbit_url, heartbeat=25, ssl=True, transport_options={'confirm_publish':True}) as conn:
                conn.connect()
                worker = Worker(conn, queues, function, name, minio_client, monitor_url, debug_mode)
                worker.run()

            
        else:
            logger.info("Not using ssl")
            #initialise the celery worker
            rabbit_url = "amqp://%s:%s@%s:%s/"%(environ["AMQP_USER"], environ["AMQP_PASS"], environ["AMQP_HOST"], environ["AMQP_PORT"])
            
            #set up celery connection
            #set up celery connection
            queues = [Queue(name)]
            with Connection(rabbit_url, heartbeat=25, transport_options={'confirm_publish':True}) as conn:
                conn.connect()
                worker = Worker(conn, queues, function, name, minio_client, monitor_url, debug_mode)
                worker.run()

    else:
        logger.info("Not using ssl")

        #initialise the celery worker
        rabbit_url = "amqp://%s:%s@%s:%s/"%(environ["AMQP_USER"], environ["AMQP_PASS"], environ["AMQP_HOST"], environ["AMQP_PORT"])
        
        #set up celery connection
        queues = [Queue(name)]
        with Connection(rabbit_url, heartbeat=25, transport_options={'confirm_publish':True}) as conn:
            conn.connect()
            worker = Worker(conn, queues, function, name, minio_client, monitor_url, debug_mode)
            worker.run()

        
        
        

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
