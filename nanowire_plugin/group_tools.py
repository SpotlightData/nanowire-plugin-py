# -*- coding: utf-8 -*-
"""
Created on Fri Mar 23 11:33:24 2018

@author: stuart
"""

#group tools for nanowire-plugin



############################
### Group specific tools ###
############################

from minio import Minio

import os
import tarfile
import traceback
import logging
import json
from os import environ
import inspect

#from ssl import PROTOCOL_TLSv1_2
#import ssl
from threading import Thread

from kombu.mixins import ConsumerMixin
from kombu import Connection, Exchange, Queue

#import time
import sys
import pika
import datetime
import shutil
#import hashlib

from nanowire_plugin import send, set_status, validate_payload

#import the relavant version of urllib depending on the version of python we are
import urllib

#set up the logger globally
logger = logging.getLogger("nanowire-plugin")



#This is the worker that does the uploading
class GroupWorker(ConsumerMixin):
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

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.queues,
                         callbacks=[self.on_message],
                         prefetch_count=1, no_ack=False)]

    def on_message(self, body, message):
        logger.info("new message to internal queue")
        self.q.put((body, message))

    def run_tasks(self):
        while True:
            try:
                self.on_task(*self.q.get())
            except Exception as ex:
                logging.error(ex)

            except KeyboardInterrupt:
                break

    def on_task(self, body, message):
        logger.info("run task")
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
            
        
        result = ''
        
        nmo = payload['nmo']
        
        tar_url = pull_tarball_url(nmo)
        
        if tar_url != None:
        
            pull_and_extract_tarball(tar_url, '/cache')

            read_obj = reader()
            write_obj = writer(nmo)
            #************** There needs to be some way of getting the url before we hit this
            
            try:
                #result = self.function(nmo, jsonld, url)
                result = run_group_function(self.function, read_obj, write_obj, nmo)
                
            except Exception as exp:
                if self.debug_mode > 0:
                    result = traceback.format_exc()
                    logger.info("THERE WAS A PROBLEM RUNNING THE MAIN FUNCTION: %s"%str(result))
                else:
                    result = exp
                    logger.info("THERE WAS A PROBLEM RUNNING THE MAIN FUNCTION: %s"%str(result))
            
        else:

            result = "GROUP TARBALL IS MISSING"

        #send the result to minio and close everything down
        minio_sender = Minio_tool(self.minio_client)


        if result != "GROUP TARBALL IS MISSING":

            try:
                nmo = minio_sender.send_file("/output/results.json", nmo, self.name)
                result = {"nmo":nmo}
                
            except Exception as exp:
                logger.info("FAILED TO SEND RESULT: %s"%str(exp))
                
                result = exp
            
        #send our results to the next plugin in the queue
        job_stats = send(self.name, payload, result, self.connection, self.out_channel, self.minio_client, self.monitor_url, message, self.debug_mode)
    
        if self.debug_mode >= 3:
            logger.info(job_stats)
            
            
        logger.info("FINISHED RUNNING USER CODE AT %s"%str(datetime.datetime.now()))
        logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        #message.ack()


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
            group_store_dict['doc']['meta']['jobId'] = self.nmo['job']['job_id']
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
            if 'nanowire_plugin.group_tools.single_file' not in str(type(single_file)):
                raise Exception("You can only write a nanowire plugin single_file object to the output using the append task command. You have tried to send an invalid %s object"%str(type(single_file)))
        else:
            #logger.info(str(single_file))
            if 'nanowire_plugin.group_tools.single_file' not in str(single_file):
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



def group_bind(function, name, version="1.0.0", pulserate=25, debug_mode=0):
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
    
    
    #set up the minio client
    minio_client = Minio(
        environ["MINIO_HOST"] + ":" + environ["MINIO_PORT"],
        access_key=environ["MINIO_ACCESS"],
        secret_key=environ["MINIO_SECRET"],
        secure=True if environ["MINIO_SCHEME"] == "https" else False)
        
    minio_client.set_app_info(name, version)

    monitor_url = environ["MONITOR_URL"]


    if environ.get("AMQP_SECURE") != None:
        if environ["AMQP_SECURE"] == "1":
            logger.info("Using ssl connection")
            
            rabbit_url = "amqp://%s:%s@%s:%s/"%(environ["AMQP_USER"], environ["AMQP_PASS"], environ["AMQP_HOST"], environ["AMQP_PORT"])
            
            queues = [Queue(name)]
            with Connection(rabbit_url, heartbeat=25, ssl=True) as conn:
                worker = GroupWorker(conn, queues, function, name, minio_client, monitor_url, debug_mode)
                logger.info("Starting consuming")
                worker.run()
                
        else:
            logger.info("Not ssl connection as per instruction")
            
            rabbit_url = "amqp://%s:%s@%s:%s/"%(environ["AMQP_USER"], environ["AMQP_PASS"], environ["AMQP_HOST"], environ["AMQP_PORT"])
            
            queues = [Queue(name)]
            with Connection(rabbit_url, heartbeat=25) as conn:
                worker = GroupWorker(conn, queues, function, name, minio_client, monitor_url, debug_mode)
                logger.info("Starting consuming")
                worker.run()
                
    else:
    
        logger.info("Not ssl connection, no instruction")
            
        rabbit_url = "amqp://%s:%s@%s:%s/"%(environ["AMQP_USER"], environ["AMQP_PASS"], environ["AMQP_HOST"], environ["AMQP_PORT"])
        
        queues = [Queue(name)]
        with Connection(rabbit_url, heartbeat=25) as conn:
            worker = GroupWorker(conn, queues, function, name, minio_client, monitor_url, debug_mode)
            logger.info("Starting consuming")
            worker.run()
            
    logger.info("Passed consumer, something is very wrong...")


'''
#create a class so we can feed things into the on_request function
class group_on_request_class():
    
    def __init__(self, connection, function, name, minio_client, output_channel, monitor_url, debug_setting):
        
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
        self.debug_mode = debug_setting

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
                if sys.version_info.major == 2:
                    set_status(self.monitor_url, u"Unknown", u"Unknown", self.name, error="Message passed to %s is incomplete")
                else:
                    set_status(self.monitor_url, "Unknown", "Unknown", self.name, error="Message passed to %s is incomplete")
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
        
        p = Process(target=self.run_processing_thread, args=())
        p.start()        
        
        
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
                
                #reset the timer on the pacemaker
                beat_time = time.time()
                time_since_last_heartbeat = 0
            
            messages = self.process_queue.qsize()
            
            if not self.process_queue.empty():
            
                if messages == 1:
                    try:
                        output = self.process_queue.get_nowait()
                        processing = False
                    except:
                        logger.warning(self.process_queue.qsize())
                        logger.warning("=========================")
                        logger.warning(traceback.format_exc())
                        #'Result did not get put onto the processing queue'
                    
                elif messages > 1:
                    raise Exception("Something has gone wrong, there are multiple messages on the queue: %s"%str(self.process_queue.queue))
        
        #logger.info(json.dumps(self.payload))
        
        #run the send command with a 2 minute timeout
        send(self.name, self.payload, output, ch, self.output_channel, method, self.minio_client, self.monitor_url, self.debug_mode)
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
                
                
                
            except Exception as exp:
                if self.debug_mode > 0:
                    result = traceback.format_exc()
                    logger.info("THERE WAS A PROBLEM RUNNING THE MAIN FUNCTION: %s"%str(result))
                else:
                    result = exp
                
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
        #logger.info("Putting result on thread %s"%json.dumps(result))
        self.process_queue.put_nowait(result)
'''







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
    