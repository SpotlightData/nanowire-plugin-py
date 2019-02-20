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
import subprocess
import os
import tarfile
import traceback
import logging
import json
from os import environ
import inspect

import requests

import time


#import time
import sys
import datetime
import shutil
#import hashlib

from nanowire_plugin import send

#import the relavant version of urllib depending on the version of python we are
import urllib

#set up the logger globally
logger = logging.getLogger("nanowire-plugin")



#This is the worker that does the uploading
class GroupWorker(object):
    def __init__(self, function, minio_client, monitor_url):
        
        self.function = function
        self.minio_client = minio_client
        self.monitor_url = monitor_url
        self.connect_max_retries=5

            
    def run(self):
        while True:
            message = requests.get(os.environ['CONTROLLER_BASE_URI'] + '/v1/tasks/?pluginId=' + os.environ['PLUGIN_ID'] + '&pluginInstance=' + os.environ['POD_NAME'])
            
            code = message.status_code
            
            if code == 200:
                
                payload = json.loads(message.text)
                
                meta = payload['metadata']
                
                    
                try:
                    tar_url = meta['task']['metadata']['cacheURL']
                    found_tarball=True
                except:
                    tar_url = None
                    
    
                if meta['job']['workflow']['type'] != 'GROUP':
                    logger.warning("GROUP PLUGIN WAS SENT A NONE GROUP JOB")
                    
                else:
                    if tar_url != None:
                        try:
                            pull_and_extract_tarball(tar_url, '/cache')
                        except Exception as exp:
                            
                            if "COULD NOT FIND TARBALL AT" in str(exp):
                                found_tarball = False                    
            
            
                        if found_tarball:
                            read_obj = reader()
                            write_obj = writer(meta)
                            #************** There needs to be some way of getting the url before we hit this
                            
                            try:
                                #result = self.function(meta, jsonld, url)
                                result = run_group_function(self.function, read_obj, write_obj, meta)
                                
                            except Exception as exp:
                                result = str(exp)
                                logger.info("THERE WAS A PROBLEM RUNNING THE MAIN FUNCTION: %s"%str(result))
                                    
                        else:
                            result = "GROUP TARBALL IS MISSING"
                        
                    else:
            
                        result = "GROUP TARBALL IS MISSING"
            
                    #send the result to minio and close everything down
                    minio_sender = Minio_tool(self.minio_client)
            
            
                    if not isinstance(result, str):
            
                        try:
                            new_payloads = minio_sender.send_file("/output/results.json", meta)
                            
                            if isinstance(result, dict):
                                result['storePayloads'] = new_payloads
                            elif isinstance(result, str):
                                pass
                            else:
                                result = {'storePayloads':new_payloads}
                                
                            #out_dict = {'storePayloads':new_payloads}
                            
                        except Exception as exp:
                            logger.info("FAILED TO SEND RESULT: %s"%str(exp))
                            
                            result = str(exp)
                        
                    #send our results to the next plugin in the queue
                        
                    logger.info("FINISHED RUNNING USER CODE AT %s"%str(datetime.datetime.now()))
                    logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    
                    #logger.info(json.dumps(result))
                    #logger.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                    send(meta, result)
                    
                
            elif code == 404:
                time.sleep(1)
                
                

#check the meta to see if we're working with groups
def check_for_group(meta):
    
    if not isinstance(meta, dict):
        raise Exception('meta should be a dictionary, is actually: %s, a %s'%(str(meta), str(type(meta))))
    
    try:
        result = (meta['job']['workflow']['type'] == 'GROUP')
    except:
        result = False
        
    return result

#download a tarball from a url
def pull_tarball_url(meta):
    
    
    if not isinstance(meta, dict):
        raise Exception('meta should be a dictionary, is actually: %s, a %s'%(str(meta), str(type(meta))))
    
    try:
        url = meta['task']['metadata']['cacheURL']
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
    
    def __init__(self, meta):
        
        if not isinstance(meta, dict):
            raise Exception("metadata should be in a dictionary, is actually: %s, a %s"%(str(meta), str(type(meta))))
        
        self.meta = meta
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
            
    #create a jsonld for the whole group job
    def add_group_jsonld(self, group_jsonld):
    
        if group_jsonld != {}:
            #the group jsonld needs to be labeled with a line thats a json formatted like this
            group_line_dict = {}
            group_line_dict['update'] = {}
            group_line_dict['update']['_id'] = self.meta['task']['_id']
            group_line_dict['update']['_type'] = 'groupResults'
            group_line_dict['update']['_index'] = 'group'
            
            
            group_store_dict = {}
            group_store_dict['doc_as_upsert'] = True
            group_store_dict['doc'] = {}
            group_store_dict['doc']['meta'] = {}
            group_store_dict['doc']['meta']['userId'] = self.meta['task']['userId']
            group_store_dict['doc']['meta']['projectId'] = self.meta['task']['projectId']
            group_store_dict['doc']['meta']['jobId'] = self.meta['task']['jobId']
            group_store_dict['doc']['meta']['taskId'] = self.meta['task']['_id']
            group_store_dict['doc']['meta']['storedAt'] = datetime.datetime.utcnow().isoformat()
            group_store_dict['doc']['jsonLD'] = group_jsonld
            
            
            with open(self.out_file, "r+") as f:
                
                lines = f.readlines()
                
                f.seek(0)
               
                f.write(json.dumps(group_line_dict) + '\n')
        
                f.write(json.dumps(group_store_dict) + '\n')
    
                f.writelines(lines)
    
    #create a jsonld for the info added to a given single file. This might be the
    #topic modeling results for this specific single file for example
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
            task_line['update']['_id'] = single_file.filename.split("/")[-1] + ':' + self.meta['task']['_id']
            task_line['update']['_type'] = 'taskResults'
            task_line['update']['_index'] = 'group'
            task_line['update']['_parent'] = self.meta['task']['_id']
            
            #This is how we store the individual jsonlds
            task_store_line = {}
            task_store_line['doc'] = single_file.change_dict
            task_store_line['doc_as_upsert'] = True
        
            with open(self.out_file, 'a') as f:
                
                f.write(json.dumps(task_line) + '\n')
                f.write(json.dumps(task_store_line) + '\n')
                

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
        
        
    def send_file(self, filename, meta):
        
        
        try:
            if 'MINIO_BUCKET' in os.environ.keys():
                bucket_name = os.environ['MINIO_BUCKET']
            else:
                bucket_name = meta['job']['_id']
            job_id = meta['job']['_id']
            task_id = meta['task']['_id']
            
        except Exception as e:
            logger.warning(str(e))
            raise Exception("Key information missing from metadta either job_id or task_id. metadata is: %s"%json.dumps(meta))
        
        logger.info("ENSURING EXISTANCE OF BUCKET: %s"%bucket_name)
        #first check the bucket we want to save to exists
        if not self.minioClient.bucket_exists(bucket_name):
            
            self.minioClient.make_bucket(bucket_name)
            
        save_name = '%s/%s/group.bin'%(job_id, task_id)
        
        #read the outputted jsonld for storage
        logger.info("READING SAVE FILE")
        if not os.path.exists(filename):
            raise Exception("Tried to send non-existant file: %s"%filename)
        
        file_stat = os.stat(filename)
        file_data = open(filename, 'rb')
        
        #send the outputted jsonld to minio
        logger.info("PUTTING OBJECT")
        self.minioClient.put_object(bucket_name, save_name, file_data, file_stat.st_size)
        
        #remove the cache from the pod
        logger.info("CLEANING UP")
        self.clean_up_after_sending()
        
        #add the minio storage point to the metadata
        
        
        try:
            new_store_payloads = meta['task']['metadata']['storePayloads']
            new_store_payloads.append(save_name)
        except:
            new_store_payloads = [save_name]

        
        return new_store_payloads
        
        
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
        
        
def run_group_function(function , read_tool, write_tool, meta):
    
    arguments = inspect.getargspec(function)[0]
    
    #3 argument calls
    if arguments == ['reader', 'writer', 'meta'] or arguments == ['self', 'reader', 'writer', 'meta']:
        
        out = function(read_tool, write_tool, meta)
        
        return out
        
    if arguments == ['reader', 'meta', 'writer'] or arguments == ['self', 'reader', 'meta', 'writer']:
        
        out = function(read_tool, meta, write_tool)
        
        return out
        
    elif arguments == ['meta', 'reader', 'writer'] or arguments == ['self', 'meta', 'reader', 'writer']:
        
        out = function(meta, read_tool, write_tool)
        
        return out
        
    elif arguments == ['meta', 'writer', 'reader'] or arguments == ['self', 'meta', 'writer', 'reader']:
        
        out = function(meta, write_tool, read_tool)
        
        return out
        
    elif arguments == ['writer', 'reader', 'meta'] or arguments == ['self', 'writer', 'reader', 'meta']:
        
        out = function(write_tool, read_tool, meta)
        
        return out
        
    elif arguments == ['writer', 'meta', 'reader'] or arguments == ['self', 'writer', 'meta', 'reader']:
        
        out = function(write_tool, meta, read_tool)
        
        return out
    
    
    #2 arguments calls
    elif arguments == ['reader', 'writer'] or arguments == ['self', 'reader', 'writer']:
        
        out = function(read_tool, write_tool)
        
        return out
        
    elif arguments == ['writer', 'reader'] or arguments == ['self', 'writer', 'reader']:
        
        out = function(write_tool, read_tool)
        
        return out
        

    else:
        
        raise Exception("FUNCTION MUST ACCEPT VALID ARGUMENTS, CURRENT ARGUMENTS ARE %s"%str(arguments))



def group_bind(function, version="1.0.0"):
    """binds a function to the input message queue"""
    
    #set up the logging
    logger.setLevel(logging.DEBUG)
    
    #pools.set_limit(None)


    #write to screen to ensure logging is working ok
    #print "Initialising nanowire lib, this is a print"
    logger.info("initialising nanowire lib")

    
    #set up the minio client
    minio_client = Minio(
        environ["MINIO_HOST"] + ":" + environ["MINIO_PORT"],
        access_key=environ["MINIO_ACCESS"],
        secret_key=environ["MINIO_SECRET"],
        secure=True if environ["MINIO_SCHEME"] == "https" else False)
        
    #minio_client.set_app_info(name, version)

    monitor_url = environ["MONITOR_URL"]

    worker = GroupWorker(function, minio_client, monitor_url)
    logger.info("Starting consuming")
    worker.run()
                


def validate_group_function(function):
    
    if sys.version_info.major == 3:
        
        arguments = list(inspect.signature(function).parameters)
      
    elif sys.version_info.major == 2:
        
        arguments = inspect.getargspec(function)[0]
        
    allowed = ['self', 'meta', 'reader', 'writer']
    
    arg_dict = set()
    
    for arg in arguments:
        
        if arg not in arg_dict:
            arg_dict.add(arg)
        else:
            raise Exception("ARGUMENTS MAY NOT BE REPEATED")
            
        if arg not in allowed:
            raise Exception("FUNCTION MAY ONLY USE ALLOWED ARGUMENTS, ALLOWED ARGUMENTS ARE: reader, writer, meta YOU HAVE USED THE ARGUMENT %s"%arg)
    
    if 'reader' not in arguments:
        raise Exception("GROUP ANALYSIS FUNCTION MUST TAKE reader AS AN ARGUMENT. THIS IS A CLASS FOR READING DATA")
        
    if 'writer' not in arguments:
        raise Exception("GROUP ANALYSIS FUNCTION MUST TAKE writer AS AN ARGUMENT. THIS IS A CLASS FOR WRITING RESULTS")
    