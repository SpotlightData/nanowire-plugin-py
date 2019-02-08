#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 25 11:30:46 2017

@author: Jonathan Balls
@author: Stuart Bowe
"""

import datetime
import json
import logging
import os
import requests
import sys
import time
import traceback
import uuid
from os import environ
from os.path import join

#import the relavant version of urllib depending on the version of python we are
if sys.version_info.major == 3:
    import urllib
elif sys.version_info.major == 2:
    import urllib
    import urllib2
else:
    import urllib
    
class NanowireLogFormatter(logging.Formatter):
    def format(self, record):
        structured_message = {
            '@timestamp': datetime.datetime.now().isoformat(),
            '@message': record.getMessage(),
            '@fields': {
                '@level': record.levelname
            }
        }
        return json.dumps(structured_message)

#set up the logger globally
logger = logging.getLogger("nanowire-plugin")
logHandler = logging.StreamHandler()
formatter = NanowireLogFormatter()
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)


##################################################################
### These tools are used by both group and single file plugins ###
##################################################################

def report_task_success(taskId, jsonld):
    print("Success!")
    pass

def report_task_failure(taskId, error):
    print("Failure!")
    pass
