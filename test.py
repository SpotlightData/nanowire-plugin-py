from nanowire_plugin.group_tools import group_bind
from nanowire_plugin.logging import getLogHandler

import logging

logger = logging.getLogger(None)
logger.addHandler(getLogHandler())
logger.setLevel(logging.INFO)

def my_analysis(metadata, jsonld, url):
    logger.info("this is from my personal logger")
    raise Exception("ayylmaooooooo")
    return jsonld

group_bind(my_analysis, 'myanalysis')

