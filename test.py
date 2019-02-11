from nanowire_plugin.single_file_tools import task_handler
from nanowire_plugin.logging import getLogHandler

import logging

logger = logging.getLogger(None)
logger.addHandler(getLogHandler())
logger.setLevel(logging.INFO)

@task_handler
def my_analysis(metadata, jsonld, url):
    logger.info("this is from my personal logger")
    raise Exception("ayylmaooooooo")
    return jsonld
