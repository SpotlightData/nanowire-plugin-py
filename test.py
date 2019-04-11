from nanowire_plugin import task_handler
from nanowire_plugin.nanowire_logging import getLogHandler
import nanowire_plugin

import logging

logger = logging.getLogger(None)
logger.addHandler(getLogHandler())
logger.setLevel(logging.INFO)

@task_handler
def my_analysis(metadata, jsonld, url):
    logger.info("this is from my personal logger")
    return jsonld
