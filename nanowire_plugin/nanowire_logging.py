import datetime
import json
import logging

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


def getLogHandler():
    logHandler = logging.StreamHandler()
    logHandler.setFormatter(NanowireLogFormatter())
    return logHandler
