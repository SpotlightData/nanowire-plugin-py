#!/usr/bin/env python3

import logging
from os import environ

from nanowire_plugin import bind

from pythonjsonlogger import jsonlogger


LOG = logging.getLogger()
HND = logging.StreamHandler()
HND.setFormatter(jsonlogger.JsonFormatter())
LOG.addHandler(HND)
LOG.setLevel(logging.INFO)


def entry(nmo, jsonld, source):
    LOG.info("PLUGIN: %s:%s:%s", nmo["job"]["job_id"], nmo["task"]["task_id"], str(jsonld))

    LOG.info(str(environ))

    if "TEST_VAR" not in environ:
        raise Exception("TEST_VAR not found in env vars")

    return {"@type": "TextDigitalDocument"}

bind(entry, "extract")
