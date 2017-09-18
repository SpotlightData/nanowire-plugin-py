#!/usr/bin/env python3

import logging

from nanowire_plugin import bind

from pythonjsonlogger import jsonlogger


LOG = logging.getLogger()
HND = logging.StreamHandler()
HND.setFormatter(jsonlogger.JsonFormatter())
LOG.addHandler(HND)
LOG.setLevel(logging.DEBUG)


def entry(nmo: dict, jsonld: dict, source: str) -> dict:
    LOG.info("%s:%s:%s", nmo["job"]["job_id"], nmo["task"]["task_id"], str(jsonld))
    return {"@type": "TextDigitalDocument"}

bind(entry, "extract")
