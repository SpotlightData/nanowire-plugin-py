#!/usr/bin/env python3

import logging

from nanowire_plugin import bind

from pythonjsonlogger import jsonlogger


LOG = logging.getLogger()
HND = logging.StreamHandler()
HND.setFormatter(jsonlogger.JsonFormatter())
LOG.addHandler(HND)


def entry(nmo: dict, jsonld: dict, source: str) -> dict:
    LOG.info(nmo["job"]["job_id"], nmo["task"]["task_id"], jsonld)
    return {"@type": "TextDigitalDocument"}

bind(entry, "extract")
