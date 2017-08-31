#!/usr/bin/env python3

import sld


def entry(nmo: dict, jsonld: dict, source: str) -> dict:
    print(nmo["job"]["job_id"], nmo["task"]["task_id"], jsonld)
    return {"@type": "TextDigitalDocument"}

sld.bind(entry, "extract")
