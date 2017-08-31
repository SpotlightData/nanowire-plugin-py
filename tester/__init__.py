#!/usr/bin/env python3

from nanowire_plugin import bind


def entry(nmo: dict, jsonld: dict, source: str) -> dict:
    print(nmo["job"]["job_id"], nmo["task"]["task_id"], jsonld)
    return {"@type": "TextDigitalDocument"}

bind(entry, "extract")
