#!/usr/bin/env python3

import sld


def entry(data):
    print(data)
    return {"@type": "TextDigitalDocument"}

sld.bind(entry, "extract")
