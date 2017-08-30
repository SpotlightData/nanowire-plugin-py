#!/usr/bin/env python3

from distutils.core import setup

setup(
    name='sld',
    version='0.1.0',
    packages=['sld'],
    requires=[
        'pika',
        'minio'
    ]
)
