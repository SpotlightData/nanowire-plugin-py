#!/usr/bin/env python3

from setuptools import setup, find_packages
import os

VERSION = None
with open("./VERSION") as f:
    VERSION = f.read()

setup(
    name='nanowire_plugin',
    description='Tool for creating python nanowire plugins',
    version=VERSION,
    packages=find_packages(),
    keywords=['nanowire', 'spotlight data'],
    install_requires=[
        'cerberus==1.2',
        'requests==2.21.0'
    ],
    url="https://github.com/SpotlightData/nanowire-plugin-py",
    author='Stuart Bowe/Barnaby "Southclaws" Keene',
    author_email='stuart@spotlightdata.co.uk',
    maintainer='Stuart Bowe',
    maintainer_email='stuart@spotlightdata.co.uk',
    license='MIT',
    include_package_data=True,
    data_files=[
        ('.', ['VERSION'])
    ]
)
