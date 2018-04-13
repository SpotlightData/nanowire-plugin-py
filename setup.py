#!/usr/bin/env python3

from setuptools import setup, find_packages
import os

here = os.getcwd()
contents = os.listdir(here)

print(here)
print(contents)


VERSION = None
with open("./VERSION") as f:
    VERSION = f.read()

setup(
    name='nanowire_plugin',
    description='Tool for creating python nanowire plugins',
    version=VERSION,
    packages=find_packages(),
    keywords=['nanowire', 'spotlight data'],
    requires=['pika', 'minio'],
    url="https://github.com/SpotlightData/nanowire-plugin-py",
    author='Barnaby "Southclaws" Keene/Stuart Bowe',
    author_email='stuart@spotlightdata.co.uk',
    maintainer='Stuart Bowe',
    maintainer_email='stuart@spotlightdata.co.uk',
    license='MIT',
    description='This library is designed for developers using the nanowire platform to produce plugins using python. There are two main functions they should be aware of. The first is bind which is used to create single file plugins and the second is group_bind which is used to create group plugins. You should see the nanowire documentation for advice on the use of these functions and instructions for building your own plugins',
    include_package_data=True,
    install_requires=["minio", "pika"],
    data_files=[
        ('.', ['VERSION'])
    ]
)
