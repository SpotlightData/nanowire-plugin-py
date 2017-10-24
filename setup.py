#!/usr/bin/env python3

from setuptools import setup
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
    version=VERSION,
    packages=['nanowire_plugin'],
    requires=['pika', 'minio'],
    url="https://github.com/SpotlightData/nanowire-plugin-py",
    author='Barnaby "Southclaws" Keene',
    author_email='southclaws@gmail.com',
    license='MIT',
    include_package_data=True,
    data_files=[
        ('.', ['VERSION'])
    ]
)
