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
    requires=['pika', 'minio', "kombu"],
    url="https://github.com/SpotlightData/nanowire-plugin-py",
    author='Barnaby "Southclaws" Keene/Stuart Bowe',
    author_email='stuart@spotlightdata.co.uk',
    maintainer='Stuart Bowe',
    maintainer_email='stuart@spotlightdata.co.uk',
    license='MIT',
    include_package_data=True,
    install_requires=["minio", "kombu"],
    data_files=[
        ('.', ['VERSION'])
    ]
)
