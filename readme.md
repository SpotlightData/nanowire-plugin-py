# Nanowire Plugin Interface

This is the Python library for receiving data in a Nanowire pipeline. This library provides a wrapper around the nanowire controller API. It allows the user to easily create new plugins. NOTE: It is recommended to start by using the [Nanowire Python Plugin Skeleton](https://github.com/SpotlightData/nanowire-python-plugin-skeleton) if you are intending to make a new plugin instead of using this library from scratch.

## Installation

Library is on PyPI: https://pypi.python.org/pypi/nanowire-plugin

`pip install nanowire-plugin`

## Usage

```python
from nanowire_plugin.single_file_tools import bind

def my_analysis(jsonld, metadata, url):
    # perform analysis

    return result

bind(my_analysis)
```

Callback function parameters:

- `metadata`: a copy of the Nanowire Metadata Object (for `source.misc` access)
- `jsonld`: the JSON-LD document to mutate
- `url`: a URL pointing to the task's source file

Result is a `dict` object with the mutated JSON-LD document.
