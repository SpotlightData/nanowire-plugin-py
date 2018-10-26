# Nanowire Plugin Interface

This is the Python library for receiving data in a Nanowire pipeline.

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
