# Nanowire Plugin Interface

This is the Python library for receiving data in a Nanowire pipeline.

## Installation

Library is on PyPI: https://pypi.python.org/pypi/nanowire-plugin

`pip install nanowire-plugin`

## Usage

```python
from nanowire_plugin import bind

def my_analysis(nmo: dict, jsonld: dict, url: str) -> dict:
    # perform analysis

    return result

bind(my_analysis, "my_plugin")
```

Callback function parameters:

- `nmo`: a copy of the Nanowire Metadata Object (for `source.misc` access)
- `jsonld`: the JSON-LD document to mutate
- `url`: a URL pointing to the task's source file

Result is a `dict` object with the mutated JSON-LD document.
