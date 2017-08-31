# Nanowire Plugin Interface

This is the Python library for receiving data in a Nanowire pipeline.

## Installation

Library is on PyPI: https://pypi.python.org/pypi/nanowire-plugin

`pip install nanowire-plugin`

## Usage

```python
from nanowire_plugin import bind

def my_analysis(data: bytearray) -> dict:
    # perform analysis

    return result

bind(my_analysis, "my_plugin")
```
