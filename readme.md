# Nanowire Plugin Interface

This is the Python library for receiving data in a Nanowire pipeline.

## Usage

```python
from nanowire_plugin import bind

def my_analysis(data: bytearray) -> dict:
    # perform analysis

    return result

bind(my_analysis, "my_plugin")
```
