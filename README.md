# Typed access to Amazon S3 for humans

## Installation

```shell script
pip install sthree
```

`sthree` requires `boto3` for its operation, but it is not listed as package dependency because, if you'd want to use `sthree` in AWS Lambda, you do not need to install `boto3` there - since it is already installed.

If you're not using Lambda you may run

```shell script
pip install sthree[boto]
```

to make sure all dependencies are satisfied.

## Usage Examples

```python
from sthree import Keys
from itertools import islice    

for key in islice(Keys('my-bucket'), 10):
    print(key)

```

## Motivation

Motivation to build this library is to simplify using Amazon S3 in Python applications and to make development experience a bit more pleasant and Pythonic, as well as to benefit from Python's type hinting capabilities.   
