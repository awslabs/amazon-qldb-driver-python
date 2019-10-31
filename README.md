# AmazonQLDB Python Driver

This is the Python driver for [Amazon Quantum Ledger Database (QLDB)](https://aws.amazon.com/qldb/), which allows Python developers
to write software that makes use of AmazonQLDB.

[![Documentation Status](https://readthedocs.org/projects/amazon-qldb-driver-python/badge/?version=latest)](https://amazon-qldb-driver-python.readthedocs.io/en/latest/?badge=latest)

For our tutorial, see [Python and Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started.python.html).

## Requirements

### Basic Configuration

You need to set up your AWS security credentials and config before the driver is able to connect to AWS.

Set up credentials (in e.g. `~/.aws/credentials`):

```
[default]
aws_access_key_id = <your access key id>
aws_secret_access_key = <your secret key>
```

Set up a default region (in e.g. `~/.aws/config`):

```
[default]
region = us-east-1 <or other region>
```

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html#SettingUp.Q.GetCredentials) page for more information.

### Python 3.x

The driver requires Python 3.x. Please see the link below for more detail to install Python 3.x:

* [Python 3.x Installation](https://www.python.org/downloads/)

## Installing the driver and running the driver

First, install the driver using pip:

```pip install pyqldb```


Then from a Python interpreter, call the driver and specify the ledger name:

```python
from pyqldb.driver.pooled_qldb_driver import PooledQldbDriver

qldb_driver = PooledQldbDriver(ledger_name='test_ledger')
qldb_session = qldb_driver.get_session()

for table in qldb_session.list_tables():
    print(table)
```

## Development

### Getting Started

Assuming that you have Python and `virtualenv` installed, set up your environment and installed the dependencies
like this instead of the `pip install pyqldb` defined above:

```
$ git clone https://github.com/awslabs/amazon-qldb-driver-python
$ cd driver
$ virtualenv venv
...
$ . venv/bin/activate
$ pip install -r requirements.txt
$ pip install -e .
```

### Running Tests

You can run the unit tests with this command:

```
$ pytest --cov-report term-missing --cov=pyqldb
```

### Documentation 

Sphinx is used for documentation. You can generate HTML locally with the following:

```
$ pip install -r requirements-docs.txt
$ pip install -e .
$ cd docs
$ make html
```

## Release Notes

### Release 1.0.0-rc.2 (October 29, 2019)

Fixes for small documentation issues.

### Release 1.0.0-rc.1 (October 28, 2019)

* Initial preview release of the Amazon QLDB Driver for Python.

## License

This library is licensed under the Apache 2.0 License.
