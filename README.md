# Amazon QLDB Python Driver

This is the Python driver for [Amazon Quantum Ledger Database (QLDB)](https://aws.amazon.com/qldb/), which allows Python developers
to write software that makes use of AmazonQLDB.

[![Documentation Status](https://readthedocs.org/projects/amazon-qldb-driver-python/badge/?version=latest)](https://amazon-qldb-driver-python.readthedocs.io/en/latest/?badge=latest)

For our tutorial, see [Python and Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started.python.html).

## Requirements

### Basic Configuration

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html) for information on connecting to AWS.

### Python 3.x

The driver requires Python 3.x. Please see the link below for more detail to install Python 3.x:

* [Python 3.x Installation](https://www.python.org/downloads/)

## Getting Started

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

### See Also

1. [Amazon QLDB Python Driver Tutorial](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started.python.tutorial.html): In this tutorial, you use the QLDB Driver for Python to create an Amazon QLDB ledger and populate it with tables and sample data.
2. [Amazon QLDB Python Driver Samples](https://github.com/awslabs/amazon-qldb-driver-python): A DMV based example application which demonstrates how to use QLDB with the QLDB Driver for Python.
3. QLDB Python driver accepts and returns [Amazon ION](http://amzn.github.io/ion-docs/) Documents. Amazon Ion is a richly-typed, self-describing, hierarchical data serialization format offering interchangeable binary and text representations. For more information read the [ION docs](https://readthedocs.org/projects/ion-python/).
4. Amazon QLDB supports the [PartiQL](https://partiql.org/) query language. PartiQL provides SQL-compatible query access across multiple data stores containing structured data, semistructured data, and nested data. For more information read the [PartiQL docs](https://partiql.org/docs.html).
5. Refer the section [Common Errors while using the Amazon QLDB Drivers](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-errors.html) which describes runtime errors that can be thrown by the Amazon QLDB Driver when calling the qldb-session APIs.

## Development

### Setup

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

* Fixes for small documentation issues.

### Release 1.0.0-rc.1 (October 28, 2019)

* Initial preview release of the Amazon QLDB Driver for Python.

## License

This library is licensed under the Apache 2.0 License.