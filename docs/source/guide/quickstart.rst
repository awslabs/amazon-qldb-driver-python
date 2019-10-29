.. _guide_quickstart:

Quickstart
==========
Getting started with AmazonQLDB is easy, but requires a few steps.

Installation
------------
Install the latest AmazonQLDB driver release via :command:`pip`::

    pip install pyqldb

You may also install a specific version::

    pip install pyqldb==0.1.0b


Configuration
-------------
Before you can begin using the AmazonQLDB driver, you should set up authentication
credentials. Credentials for your AWS account can be found in the
`IAM Console <https://console.aws.amazon.com/iam/home>`_. You can
create or use an existing user. Go to manage access keys and
generate a new set of keys.

If you have the `AWS CLI <http://aws.amazon.com/cli/>`_
installed, then you can use it to configure your credentials file::

    aws configure

Alternatively, you can create the credential file yourself. By default,
its location is at ``~/.aws/credentials``::

    [default]
    aws_access_key_id = YOUR_ACCESS_KEY
    aws_secret_access_key = YOUR_SECRET_KEY

You may also want to set a default region. This can be done in the
configuration file. By default, its location is at ``~/.aws/config``::

    [default]
    region=us-east-1

Alternatively, you can pass a ``region_name`` when creating the driver.

This sets up credentials for the default profile as well as a default
region to use when creating connections.

Using AmazonQLDB Driver
-----------------------
To use AmazonQLDB, you must first import the driver and specify the ledger name::

    from pyqldb.driver.pooled_qldb_driver import PooledQldbDriver

    qldb_driver = PooledQldbDriver(ledger_name='test_ledger')


Now that you have a ``qldb_driver`` resource, you need to get a session::

    qldb_session = qldb_driver.get_session()


Now that you have a ``qldb_session`` resource, you can make requests and process
responses from AmazonQLDB. The following invokes ``list_tables()`` to print out
all table names::

    for table in qldb_session.list_tables():
        print(table)
