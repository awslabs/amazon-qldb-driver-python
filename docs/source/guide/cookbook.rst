.. _guide_cookbook:

Cookbook
========

Working with Amazon ION
***********************

Importing ION module
--------------------

.. code-block:: python

    import amazon.ion.simpleion as simpleion


Creating ION types
------------------

    From ION text

    .. code-block:: python

        ion_text = '{GovId: 'TOYENC486FH', FirstName: "Brent"}'
        ion_obj = simpleion.loads(ion_text)

        print(ion_obj['GovId']) #prints TOYENC486FH
        print(ion_obj['Name']) #prints Brent

    From a Python dict

    .. code-block:: python

        a_dict = { 'GovId': 'TOYENC486FH',
                   'FirstName': "Brent"
                 }
        ion_obj = simpleion.loads(simpleion.dumps(a_dict))

        print(ion_obj['GovId']) #prints TOYENC486FH
        print(ion_obj['FirstName']) #prints Brent

Getting ION Binary dump
-----------------------

    .. code-block:: python

        # ion_obj is an ion struct
        print(simpleion.dumps(ion_obj)) # b'\xe0\x01\x00\xea\xee\x97\x81\x83\xde\x93\x87\xbe\x90\x85GovId\x89FirstName\xde\x94\x8a\x8bTOYENC486FH\x8b\x85Brent'

Getting ION Text dump
---------------------

    .. code-block:: python

        # ion_obj is an ion struct
        print(simpleion.dumps(ion_obj, binary=False)) # prints $ion_1_0 {GovId:'TOYENC486FH',FirstName:"Brent"}

For more details check `Ion docs <http://amzn.github.io/ion-docs/>`_

Importing Driver
****************

.. code-block:: python

   from pyqldb.driver.pooled_qldb_driver import PooledQldbDriver

Driver Instantiation
********************

.. code-block:: python

    qldb_driver = PooledQldbDriver(ledger_name='vehicle-registration')

CRUD Operations
***************

CRUD (Create, Read, Update, Delete) Operations in QLDB happen as part of a transaction.

Transactions need to be strictly idempotent.

.. Warning::
    A transaction needs to be idempotent to avoid undesirable side
    effects.

    For eg: Consider a transaction which inserts a document into
    Person table. The transaction should first check if the document
    already exists in the table or not. The check makes the transaction idempotent,
    which means even if this transaction is executed multiple times, it will not cause any
    side effects.

    Without this check, we might end up with duplicate documents in
    the table. It may happen that transaction commits successfully
    on QLDB server side but the driver/client may timeout waiting for a
    response.

    In such a case if if the above Non Idempotent transaction is retried,
    it may lead to documents being inserted twice.

.. Note::
    In case a Select, Update and Delete queries uses a WHERE clause
    on a field, it is highly recommended to have indexes on those fields.
    A missing index may result in latent queries and higher number of OCC Exceptions.

.. Note::

    `pyqldb.driver.pooled_qldb_driver.PooledQldbDriver.execute_lambda` accepts a function
    that receives instance of :py:class:`pyqldb.execution.executor.Executor`
    The passed function will be executed within the context of
    an implicitly created transaction(and session). The transaction is wrapped within
    an executor instance which will be available within the context
    passed function. Post execution of the function the transaction will
    be implicitly committed.

    `pyqldb.driver.pooled_qldb_driver.PooledQldbDriver.execute_lambda` has an inbuilt
    Retry mechanism which retries the transaction in case a Retryable Error
    occurs (such as Timeout, OCCException). The number of times a transaction is configurable
    and can be configured by setting property `retry_limit` when initializing PooledQldbDriver.
    The default value for `retry_limit` is 4.

Creating Table
---------------

.. code-block:: python

    def create_table(transaction_executor):
        transaction_executor.execute_statement("Create TABLE Person")

    qldb_driver.execute_lambda(lambda executor: create_table(executor))

Creating Index
---------------

.. code-block:: python

    def create_index(transaction_executor):
        transaction_executor.execute_statement("CREATE INDEX ON Person(GovId)")

    qldb_driver.execute_lambda(lambda executor: create_index(executor))


Reading Documents
-----------------
.. code-block:: python

    # assumes that Person table has documents like - {"GovId": 'TOYENC486FH', "FirstName" : "Brent" }


    def read_documents(transaction_executor):
        cursor = transaction_executor.execute_statement("SELECT * FROM Person")

        for doc in cursor:
            print(doc["GovId"]) #prints TOYENC486FH
            print(doc["FirstName"])  # prints Brent

    qldb_driver.execute_lambda(lambda executor: read_documents(executor))

**Using query parameters**

    .. Note::
        `execute_statement()` **supports both Amazon Ion types and python native types.
        If a python native type is passed as an argument to `execute_statement`, it will be converted
        to an Ion type using** `amazon.ion.simpleion <https://ion-python.readthedocs.io/en/latest/_modules/amazon/ion/simpleion.html>`_ **module (provided conversion for that python data type
        is supported). Refer** `here <https://ion-python.readthedocs.io/en/latest/_modules/amazon/ion/simpleion.html>`_
        **for supported data types and conversion rules.**

    Native type query parameters

    .. code-block:: python

            cursor = transaction_executor.execute_statement("SELECT * FROM Person WHERE GovId = ?", 'TOYENC486FH')

    ION type query parameters

    .. code-block:: python

            name_with_annotation = ion.loads("LegalName::Brent")
            cursor = transaction_executor.execute_statement("SELECT * FROM Person WHERE FirstName = ?", name_with_annotation)


    Using multiple query parameters

    .. code-block:: python

        cursor = transaction_executor.execute_statement("SELECT * FROM Person WHERE GovId = ? AND FirstName = ?", 'TOYENC486FH', "Brent")

    Using a list of query parameters

    .. code-block:: python

        gov_ids = ['TOYENC486FH','ROEE1','YH844']
        cursor = transaction_executor.execute_statement("SELECT * FROM Person WHERE GovId IN (?,?,?)", *gov_ids)

    .. Note::
        In the above example, it is  recommended to have **index** on the field `GovId` for performance reasons.
        A missing index on `GovId` may result in latent queries and higher number of OCC Exceptions.

Inserting Documents
-------------------

.. Note::
    `execute_statement()` **supports both Amazon Ion types and python native types.
    If a python native type is passed as an argument to `execute_statement`, it will be converted
    to an Ion type using** `amazon.ion.simpleion <https://ion-python.readthedocs.io/en/latest/_modules/amazon/ion/simpleion.html>`_ **module (provided conversion for that python data type
    is supported). Refer** `here <https://ion-python.readthedocs.io/en/latest/_modules/amazon/ion/simpleion.html>`_
    **for supported data types and conversion rules.**

Inserting Native types

.. code-block:: python

    def insert_documents(transaction_executor, arg_1):
        # Check if doc with GovId:TOYENC486FH exists
        # This is critical to make this transaction idempotent
        cursor = transaction_executor.execute_statement("SELECT * FROM Person WHERE GovId = ?", 'TOYENC486FH')
        # Check if there is any record in the cursor
        first_record = next(cursor, None)

        if first_record:
            # Record already exists, no need to insert
            pass
        else:
            transaction_executor.execute_statement("INSERT INTO Person ?", arg_1)

    doc_1 = { 'FirstName': "Brent",
              'GovId': 'TOYENC486FH',
            }

    qldb_driver.execute_lambda(lambda x: insert_documents(x, doc_1))

Inserting ION data types

.. code-block:: python


    def insert_documents(transaction_executor, arg_1):
        # Check if doc with GovId:TOYENC486FH exists
        # This is critical to make this transaction idempotent
        cursor = transaction_executor.execute_statement("SELECT * FROM Person WHERE GovId = ?", 'TOYENC486FH')
        # Check if there is any record in the cursor
        first_record = next(cursor, None)

        if first_record:
            # Record already exists, no need to insert
            pass
        else:
            transaction_executor.execute_statement("INSERT INTO Person ?", arg_1)

    doc_1 = { 'FirstName': 'Brent',
              'GovId': 'TOYENC486FH',
            }

    # create a sample ion doc
    ion_doc_1 = simpleion.loads(simpleion.dumps(doc_1)))

    qldb_driver.execute_lambda(lambda x: insert_documents(x, ion_doc_1))

.. Note::

    Above mentioned transaction inserts a document into Person table. Before inserting,
    the transaction first checks if the document already exists in the table. **This check
    makes the transaction idempotent in nature.**
    So even if this transaction is executed multiple times, it will not cause any
    side effects.

    Without this check, we might end up with duplicate documents in
    the table. It may happen that transaction commits successfully
    on QLDB server side but the driver/client may timeout waiting for a
    response.

    In such a case if the above Non Idempotent transaction is retried, it may
    lead to documents being inserted twice.

.. Note::
    In the above example, it is  recommended to have **index** on the field `GovId` for performance reasons.
    A missing index on `GovId` may result in latent queries and higher number of OCC Exceptions.

Updating Records
----------------

.. Note::
    `execute_statement()` **supports both Amazon Ion types and python native types**.
    If a python native type is passed as an argument to `execute_statement`, it will be converted
    to an Ion type using `amazon.ion.simpleion <https://ion-python.readthedocs.io/en/latest/_modules/amazon/ion/simpleion.html>`_
    module (provided conversion for that python data type is supported).
    Refer `here <https://ion-python.readthedocs.io/en/latest/_modules/amazon/ion/simpleion.html>`_
    for supported data types and conversion rules.

Using Python Native types

.. code-block:: python



    def update_documents(transaction_executor, gov_id, name):
        transaction_executor.execute_statement("UPDATE Person SET FirstName = ?  WHERE GovId = ?", name, gov_id)


    gov_id = 'TOYENC486FH'
    name = 'John'

    qldb_driver.execute_lambda(lambda x: update_documents(x, gov_id, name))


Using ION data types

.. code-block:: python


    def update_documents(transaction_executor, gov_id, name):

        cursor = transaction_executor.execute_statement("UPDATE Person SET FirstName = ? WHERE GovId = ?", name, gov_id)

    # ion datatypes
    gov_id = simpleion.loads('TOYENC486FH')
    name = simpleion.loads(simpleion.dumps('John'))

    qldb_driver.execute_lambda(lambda x: update_documents(x, gov_id, name))

.. Note::
    In the above example, it is  recommended to have **index** on the field `GovId` for performance reasons.
    A missing index on `GovId` may result in latent queries and higher number of OCC Exceptions.

Deleting Records
----------------

.. Note::
    `execute_statement()` **supports both Amazon Ion types and python native types**.
    If a python native type is passed as an argument to `execute_statement`, it will be converted
    to an Ion type using `amazon.ion.simpleion <https://ion-python.readthedocs.io/en/latest/_modules/amazon/ion/simpleion.html>`_
    module (provided conversion for that python data type is supported).
    Refer `here <https://ion-python.readthedocs.io/en/latest/_modules/amazon/ion/simpleion.html>`_
    for supported data types and conversion rules.

Using Python Native types

.. code-block:: python

    def delete_documents(transaction_executor, gov_id):

        cursor = transaction_executor.execute_statement("DELETE FROM Person WHERE GovId = ?", gov_id)

    gov_id = 'TOYENC486FH'

    qldb_driver.execute_lambda(lambda x: delete_documents(x, gov_id))


Using ION data types

.. code-block:: python



    def delete_documents(transaction_executor, gov_id):

        cursor = transaction_executor.execute_statement("DELETE FROM Person WHERE GovId = ?", gov_id)

    # ion datatypes
    gov_id = simpleion.loads('TOYENC486FH')

    qldb_driver.execute_lambda(lambda x: delete_documents(x, gov_id))

.. Note::
    In the above example, it is  recommended to have **index** on the field `GovId` for performance reasons.
    A missing index on `GovId` may result in latent queries and higher number of OCC Exceptions.


Implementing Uniqueness Constraints
-----------------------------------

QLDB currently has no support for unique indexes.

But it is very easy to implement this behavior in your application.

Suppose you want to implement a uniqueness constraint On GovId.

The idea is to write a transaction which does the following:
- Assert that no documents are found with GovId = ? (where ? would the GovId to be inserted)
- Insert document if assertion passes

If a competing transaction concurrently passes the assertion, only one of the transactions will commit. The other transaction will fail under OCC.

.. code-block:: python

    def insert_documents(transaction_executor, gov_id, document):
        # Check if doc with GovId = gov_id exists
        cursor = transaction_executor.execute_statement("SELECT * FROM Person WHERE GovId = ?", gov_id)
        # Check if there is any record in the cursor
        first_record = next(cursor, None)

        if first_record:
            # Record already exists, no need to insert
            pass
        else:
            transaction_executor.execute_statement("INSERT INTO Person ?", document)

    qldb_driver.execute_lambda(lambda x: insert_documents(x, gov_id, document))

.. Note::
    In the above example, it is  recommended to have **index** on the field `GovId` for performance reasons.
    A missing index on `GovId` may result in latent queries and higher number of OCC Exceptions.