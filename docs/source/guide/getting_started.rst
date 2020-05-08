.. _guide_getting_started:

Getting Started
===============

A quick way to get started with the driver is by creating a ledger and loading
sample data (tables, indexes and documents) to it through the console.

Details on how to do that:

- Creating a Ledger - https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started-step-1.html
- Loading Sample Data - https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started-step-2.html


"""""""""""""""""""""
Driver Initialization
"""""""""""""""""""""

To use AmazonQLDB, you must first import the driver and specify the ledger name

.. code-block:: python

   from pyqldb.driver.pooled_qldb_driver import PooledQldbDriver

   qldb_driver = PooledQldbDriver(ledger_name='vehicle-registration')


""""""""""""""""""""""
Executing Transactions
""""""""""""""""""""""

A transaction can be used to Read, Write, Update and Delete documents
from a QLDB Table.

*****************
Reading Documents
*****************

.. code-block:: python
    :emphasize-lines: 6-18,27
    :lineno-start: 1

    # the first argument will be an instance of Executor which is a
    # wrapper around an implicitly created transaction.
    # to add a statement to this transaction use
    # - transaction_executor.execute_statement(...)
    # The transaction will be committed once this function returns
    def read_documents(transaction_executor):
        # Transaction started implicitly, no need to explicitly start transaction

        # cursor can be iterated like a standard python iterator
        # to get results
        cursor = transaction_executor.execute_statement("SELECT * FROM Person")

        for doc in cursor:
            print(doc["GovId"])
            print(doc["FirstName"])

        # Transaction committed implicitly on return,
        # no need to explicitly commit transaction

    # pyqldb.driver.pooled_qldb_driver.PooledQldbDriver.execute_lambda accepts
    # a function that receives instance of :py:class:`pyqldb.execution.executor.Executor`
    # The passed function will be executed within the context of
    # an implicitly created transaction(and session). The transaction is wrapped by
    # an executor instance. The executor will be available within the
    # passed function. Post execution of the function the transaction will
    # be implicitly committed.
    qldb_driver.execute_lambda(lambda executor: read_documents(executor))


.. Note::
    A single transaction can have multiple statements.
    To add another statement to the above transaction just
    simply add `transaction_executor.execute_statement(...)`

**For more details on Reading Documents (eg Query Parameters) - Check
the** `Cookbook <cookbook.html#reading-documents>`_

*******************
Inserting Documents
*******************

.. code-block:: python
    :emphasize-lines: 1-19,30
    :lineno-start: 1

    def insert_documents(transaction_executor, arg_1):
        # Check if doc with GovId:TOYENC486FH exists
        # This is critical to make this transaction idempotent
        query = "SELECT * FROM Person WHERE GovId = ?", "TOYENC486FH"
        cursor = transaction_executor.execute_statement(query)
        # Check if there is any record in the cursor
        first_record = next(cursor, None)

        if first_record:
            # Record already exists, no need to insert
            pass
        else:

            # Note : arg_1 here is a native python dict. QLDB supports Amazon Ion
            # documents.
            # So before being sent to QLDB execute_statement will first convert any non
            # Ion datatype to Ion using amazon.ion.simpleion module.

            transaction_executor.execute_statement("INSERT INTO Person ?", arg_1)


    doc_1 = {'FirstName': "Brent",
             'LastName': "Logan",
             'DOB': datetime(1963, 8, 19),
             'GovId': "TOYENC486FH",
             'GovIdType': "Driver License",
             'Address': "43 Stockert Hollow Road, Everett, WA, 98203"
            }

    qldb_driver.execute_lambda(lambda x: insert_documents(x, doc_1))

.. Warning::
    A transaction needs to be idempotent to avoid undesirable side
    effects.

    For eg: Consider the above transaction which inserts a document into
    Person table. It first checks if the document already exists in the table or not.
    So even if this transaction is executed multiple times, it will not cause any
    side effects.

    Without this check, we might end up with duplicate documents in
    the table. It may happen that transaction commits successfully
    on QLDB server side but the driver/client may timeout waiting for a
    response.

    In such a case if if the above transaction is retried, it may
    lead to documents being inserted twice (Non Idempotent transaction).


.. Note::
    For performance reasons it is highly recommended that Select queries
    make use of indexes. In above example, a missing index on GovId may
    result in latent queries and higher number of OCC Exceptions.

.. Note::
    `execute_lambda` has an inbuilt Retry mechanism which retries the
    transaction in case a Retryable Error occurs (such as Timeout, OCCException).
    The number of times a transaction is configurable and can be configured by setting
    property `retry_limit` when initializing PooledQldbDriver. The default value for
    `retry_limit` is 4.

**For more details on Inserting Documents (eg Inserting Ion documents instead of
native datatypes), Updating, Deleting - Check the** `Cookbook <cookbook.html#inserting-documents>`_

""""""""""""""""""""""""""""""
Optimistic Concurrency Control
""""""""""""""""""""""""""""""

In QLDB, concurrency control is implemented using optimistic concurrency control (OCC). OCC operates on the principle that multiple transactions can frequently complete without interfering with each other.

Using OCC, transactions in QLDB don't acquire locks on database resources and operate with full serializable isolation. QLDB executes concurrent transactions in a serial manner, such that it produces the same effect as if those transactions were executed serially.

Before committing, each transaction performs a validation check to ensure that no other committed transaction has modified the snapshot of data that it's accessing. If this check reveals conflicting modifications, or the state of the data snapshot changes, the committing transaction is rejected. However, the transaction can be restarted.

When a transaction writes to QLDB, the validation checks of the OCC model are implemented by QLDB itself. If a transaction can't be written to the journal due to a failure in the verification phase of OCC, QLDB returns an OccConflictException to the application layer. The application software is responsible for ensuring that the transaction is restarted. The application should abort the rejected transaction and then retry the whole transaction from the start.
