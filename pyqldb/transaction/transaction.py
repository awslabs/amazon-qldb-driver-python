# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
# the License. A copy of the License is located at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
from logging import getLogger

from botocore.exceptions import ClientError

from ..cursor.read_ahead_cursor import ReadAheadCursor
from ..cursor.stream_cursor import StreamCursor
from ..errors import IllegalStateError, TransactionClosedError, is_occ_conflict_exception
from ..util.qldb_hash import QldbHash

logger = getLogger(__name__)


class Transaction:
    """
    A class representing a QLDB transaction.

    Every transaction is tied to a parent (Pooled)QldbSession, meaning that if the parent session is closed or
    invalidated, the child transaction is automatically closed and cannot be used. Only one transaction can be active at
    any given time per parent session, and thus every transaction should call
    :py:meth:`pyqldb.transaction.transaction.Transaction.abort` or
    :py:meth:`pyqldb.transaction.transaction.Transaction.commit` when it is no longer needed, or when a new transaction
    is desired from the parent session.

    An InvalidSessionException indicates that the parent session is dead, and a new transaction cannot be created
    without a new (Pooled)QldbSession being created from the parent driver.

    Any unexpected errors that occur within a transaction should not be retried using the same transaction, as the state
    of the transaction is now ambiguous.

    When an OCC conflict occurs, the transaction is closed and must be handled manually by creating a new transaction
    and re-executing the desired queries.

    Child Cursor objects will be closed when the transaction is aborted or committed.

    :type session: :py:class:`pyqldb.communication.session_client.SessionClient`
    :param session: The session object representing a communication channel with QLDB.

    :type read_ahead: int
    :param read_ahead: The number of read-ahead buffers used in retrieving results.

    :type transaction_id: str
    :param transaction_id: The ID of a transaction.

    :type executor: :py:class:`concurrent.futures.thread.ThreadPoolExecutor`
    :param executor: The executor to be used by the retrieval thread.
    """
    def __init__(self, session, read_ahead, transaction_id, executor):
        self._session = session
        self._read_ahead = read_ahead
        self._cursors = []
        self._is_closed = False
        self._id = transaction_id
        self._txn_hash = QldbHash.to_qldb_hash(transaction_id)
        self._executor = executor

    def __enter__(self):
        """
        Context Manager function to support the 'with' statement.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context Manager function to support the 'with' statement.
        """
        self.close()

    @property
    def is_closed(self):
        """
        The **read-only** flag indicating if this transaction has been committed or aborted.
        """
        return self._is_closed

    @property
    def transaction_id(self):
        """
        The **read-only** ID of this transaction.
        """
        return self._id

    def abort(self):
        """
        Abort this transaction and close child cursors. No-op if already closed by commit or previous abort.
        """
        if not self._is_closed:
            self._internal_close()
            self._session.abort_transaction()

    def close(self):
        """
        Close this transaction.
        """
        try:
            self.abort()
        except ClientError as ce:
            logger.warning('Ignored error aborting transaction when closing: {}'.format(ce))

    def commit(self):
        """
        Commit this transaction and close child cursors.

        :raises IllegalStateError: When the commit digest from commit transaction result does not match.

        :raises TransactionClosedError: When this transaction is closed.

        :raises ClientError: When there is an error communicating with QLDB.
        """
        if self._is_closed:
            raise TransactionClosedError

        try:
            commit_transaction_result = self._session.commit_transaction(self._id, self._txn_hash.get_qldb_hash())
            if self._txn_hash.get_qldb_hash() != commit_transaction_result.get('CommitDigest'):
                raise IllegalStateError("Transaction's commit digest did not match returned value from QLDB. "
                                        "Please retry with a new transaction. Transaction ID: {}".format(self._id))
        except ClientError as ce:
            if is_occ_conflict_exception(ce):
                # Avoid sending courtesy abort since we know transaction is dead on OCC conflict.
                raise ce
            self.close()
            raise ce
        finally:
            self._internal_close()

    def execute_statement(self, statement, parameters=[]):
        """
        Execute the statement.

        :type statement: str/function
        :param statement: The statement to execute.

        :type parameters: list
        :param parameters: Optional list of Ion values to fill in parameters of the statement.

        :rtype: :py:class:`pyqldb.cursor.stream_cursor`/object
        :return: Cursor on the result set of the statement.

        :raises TransactionClosedError: When this transaction is closed.

        :raises ClientError: When there is an error communicating with QLDB.
        """
        if self._is_closed:
            raise TransactionClosedError

        self._update_hash(statement, parameters)
        statement_result = self._session.execute_statement(self._id, statement, parameters)
        first_page = statement_result.get('FirstPage')
        if self._read_ahead > 0:
            cursor = ReadAheadCursor(first_page, self._session, self._id, self._read_ahead, self._executor)
        else:
            cursor = StreamCursor(first_page, self._session, self._id)

        self._cursors.append(cursor)
        return cursor

    def _internal_close(self):
        """
        Mark this transaction as closed, and stop retrieval threads for any `StreamCursor` objects.
        """
        self._is_closed = True
        while len(self._cursors) != 0:
            self._cursors.pop().close()

    def _update_hash(self, statement, parameters):
        """
        Update this transaction's hash given the statement and parameters for an execute statement.
        """
        statement_hash = QldbHash.to_qldb_hash(statement)

        for param in parameters:
            statement_hash = statement_hash.dot(QldbHash.to_qldb_hash(param))

        self._txn_hash = self._txn_hash.dot(statement_hash)
