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

from amazon.ion.simpleion import dumps, loads

from ..cursor.read_ahead_cursor import ReadAheadCursor
from ..cursor.stream_cursor import StreamCursor
from ..errors import IllegalStateError
from ..util.qldb_hash import QldbHash

logger = getLogger(__name__)


class Transaction:
    """
    A class representing a QLDB transaction. This is meant for internal use only.

    Every transaction is tied to a parent QldbSession, meaning that if the parent session is closed or
    invalidated, the child transaction is automatically closed and cannot be used. Only one transaction can be active at
    any given time per parent session.

    Any unexpected errors that occur within a transaction should not be retried using the same transaction, as the state
    of the transaction is now ambiguous.

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
        self._id = transaction_id
        self._txn_hash = QldbHash.to_qldb_hash(transaction_id)
        self._executor = executor

    @property
    def transaction_id(self):
        """
        The **read-only** ID of this transaction.
        """
        return self._id

    def _commit(self):
        """
        Commit this transaction.

        :raises IllegalStateError: When the commit digest from commit transaction result does not match.

        :raises ClientError: When there is an error communicating against QLDB.
        """
        commit_transaction_result = self._session._commit_transaction(self._id, self._txn_hash.get_qldb_hash())
        if self._txn_hash.get_qldb_hash() != commit_transaction_result.get('CommitDigest'):
            raise IllegalStateError("Transaction's commit digest did not match returned value from QLDB. "
                                    "Please retry with a new transaction. Transaction ID: {}".format(self._id))

    def _execute_statement(self, statement, *parameters):
        """
        Execute the statement.

        :type statement: str/function
        :param statement: The statement to execute.

        :type parameters: Variable length argument list
        :param parameters: Ion values or Python native types that are convertible to Ion for filling in parameters
                           of the statement.

                           `Details on conversion support and rules <https://ion-python.readthedocs.io/en/latest/amazon.ion.html?highlight=simpleion#module-amazon.ion.simpleion>`_.

        :rtype: :py:class:`pyqldb.cursor.stream_cursor`/object
        :return: Cursor on the result set of the statement.

        :raises ClientError: When there is an error executing against QLDB.

        :raises TypeError: When conversion of native data type (in parameters) to Ion fails due to an unsupported type.
        """
        parameters = tuple(map(self._to_ion, parameters))
        self._update_hash(statement, parameters)
        statement_result = self._session._execute_statement(self._id, statement, parameters)

        if self._read_ahead > 0:
            cursor = ReadAheadCursor(statement_result, self._session, self._id, self._read_ahead, self._executor)
        else:
            cursor = StreamCursor(statement_result, self._session, self._id)

        self._cursors.append(cursor)
        return cursor

    def _close_child_cursors(self):
        """
        Stop retrieval threads for any `StreamCursor` objects.
        """
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

    @staticmethod
    def _to_ion(obj):
        """
        Check if the object is of Ion type; if not, convert to Ion.

        :raises TypeError in case conversion fails.
        """
        if not hasattr(obj, "ion_annotations"):
            try:
                obj = loads(dumps(obj))
            except TypeError:
                raise TypeError("Failed to convert parameter to Ion; unsupported data type: %r" % (type(obj)))

        return obj
