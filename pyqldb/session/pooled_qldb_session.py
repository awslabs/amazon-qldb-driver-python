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

from ..errors import SessionClosedError
from .base_qldb_session import BaseQldbSession

logger = getLogger(__name__)


class PooledQldbSession(BaseQldbSession):
    """
    Represents a pooled session object. See :py:class:`pyqldb.session.qldb_session` for more details.
    """
    def __init__(self, qldb_session, return_session_to_pool):
        self._qldb_session = qldb_session
        self._return_session_to_pool = return_session_to_pool
        self._is_closed = False

    @property
    def ledger_name(self):
        """
        The **read-only** ledger name.
        """
        return self._invoke_on_session(lambda: self._qldb_session.ledger_name)

    @property
    def session_token(self):
        """
        The **read-only** session token.
        """
        return self._invoke_on_session(lambda: self._qldb_session.session_token)

    def close(self):
        """
        Close this `PooledQldbSession` and return the underlying `QldbSession` to the pool.
        """
        if not self._is_closed:
            self._is_closed = True
            self._return_session_to_pool(self._qldb_session)

    def execute_statement(self, statement, parameters=[], retry_indicator=lambda execution_attempt: None):
        """
        Calls :py:meth:`pyqldb.session.qldb_session.QldbSession.execute_statement` to implicitly start a transaction,
        execute the statement, and commit the transaction, retrying up to the retry limit if an OCC conflict or
        retriable exception occurs.

        If an InvalidSessionException is received, it is considered a retriable exception by starting a new
        :py:class:`pyqldb.communication.session_client.SessionClient` to use to communicate with QLDB. Thus, as a side
        effect, this QldbSession can become valid again despite a previous InvalidSessionException from other method
        calls on this instance, any child transactions, or cursors, when this method is invoked.

        :type statement: str
        :param statement: The statement to execute.

        :type parameters: list
        :param parameters: Optional list of Ion values to fill in parameters of the statement.

        :type retry_indicator: function
        :param retry_indicator: Optional function called when the transaction execution is about to be retried due to an
                                OCC conflict or retriable exception.

        :rtype: :py:class:`pyqldb.cursor.buffered_cursor.BufferedCursor`
        :return: Fully buffered Cursor on the result set of the statement.

        :raises IllegalStateError: When the commit digest calculated by the client does not match the digest as
                                   calculated by the QLDB service.

        :raises SessionClosedError: When this session is closed.

        :raises ClientError: When there is an error communicating with QLDB.
        """
        return self._invoke_on_session(lambda: self._qldb_session.execute_statement(statement, parameters,
                                                                                    retry_indicator))

    def execute_lambda(self, query_lambda, retry_indicator=lambda execution_attempt: None):
        """
        Calls :py:meth:`pyqldb.session.qldb_session.QldbSession.execute_lambda` to implicitly start a transaction,
        execute the lambda function, and commit the transaction, retrying up to the retry limit if an OCC conflict or
        retriable exception occurs.

        If an InvalidSessionException is received, it is considered a retriable exception by starting a new
        :py:class:`pyqldb.communication.session_client.SessionClient` to use to communicate with QLDB. Thus, as a side
        effect, this QldbSession can become valid again despite a previous InvalidSessionException from other method
        calls on this instance, any child transactions, or cursors, when this method is invoked.

        :type query_lambda: function
        :param query_lambda: The lambda function to execute. A lambda function cannot have any side effects as
                             it may be invoked multiple times, and the result cannot be trusted until the transaction is
                             committed.

        :type retry_indicator: function
        :param retry_indicator: Optional function called when the transaction execution is about to be retried due to an
                                OCC conflict or retriable exception.

        :rtype: :py:class:`pyqldb.cursor.buffered_cursor.BufferedCursor`/object
        :return: The return value of the lambda function which could be a
                 :py:class:`pyqldb.cursor.buffered_cursor.BufferedCursor` on the result set of a statement within the
                 lambda.

        :raises IllegalStateError: When the commit digest from commit transaction result does not match.

        :raises SessionClosedError: When this session is closed.

        :raises ClientError: When there is an error communicating with QLDB.
        """
        return self._invoke_on_session(lambda: self._qldb_session.execute_lambda(query_lambda, retry_indicator))

    def list_tables(self):
        """
        Get the list of table names in the ledger.

        :rtype: :py:class:`pyqldb.cursor.buffered_cursor.BufferedCursor`
        :return: Iterable of table names in amazon.ion.simple_types.IonPyText format found in the ledger.

        :raises SessionClosedError: When this session is closed.
        """
        return self._invoke_on_session(self._qldb_session.list_tables)

    def start_transaction(self):
        """
        Start a transaction using an available database session.

        :rtype: :py:class:`pyqldb.transaction.transaction.Transaction`
        :return: A new transaction.

        :raises SessionClosedError: When this session is closed.
        """
        return self._invoke_on_session(self._qldb_session.start_transaction)

    def _throw_if_closed(self):
        """
        Check and throw if this session is closed.
        """
        if self._is_closed:
            logger.error(SessionClosedError())
            raise SessionClosedError

    def _invoke_on_session(self, session_function):
        """
        Handle method calls using the internal QldbSession this object wraps.
        """
        self._throw_if_closed()
        return session_function()
