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

from ..cursor.buffered_cursor import BufferedCursor
from ..cursor.stream_cursor import StreamCursor
from ..errors import ExecuteError, is_invalid_session_exception, is_occ_conflict_exception, is_retriable_exception, \
    is_transaction_expired_exception
from ..execution.executor import Executor
from ..transaction.transaction import Transaction

logger = getLogger(__name__)


class QldbSession:
    """
    The QldbSession is meant for internal use only.
    A class representing a session to a QLDB ledger for interacting with QLDB. A QldbSession is linked to the specified
    ledger in the parent driver of the instance of the QldbSession. In any given QldbSession, only one transaction can
    be active at a time. This object can have only one underlying session to QLDB, and therefore the lifespan of a
    QldbSession is tied to the underlying session, which is not indefinite, and on expiry this QldbSession will become
    invalid, and a new QldbSession needs to be created from the parent driver in order to continue usage.

    When a QldbSession is no longer needed, :py:meth:`pyqldb.session.qldb_session.QldbSession._end_session` should
    be invoked in order to clean up any resources.

    See :py:class:`pyqldb.driver.qldb_driver.QldbDriver` for an example of session lifecycle management,
    allowing the re-use of sessions when possible. There should only be one thread interacting with a session at any
    given time.

    :type session: :py:class:`pyqldb.communication.session_client.SessionClient`
    :param session: The session object representing a communication channel with QLDB.

    :type read_ahead: int
    :param read_ahead: The number of pages to read-ahead and buffer when retrieving results.

    :type executor: :py:class:`concurrent.futures.thread.ThreadPoolExecutor`
    :param executor: The executor to be used by the retrieval thread.

    :type return_session_to_pool: :function
    :param return_session_to_pool: A callback that describes how the session will be returned to the pool.
    """

    def __init__(self, session, read_ahead, executor, return_session_to_pool):
        self._is_alive = True
        self._read_ahead = read_ahead
        self._executor = executor
        self._session = session
        self._return_session_to_pool = return_session_to_pool

    def __enter__(self):
        """
        Context Manager function to support the 'with' statement.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context Manager function to support the 'with' statement.
        """
        self._release()

    @property
    def ledger_name(self):
        """
        The **read-only** ledger name.
        """
        return self._session.ledger_name

    @property
    def session_id(self):
        """
        The **read-only** session ID.
        """
        return self._session.id

    @property
    def session_token(self):
        """
        The **read-only** session token.
        """
        return self._session.token

    def _end_session(self):
        """
        End this session. No-op if already closed.
        """
        if self._is_alive:
            self._is_alive = False
            self._session._close()

    def _execute_lambda(self, query_lambda):
        """
        Implicitly start a transaction, execute the lambda function, and commit the transaction.

        :type query_lambda: function
        :param query_lambda: The lambda function to execute. A lambda function cannot have any side effects as
                             it may be invoked multiple times, and the result cannot be trusted until the transaction is
                             committed.

        :rtype: :py:class:`pyqldb.cursor.buffered_cursor.BufferedCursor`/object
        :return: The return value of the lambda function which could be a
                 :py:class:`pyqldb.cursor.buffered_cursor.BufferedCursor` on the result set of a statement within the
                 lambda.

        :raises ExecuteError: Error containing the context of a failure during execute.
        """
        transaction = None
        transaction_id = None
        try:
            transaction = self._start_transaction()
            result = query_lambda(Executor(transaction))
            if isinstance(result, StreamCursor):
                # If someone accidentally returned a StreamCursor object which would become invalidated by the
                # commit, automatically buffer it to allow them to use the result anyway.
                result = BufferedCursor(result)
            transaction._commit()
            return result
        except Exception as e:
            is_retryable = is_retriable_exception(e)
            is_session_invalid = is_invalid_session_exception(e)

            if is_session_invalid and not is_transaction_expired_exception(e):
                # Underlying session is dead on InvalidSessionException except for transaction expiry.
                self._is_alive = False
            elif not is_occ_conflict_exception(e):
                # OCC does not need session state reset as the transaction is implicitly closed.
                self._no_throw_abort(transaction)

            if transaction is not None:
                transaction_id = transaction.transaction_id
            raise ExecuteError(e, is_retryable, is_session_invalid, transaction_id)

    def _start_transaction(self):
        """
        Start a transaction using an available database session.

        :rtype: :py:class:`pyqldb.transaction.transaction.Transaction`
        :return: A new transaction.
        """
        transaction_id = self._session._start_transaction().get('TransactionId')
        transaction = Transaction(self._session, self._read_ahead, transaction_id, self._executor)
        return transaction

    def _no_throw_abort(self, transaction):
        """
        Send an abort request which will not throw on failure.
        """
        try:
            if transaction is None:
                self._session._abort_transaction()
            else:
                transaction._abort()
        except ClientError as ce:
            self._is_alive = False
            logger.warning('Ignored error aborting transaction during execution: {}'.format(ce))

    def _release(self):
        """
        Return this `QldbSession` to the pool.
        """
        self._return_session_to_pool(self)
