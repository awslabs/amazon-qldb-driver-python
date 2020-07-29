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
from time import sleep

from botocore.exceptions import ClientError, EndpointConnectionError, ConnectionClosedError, ConnectTimeoutError, \
    ReadTimeoutError

from pyqldb.util.retry import Retry
from ..errors import is_bad_request_exception, is_invalid_session_exception, is_occ_conflict_exception, \
    is_retriable_exception, SessionClosedError, LambdaAbortedError, StartTransactionError
from ..cursor.buffered_cursor import BufferedCursor
from ..cursor.stream_cursor import StreamCursor
from ..execution.executor import Executor
from ..transaction.transaction import Transaction

logger = getLogger(__name__)
SLEEP_CAP_MS = 5000
SLEEP_BASE_MS = 10

RETRYABLE_HTTP_ERRORS = (
    ReadTimeoutError, EndpointConnectionError, ConnectionClosedError, ConnectTimeoutError
)


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
        self._is_closed = False
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

    def _close(self):
        """
        Close this `QldbSession`.
        """
        self._is_closed = True

    def _end_session(self):
        """
        End this session. No-op if already closed.
        """
        if not self._is_closed:
            self._is_closed = True
            self._session._close()

    def _execute_lambda(self, query_lambda, retry_config, context):
        """
        Implicitly start a transaction, execute the lambda function, and commit the transaction, retrying up to the
        retry limit if an OCC conflict or retriable exception occurs.

        :type query_lambda: function
        :param query_lambda: The lambda function to execute. A lambda function cannot have any side effects as
                             it may be invoked multiple times, and the result cannot be trusted until the transaction is
                             committed.

        :type retry_config: :py:class:`pyqldb.config.retry_config.RetryConfig`
        :param retry_config: Config to specify max number of retries, base and custom backoff strategy for retries.

        :type context: :py:class:`pyqldb.driver.qldb_driver._LambdaExecutionContext`
        :param context: The LambdaExecutionContext instance is used for storing execution context across a lifespan of
        a lambda (including retries due to session expiry).

        :rtype: :py:class:`pyqldb.cursor.buffered_cursor.BufferedCursor`/object
        :return: The return value of the lambda function which could be a
                 :py:class:`pyqldb.cursor.buffered_cursor.BufferedCursor` on the result set of a statement within the
                 lambda.

        :raises IllegalStateError: When the commit digest from commit transaction result does not match.

        :raises ClientError: When there is an error executing against QLDB.

        :raises LambdaAbortedError: If the lambda function calls :py:class:`pyqldb.execution.executor.Executor.abort`.
        """

        while True:
            transaction = None
            error = None
            try:
                transaction = self._start_transaction()
                result = query_lambda(Executor(transaction))
                if isinstance(result, StreamCursor):
                    # If someone accidentally returned a StreamCursor object which would become invalidated by the
                    # commit, automatically buffer it to allow them to use the result anyway.
                    result = BufferedCursor(result)
                transaction._commit()
                return result
            except LambdaAbortedError as lae:
                self._no_throw_abort(transaction)
                raise lae
            except StartTransactionError as ste:
                error = ste.error
                self._no_throw_abort(transaction)
                if context.execution_attempt >= retry_config.retry_limit:
                    # raise wrapped Error
                    raise ste.error
            except RETRYABLE_HTTP_ERRORS as rhe:
                error = rhe
                self._no_throw_abort(transaction)
                if context.execution_attempt >= retry_config.retry_limit:
                    raise rhe
                logger.warning('Retryable HTTP error occurred: {}, retrying transaction'.format(rhe))
            except ClientError as ce:
                error = ce
                if is_invalid_session_exception(ce):
                    self._is_closed = True
                    raise ce
                if not is_occ_conflict_exception(ce):
                    self._no_throw_abort(transaction)
                if is_occ_conflict_exception(ce) or is_retriable_exception(ce):
                    if context.execution_attempt >= retry_config.retry_limit:
                        raise ce
                    logger.warning('OCC conflict or retriable exception occurred: {}'.format(ce))
                else:
                    raise ce
            except Exception as e:
                self._no_throw_abort(transaction)
                raise e

            context.increment_execution_attempt()
            transaction_id = None if transaction is None else transaction.transaction_id
            self._retry_sleep(retry_config, context.execution_attempt, error, transaction_id)

    def _start_transaction(self):
        """
        Start a transaction using an available database session.

        :rtype: :py:class:`pyqldb.transaction.transaction.Transaction`
        :return: A new transaction.

        :raises StartTransactionError: When this session fails to start a new transaction on the session.
        """
        try:
            transaction_id = self._session._start_transaction().get('TransactionId')
            transaction = Transaction(self._session, self._read_ahead, transaction_id, self._executor)
            return transaction
        except ClientError as ce:
            if is_bad_request_exception(ce):
                logger.warning('Error occurred while starting transaction: {}'.format(ce))
                raise StartTransactionError(ce)
            raise ce

    def _throw_if_closed(self):
        """
        Check and throw if this session is closed.
        """
        if self._is_closed:
            logger.error(SessionClosedError())
            raise SessionClosedError

    @staticmethod
    def _retry_sleep(retry_config, execution_attempt, error, transaction_id):

        sleep(Retry.calculate_backoff(retry_config, execution_attempt, error, transaction_id) / 1000)

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
            logger.warning('Ignored error aborting transaction during execution: {}'.format(ce))

    def _release(self):
        """
        Return this `QldbSession` to the pool.
        """
        self._return_session_to_pool(self)
