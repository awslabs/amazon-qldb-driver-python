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
import random

from botocore.exceptions import ClientError

from ..errors import is_invalid_session_exception, is_occ_conflict_exception, is_retriable_exception, \
    SessionClosedError, LambdaAbortedError
from ..communication.session_client import SessionClient
from ..cursor.buffered_cursor import BufferedCursor
from ..cursor.stream_cursor import StreamCursor
from ..transaction.transaction import Transaction
from ..session.executor import Executor
from .base_qldb_session import BaseQldbSession


logger = getLogger(__name__)
SLEEP_CAP_MS = 5000
SLEEP_BASE_MS = 10


class QldbSession(BaseQldbSession):
    """
    A class representing a session to a QLDB ledger for interacting with QLDB. A QldbSession is linked to the specified
    ledger in the parent driver of the instance of the QldbSession. In any given QldbSession, only one transaction can
    be active at a time. This object can have only one underlying session to QLDB, and therefore the lifespan of a
    QldbSession is tied to the underlying session, which is not indefinite, and on expiry this QldbSession will become
    invalid, and a new QldbSession needs to be created from the parent driver in order to continue usage.

    When a QldbSession is no longer needed, :py:meth:`pyqldb.session.qldb_session.QldbSession.close` should be invoked
    in order to clean up any resources.

    See :py:class:`pyqldb.driver.pooled_qldb_driver.PooledQldbDriver` for an example of session lifecycle management,
    allowing the re-use of sessions when possible. There should only be one thread interacting with a session at any
    given time.

    There are three methods of execution, ranging from simple to complex; the first two are recommended for inbuilt
    error handling:
     - :py:meth:`pyqldb.session.qldb_session.QldbSession.execute_statement` allows for a single statement to be executed
       within a transaction where the transaction is implicitly created and committed, and any recoverable errors are
       transparently handled.
     - :py:meth:`pyqldb.session.qldb_session.QldbSession.execute_lambda` allows for more complex execution sequences
       where more than one execution can occur, as well as other method calls. The transaction is implicitly created and
       committed, and any recoverable errors are transparently handled.
     - :py:meth:`pyqldb.session.qldb_session.QldbSession.start_transaction` allows for full control over when the
       transaction is committed and leaves the responsibility of OCC conflict handling up to the user. Transactions'
       methods cannot be automatically retried, as the state of the transaction is ambiguous in the case of an
       unexpected error.

    :type session: :py:class:`pyqldb.communication.session_client.SessionClient`
    :param session: The session object representing a communication channel with QLDB.

    :type read_ahead: int
    :param read_ahead: The number of pages to read-ahead and buffer when retrieving results.

    :type retry_limit: int
    :param retry_limit: The limit for retries on execute methods when an OCC conflict or retriable exception occurs.

    :type executor: :py:class:`concurrent.futures.thread.ThreadPoolExecutor`
    :param executor: The executor to be used by the retrieval thread.
    """
    def __init__(self, session, read_ahead, retry_limit, executor):
        self._is_closed = False
        self._read_ahead = read_ahead
        self._retry_limit = retry_limit
        self._session = session
        self._executor = executor

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

    def close(self):
        """
        Close this session. No-op if already closed.
        """
        if not self._is_closed:
            self._is_closed = True
            self._session.close()

    def execute_statement(self, statement, parameters=[], retry_indicator=lambda execution_attempt: None):
        """
        Implicitly start a transaction, execute the statement, and commit the transaction, retrying up to the retry
        limit if an OCC conflict or retriable exception occurs.

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

        :raises IllegalStateError: When the commit digest from commit transaction result does not match.

        :raises SessionClosedError: When this session is closed.

        :raises ClientError: When there is an error communicating with QLDB.
        """
        return self.execute_lambda(lambda executor: executor.execute_statement(statement, parameters), retry_indicator)

    def execute_lambda(self, query_lambda, retry_indicator=lambda execution_attempt: None):
        """
        Implicitly start a transaction, execute the lambda function, and commit the transaction, retrying up to the
        retry limit if an OCC conflict or retriable exception occurs.

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
        self.throw_if_closed()

        execution_attempt = 0
        while True:
            transaction = None
            try:
                transaction = self.start_transaction()
                result = query_lambda(Executor(transaction))
                if isinstance(result, StreamCursor):
                    # If someone accidentally returned a StreamCursor object which would become invalidated by the
                    # commit, automatically buffer it to allow them to use the result anyway.
                    result = BufferedCursor(result)
                transaction.commit()
                return result
            except LambdaAbortedError as lae:
                self._no_throw_abort(transaction)
                raise lae
            except ClientError as ce:
                self._no_throw_abort(transaction)
                if is_invalid_session_exception(ce) or is_occ_conflict_exception(ce) or is_retriable_exception(ce):
                    logger.warning('OCC conflict or retriable exception occurred: {}'.format(ce))
                    if execution_attempt >= self._retry_limit:
                        raise ce
                    if is_invalid_session_exception(ce):
                        logger.info('Creating a new session to QLDB; previous session is no longer valid: {}'.
                                    format(ce))
                        self._session = SessionClient.start_session(self._session.ledger_name, self._session.client)
                else:
                    raise ce

            execution_attempt += 1
            retry_indicator(execution_attempt)
            self._retry_sleep(execution_attempt)

    def list_tables(self):
        """
        Get the list of table names in the ledger.

        :rtype: :py:class:`pyqldb.cursor.buffered_cursor.BufferedCursor`
        :return: Iterable of table names in :py:class:`amazon.ion.simple_types.IonPyText` format found in the ledger.

        :raises SessionClosedError: When this session is closed.
        """
        cursor = self.execute_statement("SELECT VALUE name FROM information_schema.user_tables WHERE status = 'ACTIVE'")
        return cursor

    def start_transaction(self):
        """
        Start a transaction using an available database session.

        :rtype: :py:class:`pyqldb.transaction.transaction.Transaction`
        :return: A new transaction.

        :raises SessionClosedError: When this session is closed.
        """
        self.throw_if_closed()

        transaction_id = self._session.start_transaction()
        transaction = Transaction(self._session, self._read_ahead, transaction_id, self._executor)
        return transaction

    def throw_if_closed(self):
        """
        Check and throw if this session is closed.

        :raises SessionClosedError: When this session is closed.
        """
        if self._is_closed:
            logger.error(SessionClosedError())
            raise SessionClosedError

    def _abort_or_close(self):
        """
        Determine if this session is alive by sending an abort message. This should only be used when the session is
        known to not be in use, otherwise the state will be abandoned.

        :rtype: bool
        :return: True if the session is alive, false otherwise.
        """
        if self._is_closed:
            return False
        try:
            self._session.abort_transaction()
            return True
        except ClientError:
            self._is_closed = True
            return False

    def _no_throw_abort(self, transaction):
        """
        Send an abort request which will not throw on failure.
        """
        try:
            if transaction is None:
                self._session.abort_transaction()
            else:
                transaction.abort()
        except ClientError as ce:
            logger.warning('Ignored error aborting transaction during execution: {}'.format(ce))

    @staticmethod
    def _retry_sleep(attempt_number):
        """
        Sleeps an exponentially increasing amount relative to `attempt_number`.
        """
        jitter_rand = random.random()
        exponential_back_off = min(SLEEP_CAP_MS, pow(SLEEP_BASE_MS, attempt_number))
        sleep((jitter_rand * (exponential_back_off + 1))/1000)
