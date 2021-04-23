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
from queue import Queue, Empty
from logging import getLogger
from threading import BoundedSemaphore
from warnings import warn
from time import sleep

from boto3 import client
from boto3.session import Session
from botocore.config import Config
from pyqldb.config.retry_config import RetryConfig

from .. import __version__
from ..communication.session_client import SessionClient

from ..errors import ExecuteError, DriverClosedError, SessionPoolEmptyError
from ..session.qldb_session import QldbSession
from ..util.atomic_integer import AtomicInteger
from ..util.retry import Retry

POOL_TIMEOUT_SECONDS = 0.001
logger = getLogger(__name__)
SERVICE_DESCRIPTION = 'QLDB Driver for Python v{}'.format(__version__)
SERVICE_NAME = 'qldb-session'
SERVICE_RETRY = {'max_attempts': 0}
DEFAULT_RETRY_CONFIG = RetryConfig()


class QldbDriver:
    """
    Creates a QldbDriver instance that can be used to execute transactions against Amazon QLDB. A single instance of
    the QldbDriver is always attached to one ledger, as specified in the ledgerName parameter.

    :type ledger_name: str
    :param ledger_name: The QLDB ledger name.

    :type retry_config: :py:class:`pyqldb.config.retry_config.RetryConfig`
    :param retry_config: Config to specify max number of retries, base and custom backoff strategy for retries. Will be
                         overridden if a different retry_config
                         is passed to :py:meth:`pyqldb.driver.qldb_driver.QldbDriver.execute_lambda`.

    :type read_ahead: int
    :param read_ahead: The number of read-ahead buffers. Determines the maximum number of statement result pages that
                       can be buffered in memory. This value must be either 0, to disable read-ahead, or a minimum of 2.

    :type executor: :py:class:`concurrent.futures.thread.ThreadPoolExecutor`
    :param executor: A specific, optional, executor to be used by the retrieval thread if read-ahead is enabled.

    :type region_name: str
    :param region_name: See [1].

    :type verify: bool/str
    :param verify: See [1].

    :type endpoint_url: str
    :param endpoint_url: See [1].

    :type aws_access_key_id: str
    :param aws_access_key_id: See [1].

    :type aws_secret_access_key: str
    :param aws_secret_access_key: See [1].

    :type aws_session_token: str
    :param aws_session_token: See [1].

    :type config: :py:class:`botocore.config.Config`
    :param config: See [2]. Note that parameter user_agent_extra will be appended and retries will be overwritten.

    :type boto3_session: :py:class:`boto3.session.Session`
    :param boto3_session: The boto3 session to create the client with (see [1]). The boto3 session is expected to be
                          configured correctly.

    :type max_concurrent_transactions: int
    :param max_concurrent_transactions: Specifies the maximum number of concurrent transactions that can be executed
                                        by the driver. It is required that the property `max_pool_connections`
                                        in :param config be set equal to :param max_concurrent_transactions. Set to 0 to
                                        use the maximum possible amount allowed by the client configuration.
                                        See :param config.

    :raises TypeError: When config is not an instance of :py:class:`botocore.config.Config`.
                       When boto3_session is not an instance of :py:class:`boto3.session.Session`.
                       When retry_config is not an instance of :py:class:`pyqldb.config.retry_config.RetryConfig`.

    :raises ValueError: When `max_concurrent_transactions` exceeds the limit set by the client.
                        When `max_concurrent_transactions` is negative.
                        When `read_ahead` is not set to the specified allowed values.

    [1]: `Boto3 Session.client Reference <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client>`_.

    [2]: `Botocore Config Reference <https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html>`_.
    """

    def __init__(self, ledger_name, retry_config=None, read_ahead=0, executor=None, region_name=None, verify=None,
                 endpoint_url=None, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None,
                 config=None, boto3_session=None, max_concurrent_transactions=0):

        if read_ahead < 2 and read_ahead != 0:
            raise ValueError('Value for read_ahead must be 0 or 2 or greater.')

        self._ledger_name = ledger_name
        self._read_ahead = read_ahead
        self._executor = executor
        self._is_closed = False

        if config is not None:
            if not isinstance(config, Config):
                raise TypeError('config must be of type botocore.config.Config. Found: {}'
                                .format(type(config).__name__))
            self._config = config
            self._config.retries = SERVICE_RETRY
            if self._config.user_agent_extra:
                self._config.user_agent_extra = ' '.join([SERVICE_DESCRIPTION, self._config.user_agent_extra])
            else:
                self._config.user_agent_extra = SERVICE_DESCRIPTION
        else:
            self._config = Config(user_agent_extra=SERVICE_DESCRIPTION, retries=SERVICE_RETRY)

        if retry_config is not None:
            if not isinstance(retry_config, RetryConfig):
                raise TypeError('config must be of type pyqldb.config.retry_config.RetryConfig. Found: {}'
                                .format(type(retry_config).__name__))
            self._retry_config = retry_config
        else:
            self._retry_config = DEFAULT_RETRY_CONFIG

        if boto3_session is not None:
            if not isinstance(boto3_session, Session):
                raise TypeError('boto3_session must be of type boto3.session.Session. Found: {}'
                                .format(type(boto3_session).__name__))

            if region_name is not None or aws_access_key_id is not None or aws_secret_access_key is not None or \
                    aws_session_token is not None:
                logger.warning('Custom parameters were detected while using a specified Boto3 client and will be '
                               'ignored. Please preconfigure the Boto3 client with those parameters instead.')

            self._client = boto3_session.client(SERVICE_NAME, verify=verify, endpoint_url=endpoint_url,
                                                config=self._config)
        else:
            self._client = client(SERVICE_NAME, region_name=region_name, verify=verify, endpoint_url=endpoint_url,
                                  aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                                  aws_session_token=aws_session_token, config=self._config)

        client_pool_limit = self._config.max_pool_connections
        if max_concurrent_transactions == 0:
            self._pool_limit = client_pool_limit
        else:
            self._pool_limit = max_concurrent_transactions

        if self._pool_limit > client_pool_limit:
            raise ValueError('The session pool limit given, {}, exceeds the limit set by the client, {}. Please lower '
                             'the limit and retry.'.format(str(self._pool_limit), str(client_pool_limit)))

        self._pool_permits = BoundedSemaphore(self._pool_limit)
        self._pool_permits_counter = AtomicInteger(self._pool_limit)
        self._pool = Queue()
        self._timeout = POOL_TIMEOUT_SECONDS

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

    def close(self):
        """
        Close the driver and any sessions in the pool.
        """
        self._is_closed = True
        while not self._pool.empty():
            cur_session = self._pool.get_nowait()
            cur_session._end_session()

    def list_tables(self):
        """
        Get the list of table names in the ledger.

        :rtype: :py:class:`pyqldb.cursor.buffered_cursor.BufferedCursor`
        :return: Iterable of table names in :py:class:`amazon.ion.simple_types.IonPyText` format found in the ledger.

        :raises DriverClosedError: When this driver is closed.
        """
        cursor = self.execute_lambda(lambda txn:
                                     txn.execute_statement(
                                         "SELECT VALUE name FROM information_schema.user_tables WHERE status = 'ACTIVE'"))

        return cursor

    def execute_lambda(self, query_lambda, retry_config=None):
        """
        Execute the lambda function against QLDB within a transaction and retrieve the result. It will retry up to the
        retry limit if an OCC conflict or retryable exception occurs.
        This is the primary method to execute a transaction against Amazon QLDB ledger.

        :type query_lambda: function
        :param query_lambda: The lambda function to execute. The function receives an instance of
                             :py:class:`pyqldb.execution.executor.Executor` which can be used to execute statements.
                             The instance of :py:class:`pyqldb.execution.executor.Executor` wraps an implicitly created
                             transaction. The transaction will be implicitly committed when the passed function returns.
                             The lambda function cannot have any side effects as it may be invoked multiple
                             times, and the result cannot be trusted until the transaction is committed.

        :type retry_config: :py:class:`pyqldb.config.retry_config.RetryConfig`
        :param retry_config: Config to specify max number of retries, base and custom backoff strategy for retries.
                             This config overrides the retry config set at driver level for a particular lambda
                             execution.
                             Note that all the values of the driver level retry config will be overwritten by the new
                             config passed here.

        :rtype: :py:class:`pyqldb.cursor.buffered_cursor.BufferedCursor`/object
        :return: The return value of the lambda function which could be a
                 :py:class:`pyqldb.cursor.buffered_cursor.BufferedCursor` on the result set of a statement within the
                 lambda.

        :raises DriverClosedError: When this driver is closed.

        :raises IllegalStateError: When the commit digest from commit transaction result does not match.

        :raises ClientError: When there is an error executing against QLDB.

        :raises LambdaAbortedError: If the lambda function calls :py:class:`pyqldb.execution.executor.Executor.abort`.
        """
        retry_config = self._retry_config if retry_config is None else retry_config

        start_new_session = False
        session = None
        retry_attempt = 1

        while True:
            try:
                with self._get_session(start_new_session) as session:
                    return session._execute_lambda(query_lambda)
            except Exception as e:
                if isinstance(e, ExecuteError):
                    if e.is_retryable is True:
                        # Always retry on the first attempt if failure was caused by a stale session in the pool
                        if retry_attempt == 1 and e.is_invalid_session_exception:
                            retry_attempt += 1
                            continue

                        if retry_attempt > retry_config.retry_limit:
                            raise e.error

                        retry_attempt += 1
                        self._retry_sleep(retry_config, retry_attempt, e.error, e.transaction_id)
                        logger.info('A recoverable error has occurred. Attempting retry #{}'.format(retry_attempt))
                        logger.debug('Error cause: {}'.format(e.error))
                    else:
                        raise e.error
                else:
                    raise e
            finally:
                if session is not None and session._is_alive:
                    start_new_session = False
                else:
                    start_new_session = True

    @property
    def read_ahead(self):
        """
        The number of read-ahead buffers to be made available per `StreamCursor` instantiated by this driver.
        Determines the maximum number of result pages that can be buffered in memory.

        .. seealso:: :py:class:`pyqldb.cursor.stream_cursor.StreamCursor`
        """
        return self._read_ahead

    @property
    def retry_limit(self):
        """
        The number of automatic retries for statement executions using convenience methods on sessions when
        an OCC conflict or retriable exception occurs.
        """
        warn("The retry_limit property in QldbDriver class is deprecated. Please call RetryConfig's retry_limit "
             "property instead.", DeprecationWarning, stacklevel=2)
        return self._retry_config._retry_limit

    def _create_new_session(self):
        """
        Create a new QldbSession object.
        """
        session_client = SessionClient._start_session(self._ledger_name, self._client)
        return QldbSession(session_client, self._read_ahead, self._executor, self._release_session)

    def _release_session(self, session):
        """
        Release a session back into the pool.
        """
        self._pool_permits.release()
        self._pool_permits_counter.increment()
        logger.debug('Number of sessions in pool : {}'.format(self._pool.qsize()))

        if session is not None and session._is_alive:
            self._pool.put(session)

    def _get_session(self, start_new_session):
        """
        This method will attempt to retrieve an active, existing session, or it will start a new session with QLDB if
        none are available and the session pool limit has not been reached. If the pool limit has been reached, it will
        attempt to retrieve a session from the pool until the timeout is reached.

        :type start_new_session: bool
        :param start_new_session: A boolean value to determine whether to start a new session or retrieve a session from
                                   the session pool.

        :rtype: :py:class:`pyqldb.session.qldb_session.QldbSession`
        :return: A QldbSession object.

        :raises ExecuteError: Error containing the context of a failure during start new session.

        :raises SessionPoolEmptyError: If the timeout is reached while attempting to retrieve a session.

        :raises DriverClosedError: When this driver is closed.
        """
        if self._is_closed:
            raise DriverClosedError

        logger.debug('Getting session. Current free session count: {}. Current available permit count: {}.'.format(
            self._pool.qsize(), self._pool_permits_counter.value))

        if self._pool_permits.acquire(timeout=self._timeout):
            self._pool_permits_counter.decrement()

            session = None
            try:
                if not start_new_session:
                    try:
                        session = self._pool.get_nowait()
                        logger.debug('Reusing session from pool. Session ID: {}.'.format(session.session_id))
                        return session
                    except Empty:
                        pass

                if start_new_session or session is None:
                    logger.debug('Creating new session.')
                    return self._create_new_session()
            except Exception as e:
                raise ExecuteError(e, True, True)
        else:
            raise SessionPoolEmptyError(self._timeout)

    @staticmethod
    def _retry_sleep(retry_config, execution_attempt, error, transaction_id):
        sleep(Retry.calculate_backoff(retry_config, execution_attempt, error, transaction_id) / 1000)
