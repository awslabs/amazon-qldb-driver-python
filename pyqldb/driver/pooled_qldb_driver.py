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

from ..errors import DriverClosedError, SessionPoolEmptyError
from ..session.pooled_qldb_session import PooledQldbSession
from ..util.atomic_integer import AtomicInteger
from .base_qldb_driver import BaseQldbDriver

logger = getLogger(__name__)
DEFAULT_TIMEOUT_SECONDS = 30


class PooledQldbDriver(BaseQldbDriver):
    """
    Represents a factory for accessing pooled sessions to a specific ledger within QLDB. This class or
    :py:class:`pyqldb.driver.qldb_driver.QldbDriver` should be the main entry points to any interaction with QLDB.
    :py:meth:`pyqldb.driver.pooled_qldb_driver.PooledQldbDriver.get_session` will create a
    :py:class:`pyqldb.session.pooled_qldb_session.PooledQldbSession` to the specified ledger within QLDB as a
    communication channel. Any acquired sessions must be cleaned up with
    :py:meth:`pyqldb.session.pooled_qldb_session.PooledQldbSession.close` when they are no longer needed in order to
    return the session to the pool. If this is not done, this driver may become unusable if the pool limit is exceeded.

    This factory pools sessions and attempts to return unused but available sessions when getting new sessions. The
    advantage to using this over the non-pooling driver is that the underlying connection that sessions use to
    communicate with QLDB can be recycled, minimizing resource usage by preventing unnecessary connections and reducing
    latency by not making unnecessary requests to start new connections and end reusable, existing, ones.

    The pool does not remove stale sessions until a new session is retrieved. The default pool size is the maximum
    amount of connections the session client allows. :py:meth:`pyqldb.driver.pooled_qldb_driver.PooledQldbDriver.close`
    should be called when this factory is no longer needed in order to clean up resources, ending all sessions in the
    pool.

    :type ledger_name: str
    :param ledger_name: The QLDB ledger name.

    :type retry_limit: int
    :param retry_limit: The number of automatic retries for statement executions using convenience methods on sessions
                        when an OCC conflict or retriable exception occurs. This value must not be negative.

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
    :param config: See [2]. Note that parameter user_agent_extra will be overwritten.

    :type boto3_session: :py:class:`boto3.session.Session`
    :param boto3_session: The boto3 session to create the client with (see [1]). The boto3 session is expected to be
                          configured correctly.

    :type pool_limit: int
    :param pool_limit: The session pool limit. Set to 0 to use the maximum possible amount allowed by the client
                       configuration. See :param config.

    :type timeout: int
    :param timeout: The timeout in seconds while attempting to retrieve a session from the session pool.

    :raises TypeError: When config is not an instance of :py:class:`botocore.config.Config`.
                       When boto3_session is not an instance of :py:class:`boto3.session.Session`.

    :raises ValueError: When `pool_limit` exceeds the limit set by the client.
                        When `pool_limit` or `timeout` are negative.
                        When `read_ahead` or `retry_limit` is not set to the specified allowed values.

    [1]: `Boto3 Session.client Reference <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client>`_.

    [2]: `Botocore Config Reference <https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html>`_.
    """
    def __init__(self, ledger_name, retry_limit=4, read_ahead=0, executor=None, region_name=None, verify=None,
                 endpoint_url=None, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None,
                 config=None, boto3_session=None, pool_limit=0, timeout=DEFAULT_TIMEOUT_SECONDS):
        super().__init__(ledger_name, retry_limit, read_ahead, executor, region_name, verify, endpoint_url,
                         aws_access_key_id, aws_secret_access_key, aws_session_token, config, boto3_session)

        if pool_limit < 0:
            raise ValueError('Value for pool_limit cannot be negative.')
        if timeout < 0:
            raise ValueError('Value for timeout cannot be negative.')

        client_pool_limit = self._config.max_pool_connections
        if pool_limit == 0:
            self._pool_limit = client_pool_limit
        else:
            self._pool_limit = pool_limit

        if self._pool_limit > client_pool_limit:
            raise ValueError('The session pool limit given, {}, exceeds the limit set by the client, {}. Please lower '
                             'the limit and retry.'.format(str(self._pool_limit), str(client_pool_limit)))

        self._pool_permits = BoundedSemaphore(self._pool_limit)
        self._pool_permits_counter = AtomicInteger(self._pool_limit)
        self._pool = Queue()
        self._timeout = timeout

    def close(self):
        """
        Close the driver and any sessions in the pool.
        """
        super().close()
        while not self._pool.empty():
            cur_session = self._pool.get_nowait()
            cur_session.close()

    def get_session(self):
        """
        This method will attempt to retrieve an active, existing session, or it will start a new session with QLDB if
        none are available and the session pool limit has not been reached. If the pool limit has been reached, it will
        attempt to retrieve a session from the pool until the timeout is reached.

        :rtype: :py:class:`pyqldb.session.pooled_qldb_session.PooledQldbSession`
        :return: A PooledQldbSession object.

        :raises SessionPoolEmptyError: If the timeout is reached while attempting to retrieve a session.

        :raises DriverClosedError: When this driver is closed.
        """
        if self._is_closed:
            raise DriverClosedError

        logger.debug('Getting session. Current free session count: {}. Current available permit count: {}.'.format(
            self._pool.qsize(), self._pool_permits_counter.value))
        if self._pool_permits.acquire(timeout=self._timeout):
            self._pool_permits_counter.decrement()
            try:
                try:
                    while True:
                        cur_session = self._pool.get_nowait()
                        if cur_session._abort_or_close():
                            logger.debug('Reusing session from pool. Session ID: {}.'.format(cur_session.session_id))
                            return PooledQldbSession(cur_session, self._release_session)
                except Empty:
                    pass

                logger.debug('Creating new pooled session.')
                return PooledQldbSession(self._create_new_session(), self._release_session)
            except Exception as e:
                # If they don't get a session they don't use a permit!
                self._pool_permits.release()
                self._pool_permits_counter.increment()
                raise e
        else:
            raise SessionPoolEmptyError(self._timeout)

    def _release_session(self, session):
        """
        Release a session back into the pool.
        """
        self._pool.put(session)
        self._pool_permits.release()
        self._pool_permits_counter.increment()
        logger.debug('Session returned to pool; size is now: {}'.format(self._pool.qsize()))
