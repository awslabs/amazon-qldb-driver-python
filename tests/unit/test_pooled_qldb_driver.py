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
from queue import Queue
from unittest import TestCase
from unittest.mock import patch, Mock

from botocore.exceptions import ClientError
from botocore.config import Config
from boto3.session import Session

from pyqldb.driver.pooled_qldb_driver import PooledQldbDriver
from pyqldb.errors import DriverClosedError

DEFAULT_SESSION_NAME = 'qldb-session'
DEFAULT_POOL_LIMIT = 10
DEFAULT_READ_AHEAD = 0
DEFAULT_RETRY_LIMIT = 4
DEFAULT_TIMEOUT_SECONDS = 30
EMPTY_STRING = ''
MOCK_CONFIG = Config()
MOCK_LEDGER_NAME = 'QLDB'
MOCK_MESSAGE = 'message'
MOCK_BOTO3_SESSION = Session()


class TestPooledQldbDriver(TestCase):
    @patch('pyqldb.driver.pooled_qldb_driver.Queue')
    @patch('pyqldb.driver.pooled_qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.pooled_qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.base_qldb_driver.client')
    @patch('pyqldb.driver.base_qldb_driver.Config.merge')
    def test_constructor_with_valid_config(self, mock_config_merge, mock_client, mock_bounded_semaphore,
                                           mock_atomic_integer, mock_queue):
        mock_queue.return_value = mock_queue
        mock_atomic_integer.return_value = mock_atomic_integer
        mock_bounded_semaphore.return_value = mock_bounded_semaphore
        mock_client.return_value = mock_client
        mock_config_merge.return_value = mock_config_merge
        mock_config_merge.max_pool_connections = DEFAULT_POOL_LIMIT

        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME, config=MOCK_CONFIG)

        mock_config_merge.assert_called_once()
        mock_client.assert_called_once_with(DEFAULT_SESSION_NAME, aws_access_key_id=None,
                                            aws_secret_access_key=None, aws_session_token=None,
                                            config=mock_config_merge, endpoint_url=None, region_name=None, verify=None)
        self.assertEqual(pooled_qldb_driver._ledger_name, MOCK_LEDGER_NAME)
        self.assertEqual(pooled_qldb_driver._retry_limit, DEFAULT_RETRY_LIMIT)
        self.assertEqual(pooled_qldb_driver._read_ahead, DEFAULT_READ_AHEAD)
        self.assertEqual(pooled_qldb_driver._pool_permits, mock_bounded_semaphore)
        self.assertEqual(pooled_qldb_driver._pool_permits_counter, mock_atomic_integer)
        self.assertEqual(pooled_qldb_driver._pool, mock_queue)
        mock_bounded_semaphore.assert_called_once_with(DEFAULT_POOL_LIMIT)
        mock_atomic_integer.assert_called_once_with(DEFAULT_POOL_LIMIT)
        mock_queue.assert_called_once_with()

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_invalid_config(self, mock_client):
        mock_client.return_value = mock_client

        self.assertRaises(TypeError, PooledQldbDriver, MOCK_LEDGER_NAME, config=EMPTY_STRING)
        mock_client.assert_not_called()

    @patch('pyqldb.driver.pooled_qldb_driver.Queue')
    @patch('pyqldb.driver.pooled_qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.pooled_qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.base_qldb_driver.client')
    @patch('pyqldb.driver.base_qldb_driver.Config.merge')
    def test_default_constructor_with_parameters(self, mock_config_merge, mock_client, mock_bounded_semaphore,
                                                 mock_atomic_integer, mock_queue):
        mock_queue.return_value = mock_queue
        mock_atomic_integer.return_value = mock_atomic_integer
        mock_bounded_semaphore.return_value = mock_bounded_semaphore
        mock_client.return_value = mock_client
        mock_config_merge.return_value = mock_config_merge
        mock_config_merge.max_pool_connections = DEFAULT_POOL_LIMIT

        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME, region_name=EMPTY_STRING, verify=EMPTY_STRING,
                                              endpoint_url=EMPTY_STRING, aws_access_key_id=EMPTY_STRING,
                                              aws_secret_access_key=EMPTY_STRING, aws_session_token=EMPTY_STRING,
                                              config=MOCK_CONFIG)

        mock_config_merge.assert_called_once()
        mock_client.assert_called_once_with(DEFAULT_SESSION_NAME, region_name=EMPTY_STRING, verify=EMPTY_STRING,
                                            endpoint_url=EMPTY_STRING, aws_access_key_id=EMPTY_STRING,
                                            aws_secret_access_key=EMPTY_STRING, aws_session_token=EMPTY_STRING,
                                            config=mock_config_merge)
        self.assertEqual(pooled_qldb_driver._ledger_name, MOCK_LEDGER_NAME)
        self.assertEqual(pooled_qldb_driver._retry_limit, DEFAULT_RETRY_LIMIT)
        self.assertEqual(pooled_qldb_driver._read_ahead, DEFAULT_READ_AHEAD)
        self.assertEqual(pooled_qldb_driver._pool_permits, mock_bounded_semaphore)
        self.assertEqual(pooled_qldb_driver._pool_permits_counter, mock_atomic_integer)
        self.assertEqual(pooled_qldb_driver._pool, mock_queue)
        mock_bounded_semaphore.assert_called_once_with(DEFAULT_POOL_LIMIT)
        mock_atomic_integer.assert_called_once_with(DEFAULT_POOL_LIMIT)
        mock_queue.assert_called_once_with()

    @patch('pyqldb.driver.base_qldb_driver.Config.merge')
    def test_constructor_with_boto3_session(self, mock_config_merge):
        mock_session = Mock(spec=MOCK_BOTO3_SESSION)
        mock_config_merge.return_value = mock_config_merge
        mock_config_merge.max_pool_connections = DEFAULT_POOL_LIMIT

        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME, boto3_session=mock_session, config=MOCK_CONFIG)
        mock_session.client.assert_called_once_with(DEFAULT_SESSION_NAME, config=mock_config_merge, endpoint_url=None,
                                                    verify=None)
        self.assertEqual(pooled_qldb_driver._client, mock_session.client())

    @patch('pyqldb.driver.base_qldb_driver.logger.warning')
    @patch('pyqldb.driver.base_qldb_driver.Config.merge')
    def test_constructor_with_boto3_session_and_parameters_that_may_overwrite(self, mock_config_merge,
                                                                              mock_logger_warning):
        mock_session = Mock(spec=MOCK_BOTO3_SESSION)
        mock_config_merge.return_value = mock_config_merge
        mock_config_merge.max_pool_connections = DEFAULT_POOL_LIMIT
        region_name = 'region_name'
        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME, boto3_session=mock_session, config=MOCK_CONFIG,
                                              region_name=region_name)
        mock_session.client.assert_called_once_with(DEFAULT_SESSION_NAME, config=mock_config_merge, endpoint_url=None,
                                                    verify=None)
        self.assertEqual(pooled_qldb_driver._client, mock_session.client())
        mock_logger_warning.assert_called_once()

    def test_constructor_with_invalid_boto3_session(self):
        mock_session = Mock()

        self.assertRaises(TypeError, PooledQldbDriver, MOCK_LEDGER_NAME, botocore_session=mock_session)

    @patch('pyqldb.driver.pooled_qldb_driver.Queue')
    @patch('pyqldb.driver.pooled_qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.pooled_qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_pool_limit_0(self, mock_client, mock_bounded_semaphore, mock_atomic_integer,
                                           mock_queue):
        mock_queue.return_value = mock_queue
        mock_atomic_integer.return_value = mock_atomic_integer
        mock_bounded_semaphore.return_value = mock_bounded_semaphore
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_POOL_LIMIT

        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME)
        self.assertEqual(pooled_qldb_driver._ledger_name, MOCK_LEDGER_NAME)
        self.assertEqual(pooled_qldb_driver._retry_limit, DEFAULT_RETRY_LIMIT)
        self.assertEqual(pooled_qldb_driver._read_ahead, DEFAULT_READ_AHEAD)
        self.assertEqual(pooled_qldb_driver._pool_permits, mock_bounded_semaphore)
        self.assertEqual(pooled_qldb_driver._pool_permits_counter, mock_atomic_integer)
        self.assertEqual(pooled_qldb_driver._pool, mock_queue)
        mock_bounded_semaphore.assert_called_once_with(DEFAULT_POOL_LIMIT)
        mock_atomic_integer.assert_called_once_with(DEFAULT_POOL_LIMIT)
        mock_queue.assert_called_once_with()

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_negative_pool_limit(self, mock_client):
        mock_client.return_value = mock_client
        self.assertRaises(ValueError, PooledQldbDriver, MOCK_LEDGER_NAME, pool_limit=-1)

    @patch('pyqldb.driver.pooled_qldb_driver.Queue')
    @patch('pyqldb.driver.pooled_qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.pooled_qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_pool_limit_less_than_client_pool_limit(self, mock_client, mock_bounded_semaphore,
                                                                     mock_atomic_integer, mock_queue):
        mock_queue.return_value = mock_queue
        mock_atomic_integer.return_value = mock_atomic_integer
        mock_bounded_semaphore.return_value = mock_bounded_semaphore
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_POOL_LIMIT
        new_pool_limit = DEFAULT_POOL_LIMIT - 1
        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME, pool_limit=new_pool_limit)

        self.assertEqual(pooled_qldb_driver._ledger_name, MOCK_LEDGER_NAME)
        self.assertEqual(pooled_qldb_driver._retry_limit, DEFAULT_RETRY_LIMIT)
        self.assertEqual(pooled_qldb_driver._read_ahead, DEFAULT_READ_AHEAD)
        self.assertEqual(pooled_qldb_driver._pool_permits, mock_bounded_semaphore)
        self.assertEqual(pooled_qldb_driver._pool_permits_counter, mock_atomic_integer)
        self.assertEqual(pooled_qldb_driver._pool, mock_queue)
        mock_bounded_semaphore.assert_called_once_with(new_pool_limit)
        mock_atomic_integer.assert_called_once_with(new_pool_limit)
        mock_queue.assert_called_once_with()

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_pool_limit_greater_than_client_pool_limit(self, mock_client):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_POOL_LIMIT
        new_pool_limit = DEFAULT_POOL_LIMIT + 1
        self.assertRaises(ValueError, PooledQldbDriver, MOCK_LEDGER_NAME, pool_limit=new_pool_limit)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_default_timeout(self, mock_client):
        mock_client.return_value = mock_client
        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME)
        self.assertEqual(pooled_qldb_driver._timeout, DEFAULT_TIMEOUT_SECONDS)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_new_timeout(self, mock_client):
        mock_client.return_value = mock_client
        new_timeout = 10
        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME, timeout=new_timeout)
        self.assertEqual(pooled_qldb_driver._timeout, new_timeout)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_negative_timeout(self, mock_client):
        mock_client.return_value = mock_client
        self.assertRaises(ValueError, PooledQldbDriver, MOCK_LEDGER_NAME, timeout=-1)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_read_ahead_0(self, mock_client):
        mock_client.return_value = mock_client
        driver = PooledQldbDriver(MOCK_LEDGER_NAME, read_ahead=0)

        self.assertEqual(driver._read_ahead, 0)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_read_ahead_1(self, mock_client):
        mock_client.return_value = mock_client
        self.assertRaises(ValueError, PooledQldbDriver, MOCK_LEDGER_NAME, read_ahead=1)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_read_ahead_2(self, mock_client):
        mock_client.return_value = mock_client
        driver = PooledQldbDriver(MOCK_LEDGER_NAME, read_ahead=2)
        self.assertEqual(driver._read_ahead, 2)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_retry_limit_negative_value(self, mock_client):
        mock_client.return_value = mock_client
        self.assertRaises(ValueError, PooledQldbDriver, MOCK_LEDGER_NAME, retry_limit=-1)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_retry_limit_positive_value(self, mock_client):
        mock_client.return_value = mock_client
        driver = PooledQldbDriver(MOCK_LEDGER_NAME, retry_limit=1)
        self.assertEqual(driver._retry_limit, 1)

    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver.close')
    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_context_manager(self, mock_client, mock_close):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_POOL_LIMIT

        with PooledQldbDriver(MOCK_LEDGER_NAME):
            pass

        mock_close.assert_called_once_with()

    @patch('pyqldb.driver.base_qldb_driver.SessionClient')
    @patch('pyqldb.driver.base_qldb_driver.BaseQldbDriver.close')
    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_context_manager_with_invalid_session_error(self, mock_client, mock_close, mock_session_client):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_POOL_LIMIT

        mock_invalid_session_error_message = {'Error': {'Code': 'InvalidSessionException',
                                                        'Message': MOCK_MESSAGE}}
        mock_invalid_session_error = ClientError(mock_invalid_session_error_message, MOCK_MESSAGE)
        mock_session_client.start_session.side_effect = mock_invalid_session_error

        with self.assertRaises(ClientError):
            with PooledQldbDriver(MOCK_LEDGER_NAME) as pooled_qldb_driver:
                pooled_qldb_driver._create_new_session()

        mock_close.assert_called_once_with()

    @patch('pyqldb.driver.base_qldb_driver.BaseQldbDriver.close')
    @patch('pyqldb.session.qldb_session.QldbSession')
    @patch('pyqldb.session.qldb_session.QldbSession')
    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_close(self, mock_client, mock_qldb_session1, mock_qldb_session2, mock_close):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_POOL_LIMIT
        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME)
        pooled_qldb_driver._pool = Queue()
        pooled_qldb_driver._pool.put(mock_qldb_session1)
        pooled_qldb_driver._pool.put(mock_qldb_session2)

        pooled_qldb_driver.close()
        mock_close.assert_called_once_with()
        mock_qldb_session1.close.assert_called_once_with()
        mock_qldb_session2.close.assert_called_once_with()

    @patch('pyqldb.driver.pooled_qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.pooled_qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._release_session')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._create_new_session')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbSession')
    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_get_session_new_session(self, mock_client, mock_pooled_qldb_session, mock_create_new_session,
                                     mock_release_session, mock_bounded_semaphore, mock_atomic_integer):
        mock_pooled_qldb_session.return_value = mock_pooled_qldb_session
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_POOL_LIMIT
        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME)

        session = pooled_qldb_driver.get_session()
        mock_bounded_semaphore().acquire.assert_called_once_with(timeout=DEFAULT_TIMEOUT_SECONDS)
        mock_atomic_integer().decrement.assert_called_once_with()
        mock_pooled_qldb_session.assert_called_once_with(mock_create_new_session(), mock_release_session)
        self.assertEqual(session, mock_pooled_qldb_session)

    @patch('pyqldb.driver.pooled_qldb_driver.logger.debug')
    @patch('pyqldb.driver.pooled_qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.pooled_qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._release_session')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbSession')
    @patch('pyqldb.session.qldb_session.QldbSession')
    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_get_session_existing_session(self, mock_client, mock_qldb_session, mock_pooled_qldb_session,
                                          mock_release_session, mock_bounded_semaphore, mock_atomic_integer,
                                          mock_logger_debug):
        mock_pooled_qldb_session.return_value = mock_pooled_qldb_session
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_POOL_LIMIT
        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME)
        pooled_qldb_driver._pool = Queue()
        pooled_qldb_driver._pool.put(mock_qldb_session)

        session = pooled_qldb_driver.get_session()
        mock_qldb_session._abort_or_close.assert_called_once_with()
        mock_pooled_qldb_session.assert_called_once_with(mock_qldb_session, mock_release_session)
        self.assertEqual(session, mock_pooled_qldb_session)
        mock_bounded_semaphore().acquire.assert_called_once_with(timeout=DEFAULT_TIMEOUT_SECONDS)
        mock_atomic_integer().decrement.assert_called_once_with()
        self.assertEqual(mock_logger_debug.call_count, 2)

    @patch('pyqldb.driver.pooled_qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.pooled_qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbSession')
    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_get_session_exception(self, mock_client, mock_pooled_qldb_session, mock_bounded_semaphore,
                                   mock_atomic_integer):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_POOL_LIMIT
        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME)
        mock_pooled_qldb_session.side_effect = Exception

        self.assertRaises(Exception, pooled_qldb_driver.get_session)
        mock_bounded_semaphore().acquire.assert_called_once_with(timeout=DEFAULT_TIMEOUT_SECONDS)
        mock_atomic_integer().decrement.assert_called_once_with()
        mock_bounded_semaphore().release.assert_called_once_with()
        mock_atomic_integer().increment.assert_called_once_with()

    @patch('pyqldb.driver.pooled_qldb_driver.logger.debug')
    @patch('pyqldb.driver.pooled_qldb_driver.SessionPoolEmptyError')
    @patch('pyqldb.driver.pooled_qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_get_session_session_pool_empty_error(self, mock_client, mock_bounded_semaphore,
                                                  mock_session_pool_empty_error, mock_logger_debug):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_POOL_LIMIT
        mock_bounded_semaphore().acquire.return_value = False
        mock_session_pool_empty_error.return_value = Exception
        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME)

        self.assertRaises(Exception, pooled_qldb_driver.get_session)
        mock_session_pool_empty_error.assert_called_once_with(DEFAULT_TIMEOUT_SECONDS)
        mock_logger_debug.assert_called_once()

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_get_session_when_closed(self, mock_client):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_POOL_LIMIT
        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME)
        pooled_qldb_driver._is_closed = True

        self.assertRaises(DriverClosedError, pooled_qldb_driver.get_session)

    @patch('pyqldb.driver.pooled_qldb_driver.logger.debug')
    @patch('pyqldb.driver.pooled_qldb_driver.SessionPoolEmptyError')
    @patch('pyqldb.driver.pooled_qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_get_session_session_with_different_timeout(self, mock_client, mock_bounded_semaphore,
                                                        mock_session_pool_empty_error, mock_logger_debug):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_POOL_LIMIT
        mock_bounded_semaphore().acquire.return_value = False
        mock_session_pool_empty_error.return_value = Exception
        new_timeout = 20
        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME, timeout=new_timeout)

        self.assertRaises(Exception, pooled_qldb_driver.get_session)
        mock_bounded_semaphore().acquire.assert_called_once_with(timeout=new_timeout)
        mock_session_pool_empty_error.assert_called_once_with(new_timeout)
        mock_logger_debug.assert_called_once()

    @patch('pyqldb.driver.base_qldb_driver.QldbSession')
    @patch('pyqldb.communication.session_client.SessionClient.start_session')
    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_create_new_session(self, mock_client, mock_session_start_session, mock_qldb_session):
        mock_session_start_session.return_value = mock_session_start_session
        mock_qldb_session.return_value = mock_qldb_session
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_POOL_LIMIT
        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME)
        session = pooled_qldb_driver._create_new_session()

        mock_session_start_session.assert_called_once_with(MOCK_LEDGER_NAME, pooled_qldb_driver._client)
        mock_qldb_session.assert_called_once_with(mock_session_start_session, pooled_qldb_driver._read_ahead,
                                                  pooled_qldb_driver._retry_limit, pooled_qldb_driver._executor)
        self.assertEqual(session, mock_qldb_session)

    @patch('pyqldb.driver.pooled_qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.pooled_qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.pooled_qldb_driver.Queue')
    @patch('pyqldb.session.qldb_session.QldbSession')
    @patch('pyqldb.driver.pooled_qldb_driver.logger.debug')
    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_release_session(self, mock_client, mock_logger_debug, mock_qldb_session, mock_queue,
                             mock_bounded_semaphore, mock_atomic_integer):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_POOL_LIMIT
        pooled_qldb_driver = PooledQldbDriver(MOCK_LEDGER_NAME)
        pooled_qldb_driver._release_session(mock_qldb_session)

        mock_queue().put.assert_called_once_with(mock_qldb_session)
        mock_bounded_semaphore().release.assert_called_once_with()
        mock_atomic_integer().increment.assert_called_once_with()
        mock_logger_debug.assert_called_once()

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_get_read_ahead(self, mock_client):
        mock_client.return_value = mock_client
        driver = PooledQldbDriver(MOCK_LEDGER_NAME)
        self.assertEqual(driver.read_ahead, driver._read_ahead)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_get_retry_limit(self, mock_client):
        mock_client.return_value = mock_client
        driver = PooledQldbDriver(MOCK_LEDGER_NAME)

        self.assertEqual(driver.retry_limit, driver._retry_limit)
