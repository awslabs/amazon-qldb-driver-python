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

from pyqldb.driver.qldb_driver import QldbDriver
from pyqldb.errors import DriverClosedError
from pyqldb.session.qldb_session import QldbSession

DEFAULT_SESSION_NAME = 'qldb-session'
DEFAULT_MAX_CONCURRENT_TRANSACTIONS = 10
DEFAULT_READ_AHEAD = 0
DEFAULT_RETRY_LIMIT = 4
DEFAULT_BACKOFF_BASE = 10
DEFAULT_TIMEOUT_SECONDS = 0.001
EMPTY_STRING = ''
MOCK_CONFIG = Config()
MOCK_LEDGER_NAME = 'QLDB'
MOCK_MESSAGE = 'message'
MOCK_BOTO3_SESSION = Session()
MOCK_LIST_TABLES_RESULT = ['Vehicle', 'Person']


class TestQldbDriver(TestCase):
    @patch('pyqldb.driver.qldb_driver.Queue')
    @patch('pyqldb.driver.qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.qldb_driver.client')
    @patch('pyqldb.driver.qldb_driver.Config.merge')
    def test_constructor_with_valid_config(self, mock_config_merge, mock_client, mock_bounded_semaphore,
                                           mock_atomic_integer, mock_queue):
        mock_queue.return_value = mock_queue
        mock_atomic_integer.return_value = mock_atomic_integer
        mock_bounded_semaphore.return_value = mock_bounded_semaphore
        mock_client.return_value = mock_client
        mock_config_merge.return_value = mock_config_merge
        mock_config_merge.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS

        qldb_driver = QldbDriver(MOCK_LEDGER_NAME, config=MOCK_CONFIG)

        mock_config_merge.assert_called_once()
        mock_client.assert_called_once_with(DEFAULT_SESSION_NAME, aws_access_key_id=None,
                                            aws_secret_access_key=None, aws_session_token=None,
                                            config=mock_config_merge, endpoint_url=None, region_name=None, verify=None)
        self.assertEqual(qldb_driver._ledger_name, MOCK_LEDGER_NAME)
        self.assertEqual(qldb_driver._retry_config.retry_limit, DEFAULT_RETRY_LIMIT)
        self.assertEqual(qldb_driver._retry_config.base, DEFAULT_BACKOFF_BASE)
        self.assertEqual(qldb_driver._read_ahead, DEFAULT_READ_AHEAD)
        self.assertEqual(qldb_driver._pool_permits, mock_bounded_semaphore)
        self.assertEqual(qldb_driver._pool_permits_counter, mock_atomic_integer)
        self.assertEqual(qldb_driver._pool, mock_queue)
        mock_bounded_semaphore.assert_called_once_with(DEFAULT_MAX_CONCURRENT_TRANSACTIONS)
        mock_atomic_integer.assert_called_once_with(DEFAULT_MAX_CONCURRENT_TRANSACTIONS)
        mock_queue.assert_called_once_with()

    @patch('pyqldb.driver.qldb_driver.client')
    def test_constructor_with_invalid_config(self, mock_client):
        mock_client.return_value = mock_client

        self.assertRaises(TypeError, QldbDriver, MOCK_LEDGER_NAME, config=EMPTY_STRING)
        mock_client.assert_not_called()

    @patch('pyqldb.driver.qldb_driver.Queue')
    @patch('pyqldb.driver.qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.qldb_driver.client')
    @patch('pyqldb.driver.qldb_driver.Config.merge')
    def test_default_constructor_with_parameters(self, mock_config_merge, mock_client, mock_bounded_semaphore,
                                                 mock_atomic_integer, mock_queue):
        mock_queue.return_value = mock_queue
        mock_atomic_integer.return_value = mock_atomic_integer
        mock_bounded_semaphore.return_value = mock_bounded_semaphore
        mock_client.return_value = mock_client
        mock_config_merge.return_value = mock_config_merge
        mock_config_merge.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS

        qldb_driver = QldbDriver(MOCK_LEDGER_NAME, region_name=EMPTY_STRING, verify=EMPTY_STRING,
                                 endpoint_url=EMPTY_STRING, aws_access_key_id=EMPTY_STRING,
                                 aws_secret_access_key=EMPTY_STRING, aws_session_token=EMPTY_STRING,
                                 config=MOCK_CONFIG)

        mock_config_merge.assert_called_once()
        mock_client.assert_called_once_with(DEFAULT_SESSION_NAME, region_name=EMPTY_STRING, verify=EMPTY_STRING,
                                            endpoint_url=EMPTY_STRING, aws_access_key_id=EMPTY_STRING,
                                            aws_secret_access_key=EMPTY_STRING, aws_session_token=EMPTY_STRING,
                                            config=mock_config_merge)
        self.assertEqual(qldb_driver._ledger_name, MOCK_LEDGER_NAME)
        self.assertEqual(qldb_driver._retry_config.retry_limit, DEFAULT_RETRY_LIMIT)
        self.assertEqual(qldb_driver._retry_config.base, DEFAULT_BACKOFF_BASE)
        self.assertEqual(qldb_driver._read_ahead, DEFAULT_READ_AHEAD)
        self.assertEqual(qldb_driver._pool_permits, mock_bounded_semaphore)
        self.assertEqual(qldb_driver._pool_permits_counter, mock_atomic_integer)
        self.assertEqual(qldb_driver._pool, mock_queue)
        mock_bounded_semaphore.assert_called_once_with(DEFAULT_MAX_CONCURRENT_TRANSACTIONS)
        mock_atomic_integer.assert_called_once_with(DEFAULT_MAX_CONCURRENT_TRANSACTIONS)
        mock_queue.assert_called_once_with()

    @patch('pyqldb.driver.qldb_driver.Config.merge')
    def test_constructor_with_boto3_session(self, mock_config_merge):
        mock_session = Mock(spec=MOCK_BOTO3_SESSION)
        mock_config_merge.return_value = mock_config_merge
        mock_config_merge.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS

        qldb_driver = QldbDriver(MOCK_LEDGER_NAME, boto3_session=mock_session, config=MOCK_CONFIG)
        mock_session.client.assert_called_once_with(DEFAULT_SESSION_NAME, config=mock_config_merge, endpoint_url=None,
                                                    verify=None)
        self.assertEqual(qldb_driver._client, mock_session.client())

    @patch('pyqldb.driver.qldb_driver.logger.warning')
    @patch('pyqldb.driver.qldb_driver.Config.merge')
    def test_constructor_with_boto3_session_and_parameters_that_may_overwrite(self, mock_config_merge,
                                                                              mock_logger_warning):
        mock_session = Mock(spec=MOCK_BOTO3_SESSION)
        mock_config_merge.return_value = mock_config_merge
        mock_config_merge.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        region_name = 'region_name'
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME, boto3_session=mock_session, config=MOCK_CONFIG,
                                 region_name=region_name)
        mock_session.client.assert_called_once_with(DEFAULT_SESSION_NAME, config=mock_config_merge, endpoint_url=None,
                                                    verify=None)
        self.assertEqual(qldb_driver._client, mock_session.client())
        mock_logger_warning.assert_called_once()

    def test_constructor_with_invalid_boto3_session(self):
        mock_session = Mock()

        self.assertRaises(TypeError, QldbDriver, MOCK_LEDGER_NAME, botocore_session=mock_session)

    @patch('pyqldb.driver.qldb_driver.Queue')
    @patch('pyqldb.driver.qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_constructor_with_max_concurrent_transactions_0(self, mock_client, mock_bounded_semaphore,
                                                            mock_atomic_integer,
                                                            mock_queue):
        mock_queue.return_value = mock_queue
        mock_atomic_integer.return_value = mock_atomic_integer
        mock_bounded_semaphore.return_value = mock_bounded_semaphore
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS

        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)
        self.assertEqual(qldb_driver._ledger_name, MOCK_LEDGER_NAME)
        self.assertEqual(qldb_driver._retry_config.retry_limit, DEFAULT_RETRY_LIMIT)
        self.assertEqual(qldb_driver._retry_config.base, DEFAULT_BACKOFF_BASE)
        self.assertEqual(qldb_driver._read_ahead, DEFAULT_READ_AHEAD)
        self.assertEqual(qldb_driver._pool_permits, mock_bounded_semaphore)
        self.assertEqual(qldb_driver._pool_permits_counter, mock_atomic_integer)
        self.assertEqual(qldb_driver._pool, mock_queue)
        mock_bounded_semaphore.assert_called_once_with(DEFAULT_MAX_CONCURRENT_TRANSACTIONS)
        mock_atomic_integer.assert_called_once_with(DEFAULT_MAX_CONCURRENT_TRANSACTIONS)
        mock_queue.assert_called_once_with()

    @patch('pyqldb.driver.qldb_driver.client')
    def test_constructor_with_negative_max_concurrent_transactions(self, mock_client):
        mock_client.return_value = mock_client
        self.assertRaises(ValueError, QldbDriver, MOCK_LEDGER_NAME, max_concurrent_transactions=-1)

    @patch('pyqldb.driver.qldb_driver.Queue')
    @patch('pyqldb.driver.qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_constructor_with_max_concurrent_transactions_less_than_client_max_concurrent_transactions(self,
                                                                                                       mock_client,
                                                                                                       mock_bounded_semaphore,
                                                                                                       mock_atomic_integer,
                                                                                                       mock_queue):
        mock_queue.return_value = mock_queue
        mock_atomic_integer.return_value = mock_atomic_integer
        mock_bounded_semaphore.return_value = mock_bounded_semaphore
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        new_max_concurrent_transactions = DEFAULT_MAX_CONCURRENT_TRANSACTIONS - 1
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME, max_concurrent_transactions=new_max_concurrent_transactions)

        self.assertEqual(qldb_driver._ledger_name, MOCK_LEDGER_NAME)
        self.assertEqual(qldb_driver._retry_config.retry_limit, DEFAULT_RETRY_LIMIT)
        self.assertEqual(qldb_driver._retry_config.base, DEFAULT_BACKOFF_BASE)
        self.assertEqual(qldb_driver._read_ahead, DEFAULT_READ_AHEAD)
        self.assertEqual(qldb_driver._pool_permits, mock_bounded_semaphore)
        self.assertEqual(qldb_driver._pool_permits_counter, mock_atomic_integer)
        self.assertEqual(qldb_driver._pool, mock_queue)
        mock_bounded_semaphore.assert_called_once_with(new_max_concurrent_transactions)
        mock_atomic_integer.assert_called_once_with(new_max_concurrent_transactions)
        mock_queue.assert_called_once_with()

    @patch('pyqldb.driver.qldb_driver.client')
    def test_constructor_with_max_concurrent_transactions_greater_than_client_max_concurrent_transactions(self,
                                                                                                          mock_client):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        new_max_concurrent_transactions = DEFAULT_MAX_CONCURRENT_TRANSACTIONS + 1
        self.assertRaises(ValueError, QldbDriver, MOCK_LEDGER_NAME,
                          max_concurrent_transactions=new_max_concurrent_transactions)

    @patch('pyqldb.driver.qldb_driver.client')
    def test_constructor_with_default_timeout(self, mock_client):
        mock_client.return_value = mock_client
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)
        self.assertEqual(qldb_driver._timeout, DEFAULT_TIMEOUT_SECONDS)

    @patch('pyqldb.driver.qldb_driver.client')
    def test_constructor_with_read_ahead_0(self, mock_client):
        mock_client.return_value = mock_client
        driver = QldbDriver(MOCK_LEDGER_NAME, read_ahead=0)

        self.assertEqual(driver._read_ahead, 0)

    @patch('pyqldb.driver.qldb_driver.client')
    def test_constructor_with_read_ahead_1(self, mock_client):
        mock_client.return_value = mock_client
        self.assertRaises(ValueError, QldbDriver, MOCK_LEDGER_NAME, read_ahead=1)

    @patch('pyqldb.driver.qldb_driver.client')
    def test_constructor_with_read_ahead_2(self, mock_client):
        mock_client.return_value = mock_client
        driver = QldbDriver(MOCK_LEDGER_NAME, read_ahead=2)
        self.assertEqual(driver._read_ahead, 2)

    @patch('pyqldb.driver.qldb_driver.QldbDriver.close')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_context_manager(self, mock_client, mock_close):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS

        with QldbDriver(MOCK_LEDGER_NAME):
            pass

        mock_close.assert_called_once_with()

    @patch('pyqldb.driver.qldb_driver.SessionClient')
    @patch('pyqldb.driver.qldb_driver.QldbDriver.close')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_context_manager_with_invalid_session_error(self, mock_client, mock_close, mock_session_client):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS

        mock_invalid_session_error_message = {'Error': {'Code': 'InvalidSessionException',
                                                        'Message': MOCK_MESSAGE}}
        mock_invalid_session_error = ClientError(mock_invalid_session_error_message, MOCK_MESSAGE)
        mock_session_client._start_session.side_effect = mock_invalid_session_error

        with self.assertRaises(ClientError):
            with QldbDriver(MOCK_LEDGER_NAME) as qldb_driver:
                qldb_driver._create_new_session()

        mock_close.assert_called_once_with()

    @patch('pyqldb.session.qldb_session.QldbSession')
    @patch('pyqldb.session.qldb_session.QldbSession')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_close(self, mock_client, mock_qldb_session1, mock_qldb_session2):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)
        qldb_driver._pool = Queue()
        qldb_driver._pool.put(mock_qldb_session1)
        qldb_driver._pool.put(mock_qldb_session2)

        qldb_driver.close()
        mock_qldb_session1._end_session.assert_called_once_with()
        mock_qldb_session2._end_session.assert_called_once_with()

    @patch('pyqldb.driver.qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.qldb_driver.QldbDriver._release_session')
    @patch('pyqldb.driver.qldb_driver.QldbSession')
    @patch('pyqldb.driver.qldb_driver.client')
    @patch('pyqldb.communication.session_client.SessionClient._start_session')
    def test_get_session_new_session(self, mock_start_session, mock_client, mock_qldb_session,
                                     mock_release_session, mock_bounded_semaphore, mock_atomic_integer):
        mock_start_session.return_value = mock_start_session
        mock_qldb_session.return_value = mock_qldb_session
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)

        session = qldb_driver._get_session()

        mock_bounded_semaphore().acquire.assert_called_once_with(timeout=DEFAULT_TIMEOUT_SECONDS)
        mock_atomic_integer().decrement.assert_called_once_with()
        mock_qldb_session.assert_called_once_with(mock_start_session, qldb_driver._read_ahead,
                                                  qldb_driver._executor, mock_release_session)
        self.assertEqual(session, mock_qldb_session)

    @patch('pyqldb.driver.qldb_driver.logger.debug')
    @patch('pyqldb.driver.qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.qldb_driver.QldbDriver._release_session')
    @patch('pyqldb.driver.qldb_driver.QldbSession')
    @patch('pyqldb.communication.session_client.SessionClient._start_session')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_get_session_existing_session(self, mock_client, mock_session_start_session, mock_qldb_session,
                                          mock_release_session, mock_bounded_semaphore, mock_atomic_integer,
                                          mock_logger_debug):
        mock_session_start_session.return_value = mock_session_start_session
        mock_qldb_session.return_value = mock_qldb_session
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)
        qldb_driver._pool = Queue()
        qldb_driver._pool.put(mock_qldb_session)

        session = qldb_driver._get_session()
        self.assertEqual(session, mock_qldb_session)
        mock_bounded_semaphore().acquire.assert_called_once_with(timeout=DEFAULT_TIMEOUT_SECONDS)
        mock_atomic_integer().decrement.assert_called_once_with()
        self.assertEqual(mock_logger_debug.call_count, 2)

    @patch('pyqldb.driver.qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.qldb_driver.QldbSession')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_get_session_exception(self, mock_client, mock_qldb_session, mock_bounded_semaphore,
                                   mock_atomic_integer):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)
        mock_qldb_session.side_effect = Exception

        self.assertRaises(Exception, qldb_driver._get_session)
        mock_bounded_semaphore().acquire.assert_called_once_with(timeout=DEFAULT_TIMEOUT_SECONDS)
        mock_atomic_integer().decrement.assert_called_once_with()
        mock_bounded_semaphore().release.assert_called_once_with()
        mock_atomic_integer().increment.assert_called_once_with()

    @patch('pyqldb.driver.qldb_driver.logger.debug')
    @patch('pyqldb.driver.qldb_driver.SessionPoolEmptyError')
    @patch('pyqldb.driver.qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_get_session_session_pool_empty_error(self, mock_client, mock_bounded_semaphore,
                                                  mock_session_pool_empty_error, mock_logger_debug):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        mock_bounded_semaphore().acquire.return_value = False
        mock_session_pool_empty_error.return_value = Exception
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)

        self.assertRaises(Exception, qldb_driver._get_session)
        mock_session_pool_empty_error.assert_called_once_with(DEFAULT_TIMEOUT_SECONDS)
        mock_logger_debug.assert_called_once()

    @patch('pyqldb.driver.qldb_driver.client')
    def test_get_session_when_closed(self, mock_client):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)
        qldb_driver._is_closed = True

        self.assertRaises(DriverClosedError, qldb_driver._get_session)

    @patch('pyqldb.driver.qldb_driver.QldbDriver._release_session')
    @patch('pyqldb.driver.qldb_driver.QldbSession')
    @patch('pyqldb.communication.session_client.SessionClient._start_session')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_create_new_session(self, mock_client, mock_session_start_session, mock_qldb_session, mock_release_session):
        mock_session_start_session.return_value = mock_session_start_session
        mock_qldb_session.return_value = mock_qldb_session
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)
        session = qldb_driver._create_new_session()

        mock_session_start_session.assert_called_once_with(MOCK_LEDGER_NAME, qldb_driver._client)
        mock_qldb_session.assert_called_once_with(mock_session_start_session,
                                                  qldb_driver._read_ahead, qldb_driver._executor, mock_release_session)
        self.assertEqual(session, mock_qldb_session)

    @patch('pyqldb.driver.qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.qldb_driver.Queue')
    @patch('pyqldb.session.qldb_session.QldbSession')
    @patch('pyqldb.driver.qldb_driver.logger.debug')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_release_session_for_active_session(self, mock_client, mock_logger_debug, mock_qldb_session, mock_queue,
                                                mock_bounded_semaphore, mock_atomic_integer):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)
        mock_qldb_session._is_closed = False
        qldb_driver._release_session(mock_qldb_session)

        mock_queue().put.assert_called_once_with(mock_qldb_session)
        mock_bounded_semaphore().release.assert_called_once_with()
        mock_atomic_integer().increment.assert_called_once_with()
        mock_logger_debug.assert_called_once()

    @patch('pyqldb.driver.qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.qldb_driver.Queue')
    @patch('pyqldb.session.qldb_session.QldbSession')
    @patch('pyqldb.driver.qldb_driver.logger.debug')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_release_session_for_closed_session(self, mock_client, mock_logger_debug, mock_qldb_session, mock_queue,
                                                mock_bounded_semaphore, mock_atomic_integer):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)
        mock_qldb_session._is_closed = True
        qldb_driver._release_session(mock_qldb_session)

        mock_queue().put.assert_not_called()
        mock_bounded_semaphore().release.assert_called_once_with()
        mock_atomic_integer().increment.assert_called_once_with()
        mock_logger_debug.assert_called_once()

    @patch('pyqldb.driver.qldb_driver.client')
    def test_get_read_ahead(self, mock_client):
        mock_client.return_value = mock_client
        driver = QldbDriver(MOCK_LEDGER_NAME)
        self.assertEqual(driver.read_ahead, driver._read_ahead)

    @patch('pyqldb.driver.qldb_driver.client')
    @patch('pyqldb.driver.qldb_driver.QldbDriver.execute_lambda')
    def test_list_tables(self, mock_execute_lambda, mock_client):
        mock_client.return_value = mock_client
        mock_execute_lambda.return_value = MOCK_LIST_TABLES_RESULT

        driver = QldbDriver(MOCK_LEDGER_NAME)
        table_names = driver.list_tables()

        count = 0
        for result in table_names:
            self.assertEqual(result, MOCK_LIST_TABLES_RESULT[count])
            count += 1

    @patch('pyqldb.driver.qldb_driver._LambdaExecutionContext')
    @patch('pyqldb.driver.qldb_driver.client')
    @patch('pyqldb.driver.qldb_driver.QldbDriver._get_session')
    def test_execute_lambda(self, mock_get_session, mock_client, mock_lambda_context):
        mock_client.return_value = mock_client
        mock_lambda = Mock()
        mock_session = mock_get_session.return_value.__enter__.return_value
        mock_session._execute_lambda.return_value = MOCK_MESSAGE
        mock_lambda_context.return_value = mock_lambda_context

        driver = QldbDriver(MOCK_LEDGER_NAME)
        result = driver.execute_lambda(mock_lambda)

        mock_get_session.assert_called_once_with()
        mock_session._execute_lambda.assert_called_once_with(mock_lambda, driver._retry_config, mock_lambda_context)
        self.assertEqual(result, MOCK_MESSAGE)

    @patch('pyqldb.driver.qldb_driver.client')
    @patch('pyqldb.driver.qldb_driver.QldbDriver._get_session')
    def test_execute_lambda_with_InvalidSessionException(self, mock_get_session, mock_client):
        """
        The test asserts that if an InvalidSessionException is thrown by session.execute_lambda, the
        driver calls _get_session
        """
        mock_client.return_value = mock_client
        mock_lambda = Mock()
        mock_session = mock_get_session.return_value.__enter__.return_value

        mock_invalid_session_error_message = {'Error': {'Code': 'InvalidSessionException',
                                                        'Message': MOCK_MESSAGE}}
        mock_invalid_session_error = ClientError(mock_invalid_session_error_message, MOCK_MESSAGE)
        mock_session._execute_lambda.side_effect = [mock_invalid_session_error, mock_invalid_session_error,
                                                    MOCK_MESSAGE]
        driver = QldbDriver(MOCK_LEDGER_NAME)

        result = driver.execute_lambda(mock_lambda, mock_lambda)

        self.assertEqual(mock_get_session.call_count, 3)
        self.assertEqual(result, MOCK_MESSAGE)

    @patch('pyqldb.driver.qldb_driver._LambdaExecutionContext.increment_execution_attempt')
    @patch('pyqldb.session.qldb_session.QldbSession._execute_lambda')
    @patch('pyqldb.session.qldb_session.QldbSession._execute_lambda')
    def test_return_session_with_invalid_session_exception(self, execute_lambda_1, execute_lambda_2,
                                                           mock_lambda_context_increment_execution_attempt):
        """
        The test asserts that a bad session is not returned to the pool.
        We add two mock sessions to the pool. mock_session_1._execute_lambda returns an InvalidSessionException
        and mock_session_2._execute_lambda succeeds.
        After executing driver.execute_lambda we assert if the pool has just one session which should be
        mock_session_2.
        """

        mock_lambda = Mock()
        session_1 = Mock()
        session_2 = Mock()
        driver = QldbDriver(MOCK_LEDGER_NAME)
        session_1 = QldbSession(session_1, driver._read_ahead, driver._executor,
                                driver._release_session)
        session_2 = QldbSession(session_2, driver._read_ahead, driver._executor,
                                driver._release_session)
        mock_invalid_session_error_message = {'Error': {'Code': 'InvalidSessionException',
                                                        'Message': MOCK_MESSAGE}}
        mock_invalid_session_error = ClientError(mock_invalid_session_error_message, MOCK_MESSAGE)
        execute_lambda_1.side_effect = mock_invalid_session_error
        session_1._execute_lambda = execute_lambda_1
        session_1._is_closed = True
        execute_lambda_2.return_value = MOCK_MESSAGE
        session_2._execute_lambda = execute_lambda_2
        session_2._is_closed = False
        # adding sessions to the driver pool
        driver._pool.put(session_1)
        driver._pool.put(session_2)

        result = driver.execute_lambda(mock_lambda)

        self.assertEqual(session_1._execute_lambda.call_count, 1)
        self.assertEqual(session_1._is_closed, True)
        self.assertEqual(session_2._execute_lambda.call_count, 1)
        self.assertEqual(session_2._is_closed, False)
        self.assertEqual(driver._pool_permits._value, 10)
        self.assertEqual(driver._pool.qsize(), 1)
        self.assertEqual(session_2, driver._pool.get_nowait())
        self.assertEqual(mock_lambda_context_increment_execution_attempt.call_count, 0)

        self.assertEqual(result, MOCK_MESSAGE)
