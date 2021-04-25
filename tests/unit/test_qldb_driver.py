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
from unittest.mock import call, patch, Mock

from botocore.exceptions import ClientError
from botocore.config import Config
from boto3.session import Session
from pyqldb.config.retry_config import RetryConfig
from pyqldb.driver.qldb_driver import QldbDriver, SERVICE_DESCRIPTION
from pyqldb.errors import DriverClosedError, ExecuteError, SessionPoolEmptyError

from .helper_functions import assert_execute_error

DEFAULT_SESSION_NAME = 'qldb-session'
DEFAULT_MAX_CONCURRENT_TRANSACTIONS = 10
DEFAULT_READ_AHEAD = 0
DEFAULT_RETRY_LIMIT = 4
DEFAULT_BACKOFF_BASE = 10
DEFAULT_TIMEOUT_SECONDS = 0.001
DEFAULT_TRANSACTION_ID = 1
EMPTY_STRING = ''
MOCK_CONFIG = Config(user_agent_extra='user_agent')
MOCK_LEDGER_NAME = 'QLDB'
MOCK_MESSAGE = 'message'
MOCK_BOTO3_SESSION = Session()
MOCK_LIST_TABLES_RESULT = ['Vehicle', 'Person']


class TestQldbDriver(TestCase):
    @patch('pyqldb.driver.qldb_driver.Queue')
    @patch('pyqldb.driver.qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_constructor_with_valid_config(self, mock_client, mock_bounded_semaphore,
                                           mock_atomic_integer, mock_queue):
        mock_queue.return_value = mock_queue
        mock_atomic_integer.return_value = mock_atomic_integer
        mock_bounded_semaphore.return_value = mock_bounded_semaphore
        mock_client.return_value = mock_client

        qldb_driver = QldbDriver(MOCK_LEDGER_NAME, config=MOCK_CONFIG)

        mock_client.assert_called_once_with(DEFAULT_SESSION_NAME, aws_access_key_id=None,
                                            aws_secret_access_key=None, aws_session_token=None,
                                            config=MOCK_CONFIG, endpoint_url=None, region_name=None, verify=None)
        self.assertEqual(qldb_driver._ledger_name, MOCK_LEDGER_NAME)
        self.assertEqual(qldb_driver.retry_limit, DEFAULT_RETRY_LIMIT)
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
    def test_default_constructor_with_parameters(self, mock_client, mock_bounded_semaphore,
                                                 mock_atomic_integer, mock_queue):
        mock_queue.return_value = mock_queue
        mock_atomic_integer.return_value = mock_atomic_integer
        mock_bounded_semaphore.return_value = mock_bounded_semaphore
        mock_client.return_value = mock_client

        qldb_driver = QldbDriver(MOCK_LEDGER_NAME, region_name=EMPTY_STRING, verify=EMPTY_STRING,
                                 endpoint_url=EMPTY_STRING, aws_access_key_id=EMPTY_STRING,
                                 aws_secret_access_key=EMPTY_STRING, aws_session_token=EMPTY_STRING,
                                 config=MOCK_CONFIG)

        mock_client.assert_called_once_with(DEFAULT_SESSION_NAME, region_name=EMPTY_STRING, verify=EMPTY_STRING,
                                            endpoint_url=EMPTY_STRING, aws_access_key_id=EMPTY_STRING,
                                            aws_secret_access_key=EMPTY_STRING, aws_session_token=EMPTY_STRING,
                                            config=MOCK_CONFIG)
        self.assertEqual(qldb_driver._ledger_name, MOCK_LEDGER_NAME)
        self.assertEqual(qldb_driver.retry_limit, DEFAULT_RETRY_LIMIT)
        self.assertEqual(qldb_driver._retry_config.base, DEFAULT_BACKOFF_BASE)
        self.assertEqual(qldb_driver._read_ahead, DEFAULT_READ_AHEAD)
        self.assertEqual(qldb_driver._pool_permits, mock_bounded_semaphore)
        self.assertEqual(qldb_driver._pool_permits_counter, mock_atomic_integer)
        self.assertEqual(qldb_driver._pool, mock_queue)
        mock_bounded_semaphore.assert_called_once_with(DEFAULT_MAX_CONCURRENT_TRANSACTIONS)
        mock_atomic_integer.assert_called_once_with(DEFAULT_MAX_CONCURRENT_TRANSACTIONS)
        mock_queue.assert_called_once_with()

    def test_constructor_with_boto3_session(self):
        mock_session = Mock(spec=MOCK_BOTO3_SESSION)

        qldb_driver = QldbDriver(MOCK_LEDGER_NAME, boto3_session=mock_session, config=MOCK_CONFIG)
        mock_session.client.assert_called_once_with(DEFAULT_SESSION_NAME, config=MOCK_CONFIG, endpoint_url=None,
                                                    verify=None)
        self.assertEqual(qldb_driver._client, mock_session.client())
        self.assertTrue(SERVICE_DESCRIPTION in qldb_driver._config.user_agent_extra)

    @patch('pyqldb.driver.qldb_driver.logger.warning')
    def test_constructor_with_boto3_session_and_parameters_that_may_overwrite(self, mock_logger_warning):
        mock_session = Mock(spec=MOCK_BOTO3_SESSION)
        region_name = 'region_name'
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME, boto3_session=mock_session, config=MOCK_CONFIG,
                                 region_name=region_name)
        mock_session.client.assert_called_once_with(DEFAULT_SESSION_NAME, config=MOCK_CONFIG, endpoint_url=None,
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
        self.assertEqual(qldb_driver.retry_limit, DEFAULT_RETRY_LIMIT)
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
        self.assertEqual(qldb_driver.retry_limit, DEFAULT_RETRY_LIMIT)
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

    @patch('pyqldb.driver.qldb_driver.QldbDriver._create_new_session')
    @patch('pyqldb.driver.qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.qldb_driver.QldbSession')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_get_session_new_session(self, mock_client, mock_qldb_session, mock_bounded_semaphore, mock_atomic_integer,
                                     mock_create_new_session):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        mock_create_new_session.return_value = mock_qldb_session
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)

        session = qldb_driver._get_session(True)

        mock_bounded_semaphore().acquire.assert_called_once_with(timeout=DEFAULT_TIMEOUT_SECONDS)
        mock_atomic_integer().decrement.assert_called_once_with()
        mock_create_new_session.assert_called_once_with()
        self.assertEqual(session, mock_qldb_session)

    @patch('pyqldb.driver.qldb_driver.QldbDriver._create_new_session')
    @patch('pyqldb.driver.qldb_driver.logger.debug')
    @patch('pyqldb.driver.qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.qldb_driver.QldbSession')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_get_session_existing_session(self, mock_client, mock_qldb_session, mock_bounded_semaphore,
                                          mock_atomic_integer, mock_logger_debug, mock_create_new_session):
        mock_qldb_session.return_value = mock_qldb_session
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)
        qldb_driver._pool = Queue()
        qldb_driver._pool.put(mock_qldb_session)

        session = qldb_driver._get_session(False)

        self.assertEqual(session, mock_qldb_session)
        mock_bounded_semaphore().acquire.assert_called_once_with(timeout=DEFAULT_TIMEOUT_SECONDS)
        mock_atomic_integer().decrement.assert_called_once_with()
        self.assertEqual(mock_logger_debug.call_count, 2)
        mock_create_new_session.assert_not_called()

    @patch('pyqldb.driver.qldb_driver.QldbDriver._create_new_session')
    @patch('pyqldb.driver.qldb_driver.logger.debug')
    @patch('pyqldb.driver.qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.qldb_driver.QldbSession')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_get_session_no_session_in_pool(self, mock_client, mock_qldb_session, mock_bounded_semaphore,
                                            mock_atomic_integer, mock_logger_debug, mock_create_new_session):
        mock_create_new_session.return_value = mock_qldb_session
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)
        qldb_driver._pool = Queue()

        session = qldb_driver._get_session(False)

        self.assertEqual(session, mock_qldb_session)
        mock_bounded_semaphore().acquire.assert_called_once_with(timeout=DEFAULT_TIMEOUT_SECONDS)
        mock_atomic_integer().decrement.assert_called_once_with()
        self.assertEqual(mock_logger_debug.call_count, 2)
        mock_create_new_session.assert_called_once_with()

    @patch('pyqldb.driver.qldb_driver.QldbDriver._create_new_session')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_get_session_exception(self, mock_client, mock_create_new_session):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        error = KeyError()
        mock_create_new_session.side_effect = error
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)

        with self.assertRaises(ExecuteError) as cm:
            qldb_driver._get_session(False)

        assert_execute_error(self, cm.exception, error, True, True, None)

    @patch('pyqldb.driver.qldb_driver.logger.debug')
    @patch('pyqldb.driver.qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_get_session_session_pool_empty_error(self, mock_client, mock_bounded_semaphore, mock_logger_debug):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        mock_bounded_semaphore().acquire.return_value = False
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)

        self.assertRaises(SessionPoolEmptyError, qldb_driver._get_session, True)
        mock_logger_debug.assert_called_once()

    @patch('pyqldb.driver.qldb_driver.client')
    def test_get_session_when_closed(self, mock_client):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)
        qldb_driver._is_closed = True

        self.assertRaises(DriverClosedError, qldb_driver._get_session, True)

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
        mock_qldb_session._is_alive = True
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
        mock_qldb_session._is_alive = False
        qldb_driver._release_session(mock_qldb_session)

        mock_queue().put.assert_not_called()
        mock_bounded_semaphore().release.assert_called_once_with()
        mock_atomic_integer().increment.assert_called_once_with()
        mock_logger_debug.assert_called_once()

    @patch('pyqldb.driver.qldb_driver.AtomicInteger')
    @patch('pyqldb.driver.qldb_driver.BoundedSemaphore')
    @patch('pyqldb.driver.qldb_driver.Queue')
    @patch('pyqldb.driver.qldb_driver.logger.debug')
    @patch('pyqldb.driver.qldb_driver.client')
    def test_release_session_for_none_session(self, mock_client, mock_logger_debug, mock_queue, mock_bounded_semaphore,
                                              mock_atomic_integer):
        mock_client.return_value = mock_client
        mock_client.max_pool_connections = DEFAULT_MAX_CONCURRENT_TRANSACTIONS
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME)
        qldb_driver._release_session(None)

        mock_queue().put.assert_not_called()
        mock_bounded_semaphore().release.assert_called_once_with()
        mock_atomic_integer().increment.assert_called_once_with()
        mock_logger_debug.assert_called_once()

    @patch('pyqldb.driver.qldb_driver.client')
    def test_get_read_ahead(self, mock_client):
        mock_client.return_value = mock_client
        driver = QldbDriver(MOCK_LEDGER_NAME)
        self.assertEqual(driver.read_ahead, driver._read_ahead)

    def test_get_retry_limit(self):
        retry_limit = 4
        retry_config = RetryConfig(retry_limit=retry_limit)
        driver = QldbDriver(MOCK_LEDGER_NAME, retry_config=retry_config)
        self.assertEqual(driver.retry_limit, retry_limit)

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

    @patch('pyqldb.driver.qldb_driver.client')
    @patch('pyqldb.driver.qldb_driver.QldbDriver._get_session')
    def test_execute_lambda(self, mock_get_session, mock_client):
        mock_client.return_value = mock_client
        mock_lambda = Mock()
        mock_session = mock_get_session.return_value.__enter__.return_value
        mock_session._execute_lambda.return_value = MOCK_MESSAGE

        driver = QldbDriver(MOCK_LEDGER_NAME)
        result = driver.execute_lambda(mock_lambda)

        mock_get_session.assert_called_once_with(False)
        mock_session._execute_lambda.assert_called_once_with(mock_lambda)
        self.assertEqual(result, MOCK_MESSAGE)

    @patch('pyqldb.driver.qldb_driver.client')
    @patch('pyqldb.driver.qldb_driver.QldbDriver._get_session')
    def test_execute_lambda_non_execute_error(self, mock_get_session, mock_client):
        mock_client.return_value = mock_client
        mock_lambda = Mock()
        mock_session = mock_get_session.return_value.__enter__.return_value

        error = Exception()
        mock_session._execute_lambda.side_effect = error
        driver = QldbDriver(MOCK_LEDGER_NAME)

        self.assertRaises(Exception, driver.execute_lambda, mock_lambda)
        mock_get_session.assert_called_once_with(False)

    @patch('pyqldb.driver.qldb_driver.client')
    @patch('pyqldb.driver.qldb_driver.QldbDriver._get_session')
    def test_execute_lambda_non_retryable_execute_error(self, mock_get_session, mock_client):
        mock_client.return_value = mock_client
        mock_lambda = Mock()
        mock_session = mock_get_session.return_value.__enter__.return_value
        executeError = ExecuteError(Exception(), False, False)
        mock_session._execute_lambda.side_effect = executeError

        driver = QldbDriver(MOCK_LEDGER_NAME)

        self.assertRaises(Exception, driver.execute_lambda, mock_lambda)
        mock_get_session.assert_called_once_with(False)

    @patch('pyqldb.driver.qldb_driver.QldbDriver._retry_sleep')
    @patch('pyqldb.driver.qldb_driver.client')
    @patch('pyqldb.driver.qldb_driver.QldbDriver._get_session')
    def test_execute_lambda_retryable_error_and_under_retry_limit(self, mock_get_session, mock_client, mock_retry_sleep):
        mock_client.return_value = mock_client
        mock_lambda = Mock()
        mock_session = mock_get_session.return_value.__enter__.return_value
        inner_error = Exception()
        retryable_execute_error = ExecuteError(inner_error, True, False, DEFAULT_TRANSACTION_ID)
        mock_session._execute_lambda.side_effect = [retryable_execute_error, MOCK_MESSAGE]

        driver = QldbDriver(MOCK_LEDGER_NAME)
        result = driver.execute_lambda(mock_lambda)

        self.assertEqual(result, MOCK_MESSAGE)

        mock_get_session.assert_has_calls([call(False), call(False)], any_order=True)
        mock_retry_sleep.assert_called_once_with(driver._retry_config, 1, inner_error, DEFAULT_TRANSACTION_ID)
        self.assertEqual(mock_session._execute_lambda.call_count, 2)

    @patch('pyqldb.driver.qldb_driver.QldbDriver._retry_sleep')
    @patch('pyqldb.driver.qldb_driver.client')
    @patch('pyqldb.driver.qldb_driver.QldbDriver._get_session')
    def test_execute_lambda_retryable_error_and_exceed_retry_limit(self, mock_get_session, mock_client, mock_retry_sleep):
        mock_client.return_value = mock_client
        mock_lambda = Mock()
        mock_session = mock_get_session.return_value.__enter__.return_value
        inner_error = Exception()
        retryable_execute_error = ExecuteError(inner_error, True, False, DEFAULT_TRANSACTION_ID)
        mock_session._execute_lambda.side_effect = [retryable_execute_error, retryable_execute_error, retryable_execute_error]

        retryConfig = RetryConfig(retry_limit=2)
        driver = QldbDriver(MOCK_LEDGER_NAME, retry_config=retryConfig)

        self.assertRaises(Exception, driver.execute_lambda, mock_lambda)
        mock_get_session.assert_has_calls([call(False), call(False), call(False)], any_order=True)
        mock_retry_sleep.assert_has_calls([call(driver._retry_config, 1, inner_error, DEFAULT_TRANSACTION_ID),
                                           call(driver._retry_config, 2, inner_error, DEFAULT_TRANSACTION_ID)])
        self.assertEqual(mock_session._execute_lambda.call_count, 3)

    @patch('pyqldb.driver.qldb_driver.QldbDriver._retry_sleep')
    @patch('pyqldb.driver.qldb_driver.client')
    @patch('pyqldb.driver.qldb_driver.QldbDriver._get_session')
    def test_execute_lambda_invalid_session_exception_and_0_retry_limit(self, mock_get_session, mock_client,
                                                                        mock_retry_sleep):
        mock_client.return_value = mock_client
        mock_lambda = Mock()
        mock_session = mock_get_session.return_value.__enter__.return_value
        invalid_session_exception = ExecuteError(Exception(), True, True)
        mock_session._execute_lambda.side_effect = [invalid_session_exception, MOCK_MESSAGE]

        retryConfig = RetryConfig(retry_limit=0)
        driver = QldbDriver(MOCK_LEDGER_NAME, retry_config=retryConfig)
        result = driver.execute_lambda(mock_lambda)

        self.assertEqual(result, MOCK_MESSAGE)
        mock_get_session.assert_has_calls([call(False), call(False)], any_order=True)
        mock_retry_sleep.assert_not_called()
        self.assertEqual(mock_session._execute_lambda.call_count, 2)

    @patch('pyqldb.driver.qldb_driver.client')
    @patch('pyqldb.driver.qldb_driver.QldbDriver._get_session')
    def test_execute_lambda_retryable_error_and_session_is_none(self, mock_get_session, mock_client):
        mock_client.return_value = mock_client
        mock_lambda = Mock()
        retryable_exception = ExecuteError(Exception(), True, True)
        mock_get_session.return_value.__enter__.side_effect = retryable_exception

        retryConfig = RetryConfig(retry_limit=2)
        driver = QldbDriver(MOCK_LEDGER_NAME, retry_config=retryConfig)

        self.assertRaises(Exception, driver.execute_lambda, mock_lambda)
        mock_get_session.assert_has_calls([call(False), call(True)], any_order=True)

    @patch('pyqldb.driver.qldb_driver.client')
    @patch('pyqldb.driver.qldb_driver.QldbDriver._get_session')
    def test_execute_lambda_retryable_error_and_session_is_alive(self, mock_get_session, mock_client):
        mock_client.return_value = mock_client
        mock_lambda = Mock()
        mock_session = mock_get_session.return_value.__enter__.return_value
        retryable_exception = ExecuteError(Exception(), True, True)
        mock_session._execute_lambda.side_effect = retryable_exception

        retryConfig = RetryConfig(retry_limit=2)
        driver = QldbDriver(MOCK_LEDGER_NAME, retry_config=retryConfig)

        self.assertRaises(Exception, driver.execute_lambda, mock_lambda)
        mock_get_session.assert_has_calls([call(False), call(False)], any_order=True)

    @patch('pyqldb.driver.qldb_driver.client')
    @patch('pyqldb.driver.qldb_driver.QldbDriver._get_session')
    def test_execute_lambda_retryable_error_and_session_is_not_alive(self, mock_get_session, mock_client):
        mock_client.return_value = mock_client
        mock_lambda = Mock()
        mock_session = mock_get_session.return_value.__enter__.return_value
        retryable_exception = ExecuteError(Exception(), True, True)
        mock_session._execute_lambda.side_effect = retryable_exception
        mock_session._is_alive = False

        retryConfig = RetryConfig(retry_limit=2)
        driver = QldbDriver(MOCK_LEDGER_NAME, retry_config=retryConfig)

        self.assertRaises(Exception, driver.execute_lambda, mock_lambda)
        mock_get_session.assert_has_calls([call(False), call(True)], any_order=True)
