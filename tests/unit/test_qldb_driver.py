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
from unittest import TestCase
from unittest.mock import Mock, patch

from boto3.session import Session
from botocore.config import Config
from botocore.exceptions import ClientError

from pyqldb.driver.qldb_driver import QldbDriver
from pyqldb.errors import DriverClosedError

DEFAULT_SESSION_NAME = 'qldb-session'
DEFAULT_READ_AHEAD = 0
DEFAULT_RETRY_LIMIT = 4
EMPTY_STRING = ''
MOCK_CONFIG = Config()
MOCK_LEDGER_NAME = 'QLDB'
MOCK_MESSAGE = 'message'
MOCK_BOTO3_SESSION = Session()


class TestQldbDriver(TestCase):
    @patch('pyqldb.driver.base_qldb_driver.client')
    @patch('pyqldb.driver.base_qldb_driver.Config.merge')
    def test_constructor_with_valid_config(self, mock_config_merge, mock_client):
        mock_client.return_value = mock_client
        mock_config_merge.return_value = mock_config_merge
        qldb_driver = QldbDriver(MOCK_LEDGER_NAME, config=MOCK_CONFIG)

        self.assertEqual(qldb_driver._ledger_name, MOCK_LEDGER_NAME)
        self.assertEqual(qldb_driver._retry_limit, DEFAULT_RETRY_LIMIT)
        self.assertEqual(qldb_driver._read_ahead, DEFAULT_READ_AHEAD)
        mock_config_merge.assert_called_once()
        mock_client.assert_called_once_with(DEFAULT_SESSION_NAME, aws_access_key_id=None,
                                            aws_secret_access_key=None, aws_session_token=None,
                                            config=mock_config_merge, endpoint_url=None, region_name=None, verify=None)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_invalid_config(self, mock_client):
        mock_client.return_value = mock_client

        self.assertRaises(TypeError, QldbDriver, MOCK_LEDGER_NAME, config=EMPTY_STRING)
        mock_client.assert_not_called()

    @patch('pyqldb.driver.base_qldb_driver.Config.merge')
    def test_constructor_with_boto3_session(self, mock_config_merge):
        mock_session = Mock(spec=MOCK_BOTO3_SESSION)
        mock_config_merge.return_value = mock_config_merge

        qldb_driver = QldbDriver(MOCK_LEDGER_NAME, boto3_session=mock_session, config=MOCK_CONFIG)
        mock_session.client.assert_called_once_with(DEFAULT_SESSION_NAME, config=mock_config_merge, endpoint_url=None,
                                                    verify=None)
        self.assertEqual(qldb_driver._client, mock_session.client())

    @patch('pyqldb.driver.base_qldb_driver.logger.warning')
    @patch('pyqldb.driver.base_qldb_driver.Config.merge')
    def test_constructor_with_boto3_session_and_parameters_that_may_overwrite(self, mock_config_merge,
                                                                              mock_logger_warning):
        mock_session = Mock(spec=MOCK_BOTO3_SESSION)
        mock_config_merge.return_value = mock_config_merge
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

    @patch('pyqldb.driver.base_qldb_driver.client')
    @patch('pyqldb.driver.base_qldb_driver.Config.merge')
    def test_default_constructor_with_parameters(self, mock_config_merge, mock_client):
        mock_client.return_value = mock_client
        mock_config_merge.return_value = mock_config_merge

        qldb_driver = QldbDriver(MOCK_LEDGER_NAME, region_name=EMPTY_STRING, verify=EMPTY_STRING,
                                 endpoint_url=EMPTY_STRING, aws_access_key_id=EMPTY_STRING,
                                 aws_secret_access_key=EMPTY_STRING, aws_session_token=EMPTY_STRING, config=MOCK_CONFIG)

        self.assertEqual(qldb_driver._ledger_name, MOCK_LEDGER_NAME)
        self.assertEqual(qldb_driver._retry_limit, DEFAULT_RETRY_LIMIT)
        self.assertEqual(qldb_driver._read_ahead, DEFAULT_READ_AHEAD)
        mock_config_merge.assert_called_once()
        mock_client.assert_called_once_with(DEFAULT_SESSION_NAME, region_name=EMPTY_STRING, verify=EMPTY_STRING,
                                            endpoint_url=EMPTY_STRING, aws_access_key_id=EMPTY_STRING,
                                            aws_secret_access_key=EMPTY_STRING, aws_session_token=EMPTY_STRING,
                                            config=mock_config_merge)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_read_ahead_0(self, mock_client):
        mock_client.return_value = mock_client
        driver = QldbDriver(MOCK_LEDGER_NAME, read_ahead=0)

        self.assertEqual(driver._read_ahead, 0)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_read_ahead_1(self, mock_client):
        mock_client.return_value = mock_client

        self.assertRaises(ValueError, QldbDriver, MOCK_LEDGER_NAME, read_ahead=1)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_read_ahead_2(self, mock_client):
        mock_client.return_value = mock_client
        driver = QldbDriver(MOCK_LEDGER_NAME, read_ahead=2)

        self.assertEqual(driver._read_ahead, 2)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_retry_limit_negative_value(self, mock_client):
        mock_client.return_value = mock_client

        self.assertRaises(ValueError, QldbDriver, MOCK_LEDGER_NAME, retry_limit=-1)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_constructor_with_retry_limit_positive_value(self, mock_client):
        mock_client.return_value = mock_client
        driver = QldbDriver(MOCK_LEDGER_NAME, retry_limit=1)

        self.assertEqual(driver._retry_limit, 1)

    @patch('pyqldb.driver.base_qldb_driver.BaseQldbDriver.close')
    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_context_manager(self, mock_client, mock_close):
        mock_client.return_value = mock_client

        with QldbDriver(MOCK_LEDGER_NAME):
            pass

        mock_close.assert_called_once_with()

    @patch('pyqldb.driver.base_qldb_driver.SessionClient')
    @patch('pyqldb.driver.base_qldb_driver.BaseQldbDriver.close')
    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_context_manager_with_invalid_session_error(self, mock_client, mock_close, mock_session_client):
        mock_client.return_value = mock_client

        mock_invalid_session_error_message = {'Error': {'Code': 'InvalidSessionException',
                                                        'Message': MOCK_MESSAGE}}
        mock_invalid_session_error = ClientError(mock_invalid_session_error_message, MOCK_MESSAGE)
        mock_session_client.start_session.side_effect = mock_invalid_session_error

        with self.assertRaises(ClientError):
            with QldbDriver(MOCK_LEDGER_NAME) as qldb_driver:
                qldb_driver.get_session()

        mock_close.assert_called_once_with()

    @patch('pyqldb.communication.session_client.SessionClient.start_session')
    @patch('pyqldb.driver.qldb_driver.logger.debug')
    @patch('pyqldb.driver.base_qldb_driver.client')
    @patch('pyqldb.driver.base_qldb_driver.QldbSession')
    def test_get_session(self, mock_qldb_session, mock_client, mock_logger_debug, mock_start_session):
        mock_start_session.return_value = mock_start_session
        mock_logger_debug.return_value = None
        mock_qldb_session.return_value = mock_qldb_session
        mock_client.return_value = mock_client
        driver = QldbDriver(MOCK_LEDGER_NAME)
        session = driver.get_session()

        mock_start_session.assert_called_once_with(driver._ledger_name, driver._client)
        mock_qldb_session.assert_called_once_with(mock_start_session, driver._read_ahead, driver._retry_limit,
                                                  driver._executor)
        self.assertEqual(session, mock_qldb_session)
        mock_logger_debug.assert_called_once()

    @patch('pyqldb.driver.base_qldb_driver.client')
    @patch('pyqldb.driver.base_qldb_driver.QldbSession')
    def test_get_session_when_closed(self, mock_qldb_session, mock_client):
        mock_qldb_session.return_value = mock_qldb_session
        mock_client.return_value = mock_client
        driver = QldbDriver(MOCK_LEDGER_NAME)
        driver._is_closed = True
        self.assertRaises(DriverClosedError, driver.get_session)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_get_read_ahead(self, mock_client):
        mock_client.return_value = mock_client
        driver = QldbDriver(MOCK_LEDGER_NAME)
        self.assertEqual(driver.read_ahead, driver._read_ahead)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_get_retry_limit(self, mock_client):
        mock_client.return_value = mock_client
        driver = QldbDriver(MOCK_LEDGER_NAME)

        self.assertEqual(driver.retry_limit, driver._retry_limit)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_close(self, mock_client):
        mock_client.return_value = mock_client
        driver = QldbDriver(MOCK_LEDGER_NAME)
        driver.close()

        self.assertTrue(driver._is_closed)

    @patch('pyqldb.driver.base_qldb_driver.client')
    def test_close_when_closed(self, mock_client):
        mock_client.return_value = mock_client
        driver = QldbDriver(MOCK_LEDGER_NAME)
        driver.close()
        driver.close()

        self.assertTrue(driver._is_closed)
