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
from unittest.mock import patch, Mock

from botocore.exceptions import ClientError

from pyqldb.session.pooled_qldb_session import PooledQldbSession
from pyqldb.errors import SessionClosedError

MOCK_LEDGER_NAME = 'ledger name'
MOCK_SESSION_TOKEN = 'session token'
MOCK_STATEMENT = 'statement'
MOCK_PARAMETER_1 = 'parameter_1'
MOCK_PARAMETER_2 = 'parameter_2'
MOCK_ERROR_CODE = 500
MOCK_MESSAGE = 'foo'


class TestPooledQldbSession(TestCase):

    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._release_session')
    @patch('pyqldb.session.qldb_session.QldbSession')
    def test_constructor(self, mock_qldb_session, mock_release_session):
        pooled_qldb_session = PooledQldbSession(mock_qldb_session, mock_release_session)

        self.assertEqual(pooled_qldb_session._qldb_session, mock_qldb_session)
        self.assertEqual(pooled_qldb_session._return_session_to_pool, mock_release_session)
        self.assertFalse(pooled_qldb_session._is_closed)

    @patch('pyqldb.session.pooled_qldb_session.PooledQldbSession.close')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._release_session')
    @patch('pyqldb.session.qldb_session.QldbSession')
    def test_context_manager(self, mock_qldb_session, mock_release_session, mock_close):
        with PooledQldbSession(mock_qldb_session, mock_release_session):
            pass
        mock_close.assert_called_once_with()

    @patch('pyqldb.session.pooled_qldb_session.PooledQldbSession.close')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._release_session')
    @patch('pyqldb.session.qldb_session.QldbSession')
    def test_context_manager_with_invalid_session_error(self, mock_qldb_session, mock_release_session, mock_close):
        mock_invalid_session_error_message = {'Error': {'Code': 'InvalidSessionException',
                                                        'Message': MOCK_MESSAGE}}
        mock_invalid_session_error = ClientError(mock_invalid_session_error_message, MOCK_MESSAGE)
        mock_qldb_session.start_transaction.side_effect = mock_invalid_session_error

        with self.assertRaises(ClientError):
            with PooledQldbSession(mock_qldb_session, mock_release_session) as pooled_qldb_session:
                pooled_qldb_session.start_transaction()
        mock_close.assert_called_once_with()

    @patch('pyqldb.session.pooled_qldb_session.PooledQldbSession._invoke_on_session')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._release_session')
    @patch('pyqldb.session.qldb_session.QldbSession')
    def test_get_ledger_name(self, mock_qldb_session, mock_release_session, mock_invoke_on_session):
        mock_invoke_on_session.return_value = MOCK_LEDGER_NAME
        pooled_qldb_session = PooledQldbSession(mock_qldb_session, mock_release_session)

        ledger_name = pooled_qldb_session.ledger_name
        self.assertEqual(ledger_name, MOCK_LEDGER_NAME)
        mock_invoke_on_session.assert_called_once()

    @patch('pyqldb.session.pooled_qldb_session.PooledQldbSession._invoke_on_session')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._release_session')
    @patch('pyqldb.session.qldb_session.QldbSession')
    def test_get_session_token(self, mock_qldb_session, mock_release_session, mock_invoke_on_session):
        mock_invoke_on_session.return_value = MOCK_SESSION_TOKEN
        pooled_qldb_session = PooledQldbSession(mock_qldb_session, mock_release_session)

        session_token = pooled_qldb_session.session_token
        self.assertEqual(session_token, MOCK_SESSION_TOKEN)
        mock_invoke_on_session.assert_called_once()

    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._release_session')
    @patch('pyqldb.session.qldb_session.QldbSession')
    def test_close(self, mock_qldb_session, mock_release_session):
        pooled_qldb_session = PooledQldbSession(mock_qldb_session, mock_release_session)
        pooled_qldb_session.close()

        self.assertTrue(pooled_qldb_session._is_closed)
        mock_release_session.assert_called_once_with(mock_qldb_session)

    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._release_session')
    @patch('pyqldb.session.qldb_session.QldbSession')
    def test_close_when_closed(self, mock_qldb_session, mock_release_session):
        pooled_qldb_session = PooledQldbSession(mock_qldb_session, mock_release_session)
        pooled_qldb_session._is_closed = True
        pooled_qldb_session.close()

        self.assertTrue(pooled_qldb_session._is_closed)
        mock_release_session.assert_not_called()

    @patch('pyqldb.session.pooled_qldb_session.PooledQldbSession._invoke_on_session')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._release_session')
    @patch('pyqldb.session.qldb_session.QldbSession')
    def test_execute_statement(self, mock_qldb_session, mock_release_session, mock_invoke_on_session):
        retry_indicator = Mock()
        mock_invoke_on_session.return_value = mock_invoke_on_session
        pooled_qldb_session = PooledQldbSession(mock_qldb_session, mock_release_session)
        result = pooled_qldb_session.execute_statement(MOCK_STATEMENT, MOCK_PARAMETER_1, MOCK_PARAMETER_2,
                                                       retry_indicator=retry_indicator)
        mock_invoke_on_session.assert_called_once()
        self.assertEqual(result, mock_invoke_on_session)

    @patch('pyqldb.session.pooled_qldb_session.PooledQldbSession._invoke_on_session')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._release_session')
    @patch('pyqldb.session.qldb_session.QldbSession')
    def test_execute_lambda(self, mock_qldb_session, mock_release_session, mock_invoke_on_session):
        retry_indicator = Mock()
        mock_lambda = Mock()
        mock_invoke_on_session.return_value = mock_invoke_on_session
        pooled_qldb_session = PooledQldbSession(mock_qldb_session, mock_release_session)
        result = pooled_qldb_session.execute_lambda(mock_lambda, retry_indicator)
        mock_invoke_on_session.assert_called_once()
        self.assertEqual(result, mock_invoke_on_session)

    @patch('pyqldb.session.pooled_qldb_session.PooledQldbSession._invoke_on_session')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._release_session')
    @patch('pyqldb.session.qldb_session.QldbSession')
    def test_list_tables(self, mock_qldb_session, mock_release_session, mock_invoke_on_session):
        mock_invoke_on_session.return_value = mock_invoke_on_session
        pooled_qldb_session = PooledQldbSession(mock_qldb_session, mock_release_session)
        result = pooled_qldb_session.list_tables()

        mock_invoke_on_session.assert_called_once()
        self.assertEqual(result, mock_invoke_on_session)

    @patch('pyqldb.session.pooled_qldb_session.PooledQldbSession._invoke_on_session')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._release_session')
    @patch('pyqldb.session.qldb_session.QldbSession')
    def test_start_transaction(self, mock_qldb_session, mock_release_session, mock_invoke_on_session):
        mock_invoke_on_session.return_value = mock_invoke_on_session
        pooled_qldb_session = PooledQldbSession(mock_qldb_session, mock_release_session)
        result = pooled_qldb_session.start_transaction()

        mock_invoke_on_session.assert_called_once()
        self.assertEqual(result, mock_invoke_on_session)

    @patch('pyqldb.session.pooled_qldb_session.logger.error')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._release_session')
    @patch('pyqldb.session.qldb_session.QldbSession')
    def test_throw_if_closed_when_closed(self, mock_qldb_session, mock_release_session, mock_logger_error):
        pooled_qldb_session = PooledQldbSession(mock_qldb_session, mock_release_session)
        pooled_qldb_session._is_closed = True

        self.assertRaises(SessionClosedError, pooled_qldb_session._throw_if_closed)
        mock_logger_error.assert_called_once()

    @patch('pyqldb.session.pooled_qldb_session.logger.error')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._release_session')
    @patch('pyqldb.session.qldb_session.QldbSession')
    def test_throw_if_closed_not_closed(self, mock_qldb_session, mock_release_session, mock_logger_error):
        pooled_qldb_session = PooledQldbSession(mock_qldb_session, mock_release_session)
        pooled_qldb_session._is_closed = False

        result = pooled_qldb_session._throw_if_closed()
        self.assertEqual(result, None)
        mock_logger_error.assert_not_called()

    @patch('pyqldb.session.pooled_qldb_session.PooledQldbSession._throw_if_closed')
    @patch('pyqldb.driver.pooled_qldb_driver.PooledQldbDriver._release_session')
    @patch('pyqldb.session.qldb_session.QldbSession')
    def test_invoke_on_session(self, mock_qldb_session, mock_release_session, mock_throw_if_closed):
        pooled_qldb_session = PooledQldbSession(mock_qldb_session, mock_release_session)
        return_val = 'hello'
        mock_lambda = Mock(return_value=return_val)

        result = pooled_qldb_session._invoke_on_session(mock_lambda)
        mock_throw_if_closed.assert_called_once_with()
        mock_lambda.assert_called_once_with()
        self.assertEqual(result, return_val)
