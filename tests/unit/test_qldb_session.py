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

from botocore.exceptions import ClientError
from pyqldb.config.retry_config import RetryConfig
from pyqldb.errors import ExecuteError
from pyqldb.session.qldb_session import QldbSession

from .helper_functions import assert_execute_error

MOCK_ERROR_CODE = '500'
MOCK_ID = 'mock_id'
MOCK_LEDGER_NAME = 'QLDB'
MOCK_MESSAGE = 'foo'
MOCK_RESULT = {'StartSession': {'SessionToken': 'token'}}
MOCK_RETRY_LIMIT = 4
MOCK_READ_AHEAD = 0
MOCK_SLEEP_CAP_MS = 5000
MOCK_SLEEP_BASE_MS = 10
MOCK_TRANSACTION_ID = 'transaction_id'
MOCK_TRANSACTION_RESULT = {'TransactionId': MOCK_TRANSACTION_ID}
MOCK_CLIENT_ERROR_MESSAGE = {'Error': {'Code': MOCK_ERROR_CODE, 'Message': MOCK_MESSAGE}}
MOCK_DEFAULT_RETRY_CONFIG = RetryConfig()
MOCK_RETRY_CONFIG_WITH_1_RETRY = RetryConfig(retry_limit=1)


class TestQldbSession(TestCase):
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_constructor(self, mock_session, mock_executor):
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)

        self.assertEqual(qldb_session._is_alive, True)
        self.assertEqual(qldb_session._read_ahead, MOCK_READ_AHEAD)
        self.assertEqual(qldb_session._session, mock_session)
        self.assertEqual(qldb_session._executor, mock_executor)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_get_ledger_name(self, mock_session, mock_executor):
        mock_session.ledger_name = MOCK_LEDGER_NAME
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)

        self.assertEqual(qldb_session.ledger_name, MOCK_LEDGER_NAME)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_get_session_id(self, mock_session, mock_executor):
        mock_session.id = MOCK_ID
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)

        self.assertEqual(qldb_session.session_id, MOCK_ID)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_get_session_token(self, mock_session, mock_executor):
        mock_session.token = mock_session
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)

        self.assertEqual(qldb_session.session_token, mock_session)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_end_session(self, mock_session, mock_executor):
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)
        qldb_session._is_alive = True
        qldb_session._end_session()

        mock_session._close.assert_called_once_with()
        self.assertFalse(qldb_session._is_alive)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_end_session_twice(self, mock_session, mock_executor):
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)
        qldb_session._is_alive = True
        qldb_session._end_session()
        qldb_session._end_session()

        mock_session._close.assert_called_once_with()
        self.assertFalse(qldb_session._is_alive)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.session.qldb_session.isinstance')
    @patch('pyqldb.session.qldb_session.BufferedCursor')
    @patch('pyqldb.session.qldb_session.StreamCursor')
    @patch('pyqldb.session.qldb_session.Transaction')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.session.qldb_session.QldbSession._start_transaction')
    def test_execute_lambda(self, mock_start_transaction, mock_session, mock_transaction, mock_stream_cursor,
                            mock_buffered_cursor, mock_is_instance, mock_executor):
        mock_start_transaction.return_value = mock_transaction
        mock_transaction.execute_lambda.return_value = MOCK_RESULT
        mock_transaction._commit.return_value = None
        mock_is_instance.return_value = True
        mock_stream_cursor.return_value = mock_stream_cursor
        mock_buffered_cursor.return_value = MOCK_RESULT
        mock_lambda = Mock()
        mock_lambda.return_value = MOCK_RESULT

        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)
        result = qldb_session._execute_lambda(mock_lambda)

        mock_start_transaction.assert_called_once_with()
        mock_lambda.assert_called_once()
        mock_transaction._commit.assert_called_once_with()
        mock_transaction._close_child_cursors.assert_called_once_with()
        mock_is_instance.assert_called_with(MOCK_RESULT, mock_stream_cursor)
        mock_buffered_cursor.assert_called_once_with(MOCK_RESULT)
        self.assertEqual(result, MOCK_RESULT)

    @patch('pyqldb.session.qldb_session.QldbSession._no_throw_abort')
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.session.qldb_session.is_invalid_session_exception')
    @patch('pyqldb.session.qldb_session.is_occ_conflict_exception')
    @patch('pyqldb.session.qldb_session.is_retriable_exception')
    @patch('pyqldb.session.qldb_session.Transaction')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.session.qldb_session.QldbSession._start_transaction')
    def test_execute_lambda_retryable_exception(self, mock_start_transaction, mock_session, mock_transaction,
                                                mock_is_retryable_exception, mock_is_occ_conflict_exception,
                                                mock_is_invalid_session_exception, mock_executor, mock_no_throw_abort):
        ce = ClientError(MOCK_CLIENT_ERROR_MESSAGE, 'message')
        mock_start_transaction.return_value = mock_transaction
        mock_transaction.transaction_id = 'transaction_id'
        mock_is_retryable_exception.return_value = True
        mock_is_invalid_session_exception.return_value = False
        mock_is_occ_conflict_exception.return_value = False

        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)
        mock_lambda = Mock()
        mock_lambda.side_effect = ce

        with self.assertRaises(ExecuteError) as cm:
            qldb_session._execute_lambda(mock_lambda)

        assert_execute_error(self, cm.exception, ce, True, False, mock_transaction.transaction_id)
        mock_no_throw_abort.assert_called_once_with()
        self.assertTrue(qldb_session._is_alive)
        mock_transaction._commit.assert_not_called()
        mock_transaction._close_child_cursors.assert_called_once_with()

    @patch('pyqldb.session.qldb_session.QldbSession._no_throw_abort')
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.session.qldb_session.is_transaction_expired_exception')
    @patch('pyqldb.session.qldb_session.is_invalid_session_exception')
    @patch('pyqldb.session.qldb_session.is_retriable_exception')
    @patch('pyqldb.session.qldb_session.Transaction')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.session.qldb_session.QldbSession._start_transaction')
    def test_execute_lambda_invalid_session_exception(self, mock_start_transaction, mock_session, mock_transaction,
                                                      mock_is_retryable_exception, mock_is_invalid_session_exception,
                                                      mock_is_transaction_expired_exception, mock_executor,
                                                      mock_no_throw_abort):
        ce = ClientError(MOCK_CLIENT_ERROR_MESSAGE, 'message')
        mock_start_transaction.return_value = mock_transaction
        mock_transaction.transaction_id = 'transaction_id'
        mock_is_retryable_exception.return_value = False
        mock_is_invalid_session_exception.return_value = True
        mock_is_transaction_expired_exception.return_value = False

        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)
        mock_lambda = Mock()
        mock_lambda.side_effect = ce

        with self.assertRaises(ExecuteError) as cm:
            qldb_session._execute_lambda(mock_lambda)

        assert_execute_error(self, cm.exception, ce, False, True, mock_transaction.transaction_id)
        mock_no_throw_abort.assert_not_called()
        self.assertFalse(qldb_session._is_alive)
        mock_transaction._commit.assert_not_called()
        mock_transaction._close_child_cursors.assert_called_once_with()

    @patch('pyqldb.session.qldb_session.QldbSession._no_throw_abort')
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.session.qldb_session.is_occ_conflict_exception')
    @patch('pyqldb.session.qldb_session.is_transaction_expired_exception')
    @patch('pyqldb.session.qldb_session.is_invalid_session_exception')
    @patch('pyqldb.session.qldb_session.is_retriable_exception')
    @patch('pyqldb.session.qldb_session.Transaction')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.session.qldb_session.QldbSession._start_transaction')
    def test_execute_lambda_expired_transaction_exception(self, mock_start_transaction, mock_session, mock_transaction,
                                                          mock_is_retryable_exception, mock_is_invalid_session_exception,
                                                          mock_is_transaction_expired_exception,
                                                          mock_is_occ_conflict_exception, mock_executor,
                                                          mock_no_throw_abort):
        ce = ClientError(MOCK_CLIENT_ERROR_MESSAGE, 'message')
        mock_start_transaction.return_value = mock_transaction
        mock_transaction.transaction_id = 'transaction_id'
        mock_is_retryable_exception.return_value = False
        mock_is_invalid_session_exception.return_value = True
        mock_is_transaction_expired_exception.return_value = True
        mock_is_occ_conflict_exception.return_value = False

        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)
        mock_lambda = Mock()
        mock_lambda.side_effect = ce

        with self.assertRaises(ExecuteError) as cm:
            qldb_session._execute_lambda(mock_lambda)

        assert_execute_error(self, cm.exception, ce, False, True, mock_transaction.transaction_id)
        mock_no_throw_abort.assert_called_once_with()
        self.assertTrue(qldb_session._is_alive)
        mock_transaction._commit.assert_not_called()
        mock_transaction._close_child_cursors.assert_called_once_with()

    @patch('pyqldb.session.qldb_session.QldbSession._no_throw_abort')
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.session.qldb_session.is_invalid_session_exception')
    @patch('pyqldb.session.qldb_session.is_retriable_exception')
    @patch('pyqldb.session.qldb_session.is_occ_conflict_exception')
    @patch('pyqldb.session.qldb_session.Transaction')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.session.qldb_session.QldbSession._start_transaction')
    def test_execute_lambda_occ_conflict(self, mock_start_transaction, mock_session, mock_transaction,
                                         mock_is_occ_conflict_exception, mock_is_retryable_exception,
                                         mock_is_invalid_session_exception, mock_executor, mock_no_throw_abort):
        ce = ClientError(MOCK_CLIENT_ERROR_MESSAGE, MOCK_MESSAGE)
        mock_start_transaction.return_value = mock_transaction
        mock_transaction.transaction_id = 'transaction_id'
        mock_is_retryable_exception.return_value = False
        mock_is_invalid_session_exception.return_value = False
        mock_is_occ_conflict_exception.return_value = True

        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)
        mock_lambda = Mock()
        mock_lambda.side_effect = ce

        with self.assertRaises(ExecuteError) as cm:
            qldb_session._execute_lambda(mock_lambda)

        assert_execute_error(self, cm.exception, ce, False, False, mock_transaction.transaction_id)
        mock_no_throw_abort.assert_not_called()
        mock_transaction._commit.assert_not_called()
        mock_transaction._close_child_cursors.assert_called_once_with()

    @patch('pyqldb.session.qldb_session.QldbSession._no_throw_abort')
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.session.qldb_session.is_invalid_session_exception')
    @patch('pyqldb.session.qldb_session.is_retriable_exception')
    @patch('pyqldb.session.qldb_session.is_occ_conflict_exception')
    @patch('pyqldb.session.qldb_session.Transaction')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.session.qldb_session.QldbSession._start_transaction')
    def test_execute_lambda_unknown_exception(self, mock_start_transaction, mock_session, mock_transaction,
                                              mock_is_occ_conflict_exception, mock_is_retryable_exception,
                                              mock_is_invalid_session_exception, mock_executor, mock_no_throw_abort):
        error = KeyError()
        mock_start_transaction.return_value = mock_transaction
        mock_transaction.transaction_id = 'transaction_id'

        mock_is_occ_conflict_exception.return_value = False
        mock_is_retryable_exception.return_value = False
        mock_is_invalid_session_exception.return_value = False

        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)
        mock_lambda = Mock()
        mock_lambda.side_effect = error

        with self.assertRaises(ExecuteError) as cm:
            qldb_session._execute_lambda(mock_lambda)

        assert_execute_error(self, cm.exception, error, False, False, mock_transaction.transaction_id)
        self.assertTrue(qldb_session._is_alive)
        mock_no_throw_abort.assert_called_once_with()
        mock_transaction._commit.assert_not_called()
        mock_transaction._close_child_cursors.assert_called_once_with()


    @patch('pyqldb.session.qldb_session.QldbSession._no_throw_abort')
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.session.qldb_session.is_invalid_session_exception')
    @patch('pyqldb.session.qldb_session.is_retriable_exception')
    @patch('pyqldb.session.qldb_session.is_occ_conflict_exception')
    @patch('pyqldb.session.qldb_session.Transaction')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.session.qldb_session.QldbSession._start_transaction')
    def test_execute_lambda_start_transaction_error(self, mock_start_transaction, mock_session, mock_transaction,
                                                    mock_is_occ_conflict_exception, mock_is_retryable_exception,
                                                    mock_is_invalid_session_exception, mock_executor, mock_no_throw_abort):
        error = KeyError()
        mock_start_transaction.side_effect = error

        mock_is_occ_conflict_exception.return_value = False
        mock_is_retryable_exception.return_value = False
        mock_is_invalid_session_exception.return_value = False

        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)
        mock_lambda = Mock()

        with self.assertRaises(ExecuteError) as cm:
            qldb_session._execute_lambda(mock_lambda)

        assert_execute_error(self, cm.exception, error, False, False, None)
        self.assertTrue(qldb_session._is_alive)
        mock_no_throw_abort.assert_called_once_with()
        mock_transaction._close_child_cursors.assert_not_called()

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.session.qldb_session.Transaction')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_start_transaction(self, mock_session, mock_transaction, mock_executor):
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)
        mock_transaction.return_value = mock_transaction
        mock_session._start_transaction.return_value = MOCK_TRANSACTION_RESULT
        transaction = qldb_session._start_transaction()

        mock_session._start_transaction.assert_called_once_with()
        mock_transaction.assert_called_once_with(qldb_session._session, qldb_session._read_ahead, MOCK_TRANSACTION_ID,
                                                 mock_executor)
        self.assertEqual(transaction, mock_transaction)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_no_throw_abort(self, mock_session, mock_executor):
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)
        qldb_session._no_throw_abort()

        mock_session._abort_transaction.assert_called_once_with()

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_no_throw_abort_transaction_is_none(self, mock_session, mock_executor):
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)
        qldb_session._no_throw_abort()

        mock_session._abort_transaction.assert_called_once_with()

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.session.qldb_session.logger.warning')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_no_throw_abort_client_error(self, mock_session, mock_logger_warning, mock_executor):
        mock_session._abort_transaction.side_effect = ClientError(MOCK_CLIENT_ERROR_MESSAGE, 'message')
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, mock_executor)
        qldb_session._no_throw_abort()

        self.assertFalse(qldb_session._is_alive)
        mock_logger_warning.assert_called_once()
