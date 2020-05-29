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
from unittest.mock import call, Mock, patch, PropertyMock

from amazon.ion.simpleion import loads
from botocore.exceptions import ClientError

from pyqldb.errors import SessionClosedError, LambdaAbortedError, StartTransactionError
from pyqldb.session.qldb_session import QldbSession

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


class TestQldbSession(TestCase):
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_constructor(self, mock_session, mock_executor):
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, MOCK_RETRY_LIMIT, mock_executor)

        self.assertEqual(qldb_session._is_closed, False)
        self.assertEqual(qldb_session._read_ahead, MOCK_READ_AHEAD)
        self.assertEqual(qldb_session._retry_limit, MOCK_RETRY_LIMIT)
        self.assertEqual(qldb_session._session, mock_session)
        self.assertEqual(qldb_session._executor, mock_executor)

    @patch('pyqldb.session.qldb_session.QldbSession.close')
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_context_manager(self, mock_session, mock_executor, mock_close):
        mock_session.ledger_name = MOCK_LEDGER_NAME
        with QldbSession(mock_session, MOCK_READ_AHEAD, MOCK_RETRY_LIMIT, mock_executor):
            pass
        mock_close.assert_called_once_with()

    @patch('pyqldb.session.qldb_session.QldbSession.close')
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_context_manager_with_start_transaction_error(self, mock_session, mock_executor, mock_close):
        mock_invalid_session_error_message = {'Error': {'Code': 'InvalidSessionException',
                                                        'Message': MOCK_MESSAGE}}
        mock_invalid_session_error = ClientError(mock_invalid_session_error_message, MOCK_MESSAGE)

        mock_session.ledger_name = MOCK_LEDGER_NAME
        mock_session.start_transaction.side_effect = mock_invalid_session_error

        with self.assertRaises(StartTransactionError):
            with QldbSession(mock_session, MOCK_READ_AHEAD, MOCK_RETRY_LIMIT, mock_executor) as qldb_session:
                qldb_session.start_transaction()
        mock_close.assert_called_once_with()

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_get_ledger_name(self, mock_session, mock_executor):
        mock_session.ledger_name = MOCK_LEDGER_NAME
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, MOCK_RETRY_LIMIT, mock_executor)

        self.assertEqual(qldb_session.ledger_name, MOCK_LEDGER_NAME)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_get_session_id(self, mock_session, mock_executor):
        mock_session.id = MOCK_ID
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, MOCK_RETRY_LIMIT, mock_executor)

        self.assertEqual(qldb_session.session_id, MOCK_ID)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_get_session_token(self, mock_session, mock_executor):
        mock_session.token = mock_session
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, MOCK_RETRY_LIMIT, mock_executor)

        self.assertEqual(qldb_session.session_token, mock_session)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_close(self, mock_session, mock_executor):
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, MOCK_RETRY_LIMIT, mock_executor)
        qldb_session._is_closed = False
        qldb_session.close()
        self.assertTrue(qldb_session._is_closed)
        mock_session.close.assert_called_once_with()

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_close_twice(self, mock_session, mock_executor):
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, MOCK_RETRY_LIMIT, mock_executor)
        qldb_session._is_closed = False
        qldb_session.close()
        qldb_session.close()

        self.assertTrue(qldb_session._is_closed)
        mock_session.close.assert_called_once_with()

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.session.qldb_session.isinstance')
    @patch('pyqldb.session.qldb_session.BufferedCursor')
    @patch('pyqldb.session.qldb_session.StreamCursor')
    @patch('pyqldb.session.qldb_session.Transaction')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.session.qldb_session.QldbSession.start_transaction')
    def test_execute_lambda(self, mock_start_transaction, mock_session, mock_transaction, mock_stream_cursor,
                            mock_buffered_cursor, mock_is_instance, mock_executor):
        mock_start_transaction.return_value = mock_transaction
        mock_transaction.execute_lambda.return_value = MOCK_RESULT
        mock_transaction.commit.return_value = None
        mock_is_instance.return_value = True
        mock_stream_cursor.return_value = mock_stream_cursor
        mock_buffered_cursor.return_value = MOCK_RESULT

        mock_lambda = Mock()
        mock_lambda.return_value = MOCK_RESULT

        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, MOCK_RETRY_LIMIT, mock_executor)
        result = qldb_session.execute_lambda(mock_lambda, None)

        mock_start_transaction.assert_called_once_with()
        mock_lambda.assert_called_once()
        mock_transaction.commit.assert_called_once_with()
        mock_is_instance.assert_called_with(MOCK_RESULT, mock_stream_cursor)
        mock_buffered_cursor.assert_called_once_with(MOCK_RESULT)
        self.assertEqual(result, MOCK_RESULT)

    @patch('pyqldb.session.qldb_session.QldbSession._no_throw_abort')
    @patch('pyqldb.session.qldb_session.is_occ_conflict_exception')
    @patch('pyqldb.session.qldb_session.is_retriable_exception')
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.session.qldb_session.QldbSession.start_transaction')
    def test_execute_lambda_client_error(self, mock_start_transaction, mock_session, mock_executor,
                                         mock_is_retriable_exception, mock_is_occ_conflict_exception,
                                          mock_no_throw_abort):
        ce = ClientError(MOCK_CLIENT_ERROR_MESSAGE, MOCK_MESSAGE)
        mock_start_transaction.side_effect = ce
        mock_is_retriable_exception.return_value = False
        mock_is_occ_conflict_exception.return_value = False

        mock_lambda = Mock()

        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, MOCK_RETRY_LIMIT, mock_executor)
        self.assertRaises(ClientError, qldb_session.execute_lambda, mock_lambda)
        mock_no_throw_abort.assert_called_once_with(None)

    @patch('pyqldb.session.qldb_session.QldbSession._no_throw_abort')
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.session.qldb_session.logger.warning')
    @patch('pyqldb.session.qldb_session.is_retriable_exception')
    @patch('pyqldb.session.qldb_session.is_occ_conflict_exception')
    @patch('pyqldb.session.qldb_session.Transaction')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.session.qldb_session.QldbSession.start_transaction')
    def test_execute_lambda_occ_conflict(self, mock_start_transaction, mock_session, mock_transaction,
                                         mock_is_occ_conflict_exception, mock_is_retriable_exception,
                                         mock_logger_warning, mock_executor, mock_no_throw_abort):
        ce = ClientError(MOCK_CLIENT_ERROR_MESSAGE, MOCK_MESSAGE)
        mock_start_transaction.return_value = mock_transaction
        mock_is_occ_conflict_exception.return_value = True
        mock_is_retriable_exception.return_value = False
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, MOCK_RETRY_LIMIT, mock_executor)

        mock_lambda = Mock()
        mock_lambda.side_effect = ce
        self.assertRaises(ClientError, qldb_session.execute_lambda, mock_lambda)

        mock_start_transaction.assert_has_calls([call(), call(), call(), call(), call()])
        mock_no_throw_abort.assert_has_calls([call(mock_transaction), call(mock_transaction), call(mock_transaction),
                                              call(mock_transaction), call(mock_transaction)])
        mock_is_occ_conflict_exception.assert_has_calls([call(ce), call(ce), call(ce), call(ce), call(ce)])
        self.assertEqual(mock_lambda.call_count, qldb_session._retry_limit + 1)
        self.assertEqual(mock_logger_warning.call_count, qldb_session._retry_limit + 1)
        mock_transaction.commit.assert_not_called()

    @patch('pyqldb.session.qldb_session.QldbSession._no_throw_abort')
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.session.qldb_session.logger.warning')
    @patch('pyqldb.session.qldb_session.is_occ_conflict_exception')
    @patch('pyqldb.session.qldb_session.is_retriable_exception')
    @patch('pyqldb.session.qldb_session.Transaction')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.session.qldb_session.QldbSession.start_transaction')
    def test_execute_lambda_retriable_exception(self, mock_start_transaction, mock_session, mock_transaction,
                                                mock_is_retriable_exception, mock_is_occ_conflict_exception,
                                                mock_logger_warning, mock_executor,
                                                mock_no_throw_abort):
        ce = ClientError(MOCK_CLIENT_ERROR_MESSAGE, 'message')
        mock_start_transaction.return_value = mock_transaction
        mock_is_retriable_exception.return_value = True
        mock_is_occ_conflict_exception.return_value = False
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, MOCK_RETRY_LIMIT, mock_executor)

        mock_lambda = Mock()
        mock_lambda.side_effect = ce

        self.assertRaises(ClientError, qldb_session.execute_lambda, mock_lambda)

        mock_start_transaction.assert_has_calls([call(), call(), call(), call(), call()])
        mock_no_throw_abort.assert_has_calls([call(mock_transaction), call(mock_transaction), call(mock_transaction),
                                              call(mock_transaction), call(mock_transaction)])
        mock_is_retriable_exception.assert_has_calls([call(ce), call(ce), call(ce), call(ce), call(ce)])
        self.assertEqual(mock_lambda.call_count, qldb_session._retry_limit + 1)
        self.assertEqual(mock_logger_warning.call_count, qldb_session._retry_limit + 1)
        mock_transaction.commit.assert_not_called()

    @patch('pyqldb.session.qldb_session.QldbSession._no_throw_abort')
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.session.qldb_session.logger')
    @patch('pyqldb.session.qldb_session.is_invalid_session_exception')
    @patch('pyqldb.session.qldb_session.Transaction')
    @patch('pyqldb.session.qldb_session.SessionClient')
    @patch('pyqldb.session.qldb_session.QldbSession.start_transaction')
    def test_execute_lambda_invalid_session_exception(self, mock_start_transaction, mock_session, mock_transaction,
                                                      mock_is_invalid_session, mock_logger, mock_executor,
                                                       mock_no_throw_abort):
        mock_client = Mock()
        ce = ClientError(MOCK_CLIENT_ERROR_MESSAGE, 'message')
        mock_start_transaction.return_value = mock_transaction
        type(mock_session).client = PropertyMock(return_value=mock_client)
        type(mock_session).ledger_name = PropertyMock(return_value=MOCK_LEDGER_NAME)
        mock_session.start_session = mock_session
        mock_is_invalid_session.return_value = True
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, 1, mock_executor)

        mock_lambda = Mock()
        mock_lambda.side_effect = ce

        self.assertRaises(ClientError, qldb_session.execute_lambda, mock_lambda)
        mock_start_transaction.assert_called_with()
        mock_no_throw_abort.assert_not_called()
        mock_transaction.commit.assert_not_called()
        self.assertEqual(mock_session.start_session.call_count, 0)
        self.assertEqual(mock_lambda.call_count, 1)
        self.assertEqual(qldb_session._is_closed, True)

    @patch('pyqldb.session.qldb_session.QldbSession._no_throw_abort')
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.session.qldb_session.QldbSession._retry_sleep')
    @patch('pyqldb.session.qldb_session.logger.warning')
    @patch('pyqldb.session.qldb_session.is_retriable_exception')
    @patch('pyqldb.session.qldb_session.is_occ_conflict_exception')
    @patch('pyqldb.session.qldb_session.Transaction')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.session.qldb_session.QldbSession.start_transaction')
    def test_execute_lambda_retry_indicator(self, mock_start_transaction, mock_session, mock_transaction,
                                            mock_is_occ_conflict, mock_is_retriable_exception, mock_logger_warning,
                                            mock_retry_sleep, mock_executor, mock_no_throw_abort):
        ce = ClientError(MOCK_CLIENT_ERROR_MESSAGE, MOCK_MESSAGE)
        mock_start_transaction.return_value = mock_transaction
        mock_is_occ_conflict.return_value = True
        mock_is_retriable_exception.return_value = False
        retry_indicator = Mock()
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, MOCK_RETRY_LIMIT, mock_executor)

        mock_lambda = Mock()
        mock_lambda.side_effect = ce

        self.assertRaises(ClientError, qldb_session.execute_lambda, mock_lambda, retry_indicator)

        mock_start_transaction.assert_has_calls([call(), call(), call(), call(), call()])
        mock_no_throw_abort.assert_has_calls([call(mock_transaction), call(mock_transaction), call(mock_transaction),
                                              call(mock_transaction), call(mock_transaction)])
        mock_is_occ_conflict.assert_has_calls([call(ce), call(ce), call(ce), call(ce), call(ce)])
        self.assertEqual(mock_lambda.call_count, qldb_session._retry_limit + 1)
        self.assertEqual(retry_indicator.call_count, qldb_session._retry_limit)
        self.assertEqual(mock_logger_warning.call_count, qldb_session._retry_limit + 1)
        retry_indicator.assert_has_calls([call(1), call(2), call(3), call(4)])
        mock_retry_sleep.assert_has_calls([call(1), call(2), call(3), call(4)])
        mock_transaction.commit.assert_not_called()

    @patch('pyqldb.session.qldb_session.QldbSession._no_throw_abort')
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.session.qldb_session.is_retriable_exception')
    @patch('pyqldb.session.qldb_session.is_occ_conflict_exception')
    @patch('pyqldb.session.qldb_session.Transaction')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.session.qldb_session.QldbSession.start_transaction')
    def test_execute_lambda_lambda_aborted_error(self, mock_start_transaction, mock_session, mock_transaction,
                                                 mock_is_occ_conflict, mock_is_retriable_exception, mock_executor,
                                                 mock_no_throw_abort):
        mock_start_transaction.return_value = mock_transaction
        mock_is_occ_conflict.return_value = True
        mock_is_retriable_exception.return_value = False
        retry_indicator = Mock()
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, MOCK_RETRY_LIMIT, mock_executor)

        mock_lambda = Mock()
        mock_lambda.side_effect = LambdaAbortedError

        self.assertRaises(LambdaAbortedError, qldb_session.execute_lambda, mock_lambda, retry_indicator)
        mock_no_throw_abort.assert_called_once_with(mock_transaction)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.session.qldb_session.Transaction')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_start_transaction(self, mock_session, mock_transaction, mock_executor):
        qldb_session = QldbSession(mock_session, MOCK_READ_AHEAD, MOCK_RETRY_LIMIT, mock_executor)
        mock_transaction.return_value = mock_transaction
        mock_session.start_transaction.return_value = MOCK_TRANSACTION_RESULT
        transaction = qldb_session.start_transaction()

        mock_session.start_transaction.assert_called_once_with()
        mock_transaction.assert_called_once_with(qldb_session._session, qldb_session._read_ahead, MOCK_TRANSACTION_ID,
                                                 mock_executor)
        self.assertEqual(transaction, mock_transaction)

    @patch('pyqldb.session.qldb_session.random.random')
    @patch('pyqldb.session.qldb_session.sleep')
    def test_retry_sleep(self, mock_sleep, mock_random):
        mock_random.return_value = 1
        attempt_number = 1
        exponential_back_off = min(MOCK_SLEEP_CAP_MS, pow(MOCK_SLEEP_BASE_MS, attempt_number))
        sleep_value = 1 * (exponential_back_off + 1)/1000
        QldbSession._retry_sleep(attempt_number)
        mock_sleep.assert_called_once_with(sleep_value)
