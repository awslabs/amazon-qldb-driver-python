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
from unittest.mock import call, patch

from amazon.ion.simpleion import dumps, loads
from botocore.exceptions import ClientError

from pyqldb.errors import IllegalStateError
from pyqldb.transaction.transaction import Transaction


MOCK_ERROR_CODE = '500'
MOCK_ERROR_MESSAGE = 'foo'
MOCK_CLIENT_ERROR_MESSAGE = {'Error': {'Code': MOCK_ERROR_CODE, 'Message': MOCK_ERROR_MESSAGE}}
MOCK_ID = '123'
MOCK_PARAMETER_1 = loads('a')
MOCK_PARAMETER_2 = loads('b')
INVALID_MOCK_PARAMETER = bytearray(1)
NATIVE_PARAMETER_1 = 1
NATIVE_PARAMETER_2 = True
ION_PARAMETER_1 = loads(dumps(NATIVE_PARAMETER_1))
ION_PARAMETER_2 = loads(dumps(NATIVE_PARAMETER_2))
MOCK_READ_AHEAD = 0
MOCK_STATEMENT = 'SELECT * FROM FOO'
MOCK_FIRST_PAGE_RESULT = {
    'FirstPage': {'Values': [], 'NextPageToken': 'token'},
    'TimingInformation': {'ProcessingTimeMilliseconds': 1},
    'ConsumedIOs': {'ReadIOs': 1, 'WriteIOs': 1}
}


@patch('pyqldb.communication.session_client.SessionClient')
class TestTransaction(TestCase):

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.util.qldb_hash.QldbHash.to_qldb_hash')
    def test_Transaction(self, mock_to_qldb_hash, mock_executor, mock_session):
        mock_to_qldb_hash.return_value = mock_to_qldb_hash
        transaction = Transaction(mock_session, MOCK_READ_AHEAD, MOCK_ID, mock_executor)
        self.assertEqual(transaction._session, mock_session)
        self.assertEqual(transaction._read_ahead, MOCK_READ_AHEAD)
        self.assertEqual(transaction._cursors, [])
        self.assertEqual(transaction._id, MOCK_ID)
        self.assertEqual(transaction._txn_hash, mock_to_qldb_hash)
        self.assertEqual(transaction._executor, mock_executor)
        mock_to_qldb_hash.assert_called_once_with(MOCK_ID)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    def test_get_transaction_id(self, mock_executor, mock_session):
        transaction = Transaction(mock_session, MOCK_READ_AHEAD, MOCK_ID, mock_executor)
        transaction_id = transaction.transaction_id
        self.assertEqual(transaction_id, transaction._id)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.transaction.transaction.Transaction._close_child_cursors')
    def test_abort(self, mock_close_child_cursors, mock_executor, mock_session):
        transaction = Transaction(mock_session, MOCK_READ_AHEAD, MOCK_ID, mock_executor)
        transaction._abort()

        mock_close_child_cursors.assert_called_once_with()
        mock_session._abort_transaction.assert_called_once_with()

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.transaction.transaction.Transaction._close_child_cursors')
    def test_commit(self, mock_close_child_cursors, mock_executor, mock_session):
        transaction = Transaction(mock_session, MOCK_READ_AHEAD, MOCK_ID, mock_executor)
        mock_session._commit_transaction.return_value = {"TransactionId": transaction.transaction_id,
                                                        "CommitDigest": transaction._txn_hash.get_qldb_hash()}
        transaction._commit()

        mock_session._commit_transaction.assert_called_once_with(transaction.transaction_id,
                                                                 transaction._txn_hash.get_qldb_hash())
        mock_close_child_cursors.assert_called_once_with()

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.transaction.transaction.Transaction._close_child_cursors')
    def test_commit_with_non_matching_commit_result(self, mock_close_child_cursors, mock_executor, mock_session):
        transaction = Transaction(mock_session, MOCK_READ_AHEAD, MOCK_ID, mock_executor)
        mock_session._commit_transaction.return_value = {"CommitDigest": 'Non-matching CommitDigest'}

        self.assertRaises(IllegalStateError, transaction._commit)
        mock_close_child_cursors.assert_called_once_with()

    @patch('pyqldb.transaction.transaction.Transaction._close_child_cursors')
    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    def test_commit_client_error(self, mock_executor, mock_close_child_cursors, mock_session):
        transaction = Transaction(mock_session, MOCK_READ_AHEAD, MOCK_ID, mock_executor)
        ce = ClientError(MOCK_CLIENT_ERROR_MESSAGE, MOCK_ERROR_MESSAGE)
        mock_session._commit_transaction.side_effect = ce

        self.assertRaises(ClientError, transaction._commit)
        mock_session._commit_transaction.assert_called_once_with(transaction._id, transaction._txn_hash.get_qldb_hash())
        mock_close_child_cursors.assert_called_once_with()

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.transaction.transaction.Transaction._update_hash')
    @patch('pyqldb.transaction.transaction.StreamCursor')
    def test_execute_statement_read_ahead_0(self, mock_cursor, mock_update_hash, mock_executor, mock_session):
        mock_update_hash.return_value = None
        mock_cursor.return_value = mock_cursor
        mock_session._execute_statement.return_value = MOCK_FIRST_PAGE_RESULT
        transaction = Transaction(mock_session, MOCK_READ_AHEAD, MOCK_ID, mock_executor)
        cursor = transaction._execute_statement(MOCK_STATEMENT)

        mock_session._execute_statement.assert_called_once_with(MOCK_ID, MOCK_STATEMENT, ())
        mock_update_hash.assert_called_once_with(MOCK_STATEMENT, ())
        mock_cursor.assert_called_once_with(MOCK_FIRST_PAGE_RESULT, mock_session, MOCK_ID)
        self.assertEqual(transaction._cursors, [mock_cursor])
        self.assertEqual(cursor, mock_cursor)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.transaction.transaction.Transaction._update_hash')
    @patch('pyqldb.transaction.transaction.ReadAheadCursor')
    def test_execute_statement_read_ahead_2(self, mock_cursor, mock_update_hash, mock_executor, mock_session):
        mock_update_hash.return_value = None
        mock_cursor.return_value = mock_cursor
        mock_session._execute_statement.return_value = MOCK_FIRST_PAGE_RESULT
        transaction = Transaction(mock_session, 2, MOCK_ID, mock_executor)
        cursor = transaction._execute_statement(MOCK_STATEMENT)

        mock_session._execute_statement.assert_called_once_with(MOCK_ID, MOCK_STATEMENT, ())
        mock_update_hash.assert_called_once_with(MOCK_STATEMENT, ())
        mock_cursor.assert_called_once_with(MOCK_FIRST_PAGE_RESULT, mock_session, MOCK_ID, 2, mock_executor)
        self.assertEqual(transaction._cursors, [mock_cursor])
        self.assertEqual(cursor, mock_cursor)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.transaction.transaction.Transaction._update_hash')
    @patch('pyqldb.transaction.transaction.StreamCursor')
    def test_execute_statement_with_parameters(self, mock_cursor, mock_update_hash, mock_executor, mock_session):
        mock_update_hash.return_value = None
        mock_cursor.return_value = mock_cursor
        mock_session._execute_statement.return_value = MOCK_FIRST_PAGE_RESULT
        transaction = Transaction(mock_session, MOCK_READ_AHEAD, MOCK_ID, mock_executor)
        cursor = transaction._execute_statement(MOCK_STATEMENT, MOCK_PARAMETER_1, MOCK_PARAMETER_2)

        mock_session._execute_statement.assert_called_once_with(MOCK_ID, MOCK_STATEMENT, (MOCK_PARAMETER_1,
                                                                                          MOCK_PARAMETER_2))
        mock_update_hash.assert_called_once_with(MOCK_STATEMENT, (MOCK_PARAMETER_1, MOCK_PARAMETER_2))
        mock_cursor.assert_called_once_with(MOCK_FIRST_PAGE_RESULT, mock_session, MOCK_ID)
        self.assertEqual(transaction._cursors, [mock_cursor])
        self.assertEqual(cursor, mock_cursor)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.transaction.transaction.Transaction._update_hash')
    @patch('pyqldb.transaction.transaction.StreamCursor')
    def test_execute_statement_with_native_parameters(self, mock_cursor, mock_update_hash, mock_executor, mock_session):
        mock_update_hash.return_value = None
        mock_cursor.return_value = mock_cursor
        mock_session._execute_statement.return_value = MOCK_FIRST_PAGE_RESULT
        transaction = Transaction(mock_session, MOCK_READ_AHEAD, MOCK_ID, mock_executor)
        cursor = transaction._execute_statement(MOCK_STATEMENT, NATIVE_PARAMETER_1, NATIVE_PARAMETER_2)

        mock_session._execute_statement.assert_called_once_with(MOCK_ID, MOCK_STATEMENT, (ION_PARAMETER_1,
                                                                                          ION_PARAMETER_1))
        mock_update_hash.assert_called_once_with(MOCK_STATEMENT, (ION_PARAMETER_1, ION_PARAMETER_2))
        mock_cursor.assert_called_once_with(MOCK_FIRST_PAGE_RESULT, mock_session, MOCK_ID)
        self.assertEqual(transaction._cursors, [mock_cursor])
        self.assertEqual(cursor, mock_cursor)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    def test_execute_statement_with_invalid_parameters(self, mock_executor, mock_session):
        transaction = Transaction(mock_session, MOCK_READ_AHEAD, MOCK_ID, mock_executor)
        
        with self.assertRaises(TypeError):
            transaction._execute_statement(MOCK_STATEMENT, INVALID_MOCK_PARAMETER)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.cursor.stream_cursor.StreamCursor')
    @patch('pyqldb.cursor.stream_cursor.StreamCursor')
    def test_close_child_cursors(self, mock_cursor_1, mock_cursor_2, mock_executor, mock_session):
        transaction = Transaction(mock_session, MOCK_READ_AHEAD, MOCK_ID, mock_executor)
        transaction._cursors = [mock_cursor_1, mock_cursor_2]

        transaction._close_child_cursors()

        self.assertEqual(transaction._cursors, [])
        mock_cursor_1.close.assert_called_once_with()
        mock_cursor_2.close.assert_called_once_with()

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.util.qldb_hash.QldbHash.to_qldb_hash')
    def test_update_hash(self, mock_to_qldb_hash, mock_executor, mock_session):
        mock_to_qldb_hash.return_value = mock_to_qldb_hash
        transaction = Transaction(mock_session, MOCK_READ_AHEAD, MOCK_ID, mock_executor)
        transaction._update_hash(MOCK_STATEMENT, (MOCK_PARAMETER_1, MOCK_PARAMETER_2))

        mock_to_qldb_hash.assert_has_calls([call(MOCK_ID), call(MOCK_STATEMENT), call(MOCK_PARAMETER_1),
                                            call(MOCK_PARAMETER_2)], any_order=True)
        self.assertEqual(transaction._txn_hash, mock_to_qldb_hash.dot())

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.util.qldb_hash.QldbHash.to_qldb_hash')
    def test_update_hash_empty_list_parameters(self, mock_to_qldb_hash, mock_executor, mock_session):
        mock_to_qldb_hash.return_value = mock_to_qldb_hash
        transaction = Transaction(mock_session, MOCK_READ_AHEAD, MOCK_ID, mock_executor)
        transaction._update_hash(MOCK_STATEMENT, [])

        mock_to_qldb_hash.assert_has_calls([call(MOCK_ID), call(MOCK_STATEMENT)])
        self.assertEqual(transaction._txn_hash, mock_to_qldb_hash.dot())
