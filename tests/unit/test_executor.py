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
from unittest.mock import patch

from pyqldb.errors import LambdaAbortedError
from pyqldb.execution.executor import Executor

MOCK_ERROR_CODE = '500'
MOCK_MESSAGE = 'foo'
MOCK_STATEMENT = 'SELECT * FROM foo'
MOCK_PARAMETER_1 = 'foo'
MOCK_PARAMETER_2 = 'bar'
MOCK_CLIENT_ERROR_MESSAGE = {'Error': {'Code': MOCK_ERROR_CODE, 'Message': MOCK_MESSAGE}}


@patch('pyqldb.transaction.transaction.Transaction')
class TestExecutor(TestCase):

    def test_Executor(self, mock_transaction):
        mock_transaction.transaction_id = 'txnId'
        executor = Executor(mock_transaction)

        self.assertEqual(executor._transaction, mock_transaction)
        self.assertEqual(executor.transaction_id, 'txnId')

    def test_abort(self, mock_transaction):
        executor = Executor(mock_transaction)

        self.assertRaises(LambdaAbortedError, executor.abort)

    @patch('pyqldb.cursor.stream_cursor.StreamCursor')
    def test_execute_statement(self, mock_cursor, mock_transaction):
        mock_transaction._execute_statement.return_value = mock_cursor
        executor = Executor(mock_transaction)

        cursor = executor.execute_statement(MOCK_STATEMENT, MOCK_PARAMETER_1, MOCK_PARAMETER_2)
        mock_transaction._execute_statement.assert_called_once_with(MOCK_STATEMENT, MOCK_PARAMETER_1, MOCK_PARAMETER_2)
        self.assertEqual(cursor, mock_cursor)
