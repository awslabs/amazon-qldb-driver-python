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

from pyqldb.cursor.buffered_cursor import BufferedCursor

from .helper_functions import assert_query_stats, create_stream_cursor, generate_statement_result

MOCK_VALUES = [1, 2]
MOCK_TRANSACTION_ID = 'id'


@patch('pyqldb.cursor.stream_cursor.StreamCursor')
class TestBufferedCursor(TestCase):

    @patch('pyqldb.cursor.buffered_cursor.iter')
    def test_BufferedCursor(self, mock_iter, mock_stream_cursor):
        mock_stream_cursor.return_value = None
        mock_iter.return_value = mock_iter
        mock_stream_cursor.__iter__.return_value = MOCK_VALUES
        buffered_cursor = BufferedCursor(mock_stream_cursor)

        self.assertEqual(buffered_cursor._buffered_values, MOCK_VALUES)
        self.assertEqual(buffered_cursor._buffered_values_iterator, mock_iter)
        mock_iter.assert_called_once_with(buffered_cursor._buffered_values)

    def test_iter(self, mock_stream_cursor):
        mock_stream_cursor.return_value = None
        buffered_cursor = BufferedCursor(mock_stream_cursor)

        self.assertEqual(iter(buffered_cursor), buffered_cursor)

    def test_next(self, mock_stream_cursor):
        mock_stream_cursor.return_value = None
        buffered_cursor = BufferedCursor(mock_stream_cursor)
        buffered_cursor._buffered_values_iterator = iter(MOCK_VALUES)

        for value in MOCK_VALUES:
            self.assertEqual(next(buffered_cursor), value)
        self.assertRaises(StopIteration, next, buffered_cursor)

    @patch('pyqldb.communication.session_client.SessionClient')
    def test_null_execute_and_null_fetch_page(self, mock_session, mock_stream_cursor):
        execute_statement_result_null = generate_statement_result(None, None, None, 'token', True)
        fetch_page_statement_result_null = generate_statement_result(None, None, None, None, False)

        self.create_buffered_cursor_and_assert_query_stats(mock_session, execute_statement_result_null,
                                                           fetch_page_statement_result_null, None, None, None)

    @patch('pyqldb.communication.session_client.SessionClient')
    def test_filled_execute_and_null_fetch_page(self, mock_session, mock_stream_cursor):
        execute_statement_result_filled = generate_statement_result(1, 2, 3, 'token', True)
        fetch_page_statement_result_null = generate_statement_result(None, None, None, None, False)

        self.create_buffered_cursor_and_assert_query_stats(mock_session, execute_statement_result_filled,
                                                           fetch_page_statement_result_null, 1, 2, 3)

    @patch('pyqldb.communication.session_client.SessionClient')
    def test_null_execute_and_filled_fetch_page(self, mock_session, mock_stream_cursor):
        execute_statement_result_null = generate_statement_result(None, None, None, 'token', True)
        fetch_page_statement_result_filled = generate_statement_result(1, 2, 3, None, False)

        self.create_buffered_cursor_and_assert_query_stats(mock_session, execute_statement_result_null,
                                                           fetch_page_statement_result_filled, 1, 2, 3)

    @patch('pyqldb.communication.session_client.SessionClient')
    def test_filled_execute_and_filled_fetch_page(self, mock_session, mock_stream_cursor):
        execute_statement_result_filled = generate_statement_result(1, 2, 3, 'token', True)
        fetch_page_statement_result_filled = generate_statement_result(1, 2, 3, None, False)

        self.create_buffered_cursor_and_assert_query_stats(mock_session, execute_statement_result_filled,
                                                           fetch_page_statement_result_filled, 2, 4, 6)

    def create_buffered_cursor_and_assert_query_stats(self, mock_session, mock_statement_result_execute,
                                                      mock_statement_result_fetch, read_io_assert, write_io_assert,
                                                      processing_time_assert):
        stream_cursor = create_stream_cursor(mock_session, mock_statement_result_execute, mock_statement_result_fetch)
        buffered_cursor = BufferedCursor(stream_cursor)

        assert_query_stats(self, buffered_cursor, read_io_assert, write_io_assert, processing_time_assert)
