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

from amazon.ion.simpleion import dumps

from pyqldb.cursor.stream_cursor import StreamCursor
from pyqldb.errors import ResultClosedError
from .helper_functions import assert_query_stats, create_stream_cursor, generate_statement_result

MOCK_VALUES = [1, 2]
MOCK_ION_BINARY_VALUES = [{'IonBinary': MOCK_VALUES[0]}, {'IonBinary': MOCK_VALUES[1]}]
MOCK_TOKEN = 'mock_token'
MOCK_PAGE_WITH_TOKEN = {'Values': MOCK_ION_BINARY_VALUES, 'NextPageToken': MOCK_TOKEN}
MOCK_PAGE_WITHOUT_TOKEN = {'Values': MOCK_ION_BINARY_VALUES, 'NextPageToken': None}
MOCK_TRANSACTION_ID = 'id'

MOCK_READ_IOS = 3
MOCK_WRITE_IOS = 2
MOCK_PROCESSING_TIME = 1

MOCK_STATEMENT_RESULT = generate_statement_result(MOCK_READ_IOS, MOCK_WRITE_IOS, MOCK_PROCESSING_TIME,
                                                  MOCK_TOKEN, True, MOCK_ION_BINARY_VALUES)


class TestStreamCursor(TestCase):

    @patch('pyqldb.communication.session_client.SessionClient')
    def test_StreamCursor(self, mock_session):
        mock_session.return_value = None
        stream_cursor = StreamCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID)

        self.assertEqual(stream_cursor._page, MOCK_PAGE_WITH_TOKEN)
        self.assertEqual(stream_cursor._read_ios, MOCK_READ_IOS)
        self.assertEqual(stream_cursor._write_ios, MOCK_WRITE_IOS)
        self.assertEqual(stream_cursor._processing_time_milliseconds, MOCK_PROCESSING_TIME)
        self.assertEqual(stream_cursor._session, mock_session)
        self.assertEqual(stream_cursor._transaction_id, MOCK_TRANSACTION_ID)
        self.assertEqual(stream_cursor._index, 0)
        self.assertEqual(stream_cursor._is_open, True)

    @patch('pyqldb.communication.session_client.SessionClient')
    def test_iter(self, mock_session):
        mock_session.return_value = None
        stream_cursor = StreamCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID)

        self.assertEqual(iter(stream_cursor), stream_cursor)

    @patch('pyqldb.cursor.stream_cursor.StreamCursor._value_holder_to_ion_value')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_next(self, mock_session, mock_value_holder_to_ion_value):
        mock_session.return_value = None
        mock_value_holder_to_ion_value.side_effect = lambda val: val

        MOCK_STATEMENT_RESULT_WITHOUT_NEXT_PAGE_TOKEN = {
            'FirstPage': MOCK_PAGE_WITHOUT_TOKEN,
            'TimingInformation': {'ProcessingTimeMilliseconds': MOCK_PROCESSING_TIME},
            'ConsumedIOs': {'ReadIOs': MOCK_READ_IOS, 'WriteIOs': MOCK_WRITE_IOS}
        }

        stream_cursor = StreamCursor(MOCK_STATEMENT_RESULT_WITHOUT_NEXT_PAGE_TOKEN, mock_session, MOCK_TRANSACTION_ID)
        count = 0
        for value in MOCK_ION_BINARY_VALUES:
            self.assertEqual(stream_cursor._index, count)
            self.assertEqual(next(stream_cursor), value)
            mock_value_holder_to_ion_value.assert_called_with(value)
            count += 1
        self.assertRaises(StopIteration, next, stream_cursor)

    @patch('pyqldb.communication.session_client.SessionClient')
    def test_next_when_closed(self, mock_session):
        stream_cursor = StreamCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID)

        stream_cursor.close()
        self.assertRaises(ResultClosedError, next, stream_cursor)

    @patch('pyqldb.cursor.stream_cursor.StreamCursor._value_holder_to_ion_value')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.cursor.stream_cursor.StreamCursor._next_page')
    @patch('pyqldb.cursor.stream_cursor.StreamCursor._are_there_more_results')
    def test_next_verify_are_there_more_results_and_next_page_called(self, mock_are_there_more_results, mock_next_page,
                                                                     mock_session, mock_value_holder_to_ion_value):
        updated_result = '1'

        def update_page():
            stream_cursor._page = {'NextPageToken': None, 'Values': [updated_result]}
            stream_cursor._index = 0

        mock_are_there_more_results.return_value = True
        mock_value_holder_to_ion_value.side_effect = lambda val: val
        mock_session.return_value = None
        mock_next_page.return_value = None
        mock_next_page.side_effect = update_page
        stream_cursor = StreamCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID)
        stream_cursor._index = len(MOCK_ION_BINARY_VALUES)

        self.assertEqual(next(stream_cursor), updated_result)
        mock_next_page.assert_called_once_with()
        mock_are_there_more_results.assert_called_once_with()
        mock_value_holder_to_ion_value.assert_called_once_with(updated_result)

    @patch('pyqldb.cursor.stream_cursor.StreamCursor._next_page')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_next_when_next_page_returns_empty_values_and_none_token(self, mock_session, mock_next_page):
        mock_session.return_value = None

        def next_page():
            stream_cursor._page = {'NextPageToken': None, 'Values': []}
            stream_cursor._index = 0

        stream_cursor = StreamCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID)
        stream_cursor._index = len(MOCK_ION_BINARY_VALUES)
        mock_next_page.side_effect = next_page
        self.assertRaises(StopIteration, next, stream_cursor)

    @patch('pyqldb.communication.session_client.SessionClient')
    def test_next_with_next_page_returns_empty_values_and_not_none_token(self, mock_session):
        mock_session.return_value = None
        stream_cursor = StreamCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID)
        stream_cursor._index = len(MOCK_ION_BINARY_VALUES)

        mock_session._fetch_page.side_effect = [{'Page': {'NextPageToken': 'token', 'Values': []}},
                                                {'Page': {'NextPageToken': None, 'Values': []}}]
        self.assertRaises(StopIteration, next, stream_cursor)

    @patch('pyqldb.communication.session_client.SessionClient')
    def test_close(self, mock_session):
        mock_session.return_value = None
        stream_cursor = StreamCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID)
        stream_cursor.close()
        self.assertFalse(stream_cursor._is_open)

    @patch('pyqldb.communication.session_client.SessionClient')
    def test_are_there_more_results(self, mock_session):
        mock_session.return_value = None
        stream_cursor = StreamCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID)
        stream_cursor._page = {'NextPageToken': 'token', 'Values': []}
        self.assertTrue(stream_cursor._are_there_more_results())

        stream_cursor._page = {'NextPageToken': None, 'Values': []}
        self.assertFalse(stream_cursor._are_there_more_results())

    @patch('pyqldb.communication.session_client.SessionClient')
    def test_next_page(self, mock_session):
        mock_session.return_value = None

        fetch_page_statement_result = generate_statement_result(MOCK_READ_IOS, MOCK_WRITE_IOS, MOCK_PROCESSING_TIME,
                                                                None, False, MOCK_ION_BINARY_VALUES)
        mock_session._fetch_page.return_value = fetch_page_statement_result
        stream_cursor = StreamCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID)
        stream_cursor._next_page()

        mock_session._fetch_page.assert_called_once_with(stream_cursor._transaction_id, MOCK_TOKEN)
        self.assertEqual(stream_cursor._page, MOCK_PAGE_WITHOUT_TOKEN)
        self.assertEqual(stream_cursor._index, 0)

    # Test with null execute and null fetch page; all the query stats should be None.
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_null_execute_and_null_fetch_page(self, mock_session):
        execute_statement_result_null = generate_statement_result(None, None, None, "token", True)
        fetch_page_statement_result_null = generate_statement_result(None, None, None, None, True)

        self.create_stream_cursor_and_assert_query_stats(mock_session, execute_statement_result_null,
                                                         fetch_page_statement_result_null,
                                                         None, None, None, None, None, None)

    # Test with filled execute and null fetch page; the next page should not accumulate.
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_filled_execute_and_null_fetch_page(self, mock_session):
        execute_statement_result_filled = generate_statement_result(1, 2, 3, "token", True)
        fetch_page_statement_result_null = generate_statement_result(None, None, None, None, True)

        self.create_stream_cursor_and_assert_query_stats(mock_session, execute_statement_result_filled,
                                                         fetch_page_statement_result_null,
                                                         1, 2, 3,
                                                         1, 2, 3)

    # Test with null execute and filled fetch page; the next page should pick up the query stats.
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_null_execute_and_filled_fetch_page(self, mock_session):
        execute_statement_result_null = generate_statement_result(None, None, None, "token", True)
        fetch_page_statement_result_filled = generate_statement_result(1, 2, 3, None, True)

        self.create_stream_cursor_and_assert_query_stats(mock_session, execute_statement_result_null,
                                                         fetch_page_statement_result_filled,
                                                         None, None, None,
                                                         1, 2, 3)

    # Test with filled execute and filled fetch page; the next page should accumulate.
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_filled_execute_and_filled_fetch_page(self, mock_session):
        execute_statement_result_filled = generate_statement_result(1, 2, 3, "token", True)
        fetch_page_statement_result_filled = generate_statement_result(1, 2, 3, None, True)

        self.create_stream_cursor_and_assert_query_stats(mock_session, execute_statement_result_filled,
                                                         fetch_page_statement_result_filled,
                                                         1, 2, 3,
                                                         2, 4, 6)

    def test_value_holder_to_ion_value(self):
        ion_value = 'IonValue'
        value_holder = {'IonBinary': dumps(ion_value)}

        result = StreamCursor._value_holder_to_ion_value(value_holder)
        self.assertEqual(result, ion_value)

    def create_stream_cursor_and_assert_query_stats(self, mock_session, mock_statement_result_execute,
                                                    mock_statement_result_fetch, read_io_assert, write_io_assert,
                                                    processing_time_assert, read_io_next_page_assert,
                                                    write_io_next_page_assert, processing_time_next_page_assert):
        stream_cursor = create_stream_cursor(mock_session, mock_statement_result_execute, mock_statement_result_fetch)

        assert_query_stats(self, stream_cursor, read_io_assert, write_io_assert, processing_time_assert)

        stream_cursor._next_page()
        assert_query_stats(self, stream_cursor, read_io_next_page_assert, write_io_next_page_assert,
                           processing_time_next_page_assert)
