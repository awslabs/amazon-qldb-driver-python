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
from unittest.mock import patch

from amazon.ion.simpleion import dumps
from botocore.exceptions import ClientError

from pyqldb.cursor.read_ahead_cursor import ReadAheadCursor
from pyqldb.errors import ResultClosedError

MOCK_ERROR_CODE = '500'
MOCK_MESSAGE = 'foo'
MOCK_CLIENT_ERROR_MESSAGE = {'Error': {'Code': MOCK_ERROR_CODE, 'Message': MOCK_MESSAGE}}
MOCK_READ_AHEAD = 0
MOCK_VALUES = [1, 2]
MOCK_ION_BINARY_VALUES = [{'IonBinary': MOCK_VALUES[0]}, {'IonBinary': MOCK_VALUES[1]}]
MOCK_TOKEN = 'mock_token'
MOCK_STATEMENT_RESULT = {'Values': MOCK_ION_BINARY_VALUES, 'NextPageToken': MOCK_TOKEN}
MOCK_TRANSACTION_ID = 'ID'


class TestReadAheadCursor(TestCase):

    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.cursor.read_ahead_cursor.Queue')
    @patch('pyqldb.cursor.read_ahead_cursor.Thread')
    def test_ReadAHeadCursor_without_executor(self, mock_thread, mock_queue, mock_session):
        mock_session.return_value = None
        mock_thread.return_value = mock_thread
        mock_queue.return_value = mock_queue

        read_ahead_cursor = ReadAheadCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID, MOCK_READ_AHEAD,
                                            None)

        self.assertEqual(read_ahead_cursor._page, MOCK_STATEMENT_RESULT)
        self.assertEqual(read_ahead_cursor._session, mock_session)
        self.assertEqual(read_ahead_cursor._transaction_id, MOCK_TRANSACTION_ID)
        self.assertEqual(read_ahead_cursor._index, 0)
        self.assertEqual(read_ahead_cursor._queue, mock_queue)
        self.assertEqual(read_ahead_cursor._is_open, True)
        mock_queue.assert_called_once_with(MOCK_READ_AHEAD - 1)
        mock_thread.assert_called_once_with(target=read_ahead_cursor._populate_queue)
        mock_thread().setDaemon.assert_called_once_with(True)
        mock_thread().start.assert_called_once_with()

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.cursor.read_ahead_cursor.Queue')
    def test_ReadAheadCursor_with_executor(self, mock_queue, mock_session, mock_executor):
        mock_session.return_value = None
        mock_queue.return_value = mock_queue

        read_ahead_cursor = ReadAheadCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID, MOCK_READ_AHEAD,
                                            mock_executor)

        self.assertEqual(read_ahead_cursor._page, MOCK_STATEMENT_RESULT)
        self.assertEqual(read_ahead_cursor._session, mock_session)
        self.assertEqual(read_ahead_cursor._transaction_id, MOCK_TRANSACTION_ID)
        self.assertEqual(read_ahead_cursor._index, 0)
        self.assertEqual(read_ahead_cursor._queue, mock_queue)
        self.assertEqual(read_ahead_cursor._is_open, True)
        mock_queue.assert_called_once_with(MOCK_READ_AHEAD - 1)
        mock_executor.submit.assert_called_once_with(read_ahead_cursor._populate_queue)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_iter(self, mock_session, mock_executor):
        mock_session.return_value = None
        read_ahead_cursor = ReadAheadCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID, MOCK_READ_AHEAD,
                                            mock_executor)
        self.assertEqual(iter(read_ahead_cursor), read_ahead_cursor)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.cursor.stream_cursor.StreamCursor._value_holder_to_ion_value')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_next(self, mock_session, mock_value_holder_to_ion_value, mock_executor):
        mock_session.return_value = None
        mock_value_holder_to_ion_value.side_effect = lambda val: val
        mock_statement_result_with_none_next_page_token = MOCK_STATEMENT_RESULT.copy()
        mock_statement_result_with_none_next_page_token.update({'NextPageToken': None})
        read_ahead_cursor = ReadAheadCursor(mock_statement_result_with_none_next_page_token, mock_session,
                                            MOCK_TRANSACTION_ID, MOCK_READ_AHEAD, mock_executor)
        count = 0
        for value in MOCK_ION_BINARY_VALUES:
            self.assertEqual(read_ahead_cursor._index, count)
            self.assertEqual(next(read_ahead_cursor), value)
            mock_value_holder_to_ion_value.assert_called_with(value)
            count += 1
        self.assertRaises(StopIteration, next, read_ahead_cursor)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_next_when_closed(self, mock_session, mock_executor):
        read_ahead_cursor = ReadAheadCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID, MOCK_READ_AHEAD,
                                            mock_executor)
        read_ahead_cursor.close()
        self.assertRaises(ResultClosedError, next, read_ahead_cursor)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.cursor.stream_cursor.StreamCursor._value_holder_to_ion_value')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.cursor.read_ahead_cursor.ReadAheadCursor._next_page')
    @patch('pyqldb.cursor.read_ahead_cursor.ReadAheadCursor._are_there_more_results')
    def test_next_verify_are_there_more_results_and_next_page_called(self,
                                                                        mock_are_there_more_results,
                                                                        mock_next_page, mock_session,
                                                                        mock_value_holder_to_ion_value,
                                                                        mock_executor):
        updated_result = '1'

        def next_page():
            read_ahead_cursor._page = {'NextPageToken': None, 'Values': [updated_result]}
            read_ahead_cursor._index = 0

        mock_are_there_more_results.return_value = True
        mock_value_holder_to_ion_value.side_effect = lambda val: val
        mock_session.return_value = None
        mock_next_page.return_value = None
        mock_next_page.side_effect = next_page
        read_ahead_cursor = ReadAheadCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID, MOCK_READ_AHEAD,
                                            mock_executor)
        read_ahead_cursor._index = len(MOCK_ION_BINARY_VALUES)

        self.assertEqual(next(read_ahead_cursor), updated_result)
        mock_are_there_more_results.assert_called_once_with()
        mock_next_page.assert_called_once_with()
        mock_value_holder_to_ion_value.assert_called_once_with(updated_result)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.cursor.read_ahead_cursor.ReadAheadCursor._next_page')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_next_when_next_page_returns_empty_values_and_none_token(self, mock_session, mock_next_page, mock_executor):
        mock_session.return_value = None

        def next_page():
            read_ahead_cursor._page = {'NextPageToken': None, 'Values': []}
            read_ahead_cursor._index = 0

        read_ahead_cursor = ReadAheadCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID, MOCK_READ_AHEAD,
                                            mock_executor)
        read_ahead_cursor._index = len(MOCK_ION_BINARY_VALUES)
        mock_next_page.side_effect = next_page
        self.assertRaises(StopIteration, next, read_ahead_cursor)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_next_with_next_page_returns_empty_values_and_not_none_token(self, mock_session, mock_executor):
        read_ahead_cursor = ReadAheadCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID, MOCK_READ_AHEAD,
                                            mock_executor)
        read_ahead_cursor._queue = Queue()
        read_ahead_cursor._queue.put({'NextPageToken': 'token', 'Values': []})
        read_ahead_cursor._queue.put({'NextPageToken': None, 'Values': []})

        read_ahead_cursor._index = len(MOCK_ION_BINARY_VALUES)
        self.assertRaises(StopIteration, next, read_ahead_cursor)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_close(self, mock_session, mock_executor):
        mock_session.return_value = None
        read_ahead_cursor = ReadAheadCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID, MOCK_READ_AHEAD,
                                            mock_executor)
        read_ahead_cursor.close()
        self.assertFalse(read_ahead_cursor._is_open)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_are_there_more_results(self, mock_session, mock_executor):
        mock_session.return_value = None
        read_ahead_cursor = ReadAheadCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID, MOCK_READ_AHEAD,
                                            mock_executor)

        read_ahead_cursor._page = {'NextPageToken': 'token', 'Values': []}
        self.assertTrue(read_ahead_cursor._are_there_more_results())

        read_ahead_cursor._page = {'NextPageToken': None, 'Values': []}
        read_ahead_cursor._queue = Queue()
        self.assertFalse(read_ahead_cursor._are_there_more_results())

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.cursor.read_ahead_cursor.Queue')
    def test_populate_queue(self, mock_queue, mock_session, mock_executor):
        mock_session.return_value = None
        mock_queue.return_value = mock_queue
        mock_page = {'NextPageToken': None, 'Values': []}
        mock_session._fetch_page.return_value = {'Page': mock_page}
        mock_queue.full.return_value = False

        read_ahead_cursor = ReadAheadCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID, MOCK_READ_AHEAD,
                                            mock_executor)
        read_ahead_cursor._queue = mock_queue

        read_ahead_cursor._populate_queue()
        mock_session._fetch_page.assert_called_once_with(MOCK_TRANSACTION_ID, MOCK_STATEMENT_RESULT.get('NextPageToken'))
        mock_queue.put.assert_called_once_with(mock_page, timeout=0.05)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.cursor.read_ahead_cursor.logger.debug')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_populate_queue_client_error(self, mock_session, mock_logger_debug, mock_executor):
        mock_logger_debug.return_value = None
        mock_session.return_value = None
        mock_session._fetch_page.side_effect = ClientError(MOCK_CLIENT_ERROR_MESSAGE, MOCK_MESSAGE)

        read_ahead_cursor = ReadAheadCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID, MOCK_READ_AHEAD,
                                            mock_executor)
        read_ahead_cursor._queue = Queue(1)
        read_ahead_cursor._queue.put('value to be removed')
        read_ahead_cursor._populate_queue()

        mock_logger_debug.assert_called_once()
        self.assertIsInstance(read_ahead_cursor._queue.get(), ClientError)
        self.assertEqual(read_ahead_cursor._queue.qsize(), 0)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.cursor.read_ahead_cursor.logger.debug')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_populate_queue_result_closed_error(self, mock_session, mock_logger_debug, mock_executor):
        def close_parent_txn(txn_id, token):
            read_ahead_cursor._is_open = False
            return MOCK_STATEMENT_RESULT

        mock_logger_debug.return_value = None
        mock_session.return_value = None
        mock_session._fetch_page.side_effect = close_parent_txn
        read_ahead_cursor = ReadAheadCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID, MOCK_READ_AHEAD,
                                            mock_executor)
        read_ahead_cursor._queue = Queue(1)
        read_ahead_cursor._queue.put('value to be removed')
        read_ahead_cursor._populate_queue()

        self.assertEqual(mock_logger_debug.call_count, 2)
        self.assertIsInstance(read_ahead_cursor._queue.get(), ResultClosedError)
        self.assertEqual(read_ahead_cursor._queue.qsize(), 0)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_next_page(self, mock_session, mock_executor):
        mock_session.return_value = None
        mock_session._fetch_page.return_value = {'Page': MOCK_STATEMENT_RESULT}
        read_ahead_cursor = ReadAheadCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID, MOCK_READ_AHEAD,
                                            mock_executor)
        read_ahead_cursor._queue = Queue()
        read_ahead_cursor._queue.put(MOCK_STATEMENT_RESULT)
        read_ahead_cursor._next_page()

        self.assertEqual(read_ahead_cursor._page, MOCK_STATEMENT_RESULT)
        self.assertEqual(read_ahead_cursor._index, 0)

    @patch('concurrent.futures.thread.ThreadPoolExecutor')
    @patch('pyqldb.communication.session_client.SessionClient')
    def test_next_page_client_error(self, mock_session, mock_executor):
        mock_session.return_value = None
        mock_session._fetch_page.return_value = {'Page': MOCK_STATEMENT_RESULT}
        read_ahead_cursor = ReadAheadCursor(MOCK_STATEMENT_RESULT, mock_session, MOCK_TRANSACTION_ID, MOCK_READ_AHEAD,
                                            mock_executor)
        read_ahead_cursor._queue = Queue()
        read_ahead_cursor._queue.put(ClientError(MOCK_CLIENT_ERROR_MESSAGE, MOCK_MESSAGE))
        self.assertRaises(ClientError, read_ahead_cursor._next_page)

    def test_value_holder_to_ion_value(self):
        ion_value = 'IonValue'
        value_holder = {'IonBinary': dumps(ion_value)}

        result = ReadAheadCursor._value_holder_to_ion_value(value_holder)
        self.assertEqual(result, ion_value)
