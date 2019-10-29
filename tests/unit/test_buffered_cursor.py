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

MOCK_VALUES = [1, 2]


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
