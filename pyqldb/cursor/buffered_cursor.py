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


class BufferedCursor:
    """
    Implementation of a cursor which buffers all values in memory, rather than stream them from QLDB during retrieval.

    :type cursor: :py:class:`pyqldb.cursor.stream_cursor.StreamCursor`
    :param cursor: The cursor object to iterate through results and place into memory.
    """
    def __init__(self, cursor):
        self._buffered_values = []
        for item in cursor:
            self._buffered_values.append(item)

        self._buffered_values_iterator = iter(self._buffered_values)
        self._consumed_ios = cursor.get_consumed_ios()
        self._timing_information = cursor.get_timing_information()

    def __iter__(self):
        """
        Iterator function to implement the iterator protocol.
        """
        return self

    def __next__(self):
        """
        Iterator function to implement the iterator protocol. Get next value in _buffered_values_iterator.
        """
        return next(self._buffered_values_iterator)

    def get_consumed_ios(self):
        """
        Return a dictionary containing the total amount of IO requests for a statement execution. Return None if there
        were no read IOs for a statement execution.

        :rtype: dict/None
        :return: The amount of read IO requests for a statement execution.
        """
        return self._consumed_ios

    def get_timing_information(self):
        """
        Return a dictionary containing the total amount of processing time for a statement execution. Return None if
        there was no timing information for a statement execution.

        :rtype: dict/None
        :return: The amount of processing time in milliseconds for a statement execution.
        """
        return self._timing_information
