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
from amazon.ion.simpleion import loads

from ..errors import ResultClosedError


class StreamCursor:
    """
    An iterable class representing a stream cursor on a statement's result set.

    :type statement_result: dict
    :param statement_result: The initial result set data dictionary of the statement execution.

    :type session: :py:class:`pyqldb.communication.session_client.SessionClient`
    :param session: The parent session that represents the communication channel to QLDB.

    :type transaction_id: str
    :param transaction_id: The ID of this cursor's parent transaction, required to fetch pages.
    """
    def __init__(self, statement_result, session, transaction_id):
        self._page = statement_result.get('FirstPage')
        self._session = session
        self._index = 0
        self._is_open = True
        self._transaction_id = transaction_id
        self._read_ios = None
        self._write_ios = None
        self._processing_time_milliseconds = None

        self._accumulate_query_stats(statement_result)

    def __iter__(self):
        """
        Iterator function to implement the iterator protocol.
        """
        return self

    def __next__(self):
        """
        Iterator function to implement the iterator protocol. Pulls the next page containing the results being
        iterated on when the end of the current is reached.

        :rtype: :py:class:`amazon.ion.simple_types.IonSymbol`
        :return: The Ion value in the row that the cursor is on.

        :raises StopIteration: When there are no more results.
        """
        if not self._is_open:
            raise ResultClosedError(self._session.token)

        if self._index >= len(self._page.get('Values')):
            if not self._are_there_more_results():
                raise StopIteration
            self._next_page()
            while len(self._page.get('Values')) == 0 and self._are_there_more_results():
                self._next_page()
            if len(self._page.get('Values')) == 0:
                raise StopIteration
        row = self._page.get('Values')[self._index]
        ion_value = self._value_holder_to_ion_value(row)
        self._index += 1
        return ion_value

    def close(self):
        """
        Close this stream cursor object.
        """
        self._is_open = False

    def get_consumed_ios(self):
        """
        Return a dictionary containing the accumulated amount of IO requests for a statement execution. Return None if
        there were no read IOs for a statement execution.

        :rtype: dict/None
        :return: The amount of read IO requests for a statement execution.
        """
        return None if self._read_ios is None else {'ReadIOs': self._read_ios}

    def get_timing_information(self):
        """
        Return a dictionary containing the accumulated amount of processing time for a statement execution. Return None
        if there was no timing information for a statement execution.

        :rtype: dict/None
        :return: The amount of processing time in milliseconds for a statement execution.
        """
        return None if self._processing_time_milliseconds is None else {'ProcessingTimeMilliseconds':
                                                                            self._processing_time_milliseconds}

    def _are_there_more_results(self):
        """
        Check if there are more results.
        """
        return self._page.get('NextPageToken') is not None

    def _next_page(self):
        """
        Get the next page using this cursor's session.
        """
        statement_result = self._session._fetch_page(self._transaction_id, self._page.get('NextPageToken'))
        self._accumulate_query_stats(statement_result)

        page = statement_result.get('Page')
        self._page = page
        self._index = 0

    def _accumulate_query_stats(self, statement_result):
        """
        From the statement_result, get the query stats and accumulate them.
        """
        self._processing_time_milliseconds = self._accumulate(statement_result, 'TimingInformation',
                                                              'ProcessingTimeMilliseconds',
                                                              self._processing_time_milliseconds)
        self._read_ios = self._accumulate(statement_result, 'ConsumedIOs', 'ReadIOs', self._read_ios)
        self._write_ios = self._accumulate(statement_result, 'ConsumedIOs', 'WriteIOs', self._write_ios)

    @staticmethod
    def _accumulate(statement_result, query_statistics_key, metric_key, metric_to_accumulate):
        query_statistics = statement_result.get(query_statistics_key)
        if query_statistics:
            metric = query_statistics.get(metric_key)
            if metric:
                if metric_to_accumulate is None:
                    metric_to_accumulate = 0
                metric_to_accumulate += metric
        return metric_to_accumulate

    @staticmethod
    def _value_holder_to_ion_value(value):
        """
        Get the Ion binary out from a value holder, and convert it into an Ion value.
        """
        binary = value.get('IonBinary')
        ion_value = loads(binary)
        return ion_value
