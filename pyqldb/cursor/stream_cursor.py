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

    :type page: dict
    :param page: The page containing the initial result set data dictionary of the statement's execution.

    :type session: :py:class:`pyqldb.communication.session_client.SessionClient`
    :param session: The parent session that represents the communication channel to QLDB.

    :type transaction_id: str
    :param transaction_id: The ID of this cursor's parent transaction, required to fetch pages.
    """
    def __init__(self, page, session, transaction_id):
        self._page = page
        self._session = session
        self._index = 0
        self._is_open = True
        self._transaction_id = transaction_id

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
        page = statement_result.get('Page')
        self._page = page
        self._index = 0

    @staticmethod
    def _value_holder_to_ion_value(value):
        """
        Get the Ion binary out from a value holder, and convert it into an Ion value.
        """
        binary = value.get('IonBinary')
        ion_value = loads(binary)
        return ion_value
