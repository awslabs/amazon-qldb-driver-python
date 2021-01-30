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
from logging import getLogger
from queue import Full, Queue
from threading import Thread

from botocore.exceptions import ClientError

from .stream_cursor import StreamCursor
from ..errors import ResultClosedError

logger = getLogger(__name__)


class ReadAheadCursor(StreamCursor):
    """
    An iterable class representing a read ahead cursor on a statement's result set. This class will create a queue of
    size `read_ahead` and fetch results asynchronously to fill the queue.

    :type statement_result: dict
    :param statement_result: The initial result set data dictionary of the statement execution.

    :type session: :py:class:`pyqldb.communication.session_client.SessionClient`
    :param session: The parent session that represents the communication channel to QLDB.

    :type transaction_id: str
    :param transaction_id: The ID of this cursor's parent transaction, required to fetch pages.

    :type read_ahead: int
    :param read_ahead: The number of pages to read-ahead and buffer in this cursor.

    :type executor: :py:class:`concurrent.futures.thread.ThreadPoolExecutor`
    :param executor: The optional executor for asynchronous retrieval. If none specified, a new thread is created.
    """
    def __init__(self, statement_result, session, transaction_id, read_ahead, executor):
        super().__init__(statement_result, session, transaction_id)
        self._queue = Queue(read_ahead - 1)
        if executor is None:
            thread = Thread(target=self._populate_queue)
            thread.setDaemon(True)
            thread.start()
        else:
            executor.submit(self._populate_queue)

    def _are_there_more_results(self):
        """
        Check if there are more results.
        """
        return not (self._page.get('NextPageToken') is None and self._queue.empty())

    def _next_page(self):
        """
        Get the next page from the buffer queue.
        """
        queue_result = self._queue.get()
        if isinstance(queue_result, Exception):
            raise queue_result
        super()._accumulate_query_stats(queue_result)
        self._page = queue_result.get('Page')
        self._index = 0

    def _populate_queue(self):
        """
        Fill the buffer queue with the statement_result fetched. If ClientError is received, it is put in the queue and
        execution stops. If the parent transaction is closed, stop fetching results.
        """
        try:
            next_page_token = self._page.get('NextPageToken')
            while next_page_token is not None:
                statement_result = self._session._fetch_page(self._transaction_id, next_page_token)
                while True:
                    try:
                        # Timeout of 50ms.
                        self._queue.put(statement_result, timeout=0.05)
                        page = statement_result.get('Page')
                        next_page_token = page.get('NextPageToken')
                        break
                    except Full:
                        # When timeout is reached, check if the read-ahead retrieval thread should end.
                        if not self._is_open:
                            logger.debug('Cursor was closed; read-ahead retriever thread stopping.')
                            raise ResultClosedError(self._session.token)
        except (ClientError, ResultClosedError) as error:
            while not self._queue.empty():
                self._queue.get_nowait()
            logger.debug('Queued an exception: {}'.format(error))
            self._queue.put(error)
