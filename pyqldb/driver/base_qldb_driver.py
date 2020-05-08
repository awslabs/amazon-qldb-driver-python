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
from abc import ABC, abstractmethod
from logging import getLogger

from boto3 import client
from botocore.config import Config
from boto3.session import Session

from .. import __version__
from ..communication.session_client import SessionClient
from ..session.qldb_session import QldbSession

logger = getLogger(__name__)
SERVICE_DESCRIPTION = 'QLDB Driver for Python v{}'.format(__version__)
SERVICE_NAME = 'qldb-session'
SERVICE_RETRY = {'max_attempts': 0}
DEFAULT_CONFIG = Config(user_agent_extra=SERVICE_DESCRIPTION, retries=SERVICE_RETRY)


class BaseQldbDriver(ABC):
    """
    An abstract base class representing a factory for creating sessions.

    :type ledger_name: str
    :param ledger_name: The QLDB ledger name.

    :type retry_limit: int
    :param retry_limit: The number of automatic retries for statement executions using convenience methods on sessions
                        when an OCC conflict or retriable exception occurs. This value must not be negative.

    :type read_ahead: int
    :param read_ahead: The number of read-ahead buffers. Determines the maximum number of statement result pages that
                       can be buffered in memory. This value must be either 0, to disable read-ahead, or a minimum of 2.

    :type executor: :py:class:`concurrent.futures.thread.ThreadPoolExecutor`
    :param executor: A specific, optional, executor to be used by the retrieval thread if read-ahead is enabled.

    :type region_name: str
    :param region_name: See [1].

    :type verify: bool/str
    :param verify: See [1].

    :type endpoint_url: str
    :param endpoint_url: See [1].

    :type aws_access_key_id: str
    :param aws_access_key_id: See [1].

    :type aws_secret_access_key: str
    :param aws_secret_access_key: See [1].

    :type aws_session_token: str
    :param aws_session_token: See [1].

    :type config: :py:class:`botocore.config.Config`
    :param config: See [2]. Note that parameter user_agent_extra will be overwritten.

    :type boto3_session: :py:class:`boto3.session.Session`
    :param boto3_session: The boto3 session to create the client with (see [1]). The boto3 session is expected to be
                          configured correctly.

    :raises TypeError: When config is not an instance of :py:class:`botocore.config.Config`.
                       When boto3_session is not an instance of :py:class:`boto3.session.Session`.

    :raises ValueError: When `read_ahead` or `retry_limit` is not set to the allowed values specified.

    [1]: `Boto3 Session.client Reference <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client>`_.

    [2]: `Botocore Config Reference <https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html>`_.
    """
    def __init__(self, ledger_name, retry_limit=4, read_ahead=0, executor=None, region_name=None, verify=None,
                 endpoint_url=None, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None,
                 config=None, boto3_session=None):
        if retry_limit < 0:
            raise ValueError('Value for retry_limit cannot be negative.')
        if read_ahead < 2 and read_ahead != 0:
            raise ValueError('Value for read_ahead must be 0 or 2 or greater.')

        self._ledger_name = ledger_name
        self._retry_limit = retry_limit
        self._read_ahead = read_ahead
        self._executor = executor
        self._is_closed = False

        if config is not None:
            if not isinstance(config, Config):
                raise TypeError('config must be of type botocore.config.Config. Found: {}'
                                .format(type(config).__name__))
            self._config = config.merge(DEFAULT_CONFIG)
        else:
            self._config = DEFAULT_CONFIG

        if boto3_session is not None:
            if not isinstance(boto3_session, Session):
                raise TypeError('boto3_session must be of type boto3.session.Session. Found: {}'
                                .format(type(boto3_session).__name__))

            if region_name is not None or aws_access_key_id is not None or aws_secret_access_key is not None or \
                    aws_session_token is not None:
                logger.warning('Custom parameters were detected while using a specified Boto3 client and will be '
                               'ignored. Please preconfigure the Boto3 client with those parameters instead.')

            self._client = boto3_session.client(SERVICE_NAME, verify=verify, endpoint_url=endpoint_url,
                                                config=self._config)
        else:
            self._client = client(SERVICE_NAME, region_name=region_name, verify=verify, endpoint_url=endpoint_url,
                                  aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                                  aws_session_token=aws_session_token, config=self._config)

    def __enter__(self):
        """
        Context Manager function to support the 'with' statement.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context Manager function to support the 'with' statement.
        """
        self.close()

    @abstractmethod
    def get_session(self):
        """
        Retrieve a QldbSession object. This method must be overridden.
        """
        pass

    @abstractmethod
    def execute_lambda(self, query_lambda, retry_indicator):
        """
        Implicitly start a transaction, execute the lambda function, and commit the transaction, retrying up to the
        retry limit if an OCC conflict or retriable exception occurs. This method must be overridden.
        """
        pass

    @abstractmethod
    def list_tables(self):
        """
        Get the list of table names in the ledger. This method must be overridden.
        """
        pass

    @property
    def read_ahead(self):
        """
        The number of read-ahead buffers to be made available per `StreamCursor` instantiated by this driver.
        Determines the maximum number of result pages that can be buffered in memory.

        .. seealso:: :py:class:`pyqldb.cursor.stream_cursor.StreamCursor`
        """
        return self._read_ahead

    @property
    def retry_limit(self):
        """
        The number of automatic retries for statement executions using convenience methods on sessions when
        an OCC conflict or retriable exception occurs.
        """
        return self._retry_limit

    def close(self):
        """
        Close this driver.
        """
        self._is_closed = True

    def _create_new_session(self):
        """
        Create a new QldbSession object.
        """
        session_client = SessionClient.start_session(self._ledger_name, self._client)
        return QldbSession(session_client, self._read_ahead, self._retry_limit, self._executor)
