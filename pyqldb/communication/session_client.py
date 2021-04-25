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

from amazon.ion.simple_types import IonPyBool, IonPyBytes, IonPyDecimal, IonPyDict, IonPyFloat, IonPyInt, IonPyList, \
    IonPyNull, IonPySymbol, IonPyText, IonPyTimestamp
from amazon.ion.simpleion import dumps
from botocore.exceptions import ClientError

logger = getLogger(__name__)


class SessionClient:
    """
    A class representing an independent session to a QLDB ledger that handles endpoint requests. This class is used in
    :py:class:`pyqldb.driver.qldb_driver.QldbDriver` and :py:class:`pyqldb.session.qldb_session.QldbSession`.
    This class is not meant to be used directly by developers.

    :type ledger_name: str
    :param ledger_name: The QLDB ledger name.

    :type token: str
    :param token: The initial session token representing the session connection.

    :type client: :py:class:`botocore.client.BaseClient`
    :param: The low level service client.

    :type session_id: str
    :param: The session ID.
    """
    def __init__(self, ledger_name, token, client, session_id):
        self._ledger_name = ledger_name
        self._token = token
        self._client = client
        self._session_id = session_id

    def __enter__(self):
        """
        Context Manager function to support the 'with' statement.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context Manager function to support the 'with' statement.
        """
        self._close()

    @property
    def client(self):
        """
        The **read-only** low level service client.
        """
        return self._client

    @property
    def id(self):
        """
        The **read-only** ID of this session.
        """
        return self._session_id

    @property
    def ledger_name(self):
        """
        The **read-only** ledger name.
        """
        return self._ledger_name

    @property
    def token(self):
        """
        The **read-only** token.
        """
        return self._token

    def _abort_transaction(self):
        """
        Send request to abort the currently active transaction.

        :rtype: dict
        :return: The abort transaction result response from the endpoint.
        """
        request = {'SessionToken': self.token, 'AbortTransaction': {}}
        result = self._send_command(request)
        abort_transaction_output = result.get('AbortTransaction')
        return abort_transaction_output

    def _close(self):
        """
        Close this session.
        """
        try:
            self._end_session()
        except ClientError as ce:
            # We will only log issues closing the session, as QLDB will clean them after a timeout.
            logger.warning('Errors closing session: {}'.format(ce))

    def _commit_transaction(self, transaction_id, commit_digest):
        """
        Send request to commit the currently active transaction.

        :type transaction_id: str
        :param transaction_id: The ID of the transaction.

        :type commit_digest: str
        :param commit_digest: The digest hash of the transaction to commit.

        :rtype: dict
        :return: The commit transaction result response from the endpoint.
        """
        request = {'SessionToken': self.token, 'CommitTransaction': {'TransactionId': transaction_id,
                                                                     'CommitDigest': commit_digest}}
        result = self._send_command(request)
        commit_transaction_output = result.get('CommitTransaction')
        return commit_transaction_output

    def _end_session(self):
        """
        Send request to end the independent session represented by the instance of this class.

        :rtype: dict
        :return: The end session result response from the endpoint.
        """
        request = {'SessionToken': self.token, 'EndSession': {}}
        result = self._send_command(request)
        end_session_output = result.get('EndSession')
        return end_session_output

    def _execute_statement(self, transaction_id, statement, parameters):
        """
        Send an execute request with parameters to QLDB.

        :type transaction_id: str
        :param transaction_id: The ID of the transaction.

        :type statement: str
        :param statement: The statement to execute.

        :type parameters: list
        :param parameters: List of Ion values to fill in parameters of the statement.

        :rtype: dict
        :return: The statement result response from the endpoint.
        """
        parameters = list(map(self._to_value_holder, parameters))

        request = {'SessionToken': self.token, 'ExecuteStatement': {'TransactionId': transaction_id,
                                                                    'Statement': statement,
                                                                    'Parameters': parameters}}
        result = self._send_command(request)
        statement_result = result.get('ExecuteStatement')
        return statement_result

    def _fetch_page(self, transaction_id, next_page_token):
        """
        Send fetch result request to QLDB, retrieving the next chunk of data for the result.

        :type transaction_id: str
        :param transaction_id: The ID of the transaction.

        :type next_page_token: str
        :param next_page_token: The next page token.

        :rtype: dict
        :return: The fetch result response from the endpoint.
        """
        request = {'SessionToken': self.token, 'FetchPage': {'TransactionId': transaction_id,
                                                             'NextPageToken': next_page_token}}
        result = self._send_command(request)
        statement_result = result.get('FetchPage')
        return statement_result

    def _start_transaction(self):
        """
        Send request to start a transaction.

        :rtype: dict
        :return: The start transaction response.
        """
        request = {'SessionToken': self.token, 'StartTransaction': {}}
        result = self._send_command(request)
        return result.get('StartTransaction')

    def _send_command(self, request):
        """
        Call send_command method of the low level client.
        """
        logger.debug('Sending request: {}'.format(request))
        result = self._client.send_command(**request)
        logger.debug('Received response: {}'.format(result))
        return result

    @staticmethod
    def _start_session(ledger_name, client):
        """
        Factory method for constructing a new :py:class:`pyqldb.communication.session_client.SessionClient`, creating a
        new session to QLDB on construction.

        :type ledger_name: str
        :param ledger_name: The ledger name to create session.

        :type client: :py:class:`botocore.client.BaseClient`
        :param client: The low level service client.

        :rtype: :py:class:`pyqldb.communication.session_client.SessionClient`
        :return: A new SessionClient object.
        """
        logger.debug('Initiating new session.')
        result = client.send_command(StartSession={'LedgerName': ledger_name})
        token = result.get('StartSession').get('SessionToken')
        session_id = result.get('ResponseMetadata').get('HTTPHeaders').get('x-amzn-requestid')
        return SessionClient(ledger_name, token, client, session_id)

    @staticmethod
    def _to_value_holder(parameter):
        """
        Convert Python or Ion value to binary, and store in a value holder.
        """
        parameter_binary = dumps(parameter)
        value_holder = {'IonBinary': parameter_binary}
        return value_holder
