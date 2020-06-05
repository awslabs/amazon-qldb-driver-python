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
from unittest.mock import patch, call

from amazon.ion.simpleion import loads
from botocore.exceptions import ClientError

from pyqldb.communication.session_client import SessionClient


MOCK_COMMIT_DIGEST = 'commit_digest'
MOCK_DUMPS_RETURN_VALUE = 'dumps_return_value'
MOCK_ERROR_CODE = '200'
MOCK_ERROR_MESSAGE = 'error_message'
MOCK_ID = 'id'
MOCK_LEDGER_NAME = 'ledger name'
MOCK_PARAMETERS = ('parameter1', 'parameter2')
MOCK_STATEMENT = 'statement'
MOCK_TRANSACTION_ID = 'transaction_id'
MOCK_TOKEN = 'token'
MOCK_VALUES = 'mock_values'
MOCK_TRANSACTION_RESULT = {'TransactionId': MOCK_TRANSACTION_ID}
MOCK_ABORT_TRANSACTION_RESULT = {'AbortTransaction': MOCK_VALUES}
MOCK_COMMIT_TRANSACTION_RESULT = {'CommitTransaction': MOCK_VALUES}
MOCK_END_SESSION_RESULT = {'EndSession': MOCK_VALUES}
MOCK_RESULT_RESULT = {"FirstPage": {'Values': MOCK_VALUES}}
MOCK_FETCH_PAGE_RESULT = {'FetchPage': MOCK_RESULT_RESULT}
MOCK_EXECUTE_STATEMENT_RESULT = {'ExecuteStatement': MOCK_RESULT_RESULT}
MOCK_CLIENT_ERROR_MESSAGE = {'Error': {'Code': MOCK_ERROR_CODE, 'Message': MOCK_ERROR_MESSAGE}}
MOCK_START_TRANSACTION_RESULT = {'StartTransaction': MOCK_TRANSACTION_RESULT}
MOCK_START_SESSION_RESULT = {'StartSession': {'SessionToken': MOCK_TOKEN},
                             'ResponseMetadata': {'HTTPHeaders': {'x-amzn-requestid': MOCK_ID}}}


class TestSessionClient(TestCase):

    @patch('botocore.client.BaseClient')
    def test_SessionClient(self, mock_client):
        mock_client.return_value = mock_client
        session = SessionClient(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID)

        self.assertEqual(session._ledger_name, MOCK_LEDGER_NAME)
        self.assertEqual(session._token, MOCK_TOKEN)
        self.assertEqual(session._client, mock_client)
        self.assertEqual(session._session_id, MOCK_ID)

    @patch('pyqldb.communication.session_client.SessionClient._close')
    @patch('botocore.client.BaseClient')
    def test_context_manager(self, mock_client, mock_close):
        mock_client.return_value = mock_client
        with SessionClient(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID):
            pass

        mock_close.assert_called_once_with()

    @patch('botocore.client.BaseClient')
    def test_get_id(self, mock_client):
        mock_client.return_value = mock_client
        session = SessionClient(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID)

        self.assertEqual(session.id, MOCK_ID)

    @patch('botocore.client.BaseClient')
    def test_get_client(self, mock_client):
        mock_client.return_value = mock_client
        session = SessionClient(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID)

        self.assertEqual(session.client, mock_client)

    @patch('botocore.client.BaseClient')
    def test_get_ledger_name(self, mock_client):
        mock_client.return_value = mock_client
        session = SessionClient(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID)

        self.assertEqual(session.ledger_name, MOCK_LEDGER_NAME)

    @patch('botocore.client.BaseClient')
    def test_get_token(self, mock_client):
        mock_client.return_value = mock_client
        session = SessionClient(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID)

        self.assertEqual(session.token, MOCK_TOKEN)

    @patch('botocore.client.BaseClient')
    @patch('pyqldb.communication.session_client.SessionClient._send_command')
    def test_abort_transaction(self, mock_send_command, mock_client):
        mock_client.return_value = mock_client
        mock_send_command.return_value = MOCK_ABORT_TRANSACTION_RESULT
        session = SessionClient(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID)
        result = session._abort_transaction()

        mock_send_command.assert_called_once_with({'SessionToken': MOCK_TOKEN, 'AbortTransaction': {}})
        self.assertEqual(result, MOCK_VALUES)

    @patch('pyqldb.communication.session_client.SessionClient._end_session')
    @patch('botocore.client.BaseClient')
    def test_close(self, mock_client, mock_end_session):
        mock_client.return_value = mock_client
        session = SessionClient(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID)
        session._close()

        mock_end_session.assert_called_once_with()

    @patch('pyqldb.communication.session_client.logger.warning')
    @patch('pyqldb.communication.session_client.SessionClient._end_session')
    @patch('botocore.client.BaseClient')
    def test_close_client_error(self, mock_client, mock_end_session, mock_logger_warning):
        mock_client.return_value = mock_client
        session = SessionClient(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID)
        ce = ClientError(MOCK_CLIENT_ERROR_MESSAGE, MOCK_ERROR_MESSAGE)
        mock_end_session.side_effect = ce

        session._close()
        mock_end_session.assert_called_once_with()
        mock_logger_warning.assert_called_once()

    @patch('botocore.client.BaseClient')
    @patch('pyqldb.communication.session_client.SessionClient._send_command')
    def test_commit_transaction(self, mock_send_command, mock_client):
        mock_client.return_value = mock_client
        mock_send_command.return_value = MOCK_COMMIT_TRANSACTION_RESULT
        session = SessionClient(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID)
        result = session._commit_transaction(MOCK_TRANSACTION_ID, MOCK_COMMIT_DIGEST)

        mock_send_command.assert_called_once_with({'SessionToken': MOCK_TOKEN, 'CommitTransaction':
                                                  {'TransactionId': MOCK_TRANSACTION_ID,
                                                   'CommitDigest': MOCK_COMMIT_DIGEST}})
        self.assertEqual(result, MOCK_VALUES)

    @patch('botocore.client.BaseClient')
    @patch('pyqldb.communication.session_client.SessionClient._send_command')
    def test_end_session(self, mock_send_command, mock_client):
        mock_client.return_value = mock_client
        mock_send_command.return_value = MOCK_END_SESSION_RESULT
        session = SessionClient(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID)
        result = session._end_session()

        mock_send_command.assert_called_once_with({'SessionToken': MOCK_TOKEN, 'EndSession': {}})
        self.assertEqual(result, MOCK_VALUES)

    @patch('botocore.client.BaseClient')
    @patch('pyqldb.communication.session_client.SessionClient._to_value_holder')
    @patch('pyqldb.communication.session_client.SessionClient._send_command')
    def test_execute_statement(self, mock_send_command, mock_to_value_holder, mock_client):
        mock_client.return_value = mock_client
        mock_send_command.return_value = MOCK_EXECUTE_STATEMENT_RESULT
        mock_to_value_holder.return_value = mock_to_value_holder
        session = SessionClient(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID)
        result = session._execute_statement(MOCK_TRANSACTION_ID, MOCK_STATEMENT, MOCK_PARAMETERS)

        mock_send_command.assert_called_once_with({'SessionToken': MOCK_TOKEN, 'ExecuteStatement':
                                                  {'TransactionId': MOCK_TRANSACTION_ID, 'Statement': MOCK_STATEMENT,
                                                   'Parameters': [mock_to_value_holder, mock_to_value_holder]}})
        mock_to_value_holder.assert_has_calls([call(MOCK_PARAMETERS[0]), call(MOCK_PARAMETERS[1])])
        self.assertEqual(result, MOCK_RESULT_RESULT)

    @patch('botocore.client.BaseClient')
    @patch('pyqldb.communication.session_client.SessionClient._to_value_holder')
    @patch('pyqldb.communication.session_client.SessionClient._send_command')
    def test_execute_statement_default_parameters(self, mock_send_command, mock_to_value_holder, mock_client):
        mock_client.return_value = mock_client
        mock_send_command.return_value = MOCK_EXECUTE_STATEMENT_RESULT
        session = SessionClient(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID)
        result = session._execute_statement(MOCK_TRANSACTION_ID, MOCK_STATEMENT, [])

        mock_send_command.assert_called_once_with({'SessionToken': MOCK_TOKEN, 'ExecuteStatement':
                                                  {'TransactionId': MOCK_TRANSACTION_ID, 'Statement': MOCK_STATEMENT,
                                                   'Parameters': []}})
        mock_to_value_holder.assert_not_called()
        mock_send_command.assert_called_once()
        self.assertEqual(result, MOCK_RESULT_RESULT)

    @patch('botocore.client.BaseClient')
    @patch('pyqldb.communication.session_client.SessionClient._send_command')
    def test_fetch_page(self, mock_send_command, mock_client):
        mock_client.return_value = mock_client
        mock_send_command.return_value = MOCK_FETCH_PAGE_RESULT
        session = SessionClient(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID)
        result = session._fetch_page(MOCK_TRANSACTION_ID, MOCK_TOKEN)

        mock_send_command.assert_called_once_with({'SessionToken': MOCK_TOKEN, 'FetchPage':
                                                  {'TransactionId': MOCK_TRANSACTION_ID, 'NextPageToken': MOCK_TOKEN}})
        self.assertEqual(result, MOCK_RESULT_RESULT)

    @patch('botocore.client.BaseClient')
    @patch('pyqldb.communication.session_client.SessionClient._send_command')
    def test_start_transaction(self, mock_send_command, mock_client):
        mock_client.return_value = mock_client
        mock_send_command.return_value = MOCK_START_TRANSACTION_RESULT
        session = SessionClient(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID)
        result = session._start_transaction().get('TransactionId')

        self.assertEqual(result, MOCK_TRANSACTION_ID)
        mock_send_command.assert_called_once_with({'SessionToken': MOCK_TOKEN, 'StartTransaction': {}})

    @patch('botocore.client.BaseClient')
    @patch('pyqldb.communication.session_client.logger.debug')
    def test_send_command(self, mock_logger_debug, mock_client):
        mock_client.return_value = mock_client
        mock_client.send_command.return_value = MOCK_EXECUTE_STATEMENT_RESULT
        session = SessionClient(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID)
        request = {'key': 'value'}
        result = session._send_command(request)

        mock_client.send_command.assert_called_once_with(**request)
        self.assertEqual(result, MOCK_EXECUTE_STATEMENT_RESULT)
        self.assertEqual(mock_logger_debug.call_count, 2)

    @patch('pyqldb.communication.session_client.SessionClient')
    @patch('pyqldb.communication.session_client.logger.debug')
    @patch('botocore.client.BaseClient')
    def test_start_session(self, mock_client, mock_logger_debug, mock_session_client):
        mock_client.return_value = mock_client
        mock_client.send_command.return_value = MOCK_START_SESSION_RESULT
        result = SessionClient._start_session(MOCK_LEDGER_NAME, mock_client)

        mock_client.send_command.assert_called_once_with(StartSession={'LedgerName': MOCK_LEDGER_NAME})
        mock_logger_debug.assert_called_once()
        mock_session_client.assert_called_once_with(MOCK_LEDGER_NAME, MOCK_TOKEN, mock_client, MOCK_ID)
        self.assertEqual(result, mock_session_client())

    @patch('pyqldb.communication.session_client.dumps')
    def test_to_value_holder_type(self, mock_dumps):
        ion_value = loads("{'key' : 'value'}")
        mock_dumps.return_value = MOCK_DUMPS_RETURN_VALUE
        val = SessionClient._to_value_holder(ion_value)

        self.assertEqual(val, {'IonBinary': MOCK_DUMPS_RETURN_VALUE})
