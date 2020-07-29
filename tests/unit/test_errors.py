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

from pyqldb.errors import is_occ_conflict_exception, is_invalid_session_exception, is_retriable_exception, \
    is_bad_request_exception, is_transaction_expired_exception


class TestErrors(TestCase):

    @patch('botocore.exceptions.ClientError')
    def test_is_bad_request_exception_true(self, mock_client_error):
        mock_client_error.response = {'Error': {'Code': 'BadRequestException'}}
        self.assertTrue(is_bad_request_exception(mock_client_error))

    @patch('botocore.exceptions.ClientError')
    def test_is_bad_request_exception_false(self, mock_client_error):
        mock_client_error.response = {'Error': {'Code': 'NotBadRequestException'}}
        self.assertFalse(is_bad_request_exception(mock_client_error))


    @patch('botocore.exceptions.ClientError')
    def test_is_transaction_expired_exception(self, mock_client_error):
        mock_client_error.response = {'Error': {'Code': 'InvalidSessionException',
                                                'Message': 'Transaction xyz has expired'}}
        self.assertTrue(is_transaction_expired_exception(mock_client_error))

    @patch('botocore.exceptions.ClientError')
    def test_is_bad_request_exception_false(self, mock_client_error):
        mock_client_error.response = {'Error': {'Code': 'InvalidSessionException',
                                                'Message': 'Transaction xyz has not expired'}}
        self.assertFalse(is_transaction_expired_exception(mock_client_error))

    @patch('botocore.exceptions.ClientError')
    def test_is_occ_conflict_exception_true(self, mock_client_error):
        mock_client_error.response = {'Error': {'Code': 'OccConflictException'}}
        self.assertTrue(is_occ_conflict_exception(mock_client_error))

    @patch('botocore.exceptions.ClientError')
    def test_is_occ_conflict_exception_false(self, mock_client_error):
        mock_client_error.response = {'Error': {'Code': 'NotOccConflictException'}}
        self.assertFalse(is_occ_conflict_exception(mock_client_error))

    @patch('botocore.exceptions.ClientError')
    def test_is_invalid_session_true(self, mock_client_error):
        mock_client_error.response = {'Error': {'Code': 'InvalidSessionException'}}
        self.assertTrue(is_invalid_session_exception(mock_client_error))

    @patch('botocore.exceptions.ClientError')
    def test_is_invalid_session_false(self, mock_client_error):
        mock_client_error.response = {'Error': {'Code': 'NotInvalidSessionException'}}
        self.assertFalse(is_invalid_session_exception(mock_client_error))

    @patch('botocore.exceptions.ClientError')
    def test_is_retriable_exception_is_500_response_code(self, mock_client_error):
        mock_client_error.response = {'ResponseMetadata': {'HTTPStatusCode': 500},
                                      'Error': {'Code': 'NotRetriableException'}}
        self.assertTrue(is_retriable_exception(mock_client_error))

    @patch('botocore.exceptions.ClientError')
    def test_is_retriable_exception_is_503_response_code(self, mock_client_error):
        mock_client_error.response = {'ResponseMetadata': {'HTTPStatusCode': 503},
                                      'Error': {'Code': 'NotRetriableException'}}
        self.assertTrue(is_retriable_exception(mock_client_error))

    @patch('botocore.exceptions.ClientError')
    def test_is_retriable_exception_is_NoHttpResponseException(self, mock_client_error):
        mock_client_error.response = {'Error': {'Code': 'NoHttpResponseException'},
                                      'ResponseMetadata': {'HTTPStatusCode': 200}}
        self.assertTrue(is_retriable_exception(mock_client_error))

    @patch('botocore.exceptions.ClientError')
    def test_is_retriable_exception_is_SocketTimeoutException(self, mock_client_error):
        mock_client_error.response = {'Error': {'Code': 'SocketTimeoutException'},
                                      'ResponseMetadata': {'HTTPStatusCode': 200}}
        self.assertTrue(is_retriable_exception(mock_client_error))

    @patch('botocore.exceptions.ClientError')
    def test_is_retriable_exception_false(self, mock_client_error):
        mock_client_error.response = {'Error': {'Code': 'NotRetriableException'},
                                      'ResponseMetadata': {'HTTPStatusCode': 200}}
        self.assertFalse(is_retriable_exception(mock_client_error))
