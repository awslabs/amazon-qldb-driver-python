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

from botocore.exceptions import ClientError

from pyqldb.errors import is_occ_conflict_exception, is_invalid_session_exception, is_retriable_exception, \
    is_bad_request_exception, is_transaction_expired_exception


class TestErrors(TestCase):

    def test_is_bad_request_exception_true(self):
        clientError = ClientError({'Error': {'Code': 'BadRequestException'}}, 'SendCommand')
        self.assertTrue(is_bad_request_exception(clientError))

    def test_is_bad_request_exception_false(self):
        clientError = ClientError({'Error': {'Code': 'NotBadRequestException'}}, 'SendCommand')
        self.assertFalse(is_bad_request_exception(clientError))

    def test_is_bad_request_exception_not_client_error(self):
        self.assertFalse(is_bad_request_exception(Exception()))

    def test_is_transaction_expired_exception_true(self):
        clientError = ClientError({'Error': {'Code': 'InvalidSessionException',
                                             'Message': 'Transaction xyz has expired'}}, 'SendCommand')
        self.assertTrue(is_transaction_expired_exception(clientError))

    def test_is_transaction_expired_exception_false(self):
        clientError = ClientError({'Error': {'Code': 'InvalidSessionException',
                                             'Message': 'Transaction xyz has not expired'}}, 'SendCommand')
        self.assertFalse(is_transaction_expired_exception(clientError))

    def test_is_transaction_expired_exception_not_client_error(self):
        self.assertFalse(is_transaction_expired_exception(Exception()))

    def test_is_occ_conflict_exception_true(self):
        clientError = ClientError({'Error': {'Code': 'OccConflictException'}}, 'SendCommand')
        self.assertTrue(is_occ_conflict_exception(clientError))

    def test_is_occ_conflict_exception_false(self):
        clientError = ClientError({'Error': {'Code': 'NotOccConflictException'}}, 'SendCommand')
        self.assertFalse(is_occ_conflict_exception(clientError))

    def test_is_occ_conflict_exception_not_client_error(self):
        self.assertFalse(is_occ_conflict_exception(Exception()))

    def test_is_invalid_session_true(self):
        clientError = ClientError({'Error': {'Code': 'InvalidSessionException'}}, 'SendCommand')
        self.assertTrue(is_invalid_session_exception(clientError))

    def test_is_invalid_session_false(self):
        clientError = ClientError({'Error': {'Code': 'NotInvalidSessionException'}}, 'SendCommand')
        self.assertFalse(is_invalid_session_exception(clientError))

    def test_is_invalid_session_not_client_error(self):
        self.assertFalse(is_invalid_session_exception(Exception()))

    def test_is_retryable_exception_is_500_response_code(self):
        clientError = ClientError({'ResponseMetadata': {'HTTPStatusCode': 500},
                                   'Error': {'Code': 'RetryableException'}}, 'SendCommand')
        self.assertTrue(is_retriable_exception(clientError))

    def test_is_retryable_exception_is_503_response_code(self):
        clientError = ClientError({'ResponseMetadata': {'HTTPStatusCode': 503},
                                   'Error': {'Code': 'RetryableException'}}, 'SendCommand')
        self.assertTrue(is_retriable_exception(clientError))

    def test_is_retryable_exception_is_NoHttpResponseException(self):
        clientError = ClientError({'Error': {'Code': 'NoHttpResponseException'},
                                   'ResponseMetadata': {'HTTPStatusCode': 200}}, 'SendCommand')
        self.assertTrue(is_retriable_exception(clientError))

    def test_is_retryable_exception_is_SocketTimeoutException(self):
        clientError = ClientError({'Error': {'Code': 'SocketTimeoutException'},
                                   'ResponseMetadata': {'HTTPStatusCode': 200}}, 'SendCommand')
        self.assertTrue(is_retriable_exception(clientError))

    def test_is_retryable_exception_is_occ_conflict_exception(self):
        clientError = ClientError({'Error': {'Code': 'OccConflictException'},
                                   'ResponseMetadata': {'HTTPStatusCode': 100}}, 'SendCommand')
        self.assertTrue(is_retriable_exception(clientError))

    def test_is_retryable_exception_is_invalid_session_exception(self):
        clientError = ClientError({'Error': {'Code': 'InvalidSessionException'},
                                   'ResponseMetadata': {'HTTPStatusCode': 100}}, 'SendCommand')
        self.assertTrue(is_retriable_exception(clientError))

    def test_is_retryable_exception_is_expired_transaction_exception(self):
        clientError = ClientError({'Error': {'Code': 'InvalidSessionException',
                                             'Message': 'Transaction xyz has expired'},
                                   'ResponseMetadata': {'HTTPStatusCode': 100}}, 'SendCommand')
        self.assertFalse(is_retriable_exception(clientError))

    def test_is_retryable_exception_false(self):
        clientError = ClientError({'Error': {'Code': 'NotRetriableException'},
                                   'ResponseMetadata': {'HTTPStatusCode': 200}}, 'SendCommand')
        self.assertFalse(is_retriable_exception(clientError))

    def test_is_retryable_exception_not_client_exception(self):
        self.assertFalse(is_retriable_exception(Exception()))
