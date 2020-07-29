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

import re


class IllegalStateError(Exception):
    pass


class DriverClosedError(IllegalStateError):
    def __init__(self):
        super().__init__('Cannot invoke methods on a closed driver. Please create a new driver and retry.')


class ResultClosedError(IllegalStateError):
    def __init__(self, session_token):
        super().__init__('A streamed result is only valid when the parent transaction is open. Please start a new '
                         'transaction and retry.\nSessionToken: {}'.format(session_token))


class SessionClosedError(IllegalStateError):
    def __init__(self):
        super().__init__('Cannot invoke methods on a closed session. Please start a new session and retry.')


class TransactionClosedError(IllegalStateError):
    def __init__(self):
        super().__init__('Cannot invoke methods on a closed transaction. Please start a new transaction and retry.')


class LambdaAbortedError(Exception):
    def __init__(self):
        super().__init__('Abort invoked; halting execution of lambda function.')


class SessionPoolEmptyError(Exception):
    def __init__(self, timeout):
        super().__init__('Session pool is empty after waiting for {} seconds. Please close existing sessions first '
                         'before retrying.'.format(str(timeout)))


class StartTransactionError(Exception):
    def __init__(self, error):
        super().__init__('Failed to start transaction')
        self.error = error


def is_occ_conflict_exception(e):
    """
    Is the exception an OccConflictException?

    :type e: :py:class:`botocore.exceptions.ClientError`
    :param e: The ClientError caught.

    :rtype: bool
    :return: True if the exception is an OccConflictException. False otherwise.
    """
    is_occ = e.response['Error']['Code'] == 'OccConflictException'
    return is_occ


def is_bad_request_exception(e):
    """
    Is the exception a BadRequestException?

    :type e: :py:class:`botocore.exceptions.ClientError`
    :param e: The ClientError caught.

    :rtype: bool
    :return: True if the exception is an BadRequestException. False otherwise.
    """
    is_bad_request = e.response['Error']['Code'] == "BadRequestException"
    return is_bad_request


def is_invalid_session_exception(e):
    """
    Is the exception an InvalidSessionException?

    :type e: :py:class:`botocore.exceptions.ClientError`
    :param e: The ClientError caught.

    :rtype: bool
    :return: True if the exception is an InvalidSessionException. False otherwise.
    """
    is_invalid_session = e.response['Error']['Code'] == 'InvalidSessionException'
    return is_invalid_session


def is_transaction_expired_exception(e):
    """
    Does this exception denote that a transaction has expired?

    :type e: :py:class:`botocore.exceptions.ClientError`
    :param e: The ClientError caught.

    :rtype: bool
    :return: True if the exception denote that a transaction has expired. False otherwise.
    """
    is_invalid_session = e.response['Error']['Code'] == 'InvalidSessionException'

    if "Message" in e.response["Error"]:
        return is_invalid_session and re.search("Transaction .* has expired", e.response["Error"]["Message"])

    return False


def is_retriable_exception(e):
    """
    Is the exception a retriable exception?

    :type e: :py:class:`botocore.exceptions.ClientError`
    :param e: The ClientError caught.

    :rtype: bool
    :return: True if the exception is a retriable exception. False otherwise.
    """
    is_retriable = e.response['ResponseMetadata']['HTTPStatusCode'] == 500 or \
                   e.response['ResponseMetadata']['HTTPStatusCode'] == 503 or \
                   e.response['Error']['Code'] == 'NoHttpResponseException' or \
                   e.response['Error']['Code'] == 'SocketTimeoutException'
    return is_retriable
