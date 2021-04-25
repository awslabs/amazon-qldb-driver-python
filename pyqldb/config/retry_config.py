# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
# the License. A copy of the License is located at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.


class RetryConfig:
    """
    Retry and Backoff Config for QldbDriver

    :type retry_limit: int
    :param retry_limit: The number of automatic retries for statement executions
                        using :py:meth:`pyqldb.driver.qldb_driver.QldbDriver.execute_lambda`
                        when an OCC conflict or retriable exception occurs. This value must not be negative.

    :type base: int
    :param base: The base number of milliseconds to use in the exponential backoff for operation retries.
                 Defaults to 10 ms.

    :type custom_backoff: function
    :param custom_backoff: A custom function that accepts a retry count, error, transaction id and returns the amount
                           of time to delay in milliseconds. If the result is a non-zero negative value the backoff will
                           be considered to be zero.
                           The base option will be ignored if this option is supplied.

    :raises ValueError: When `base` or `retry_limit` are negative.
    """

    def __init__(self, retry_limit=4, base=10, custom_backoff=None):
        self._validate_base(base)
        self._validate_retry_limit(retry_limit)
        self._base = base
        self._retry_limit = retry_limit
        self._custom_backoff = custom_backoff

    @property
    def retry_limit(self):
        """
        The number of automatic retries for statement executions using
        :py:meth:`pyqldb.driver.qldb_driver.QldbDriver.execute_lambda` when an OCC conflict or
        retriable exception occurs. This value must not be negative.
        """
        return self._retry_limit

    @property
    def base(self):
        """
        The base number of milliseconds to use in the exponential backoff for operation retries.
        Defaults to 10 ms.
        """
        return self._base

    @property
    def custom_backoff(self):
        """
        A custom function that accepts a retry count, error, transaction id and returns the amount
        of time to delay in milliseconds. If the result is a non-zero negative value the backoff will
        be considered to be zero and will result in no delay.
        The base option will be ignored if this option is supplied.
        """
        return self._custom_backoff

    @staticmethod
    def _validate_base(base):
        if base < 0:
            raise ValueError("Base cannot be negative")

    @staticmethod
    def _validate_retry_limit(retry_limit):
        if retry_limit < 0:
            raise ValueError("retry_limit cannot be negative")
