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
from unittest import TestCase

from unittest.mock import Mock, patch
from pyqldb.config.retry_config import RetryConfig
from pyqldb.util.retry import Retry


def custom_backoff(retry_attempt, error, transaction_id):
    return 1000

def custom_backoff_with_negative_value(retry_attempt, error, transaction_id):
    return -10

RETRY_CONFIG_WITH_CUSTOM_BACKOFF = RetryConfig(custom_backoff=custom_backoff)
RETRY_CONFIG_WITH_NEGATIVE_BACKOFF = RetryConfig(custom_backoff=custom_backoff_with_negative_value)
RETRY_CONFIG_WITH_DEFAULT_BACKOFF = RetryConfig()
mock_error = Mock()


class TestRetryUtil(TestCase):

    def test_config_with_custom_backoff(self):
        self.assertEqual(Retry.calculate_backoff(RETRY_CONFIG_WITH_CUSTOM_BACKOFF, 1, mock_error, 1), 1000)

    def test_config_with_negative_backoff(self):
        self.assertEqual(Retry.calculate_backoff(RETRY_CONFIG_WITH_NEGATIVE_BACKOFF, 1, mock_error, 1), 0)

    @patch('pyqldb.util.retry.Retry._get_delay_with_equal_jitter')
    def test_config_with_default_backoff(self, mock_delay_method):
        Retry.calculate_backoff(RETRY_CONFIG_WITH_DEFAULT_BACKOFF, 1, mock_error, 1)
        mock_delay_method.assert_called_once_with(1, RETRY_CONFIG_WITH_DEFAULT_BACKOFF)