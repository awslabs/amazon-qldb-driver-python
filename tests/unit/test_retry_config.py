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

from pyqldb.config.retry_config import RetryConfig

DEFAULT_RETRY_LIMIT = 4
DEFAULT_BASE = 10


class TestRetryConfig(TestCase):

    def test_default_config(self):
        config = RetryConfig()
        self.assertEqual(config.retry_limit, DEFAULT_RETRY_LIMIT)
        self.assertEqual(config.custom_backoff, None)
        self.assertEqual(config.base, 10)

    def test_config_custom_settings(self):
        def custom_backoff(retry_attempt, error, txn_id):
            return 1000

        config = RetryConfig(retry_limit=6, custom_backoff=custom_backoff, base=100)
        self.assertEqual(config.retry_limit, 6)
        self.assertEqual(config.custom_backoff, custom_backoff)
        self.assertEqual(config.base, 100)

    def test_config_with_negative_retry_limit(self):
        self.assertRaises(ValueError, RetryConfig, retry_limit=-8)

    def test_config_with_negative_base(self):
        self.assertRaises(ValueError, RetryConfig, base=-10)
