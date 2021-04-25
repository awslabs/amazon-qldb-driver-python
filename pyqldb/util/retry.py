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

import random
import sys

MAX_POW = sys.maxsize.bit_length() - 1
MAX_BACKOFF = 5000


class Retry:
    """
    This util class contains helpers for calculating backoff for retry.
    The class is purely meant for internal use.
    """

    @staticmethod
    def calculate_backoff(retry_config, retry_attempt, error, transaction_id):
        if retry_config.custom_backoff:
            delay = retry_config.custom_backoff(retry_attempt, error, transaction_id)
            if delay < 0:
                delay = 0
        else:
            delay = Retry._get_delay_with_equal_jitter(retry_attempt, retry_config)

        return delay

    @staticmethod
    def _get_delay_with_equal_jitter(retry_attempt, retry_config):
        capped_retries = min(retry_attempt, MAX_POW)
        delay_seed = min(retry_config.base * (1 << capped_retries), MAX_BACKOFF)
        delay = delay_seed / 2 + random.randint(0, delay_seed / 2)

        return delay
