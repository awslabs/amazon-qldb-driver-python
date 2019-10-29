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
from threading import Lock


class AtomicInteger:
    """
    An integer value that may be updated atomically.
    """
    def __init__(self, value=0):
        self._value = value
        self._lock = Lock()

    def increment(self):
        """
        Atomically increments the current value by one.

        :rtype: int
        :return: The updated integer value.
        """
        with self._lock:
            self._value += 1
            return self._value

    def decrement(self):
        """
        Atomically decrements the current value by one.

        :rtype: int
        :return: The updated integer value.
        """
        with self._lock:
            self._value -= 1
            return self._value

    @property
    def value(self):
        """
        Returns the current value.

        :rtype: int
        :return: The integer value.
        """
        with self._lock:
            return self._value
