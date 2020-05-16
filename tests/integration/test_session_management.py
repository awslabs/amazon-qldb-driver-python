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
import pytest
from queue import Queue
from unittest import TestCase

from .constants import *
from .integration_test_base import IntegrationTestBase, ThreadThatSavesException

from botocore.exceptions import ClientError
from pyqldb.errors import DriverClosedError, SessionPoolEmptyError


@pytest.mark.usefixtures("config_variables")
class TestSessionManagement(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.integration_test_base = IntegrationTestBase(LEDGER_NAME+cls.ledger_suffix, cls.region)
        cls.integration_test_base.force_delete_ledger()
        cls.integration_test_base.create_ledger()

    @classmethod
    def tearDownClass(cls):
        cls.integration_test_base.delete_ledger()

    def test_connect_to_non_existent_ledger(self):
        with self.integration_test_base.qldb_driver("nonExistentLedger") as qldb_driver:
            self.assertRaises(ClientError, qldb_driver.list_tables)

    def test_get_session_when_pool_does_not_have_session_and_has_not_hit_limit(self):
        # Start a pooled driver with default pool limit so it doesn't have sessions in the pool
        # and has not hit the limit.
        with self.integration_test_base.qldb_driver() as qldb_driver:
            try:
                qldb_driver.list_tables()
            except ClientError as e:
                self.fail(repr(e))

    def test_get_session_when_pool_has_session_and_has_not_hit_limit(self):
        try:
            # Start a pooled driver with default pool limit so it doesn't have sessions in the pool
            # and has not hit the limit.
            with self.integration_test_base.qldb_driver() as qldb_driver:
                # Call the first list_tables() to start session and put into pool.
                qldb_driver.list_tables()
                # Call the second list_tables() to use session from pool and is expected to execute successfully.
                qldb_driver.list_tables()
        except ClientError as e:
            self.fail(repr(e))

    def test_get_session_when_pool_does_not_have_session_and_has_hit_limit(self):
        # Saving exceptions in a queue because Python threads execute in their own stack.
        bucket = Queue()

        # With the time out set to 1 ms, only one thread should go through.
        # The other thread will try to acquire the session, but because it can wait for only 1ms, it will error out.
        with self.integration_test_base.qldb_driver(pool_limit=1, time_out=0.001) as qldb_driver:
            thread_1 = ThreadThatSavesException(target=qldb_driver.list_tables, bucket=bucket)
            thread_2 = ThreadThatSavesException(target=qldb_driver.list_tables, bucket=bucket)

            thread_1.start()
            thread_2.start()

            thread_1.join()
            thread_2.join()

        self.assertEqual(1, bucket.qsize())
        self.assertIsInstance(bucket.get(), SessionPoolEmptyError)

    def test_get_session_when_pool_does_not_have_session_and_has_hit_limit_and_session_is_returned_to_pool(self):
        # Saving exceptions in a queue because Python threads execute in their own stack.
        bucket = Queue()

        # Start a pooled driver with pool limit of 1 and default timeout of 30 seconds.
        with self.integration_test_base.qldb_driver(pool_limit=1, time_out=30) as qldb_driver:
            # Start two threads to execute list_tables() concurrently which will hit the session pool limit but
            # will succeed because session is returned to pool before timing out.
            thread_1 = ThreadThatSavesException(target=qldb_driver.list_tables, bucket=bucket)
            thread_2 = ThreadThatSavesException(target=qldb_driver.list_tables, bucket=bucket)

            thread_1.start()
            thread_2.start()

            thread_1.join()
            thread_2.join()

        self.assertEqual(0, bucket.qsize())

    def test_get_session_when_driver_is_closed(self):
        qldb_driver = self.integration_test_base.qldb_driver()

        qldb_driver.close()
        self.assertRaises(DriverClosedError, qldb_driver.list_tables)
