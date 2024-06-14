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
from logging import basicConfig, getLogger, INFO
from threading import Thread
from time import sleep

import boto3
from botocore.exceptions import ClientError
from pyqldb.config.retry_config import RetryConfig
from pyqldb.driver.qldb_driver import QldbDriver

logger = getLogger(__name__)
basicConfig(level=INFO)


class IntegrationTestBase:

    def __init__(self, ledger_name, region):
        self.ledger_name = ledger_name
        self.region = region
        session = boto3.Session()
        self.qldb = session.client(service_name="qldb", region_name=self.region)

    def force_delete_ledger(self):
        try:
            logger.info("Deleting ledger %s", self.ledger_name)
            self.delete_ledger()
        except (self.qldb.exceptions.ResourceInUseException, self.qldb.exceptions.ResourcePreconditionNotMetException) as e:
            logger.warning(e)
            # Test deleting state
            self.wait_for_active()
            self.delete_ledger()

    def delete_ledger(self):
        logger.info("Deleting ledger %s", self.ledger_name)
        try:
            self.qldb.update_ledger(Name=self.ledger_name, DeletionProtection=False)
        except self.qldb.exceptions.ResourceNotFoundException:
            return
        self.qldb.delete_ledger(Name=self.ledger_name)
        self.wait_for_deletion()

    def create_ledger(self):
        logger.info("Creating ledger named: {}...".format(self.ledger_name))
        self.qldb.create_ledger(Name=self.ledger_name, PermissionsMode='ALLOW_ALL')
        self.wait_for_active()

    def wait_for_active(self):
        logger.info('Waiting for ledger to become active...')
        while True:
            result = self.qldb.describe_ledger(Name=self.ledger_name)
            if result.get('State') == "ACTIVE":
                logger.info('Success. Ledger is active and ready to use.')
                return result
            logger.info('The ledger is still creating. Please wait...')
            sleep(5)

    def wait_for_deletion(self):
        logger.info('Waiting for ledger to be deleted...')
        while True:
            try:
                self.qldb.describe_ledger(Name=self.ledger_name)
                sleep(5)
                logger.info('The ledger is still deleting. Please wait...')
            except self.qldb.exceptions.ResourceNotFoundException:
                logger.info('The ledger is deleted')
                return

    def qldb_driver(self, ledger_name=None, max_concurrent_transactions=0, retry_limit=4, custom_backoff=None):
        if ledger_name is not None:
            ledger_name = ledger_name
        else:
            ledger_name = self.ledger_name

        retry_config = RetryConfig(retry_limit=retry_limit, custom_backoff=custom_backoff)

        return QldbDriver(ledger_name=ledger_name, region_name=self.region,
                          max_concurrent_transactions=max_concurrent_transactions, retry_config=retry_config)


class ThreadThatSavesException(Thread):
    """
    Extend the Python Thread library to pass in a queue for saving exceptions.
    """

    def __init__(self, target, bucket, args=()):
        Thread.__init__(self, target=target, args=args)
        self.bucket = bucket

    def run(self):
        try:
            Thread.run(self)
        except Exception as e:
            self.bucket.put(e)
