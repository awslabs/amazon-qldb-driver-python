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
from logging import getLogger

from ..errors import DriverClosedError
from .base_qldb_driver import BaseQldbDriver

logger = getLogger(__name__)


class QldbDriver(BaseQldbDriver):
    """
    Represents a factory for creating sessions to a specific ledger within QLDB. This class or
    :py:class:`pyqldb.driver.pooled_qldb_driver.PooledQldbDriver` should be the main entry points to any interaction
    with QLDB. :py:meth:`pyqldb.driver.qldb_driver.QldbDriver.get_session` will create a
    :py:class:`pyqldb.session.qldb_session.QldbSession` to the specified edger within QLDB as a communication channel.
    Any sessions acquired should be cleaned up with :py:meth:`pyqldb.session.qldb_session.QldbSession.close` to free up
    resources.

    This factory does not attempt to re-use or manage sessions in any way. It is recommended to use
    :py:class:`pyqldb.driver.pooled_qldb_driver.PooledQldbDriver` for both less resource usage and lower latency.
    """

    def get_session(self):
        """
        Create and return a newly instantiated QldbSession object. This will implicitly start a new session with QLDB.

        :rtype: :py:class:`pyqldb.session.qldb_session.QldbSession`
        :return: A QldbSession object.

        :raises DriverClosedError: When this driver is closed.
        """
        if self._is_closed:
            raise DriverClosedError

        logger.debug('Creating a new session.')
        return self._create_new_session()
