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

from ..errors import LambdaAbortedError

logger = getLogger(__name__)


class Executor:
    """
    A class to handle lambda execution.

    :type transaction: :py:class:`pyqldb.transaction.transaction.Transaction`
    :param transaction: The transaction that this executor is running within.
    """
    def __init__(self, transaction):
        self._transaction = transaction

    @property
    def transaction_id(self):
        """
        The **read-only** ID of this transaction.
        """
        return self._transaction.transaction_id

    def abort(self):
        """
        Abort the transaction and roll back any changes.

        :raises LambdaAbortedError: When invoked.
        """
        raise LambdaAbortedError

    def execute_statement(self, statement, *parameters):
        """
        Execute the statement.

        :type statement: str
        :param statement: The statement to execute.

        :type parameters: Variable length argument list
        :param parameters: Ion values or Python native types that are convertible to Ion for filling in parameters
                           of the statement.

                           `Details on conversion support and rules <https://ion-python.readthedocs.io/en/latest/amazon.ion.html?highlight=simpleion#module-amazon.ion.simpleion>`_.

        :rtype: :py:class:`pyqldb.cursor.stream_cursor.StreamCursor`
        :return: Cursor on the result set of the statement.

        :raises TransactionClosedError: When this transaction is closed.
        """
        cursor = self._transaction._execute_statement(statement, *parameters)
        return cursor
