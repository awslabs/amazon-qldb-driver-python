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
from abc import ABC, abstractmethod

from ..execution.executable import Executable


class BaseQldbSession(Executable, ABC):
    """
    An abstract base class representing a session to a specific ledger within QLDB.
    """
    def __enter__(self):
        """
        Context Manager function to support the 'with' statement.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context Manager function to support the 'with' statement.
        """
        self.close()

    @property
    @abstractmethod
    def ledger_name(self):
        """
        Get the ledger name. This method must be overridden.
        """
        pass

    @property
    @abstractmethod
    def session_token(self):
        """
        Get the session token. This method must be overridden.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Close this QldbSession. This method must be overridden.
        """
        pass

    @abstractmethod
    def list_tables(self):
        """
        Get the list of table names in the ledger. This method must be overridden.
        """
        pass

    @abstractmethod
    def start_transaction(self):
        """
        Start a transaction using an available database session. This method must be overridden.
        """
        pass
