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


class Executable(ABC):
    """
    An abstract base class representing the functionality of execution against QLDB.
    """
    @abstractmethod
    def execute_statement(self, statement, *parameters, retry_indicator):
        """
        Implicitly start a transaction, execute the statement, and commit the transaction, retrying up to the
        retry limit if an OCC conflict or retriable exception occurs. This method must be overridden.
        """
        pass

    @abstractmethod
    def execute_lambda(self, query_lambda, retry_indicator):
        """
        Implicitly start a transaction, execute the lambda function, and commit the transaction, retrying up to the
        retry limit if an OCC conflict or retriable exception occurs. This method must be overridden.
        """
        pass
