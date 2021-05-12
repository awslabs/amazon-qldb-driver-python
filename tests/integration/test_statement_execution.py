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
from time import sleep
import pytest
from unittest import TestCase

from amazon.ion.simple_types import IonPyNull
from amazon.ion.simpleion import dumps, loads
from botocore.exceptions import ClientError
from pyqldb.errors import is_occ_conflict_exception

from .constants import CREATE_TABLE_NAME, COLUMN_NAME, MULTIPLE_DOCUMENT_VALUE_1, MULTIPLE_DOCUMENT_VALUE_2, \
    MULTIPLE_DOCUMENT_VALUE_3, INDEX_ATTRIBUTE, LEDGER_NAME, SINGLE_DOCUMENT_VALUE, TABLE_NAME
from .integration_test_base import IntegrationTestBase


@pytest.mark.usefixtures("config_variables")
class TestStatementExecution(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.integration_test_base = IntegrationTestBase(LEDGER_NAME+cls.ledger_suffix, cls.region)
        cls.integration_test_base.force_delete_ledger()
        cls.integration_test_base.create_ledger()

        cls.qldb_driver = cls.integration_test_base.qldb_driver()

        def custom_backoff(retry_attempt, error, txn_id):
            return 0

        cls.qldb_driver_with_custom_backoff = cls.integration_test_base.qldb_driver(custom_backoff=custom_backoff)

        # Create table.
        cls.qldb_driver.execute_lambda(lambda txn:
                                       txn.execute_statement("CREATE TABLE {}".format(TABLE_NAME)))
        cls.qldb_driver.list_tables()

    @classmethod
    def tearDownClass(cls):
        cls.qldb_driver.close()
        cls.integration_test_base.delete_ledger()

    def tearDown(self):
        # Delete all documents in table.
        self.qldb_driver.execute_lambda(lambda txn: txn.execute_statement("DELETE FROM {}".format(TABLE_NAME)))

    def test_drop_an_existing_table(self):
        # Given.
        query = "CREATE TABLE {}".format(CREATE_TABLE_NAME)

        def execute_statement_and_return_count(txn, query):
            cursor = txn.execute_statement(query)
            count = 0
            for row in cursor:
                count += 1
            return count

        create_table_count = self.qldb_driver.execute_lambda(lambda txn:
                                                             execute_statement_and_return_count(txn, query))
        self.assertEqual(1, create_table_count)

        table_cursor = self.qldb_driver.list_tables()
        tables = list()
        for row in table_cursor:
            tables.append(row)

        self.assertTrue(CREATE_TABLE_NAME in tables)

        query = "DROP TABLE {}".format(CREATE_TABLE_NAME)

        def execute_statement_and_return_count(txn, query):
            cursor = txn.execute_statement(query)
            count = 0
            for row in cursor:
                count += 1
            return count

        # When.
        drop_table_count = self.qldb_driver.execute_lambda(lambda txn:
                                                           execute_statement_and_return_count(txn, query))
        # Then.
        self.assertEqual(1, drop_table_count)

    def test_list_tables(self):
        # When.
        cursor = self.qldb_driver.list_tables()

        # Then.
        tables = list()
        for row in cursor:
            tables.append(row)

        self.assertTrue(TABLE_NAME in tables)
        self.assertTrue(len(tables) == 1)

    def test_list_tables_custom_backoff(self):
        # When.
        cursor = self.qldb_driver_with_custom_backoff.list_tables()

        # Then.
        tables = list()
        for row in cursor:
            tables.append(row)

        self.assertTrue(TABLE_NAME in tables)
        self.assertTrue(len(tables) == 1)

    def test_create_table_that_already_exists(self):
        # Given.
        query = "CREATE TABLE {}".format(TABLE_NAME)

        # Then.
        self.assertRaises(ClientError, self.qldb_driver.execute_lambda, lambda txn: txn.execute_statement(query))

    def test_create_index(self):
        # Given.
        query = "CREATE INDEX on {} ({})".format(TABLE_NAME, INDEX_ATTRIBUTE)

        def execute_statement_and_return_count(txn, query):
            cursor = txn.execute_statement(query)
            count = 0
            for row in cursor:
                count += 1
            return count

        # When.
        count = self.qldb_driver.execute_lambda(lambda txn: execute_statement_and_return_count(txn, query))
        self.assertEqual(1, count)

        # Then.
        search_query = "SELECT VALUE indexes[0] FROM information_schema.user_tables WHERE status = 'ACTIVE' " \
                       "AND name = '{}'".format(TABLE_NAME)

        def execute_statement_and_return_index_value(txn, query):
            cursor = txn.execute_statement(query)
            # Extract the index name by quering the information_schema.
            # This gives:
            # {
            #    expr: "[MyColumn]"
            # }
            for row in cursor:
                return row['expr']

        value = self.qldb_driver.execute_lambda(lambda txn:
                                                execute_statement_and_return_index_value(txn, search_query))
        self.assertEqual("[" + INDEX_ATTRIBUTE + "]", value)

    def test_returns_empty_when_no_records_are_found(self):
        # Given.
        query = "SELECT * FROM {}".format(TABLE_NAME)

        def execute_statement_and_return_count(txn, query):
            cursor = txn.execute_statement(query)
            count = 0
            for row in cursor:
                count += 1
            return count

        # When.
        count = self.qldb_driver.execute_lambda(lambda txn: execute_statement_and_return_count(txn, query))

        # Then.
        self.assertEqual(0, count)

    def test_insert_document(self):
        # Given.
        # Create Ion struct to insert.
        ion_value = loads(dumps({COLUMN_NAME: SINGLE_DOCUMENT_VALUE}))

        query = "INSERT INTO {} ?".format(TABLE_NAME)

        def execute_statement_with_parameter_and_return_count(txn, query, parameter):
            cursor = txn.execute_statement(query, parameter)
            count = 0
            for row in cursor:
                count += 1
            return count

        # When.
        count = self.qldb_driver.execute_lambda(lambda txn:
                                                execute_statement_with_parameter_and_return_count(txn, query,
                                                                                                  ion_value))
        self.assertEqual(1, count)

        # Then.
        search_query = "SELECT VALUE {} FROM {} WHERE {} = ?".format(COLUMN_NAME, TABLE_NAME, COLUMN_NAME)
        ion_string = loads(dumps(SINGLE_DOCUMENT_VALUE))

        value = self.qldb_driver.execute_lambda(
            lambda txn: execute_statement_and_return_value(txn, search_query, ion_string))
        self.assertEqual(SINGLE_DOCUMENT_VALUE, value)

    def test_read_single_field(self):
        # Given.
        # Create Ion struct to insert.
        ion_value = loads(dumps({COLUMN_NAME: SINGLE_DOCUMENT_VALUE}))

        query = "INSERT INTO {} ?".format(TABLE_NAME)

        def execute_statement_with_parameter_and_return_count(txn, query, parameter):
            cursor = txn.execute_statement(query, parameter)
            count = 0
            for row in cursor:
                count += 1
            return count

        count = self.qldb_driver.execute_lambda(
            lambda txn: execute_statement_with_parameter_and_return_count(txn, query, ion_value))
        self.assertEqual(1, count)

        search_query = "SELECT VALUE {} FROM {} WHERE {} = ?".format(COLUMN_NAME, TABLE_NAME, COLUMN_NAME)
        ion_string = loads(dumps(SINGLE_DOCUMENT_VALUE))

        # When.
        value = self.qldb_driver.execute_lambda(
            lambda txn: execute_statement_and_return_value(txn, search_query, ion_string))

        # Then.
        self.assertEqual(SINGLE_DOCUMENT_VALUE, value)

    def test_query_table_enclosed_in_quotes(self):
        # Given.
        # Create Ion struct to insert.
        ion_value = loads(dumps({COLUMN_NAME: SINGLE_DOCUMENT_VALUE}))

        query = "INSERT INTO {} ?".format(TABLE_NAME)

        def execute_statement_with_parameter_and_return_count(txn, query, parameter):
            cursor = txn.execute_statement(query, parameter)
            count = 0
            for row in cursor:
                count += 1
            return count

        count = self.qldb_driver.execute_lambda(
            lambda txn: execute_statement_with_parameter_and_return_count(txn, query, ion_value))
        self.assertEqual(1, count)

        search_query = "SELECT VALUE {} FROM \"{}\" WHERE {} = ?".format(COLUMN_NAME, TABLE_NAME, COLUMN_NAME)
        ion_string = loads(dumps(SINGLE_DOCUMENT_VALUE))

        # When.
        value = self.qldb_driver.execute_lambda(
            lambda txn: execute_statement_and_return_value(txn, search_query, ion_string))

        # Then.
        self.assertEqual(SINGLE_DOCUMENT_VALUE, value)

    def test_query_stats(self):
        def fill_table():
            # Create Ion structs to insert to populate table
            parameter_1 = loads(dumps({COLUMN_NAME: MULTIPLE_DOCUMENT_VALUE_1}))
            parameter_2 = loads(dumps({COLUMN_NAME: MULTIPLE_DOCUMENT_VALUE_2}))
            parameter_3 = loads(dumps({COLUMN_NAME: MULTIPLE_DOCUMENT_VALUE_3}))
            query = "INSERT INTO {} <<?, ?, ?>>".format(TABLE_NAME)

            self.qldb_driver.execute_lambda(
                lambda txn: txn.execute_statement(query, parameter_1, parameter_2, parameter_3))

        def assert_io_usage_and_timing_information(cursor):
            # Consumed IO test
            consumed_ios = cursor.get_consumed_ios()
            self.assertIsNotNone(consumed_ios)
            read_ios = consumed_ios.get('ReadIOs')
            self.assertTrue(read_ios >= 0)

            # Timing information test
            timing_information = cursor.get_timing_information()
            self.assertIsNotNone(timing_information)
            processing_time_milliseconds = timing_information.get('ProcessingTimeMilliseconds')
            self.assertTrue(processing_time_milliseconds >= 0)

        def stream_cursor_query_stats(query):
            def execute_statement_and_test_stream_cursor(txn, query):
                cursor = txn.execute_statement(query)
                for row in cursor:
                    assert_io_usage_and_timing_information(cursor)

            self.qldb_driver.execute_lambda(
                lambda txn: execute_statement_and_test_stream_cursor(txn, query))

        def buffered_cursor_query_stats(query):
            # Test buffered cursor's accumulated values of execution stats
            def execute_statement_and_test_buffered_cursor(txn, query):
                cursor = txn.execute_statement(query)
                return cursor

            buffered_cursor = self.qldb_driver.execute_lambda(
                lambda txn: execute_statement_and_test_buffered_cursor(txn, query))
            assert_io_usage_and_timing_information(buffered_cursor)

        search_query = "SELECT * FROM {} as n1, {} as n2, {} as n3, {} as n4, {} as n5, {} as n6".format(TABLE_NAME,
                                                                                                         TABLE_NAME,
                                                                                                         TABLE_NAME,
                                                                                                         TABLE_NAME,
                                                                                                         TABLE_NAME,
                                                                                                         TABLE_NAME)
        # Call the routines
        fill_table()
        stream_cursor_query_stats(search_query)
        buffered_cursor_query_stats(search_query)

    def test_insert_multiple_documents(self):
        # Given.
        # Create Ion structs to insert.
        parameter_1 = loads(dumps({COLUMN_NAME: MULTIPLE_DOCUMENT_VALUE_1}))
        parameter_2 = loads(dumps({COLUMN_NAME: MULTIPLE_DOCUMENT_VALUE_2}))

        query = "INSERT INTO {} <<?, ?>>".format(TABLE_NAME)

        def execute_statement_with_parameters_and_return_count(txn, query, parameter_1, parameter_2):
            cursor = txn.execute_statement(query, parameter_1, parameter_2)
            count = 0
            for row in cursor:
                count += 1
            return count

        # When.
        count = self.qldb_driver.execute_lambda(
            lambda txn: execute_statement_with_parameters_and_return_count(txn, query, parameter_1, parameter_2))
        self.assertEqual(2, count)

        # Then.
        search_query = "SELECT VALUE {} FROM {} WHERE {} IN (?,?)".format(COLUMN_NAME, TABLE_NAME, COLUMN_NAME)

        ion_string_1 = loads(dumps(MULTIPLE_DOCUMENT_VALUE_1))
        ion_string_2 = loads(dumps(MULTIPLE_DOCUMENT_VALUE_2))

        def execute_statement_with_parameters_and_return_list_of_values(txn, query, *parameters):
            cursor = txn.execute_statement(query, *parameters)
            values = list()
            for row in cursor:
                values.append(row)
            return values

        values = self.qldb_driver.execute_lambda(
            lambda txn: execute_statement_with_parameters_and_return_list_of_values(txn, search_query, ion_string_1,
                                                                                    ion_string_2))
        self.assertTrue(MULTIPLE_DOCUMENT_VALUE_1 in values)
        self.assertTrue(MULTIPLE_DOCUMENT_VALUE_2 in values)

    def test_delete_single_document(self):
        # Given.
        # Create Ion struct to insert.
        ion_value = loads(dumps({COLUMN_NAME: SINGLE_DOCUMENT_VALUE}))

        query = "INSERT INTO {} ?".format(TABLE_NAME)

        def execute_statement_with_parameter_and_return_count(txn, query, parameter):
            cursor = txn.execute_statement(query, parameter)
            count = 0
            for row in cursor:
                count += 1
            return count

        count = self.qldb_driver.execute_lambda(
            lambda txn: execute_statement_with_parameter_and_return_count(txn, query, ion_value))
        self.assertEqual(1, count)

        # When.
        delete_query = "DELETE FROM {} WHERE {} = ?".format(TABLE_NAME, COLUMN_NAME)
        ion_string = loads(dumps(SINGLE_DOCUMENT_VALUE))

        def execute_statement_and_return_count(txn, query, *parameters):
            cursor = txn.execute_statement(query, *parameters)
            count = 0
            for row in cursor:
                count += 1
            return count

        count = self.qldb_driver.execute_lambda(
            lambda txn: execute_statement_and_return_count(txn, delete_query, ion_string))
        self.assertEqual(1, count)

        # Then.
        def execute_count_statement_and_return_count(txn):
            search_query = "SELECT COUNT(*) FROM {}".format(TABLE_NAME)
            # This gives:
            # {
            #    _1: 1
            # }
            cursor = txn.execute_statement(search_query)
            for row in cursor:
                return row['_1']

        count = self.qldb_driver.execute_lambda(lambda txn: execute_count_statement_and_return_count(txn))
        self.assertEqual(0, count)

    def test_delete_all_documents(self):
        # Given.
        # Create Ion structs to insert.
        parameter_1 = loads(dumps({COLUMN_NAME: MULTIPLE_DOCUMENT_VALUE_1}))
        parameter_2 = loads(dumps({COLUMN_NAME: MULTIPLE_DOCUMENT_VALUE_2}))

        query = "INSERT INTO {} <<?, ?>>".format(TABLE_NAME)

        def execute_statement_with_parameters_and_return_count(txn, query, parameter_1, parameter_2):
            cursor = txn.execute_statement(query, parameter_1, parameter_2)
            count = 0
            for row in cursor:
                count += 1
            return count

        count = self.qldb_driver.execute_lambda(
            lambda txn: execute_statement_with_parameters_and_return_count(txn, query, parameter_1, parameter_2))
        self.assertEqual(2, count)

        # When.
        delete_query = "DELETE FROM {}".format(TABLE_NAME, COLUMN_NAME)

        def execute_statement_and_return_count(txn, query):
            cursor = txn.execute_statement(query)
            count = 0
            for row in cursor:
                count += 1
            return count

        count = self.qldb_driver.execute_lambda(
            lambda txn: execute_statement_and_return_count(txn, delete_query))
        self.assertEqual(2, count)

        # Then.
        def execute_count_statement_and_return_count(txn):
            search_query = "SELECT COUNT(*) FROM {}".format(TABLE_NAME)
            # This gives:
            # {
            #    _1: 1
            # }
            cursor = txn.execute_statement(search_query)
            for row in cursor:
                return row['_1']

        count = self.qldb_driver.execute_lambda(lambda txn: execute_count_statement_and_return_count(txn))
        self.assertEqual(0, count)

    def test_occ_exception_is_thrown(self):
        # Create driver with zero retry limit to trigger OCC exception.
        driver = self.integration_test_base.qldb_driver(retry_limit=0)

        # Insert document.
        ion_value = loads(dumps({COLUMN_NAME: 0}))

        def execute_statement_with_parameter_and_return_count(txn, query, parameter):
            cursor = txn.execute_statement(query, parameter)
            count = 0
            for row in cursor:
                count += 1
            return count

        query = "INSERT INTO {} ?".format(TABLE_NAME)
        count = driver.execute_lambda(lambda txn: execute_statement_with_parameter_and_return_count(txn, query,
                                                                                                    ion_value))
        self.assertEqual(1, count)

        # For testing purposes only. Forcefully causes an OCC conflict to occur.
        # Do not invoke driver.execute_lambda within the parameter function under normal circumstances.
        def query_and_update_record(transaction_executor):
            # Query document.
            transaction_executor.execute_statement("SELECT VALUE {} FROM {}".format(COLUMN_NAME, TABLE_NAME))
            # The following update document will be committed before query document thus resulting in OCC.
            driver.execute_lambda(lambda transaction_executor:
                                  transaction_executor.execute_statement(
                                      "UPDATE {} SET {} = ?".format(TABLE_NAME, COLUMN_NAME), 5))

        try:
            driver.execute_lambda(lambda txn: query_and_update_record(txn))
            self.fail("Did not throw OCC exception.")
        except ClientError as ce:
            self.assertTrue(is_occ_conflict_exception(ce))

    def test_insert_and_read_ion_types(self):
        # Use subTest context manager to setup parameterized tests.
        for ion_value in create_ion_values():
            with self.subTest(ion_value=ion_value):
                # Given.
                # Create Ion struct to insert.
                ion_struct = loads(dumps({COLUMN_NAME: ion_value}))
                query = "INSERT INTO {} ?".format(TABLE_NAME)

                def execute_statement_and_return_count(txn, query, *parameter):
                    cursor = txn.execute_statement(query, *parameter)
                    count = 0
                    for row in cursor:
                        count += 1
                    return count

                # When.
                count = self.qldb_driver.execute_lambda(
                    lambda txn: execute_statement_and_return_count(txn, query, ion_struct))
                self.assertEqual(1, count)

                # Then.
                if isinstance(ion_value, IonPyNull):
                    search_query = "SELECT VALUE {} FROM {} WHERE {} IS NULL".format(COLUMN_NAME, TABLE_NAME,
                                                                                     COLUMN_NAME)
                    value = self.qldb_driver.execute_lambda(lambda txn:
                                                            execute_statement_and_return_value(txn,
                                                                                               search_query))

                else:
                    search_query = "SELECT VALUE {} FROM {} WHERE {} = ?".format(COLUMN_NAME, TABLE_NAME, COLUMN_NAME)
                    value = self.qldb_driver.execute_lambda(lambda txn:
                                                            execute_statement_and_return_value(txn,
                                                                                               search_query,
                                                                                               ion_value))

                self.assertEqual(ion_value.ion_type, value.ion_type)

                # Delete documents in table for testing next Ion value.
                self.qldb_driver.execute_lambda(lambda txn:
                                                txn.execute_statement("DELETE FROM {}".format(TABLE_NAME,
                                                                                              COLUMN_NAME)))

    def test_update_ion_types(self):
        # Given.
        # Create Ion struct to insert.
        ion_value = loads(dumps({COLUMN_NAME: SINGLE_DOCUMENT_VALUE}))

        def execute_statement_and_return_count(txn, query, *parameter):
            cursor = txn.execute_statement(query, *parameter)
            count = 0
            for row in cursor:
                count += 1
            return count

        # Insert first record that will be subsequently updated.
        query = "INSERT INTO {} ?".format(TABLE_NAME)
        count = self.qldb_driver.execute_lambda(lambda txn:
                                                execute_statement_and_return_count(txn, query, ion_value))
        self.assertEqual(1, count)

        # Use subTest context manager to setup parameterized tests.
        for ion_value in create_ion_values():
            with self.subTest(ion_value=ion_value):
                # When.
                query = "UPDATE {} SET {} = ?".format(TABLE_NAME, COLUMN_NAME)
                count = self.qldb_driver.execute_lambda(lambda txn:
                                                        execute_statement_and_return_count(txn, query,
                                                                                           ion_value))
                self.assertEqual(1, count)

                # Then.
                if isinstance(ion_value, IonPyNull):
                    search_query = "SELECT VALUE {} FROM {} WHERE {} IS NULL".format(COLUMN_NAME, TABLE_NAME,
                                                                                     COLUMN_NAME)
                    value = self.qldb_driver.execute_lambda(lambda txn:
                                                            execute_statement_and_return_value(txn,
                                                                                               search_query))

                else:
                    search_query = "SELECT VALUE {} FROM {} WHERE {} = ?".format(COLUMN_NAME, TABLE_NAME, COLUMN_NAME)
                    value = self.qldb_driver.execute_lambda(lambda txn:
                                                            execute_statement_and_return_value(txn,
                                                                                               search_query,
                                                                                               ion_value))

                self.assertEqual(ion_value.ion_type, value.ion_type)

    def test_execute_lambda_that_does_not_return_value(self):
        # Given.
        # Insert Ion struct to insert.
        ion_struct = loads(dumps({COLUMN_NAME: SINGLE_DOCUMENT_VALUE}))

        # When.
        query = "INSERT INTO {} ?".format(TABLE_NAME)
        self.qldb_driver.execute_lambda(lambda txn: txn.execute_statement(query, ion_struct))

        # Then.
        search_query = "SELECT VALUE {} FROM {} WHERE {} = ?".format(COLUMN_NAME, TABLE_NAME, COLUMN_NAME)
        ion_string = loads(dumps(SINGLE_DOCUMENT_VALUE))

        value = self.qldb_driver.execute_lambda(
            lambda txn: execute_statement_and_return_value(txn, search_query, ion_string))
        self.assertEqual(SINGLE_DOCUMENT_VALUE, value)

    def test_delete_table_that_does_not_exist(self):
        # Given.
        query = "DELETE FROM NonExistentTable"

        # When.
        self.assertRaises(ClientError, self.qldb_driver.execute_lambda, lambda txn: txn.execute_statement(query))

    def test_error_when_transaction_expires(self):
        # Given.
        def query_lambda(txn):
            # wait for the transaction to expire
            sleep(40)

        # When.
        self.assertRaises(ClientError, self.qldb_driver.execute_lambda, lambda executor: query_lambda(executor))


def create_ion_values():
    ion_values = list()

    ion_clob = loads('{{"This is a CLOB of text."}}')
    ion_values.append(ion_clob)
    ion_blob = loads('{{aGVsbG8=}}')
    ion_values.append(ion_blob)
    ion_bool = loads('true')
    ion_values.append(ion_bool)
    ion_decimal = loads('0.1')
    ion_values.append(ion_decimal)
    ion_float = loads('0.2e0')
    ion_values.append(ion_float)
    ion_int = loads('1')
    ion_values.append(ion_int)
    ion_list = loads('[1,2]')
    ion_values.append(ion_list)
    ion_null = loads('null')
    ion_values.append(ion_null)
    ion_sexp = loads('(cons 1 2)')
    ion_values.append(ion_sexp)
    ion_string = loads('\"string\"')
    ion_values.append(ion_string)
    ion_struct = loads('{a:1}')
    ion_values.append(ion_struct)
    ion_symbol = loads('abc')
    ion_values.append(ion_symbol)
    ion_timestamp = loads('2016-12-20T05:23:43.000000-00:00')
    ion_values.append(ion_timestamp)

    ion_null_clob = loads('null.clob')
    ion_values.append(ion_null_clob)
    ion_null_blob = loads('null.blob')
    ion_values.append(ion_null_blob)
    ion_null_bool = loads('null.bool')
    ion_values.append(ion_null_bool)
    ion_null_decimal = loads('null.decimal')
    ion_values.append(ion_null_decimal)
    ion_null_float = loads('null.float')
    ion_values.append(ion_null_float)
    ion_null_int = loads('null.int')
    ion_values.append(ion_null_int)
    ion_null_list = loads('null.list')
    ion_values.append(ion_null_list)
    ion_null_sexp = loads('null.sexp')
    ion_values.append(ion_null_sexp)
    ion_null_string = loads('null.string')
    ion_values.append(ion_null_string)
    ion_null_struct = loads('null.struct')
    ion_values.append(ion_null_struct)
    ion_null_symbol = loads('null.symbol')
    ion_values.append(ion_null_symbol)
    ion_null_timestamp = loads('null.timestamp')
    ion_values.append(ion_null_timestamp)

    ion_clob_with_annotation = loads('annotation::{{"This is a CLOB of text."}}')
    ion_values.append(ion_clob_with_annotation)
    ion_blob_with_annotation = loads('annotation::{{aGVsbG8=}}')
    ion_values.append(ion_blob_with_annotation)
    ion_bool_with_annotation = loads('annotation::true')
    ion_values.append(ion_bool_with_annotation)
    ion_decimal_with_annotation = loads('annotation::0.1')
    ion_values.append(ion_decimal_with_annotation)
    ion_float_with_annotation = loads('annotation::0.2e0')
    ion_values.append(ion_float_with_annotation)
    ion_int_with_annotation = loads('annotation::1')
    ion_values.append(ion_int_with_annotation)
    ion_list_with_annotation = loads('annotation::[1,2]')
    ion_values.append(ion_list_with_annotation)
    ion_null_with_annotation = loads('annotation::null')
    ion_values.append(ion_null_with_annotation)
    ion_sexp_with_annotation = loads('annotation::(cons 1 2)')
    ion_values.append(ion_sexp_with_annotation)
    ion_string_with_annotation = loads('annotation::\"string\"')
    ion_values.append(ion_string_with_annotation)
    ion_struct_with_annotation = loads('annotation::{a:1}')
    ion_values.append(ion_struct_with_annotation)
    ion_symbol_with_annotation = loads('annotation::abc')
    ion_values.append(ion_symbol_with_annotation)
    ion_timestamp_with_annotation = loads('annotation::2016-12-20T05:23:43.000000-00:00')
    ion_values.append(ion_timestamp_with_annotation)

    ion_null_clob_with_annotation = loads('annotation::null.clob')
    ion_values.append(ion_null_clob_with_annotation)
    ion_null_blob_with_annotation = loads('annotation::null.blob')
    ion_values.append(ion_null_blob_with_annotation)
    ion_null_bool_with_annotation = loads('annotation::null.bool')
    ion_values.append(ion_null_bool_with_annotation)
    ion_null_decimal_with_annotation = loads('annotation::null.decimal')
    ion_values.append(ion_null_decimal_with_annotation)
    ion_null_float_with_annotation = loads('annotation::null.float')
    ion_values.append(ion_null_float_with_annotation)
    ion_null_int_with_annotation = loads('annotation::null.int')
    ion_values.append(ion_null_int_with_annotation)
    ion_null_list_with_annotation = loads('annotation::null.list')
    ion_values.append(ion_null_list_with_annotation)
    ion_null_sexp_with_annotation = loads('annotation::null.sexp')
    ion_values.append(ion_null_sexp_with_annotation)
    ion_null_string_with_annotation = loads('annotation::null.string')
    ion_values.append(ion_null_string_with_annotation)
    ion_null_struct_with_annotation = loads('annotation::null.struct')
    ion_values.append(ion_null_struct_with_annotation)
    ion_null_symbol_with_annotation = loads('annotation::null.symbol')
    ion_values.append(ion_null_symbol_with_annotation)
    ion_null_timestamp_with_annotation = loads('annotation::null.timestamp')
    ion_values.append(ion_null_timestamp_with_annotation)

    return ion_values


def execute_statement_and_return_value(txn, query, *parameters):
    cursor = txn.execute_statement(query, *parameters)
    for row in cursor:
        return row
