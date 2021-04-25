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

from unittest.mock import MagicMock

from pyqldb.cursor.stream_cursor import StreamCursor


def generate_statement_result(read_io, write_io, processing_time, next_page_token, is_first_page,
                              values=[{'IonBinary': 1}]):
    """
    Generate a statement result dictionary for testing purposes.
    """
    page = {'Values': values, 'NextPageToken': next_page_token}

    statement_result = {}
    if read_io:
        statement_result['ConsumedIOs'] = {'ReadIOs': read_io, 'WriteIOs': write_io}

    if processing_time:
        statement_result['TimingInformation'] = {'ProcessingTimeMilliseconds': processing_time}

    if is_first_page:
        statement_result['FirstPage'] = page
    else:
        statement_result['Page'] = page
    return statement_result


def assert_query_stats(test_case, buffered_cursor, read_ios_assert, write_ios_assert, timing_information_assert):
    """
    Asserts the query statistics returned by the cursor.
    """
    consumed_ios = buffered_cursor.get_consumed_ios()
    if read_ios_assert is not None:
        test_case.assertIsNotNone(consumed_ios)
        read_ios = consumed_ios.get('ReadIOs')

        test_case.assertEqual(read_ios, read_ios_assert)
    else:
        test_case.assertEqual(consumed_ios, None)

    timing_information = buffered_cursor.get_timing_information()
    if timing_information_assert is not None:
        test_case.assertIsNotNone(timing_information)
        processing_time_milliseconds = timing_information.get('ProcessingTimeMilliseconds')

        test_case.assertEqual(processing_time_milliseconds, timing_information_assert)
    else:
        test_case.assertEqual(timing_information_assert, None)


def create_stream_cursor(mock_session, mock_statement_result_execute, mock_statement_result_fetch):
    """
    Create a stream cursor with execute and fetch page statement results.
    """
    mock_session.return_value = None
    mock_session._fetch_page.return_value = mock_statement_result_fetch

    stream_cursor = StreamCursor(mock_statement_result_execute, mock_session, "1")
    stream_cursor._value_holder_to_ion_value = MagicMock(name='_value_holder_to_ion_value')

    return stream_cursor


def check_execute_error(test_case, execute_error, expected_inner_error, expected_is_retryable,
                        expected_is_invalid_session, expected_transaction_id):
    test_case.assertEqual(execute_error.error, expected_inner_error)
    test_case.assertEqual(execute_error.is_retryable, expected_is_retryable)
    test_case.assertEqual(execute_error.is_invalid_session_exception, expected_is_invalid_session)
    test_case.assertEqual(execute_error.transaction_id, expected_transaction_id)
