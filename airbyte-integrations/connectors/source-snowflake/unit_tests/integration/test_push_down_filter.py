from datetime import datetime
from typing import Optional, List
from unittest import TestCase, mock

from airbyte_protocol.models import SyncMode, ConfiguredAirbyteCatalog, AirbyteStateMessage, StreamDescriptor, AirbyteStateBlob
from parameterized import parameterized

from integration.config import ConfigBuilder
from integration.snowflake_stream_builder import SnowflakeStreamBuilder
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from test.entrypoint_wrapper import EntrypointOutput, read
from test.mock_http.response_builder import FieldPath
from integration.test_table import _SCHEMA, _TABLE, a_snowflake_response, _config, _given_get_timezone, _given_read_schema, \
    _given_table_catalog, \
    _given_table_with_primary_keys, table_request, _REQUESTID, snowflake_response, _HANDLE, _source, _read
from airbyte_cdk.test.mock_http import HttpMocker
from airbyte_cdk.test.mock_http.response_builder import FieldPath

from integration.response_builder import JsonPath
from test.state_builder import StateBuilder


class FullRefreshPushDownFilterTest(TestCase):

    def setUp(self):
        self.push_down_filter_name = "higher_than_2"
        self.where_clause = 'TEST_COLUMN_1>2'
        self.test_colum_1_index = 1
        self.push_down_filter = {
            "name": self.push_down_filter_name,
            "parent_stream": f"{_SCHEMA}.{_TABLE}",
            "where_clause": self.where_clause
        }
        self._a_record = lambda: a_snowflake_response("response_get_table")

        config_builder = _config()
        self.config_builder = config_builder.with_push_down_filter(self.push_down_filter)

        sync_mode = SyncMode.full_refresh
        self.catalog = CatalogBuilder().with_stream(self.push_down_filter_name, sync_mode).build()

    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_given_one_page_when_read_then_return_records(self, uuid_mock, mock_auth, http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_read_schema(http_mocker)
        _given_table_catalog(http_mocker)
        _given_table_with_primary_keys(http_mocker)

        http_mocker.post(
            table_request()
            .with_table(_TABLE)
            .with_requestID(_REQUESTID)
            .with_async()
            .with_where_clause(self.where_clause)
            .build(),
            snowflake_response("async_response", FieldPath("statementStatusUrl"))
            .with_handle(_HANDLE)
            .build()
        )

        http_mocker.get(
            table_request()
            .with_handle(_HANDLE)
            .build(is_get=True),
            snowflake_response("response_get_table")
            .with_record(self._a_record().with_field(JsonPath(f"$.[{self.test_colum_1_index}]"), value=3))
            .build()
        )

        output = self._read(self.config_builder, self.catalog)
        assert len(output.records) == 1

    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_given_three_pages_read_then_return_records(self, uuid_mock, mock_auth, http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_read_schema(http_mocker)
        _given_table_catalog(http_mocker)
        _given_table_with_primary_keys(http_mocker)

        http_mocker.post(
            table_request()
            .with_table(_TABLE)
            .with_requestID(_REQUESTID)
            .with_async()
            .with_where_clause(self.where_clause)
            .build(),
            snowflake_response("async_response", FieldPath("statementStatusUrl"))
            .with_handle(_HANDLE)
            .build()
        )

        http_mocker.get(
            table_request()
            .with_handle(_HANDLE)
            .build(is_get=True),
            snowflake_response("response_get_table")
            .with_pagination()
            .with_record(self._a_record().with_field(JsonPath(f"$.[{self.test_colum_1_index}]"), value=3))
            .with_record(self._a_record().with_field(JsonPath(f"$.[{self.test_colum_1_index}]"), value=4))
            .with_record(self._a_record().with_field(JsonPath(f"$.[{self.test_colum_1_index}]"), value=5))
            .build()
        )

        http_mocker.get(
            table_request()
            .with_handle(_HANDLE)
            .with_partition(1)
            .build(is_get=True),
            snowflake_response("response_get_table")
            .with_record(self._a_record().with_field(JsonPath(f"$.[{self.test_colum_1_index}]"), value=6))
            .with_record(self._a_record().with_field(JsonPath(f"$.[{self.test_colum_1_index}]"), value=7))
            .build()
        )

        http_mocker.get(
            table_request()
            .with_handle(_HANDLE)
            .with_partition(2)
            .build(is_get=True),
            snowflake_response("response_get_table")
            .with_record(self._a_record().with_field(JsonPath(f"$.[{self.test_colum_1_index}]"), value=8))
            .build()
        )

        output = self._read(self.config_builder, self.catalog)
        assert len(output.records) == 6

    @classmethod
    def _read(cls,
              config_builder: ConfigBuilder,
              catalog: ConfiguredAirbyteCatalog,
              state: Optional[List[AirbyteStateMessage]] = None,
              expecting_exception: bool = False
              ) -> EntrypointOutput:
        config = config_builder.build()
        return read(_source(catalog, config, state), config, catalog, state, expecting_exception)


class IncrementalPushDownFilterTest(TestCase):

    def setUp(self):
        self.push_down_filter_name = "higher_than_2"
        self.where_clause = 'TEST_COLUMN_1>2'
        self.test_colum_1_index = 1
        self.push_down_filter = {
            "name": self.push_down_filter_name,
            "parent_stream": f"{_SCHEMA}.{_TABLE}",
            "where_clause": self.where_clause
        }
        self._a_record = lambda: a_snowflake_response("response_get_table")

        config_builder = _config()
        self.config_builder = config_builder.with_push_down_filter(self.push_down_filter)

        sync_mode = SyncMode.full_refresh
        self.catalog = CatalogBuilder().with_stream(self.push_down_filter_name, sync_mode).build()
        self.sync_mode = SyncMode.incremental
        self.stream_name = self.push_down_filter_name
        self._a_record = lambda cursor_path=None: a_snowflake_response(file_name="response_get_table",
                                                                       datafield=JsonPath("$.'data'[*]"),
                                                                       cursor_path=cursor_path)

    @parameterized.expand([
        ("ID", 0, 3, (1, 2, 3), 'ORDER BY ID ASC'),
        ("TEST_COLUMN_20", 12, datetime(1970, 1, 4).strftime("%Y-%m-%d"), ("1", "2", "3"),
         "ORDER BY TEST_COLUMN_20 ASC"),
        ("TEST_COLUMN_26", 18, "2018-03-22T12:00:01.123001+05:00", ("1521702000.123000000 1740",
                                                                    "1521702001.123000000 1740",
                                                                    "1521702001.123001000 1740"),
         "ORDER BY TEST_COLUMN_26 ASC"),
        ("TEST_COLUMN_14", 5, "b", ("aaaaaaa", "aaaaaaa154", "b"),
         "ORDER BY TEST_COLUMN_14 ASC"),
    ])
    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_incremental_without_initial_state(self,
                                               cursor_field,
                                               cursor_index,
                                               expected_cursor_value,
                                               cursor_values,
                                               statement,
                                               uuid_mock,
                                               mock_auth,
                                               http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_read_schema(http_mocker)
        _given_table_catalog(http_mocker)
        _given_table_with_primary_keys(http_mocker)

        cursor_path = JsonPath(f"$.[{cursor_index}]")
        cursor_value_1, cursor_value_2, cursor_value_3 = cursor_values

        http_mocker.post(
            table_request()
            .with_table(_TABLE)
            .with_requestID(_REQUESTID)
            .with_async()
            .with_statement(f"{self.where_clause} {statement}")
            .build(),
            snowflake_response("async_response", FieldPath("statementStatusUrl"))
            .with_handle(_HANDLE)
            .build()
        )

        http_mocker.get(
            table_request()
            .with_handle(_HANDLE)
            .build(is_get=True),
            snowflake_response("response_get_table")
            .with_record(self._a_record(cursor_path).with_cursor(cursor_value_1))
            .with_record(self._a_record(cursor_path).with_cursor(cursor_value_2))
            .with_record(self._a_record(cursor_path).with_cursor(cursor_value_3))
            .build()
        )

        stream_builder = (SnowflakeStreamBuilder()
                          .with_name(self.stream_name)
                          .with_cursor_field([cursor_field])
                          .with_sync_mode(self.sync_mode))

        catalog = CatalogBuilder().with_stream(stream_builder).build()

        output = self._read(self.config_builder, catalog)
        most_recent_state = output.most_recent_state

        assert len(output.records) == 3
        assert most_recent_state.stream_descriptor == StreamDescriptor(name=self.stream_name)
        assert most_recent_state.stream_state == AirbyteStateBlob(**{f"{cursor_field}": expected_cursor_value})

    @parameterized.expand([
        ("ID", 0, 3, (1, 2, 3), 2, "ID>=2 ORDER BY ID ASC"),
        ("TEST_COLUMN_20",
         12,
         datetime(1970, 1, 4).strftime("%Y-%m-%d"),
         ("1", "2", "3"),
         datetime(1970, 1, 3).strftime("%Y-%m-%d"),
         "TO_TIMESTAMP(TEST_COLUMN_20)>=TO_TIMESTAMP('1970-01-03') ORDER BY TEST_COLUMN_20 ASC"),
        ("TEST_COLUMN_26",
         18,
         "2018-03-22T12:00:01.123001+05:00",
         ("1521702000.123000000 1740",
          "1521702001.123000000 1740",
          "1521702001.123001000 1740"),
         "2018-03-20T12:00:01.123001+05:00",
         "TO_TIMESTAMP_TZ(TEST_COLUMN_26)>=TO_TIMESTAMP_TZ('2018-03-20T12:00:01.123001+05:00') ORDER BY TEST_COLUMN_26 ASC"
         ),
        ("TEST_COLUMN_14", 5, "c", ("a", "b", "c"), "a", "TEST_COLUMN_14>='a' ORDER BY TEST_COLUMN_14 ASC"),
    ])
    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_incremental_with_initial_state_lower_than_highest_record_state(self,
                                                                            cursor_field,
                                                                            cursor_index,
                                                                            expected_cursor_value,
                                                                            cursor_values,
                                                                            initial_state_value,
                                                                            statement,
                                                                            uuid_mock,
                                                                            mock_auth,
                                                                            http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_read_schema(http_mocker)
        _given_table_catalog(http_mocker)
        _given_table_with_primary_keys(http_mocker)

        initial_state = {cursor_field: initial_state_value}
        cursor_path = JsonPath(f"$.[{cursor_index}]")
        cursor_value_1, cursor_value_2, cursor_value_3 = cursor_values

        http_mocker.post(
            table_request()
            .with_table(_TABLE)
            .with_requestID(_REQUESTID)
            .with_statement(f"{self.where_clause} AND {statement}")
            .with_async()
            .build(),
            snowflake_response("async_response", FieldPath("statementStatusUrl"))
            .with_handle(_HANDLE)
            .build()
        )

        http_mocker.get(
            table_request()
            .with_handle(_HANDLE)
            .build(is_get=True),
            snowflake_response("response_get_table")
            .with_record(self._a_record(cursor_path).with_cursor(cursor_value_1))
            .with_record(self._a_record(cursor_path).with_cursor(cursor_value_2))
            .with_record(self._a_record(cursor_path).with_cursor(cursor_value_3))
            .build()
        )

        stream_builder = (SnowflakeStreamBuilder()
                          .with_name(self.stream_name)
                          .with_cursor_field([cursor_field])
                          .with_sync_mode(self.sync_mode))
        catalog = CatalogBuilder().with_stream(stream_builder).build()
        state = StateBuilder().with_stream_state(self.stream_name, initial_state).build()

        output = self._read(self.config_builder, catalog, state)
        most_recent_state = output.most_recent_state
        assert len(output.records) == 3
        assert most_recent_state.stream_descriptor == StreamDescriptor(name=self.stream_name)
        assert most_recent_state.stream_state == AirbyteStateBlob(**{f"{cursor_field}": expected_cursor_value})

    @parameterized.expand([
        ("ID", 0, 4, (1, 2, 3), 4, "ID>=4 ORDER BY ID ASC"),
        ("TEST_COLUMN_20",
         12,
         datetime(1970, 1, 8).strftime("%Y-%m-%d"),
         ("1", "2", "3"),
         datetime(1970, 1, 8).strftime("%Y-%m-%d"),
         "TO_TIMESTAMP(TEST_COLUMN_20)>=TO_TIMESTAMP('1970-01-08') ORDER BY TEST_COLUMN_20 ASC"),
        ("TEST_COLUMN_26",
         18,
         "2018-03-25T12:00:01.123001+05:00",
         ("1521702000.123000000 1740",
          "1521702001.123000000 1740",
          "1521702001.123001000 1740"),
         "2018-03-25T12:00:01.123001+05:00",
         "TO_TIMESTAMP_TZ(TEST_COLUMN_26)>=TO_TIMESTAMP_TZ('2018-03-25T12:00:01.123001+05:00') ORDER BY TEST_COLUMN_26 ASC"
         ),
        ("TEST_COLUMN_14", 5, "d", ("a", "b", "c"), "d", "TEST_COLUMN_14>='d' ORDER BY TEST_COLUMN_14 ASC"),
    ])
    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_incremental_with_initial_state_higher_than_highest_record_state(self,
                                                                             cursor_field,
                                                                             cursor_index,
                                                                             expected_cursor_value,
                                                                             cursor_values,
                                                                             initial_state_value,
                                                                             statement,
                                                                             uuid_mock,
                                                                             mock_auth,
                                                                             http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_read_schema(http_mocker)
        _given_table_catalog(http_mocker)
        _given_table_with_primary_keys(http_mocker)

        initial_state = {cursor_field: initial_state_value}
        cursor_path = JsonPath(f"$.[{cursor_index}]")
        cursor_value_1, cursor_value_2, cursor_value_3 = cursor_values

        http_mocker.post(
            table_request()
            .with_table(_TABLE)
            .with_requestID(_REQUESTID)
            .with_statement(f"{self.where_clause} AND {statement}")
            .with_async()
            .build(),
            snowflake_response("async_response", FieldPath("statementStatusUrl"))
            .with_handle(_HANDLE)
            .build()
        )

        http_mocker.get(
            table_request()
            .with_handle(_HANDLE)
            .build(is_get=True),
            snowflake_response("response_get_table")
            .with_record(self._a_record(cursor_path).with_cursor(cursor_value_1))
            .with_record(self._a_record(cursor_path).with_cursor(cursor_value_2))
            .with_record(self._a_record(cursor_path).with_cursor(cursor_value_3))
            .build()
        )

        stream_builder = (SnowflakeStreamBuilder()
                          .with_name(self.stream_name)
                          .with_cursor_field([cursor_field])
                          .with_sync_mode(self.sync_mode))
        catalog = CatalogBuilder().with_stream(stream_builder).build()
        state = StateBuilder().with_stream_state(self.stream_name, initial_state).build()

        output = self._read(self.config_builder, catalog, state)
        most_recent_state = output.most_recent_state

        assert most_recent_state.stream_descriptor == StreamDescriptor(name=self.stream_name)
        assert most_recent_state.stream_state == AirbyteStateBlob(**{f"{cursor_field}": expected_cursor_value})

    @classmethod
    def _read(cls,
              config_builder: ConfigBuilder,
              catalog: ConfiguredAirbyteCatalog,
              state: Optional[List[AirbyteStateMessage]] = None,
              expecting_exception: bool = False
              ) -> EntrypointOutput:
        config = config_builder.build()
        return read(_source(catalog, config, state), config, catalog, state, expecting_exception)
