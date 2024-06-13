from datetime import datetime
from typing import Optional, List
from unittest import TestCase, mock

from airbyte_protocol.models import SyncMode, ConfiguredAirbyteCatalog, AirbyteStateMessage, StreamDescriptor, AirbyteStateBlob

from integration.config import ConfigBuilder
from integration.snowflake_stream_builder import SnowflakeStreamBuilder
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from test.entrypoint_wrapper import EntrypointOutput, read
from test.mock_http.response_builder import FieldPath
from integration.test_table import _SCHEMA, _TABLE, a_snowflake_response, _config, _given_get_timezone, _given_read_schema, _given_table_catalog, \
    _given_table_with_primary_keys, table_request, _REQUESTID, snowflake_response, _HANDLE, _source, _read
from airbyte_cdk.test.mock_http import HttpMocker
from airbyte_cdk.test.mock_http.response_builder import FieldPath

from integration.response_builder import JsonPath


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
            .with_where_statement(self.where_clause)
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
            .with_where_statement(self.where_clause)
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

    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_incremental_without_initial_state_with_number_cursor(self, uuid_mock, mock_auth, http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_read_schema(http_mocker)
        _given_table_catalog(http_mocker)
        _given_table_with_primary_keys(http_mocker)

        cursor_field = "ID"
        id_index = 0
        expected_cursor_value = 3
        cursor_path = JsonPath(f"$.[{id_index}]")


        http_mocker.post(
            table_request()
            .with_table(_TABLE)
            .with_requestID(_REQUESTID)
            .with_async()
            .with_cursor_field(cursor_field)
            .with_where_statement(self.where_clause)
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
            .with_record(self._a_record(cursor_path).with_cursor(1))
            .with_record(self._a_record(cursor_path).with_cursor(2))
            .with_record(self._a_record(cursor_path).with_cursor(expected_cursor_value))
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
        assert most_recent_state.stream_state == AirbyteStateBlob(ID=expected_cursor_value)

    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_incremental_without_initial_state_with_date_cursor(self, uuid_mock, mock_auth, http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_read_schema(http_mocker)
        _given_table_catalog(http_mocker)
        _given_table_with_primary_keys(http_mocker)

        cursor_field = "TEST_COLUMN_20"
        id_index = 12
        most_recent_cursor = datetime(1970, 1, 4)
        expected_cursor_value = most_recent_cursor.strftime("%Y-%m-%d")
        most_recent_cursor_value_in_record = str((most_recent_cursor - datetime(1970, 1, 1)).days)
        cursor_path = JsonPath(f"$.[{id_index}]")


        http_mocker.post(
            table_request()
            .with_table(_TABLE)
            .with_requestID(_REQUESTID)
            .with_async()
            .with_cursor_field(cursor_field)
            .with_where_statement(self.where_clause)
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
            .with_record(self._a_record(cursor_path).with_cursor("1"))
            .with_record(self._a_record(cursor_path).with_cursor("2"))
            .with_record(self._a_record(cursor_path).with_cursor(most_recent_cursor_value_in_record))
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
        assert most_recent_state.stream_state == AirbyteStateBlob(TEST_COLUMN_20=expected_cursor_value)
    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_incremental_without_initial_state_with_timestamp_time_zone_cursor(self, uuid_mock, mock_auth, http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_read_schema(http_mocker)
        _given_table_catalog(http_mocker)
        _given_table_with_primary_keys(http_mocker)

        cursor_field = "TEST_COLUMN_26"
        id_index = 18
        expected_cursor_value = "2018-03-22T12:00:01.123001+05:00"
        cursor_path = JsonPath(f"$.[{id_index}]")

        http_mocker.post(
            table_request()
            .with_table(_TABLE)
            .with_requestID(_REQUESTID)
            .with_async()
            .with_cursor_field(cursor_field)
            .with_where_statement(self.where_clause)
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
            .with_record(self._a_record(cursor_path).with_cursor("1"))
            .with_record(self._a_record(cursor_path).with_cursor("2"))
            .with_record(self._a_record(cursor_path).with_cursor(expected_cursor_value))
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
        assert most_recent_state.stream_state == AirbyteStateBlob(TEST_COLUMN_26=expected_cursor_value)


    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_incremental_without_initial_state_with_string_cursor(self, uuid_mock, mock_auth, http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_read_schema(http_mocker)
        _given_table_catalog(http_mocker)
        _given_table_with_primary_keys(http_mocker)

        cursor_field = "TEST_COLUMN_14"
        id_index = 5
        expected_cursor_value = "b"
        cursor_path = JsonPath(f"$.[{id_index}]")

        http_mocker.post(
            table_request()
            .with_table(_TABLE)
            .with_requestID(_REQUESTID)
            .with_async()
            .with_cursor_field(cursor_field)
            .with_where_statement(self.where_clause)
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
            .with_record(self._a_record(cursor_path).with_cursor("aaaaaaa"))
            .with_record(self._a_record(cursor_path).with_cursor("aaaaaa154"))
            .with_record(self._a_record(cursor_path).with_cursor(expected_cursor_value))
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
        assert most_recent_state.stream_state == AirbyteStateBlob(TEST_COLUMN_14=expected_cursor_value)




    @classmethod
    def _read(cls,
              config_builder: ConfigBuilder,
              catalog: ConfiguredAirbyteCatalog,
              state: Optional[List[AirbyteStateMessage]] = None,
              expecting_exception: bool = False
              ) -> EntrypointOutput:
        config = config_builder.build()
        return read(_source(catalog, config, state), config, catalog, state, expecting_exception)

