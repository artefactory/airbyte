from operator import is_
import re
from typing import Any, Dict, List, Optional
from unittest import TestCase, mock

import freezegun
from airbyte_cdk.sources.source import TState
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, read
from integration.response_builder import JsonPath, SnowflakeResponseBuilder, create_response_builder
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_cdk.test.mock_http.response_builder import (
    FieldPath,
    Path,
    NestedPath,
    RecordBuilder,
    create_record_builder,
    find_template,
)
from airbyte_cdk.test.state_builder import StateBuilder
from airbyte_protocol.models import (
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    FailureType,
    StreamDescriptor,
    SyncMode,
)
from pydantic import Field, Json
import pytest

from integration.pagination import SnowflakePaginationStrategy
from integration.request_builder import SnowflakeRequestBuilder
from integration.config import ConfigBuilder
from integration.snowflake_stream_builder import SnowflakeStreamBuilder
from source_snowflake import SourceSnowflake

_WAREHOUSE = "WAREHOUSE"
_DATABASE = "DATABASE"
_SCHEMA = "INTEGRATION_TEST"
_TABLE = "TEST_TABLE"
_ROLE = "ROLE"
_HOST = "host.com"
_REQUESTID = "123"
_HANDLE = "123-DUMMY-HANDLE-123"


def _config() -> ConfigBuilder:
    return ConfigBuilder(jwt_token="123", host=_HOST, schema=_SCHEMA, database=_DATABASE, role=_ROLE, warehouse=_WAREHOUSE)


def table_request() -> SnowflakeRequestBuilder:
    return SnowflakeRequestBuilder.statement_endpoint(host=_HOST, schema=_SCHEMA, database=_DATABASE, role=_ROLE, warehouse=_WAREHOUSE)


def _catalog(sync_mode: SyncMode, cursor_field=None) -> ConfiguredAirbyteCatalog:
    builder = SnowflakeStreamBuilder().with_name(f"{_SCHEMA}.{_TABLE}").with_cursor_field(cursor_field).with_sync_mode(sync_mode)
    return CatalogBuilder().with_stream(builder).build()


def _source(catalog: ConfiguredAirbyteCatalog, config: Dict[str, Any], state: Optional[List[AirbyteStateMessage]]) -> SourceSnowflake:
    return SourceSnowflake()


def snowflake_response(file_name, datafield: Path = FieldPath("data")) -> SnowflakeResponseBuilder:
    return create_response_builder(
        find_template(file_name, __file__),
        datafield,
        pagination_strategy=SnowflakePaginationStrategy()
    )


def a_snowflake_response(file_name, datafield: Path = FieldPath("data"), id_path=None, cursor_path=None) -> RecordBuilder:
    return create_record_builder(
        find_template(file_name, __file__),
        datafield,
        record_id_path=id_path,
        record_cursor_path=cursor_path
    )


def _given_table_catalog(http_mocker: HttpMocker) -> None:
    http_mocker.post(
        table_request().with_show_catalog().with_requestID(_REQUESTID).build(),
        snowflake_response("check_connection").with_record(a_snowflake_response("check_connection")).build()
    )


def _given_table_with_primary_keys(http_mocker: HttpMocker) -> None:
    http_mocker.post(
        table_request().with_table(_TABLE).with_show_primary_keys().with_requestID(_REQUESTID).build(),
        snowflake_response("primary_keys").with_record(a_snowflake_response("primary_keys")).build()
    )


def _given_read_schema(http_mocker: HttpMocker) -> None:
    http_mocker.post(
        table_request().with_table(_TABLE).with_get_schema().with_requestID(_REQUESTID).build(),
        snowflake_response("response_get_table", JsonPath("$")).with_record(
            a_snowflake_response("response_get_table", JsonPath("$"))).build()

    )


def _given_get_timezone(http_mocker: HttpMocker) -> None:
    http_mocker.post(
        table_request().with_requestID(_REQUESTID).with_timezone().build(),
        snowflake_response("timezone", JsonPath("$.'data'")).build()
    )


def _read(
        config_builder: ConfigBuilder,
        sync_mode: SyncMode,
        state: Optional[List[AirbyteStateMessage]] = None,
        expecting_exception: bool = False
) -> EntrypointOutput:
    catalog = _catalog(sync_mode)
    config = config_builder.build()
    return read(_source(catalog, config, state), config, catalog, state, expecting_exception)


class FullRefreshTest(TestCase):
    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_given_one_page_when_read_then_return_records(self, uuid_mock, mock_auth, http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_read_schema(http_mocker)
        _given_table_catalog(http_mocker)
        _given_table_with_primary_keys(http_mocker)
        http_mocker.post(
            table_request().with_table(_TABLE).with_requestID(_REQUESTID).with_async().build(),
            snowflake_response("async_response", FieldPath("statementStatusUrl")).with_handle(_HANDLE).build()
        )
        http_mocker.get(
            table_request().with_handle(_HANDLE).build(is_get=True),
            snowflake_response("response_get_table", JsonPath("$.'data'"))
            .with_record(a_snowflake_response("response_get_table", JsonPath("$.'data'.[*]")))
            .build())

        output = _read(_config(), sync_mode=SyncMode.full_refresh)
        print(output.records)

    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_given_three_pages_read_then_return_records(self, uuid_mock, mock_auth, http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_read_schema(http_mocker)
        _given_table_catalog(http_mocker)
        _given_table_with_primary_keys(http_mocker)
        http_mocker.post(
            table_request().with_table(_TABLE).with_requestID(_REQUESTID).with_async().build(),
            snowflake_response("async_response", FieldPath("statementStatusUrl")).with_handle(_HANDLE).build()
        )
        http_mocker.get(
            table_request().with_handle(_HANDLE).build(is_get=True),
            snowflake_response("response_get_table", JsonPath("$.'data'")).with_record(
                a_snowflake_response("response_get_table", JsonPath("$.'data'.[*]"))).with_pagination().build()
        )
        http_mocker.get(
            table_request().with_handle(_HANDLE).with_partition(1).build(is_get=True),
            snowflake_response("response_get_table", JsonPath("$.'data'")).with_record(
                a_snowflake_response("response_get_table", JsonPath("$.'data'.[*]"))).with_pagination().build()
        )
        http_mocker.get(
            table_request().with_handle(_HANDLE).with_partition(2).build(is_get=True),
            snowflake_response("response_get_table", JsonPath("$.'data'")).with_record(
                a_snowflake_response("response_get_table", JsonPath("$.'data'.[*]"))).with_pagination().build()
        )
        output = _read(_config(), sync_mode=SyncMode.full_refresh)
        assert len(output.records) == 3


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
        self.get_record_builder_instance = lambda: a_snowflake_response("response_get_table")

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
            .with_record(self.get_record_builder_instance().with_field(JsonPath(f"$.[{self.test_colum_1_index}]"), value=3))
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
            .with_record(self.get_record_builder_instance().with_field(JsonPath(f"$.[{self.test_colum_1_index}]"), value=3))
            .with_record(self.get_record_builder_instance().with_field(JsonPath(f"$.[{self.test_colum_1_index}]"), value=4))
            .with_record(self.get_record_builder_instance().with_field(JsonPath(f"$.[{self.test_colum_1_index}]"), value=5))
            .build()
        )

        http_mocker.get(
            table_request()
            .with_handle(_HANDLE)
            .with_partition(1)
            .build(is_get=True),
            snowflake_response("response_get_table")
            .with_record(self.get_record_builder_instance().with_field(JsonPath(f"$.[{self.test_colum_1_index}]"), value=6))
            .with_record(self.get_record_builder_instance().with_field(JsonPath(f"$.[{self.test_colum_1_index}]"), value=7))
            .build()
        )

        http_mocker.get(
            table_request()
            .with_handle(_HANDLE)
            .with_partition(2)
            .build(is_get=True),
            snowflake_response("response_get_table")
            .with_record(self.get_record_builder_instance().with_field(JsonPath(f"$.[{self.test_colum_1_index}]"), value=8))
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


class IncrementalTest(TestCase):

    def setUp(self):
        self.sync_mode = SyncMode.incremental
        self.stream_name = f"{_SCHEMA}.{_TABLE}"
        self.get_record_builder_instance = lambda cursor_path=None: a_snowflake_response(file_name="response_get_table",
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
            .with_record(self.get_record_builder_instance(cursor_path).with_cursor(1))
            .with_record(self.get_record_builder_instance(cursor_path).with_cursor(2))
            .with_record(self.get_record_builder_instance(cursor_path).with_cursor(3))
            .build()
        )

        stream_builder = (SnowflakeStreamBuilder().with_name(self.stream_name)
                          .with_cursor_field([cursor_field])
                          .with_sync_mode(self.sync_mode))
        catalog = CatalogBuilder().with_stream(stream_builder).build()

        output = self._read(_config(), catalog)
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
        expected_cursor_value = "1970-01-04"
        cursor_path = JsonPath(f"$.[{id_index}]")

        http_mocker.post(
            table_request()
            .with_table(_TABLE)
            .with_requestID(_REQUESTID)
            .with_async()
            .with_cursor_field(cursor_field)
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
            .with_record(self.get_record_builder_instance(cursor_path).with_cursor("1"))
            .with_record(self.get_record_builder_instance(cursor_path).with_cursor("2"))
            .with_record(self.get_record_builder_instance(cursor_path).with_cursor("3"))
            .build()
        )

        stream_builder = (SnowflakeStreamBuilder().with_name(self.stream_name)
                          .with_cursor_field([cursor_field])
                          .with_sync_mode(self.sync_mode))
        catalog = CatalogBuilder().with_stream(stream_builder).build()

        output = self._read(_config(), catalog)
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
            .with_record(self.get_record_builder_instance(cursor_path).with_cursor("1521702000.123000000 1740"))
            .with_record(self.get_record_builder_instance(cursor_path).with_cursor("1521702001.123000000 1740"))
            .with_record(self.get_record_builder_instance(cursor_path).with_cursor("1521702001.123001000 1740"))
            .build()
        )

        stream_builder = (SnowflakeStreamBuilder().with_name(self.stream_name)
                          .with_cursor_field([cursor_field])
                          .with_sync_mode(self.sync_mode))
        catalog = CatalogBuilder().with_stream(stream_builder).build()

        output = self._read(_config(), catalog)
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
            .with_record(self.get_record_builder_instance(cursor_path).with_cursor("aaaaaaa"))
            .with_record(self.get_record_builder_instance(cursor_path).with_cursor("aaaaaa154"))
            .with_record(self.get_record_builder_instance(cursor_path).with_cursor("b"))
            .build()
        )

        stream_builder = (SnowflakeStreamBuilder().with_name(self.stream_name)
                          .with_cursor_field([cursor_field])
                          .with_sync_mode(self.sync_mode))
        catalog = CatalogBuilder().with_stream(stream_builder).build()

        output = self._read(_config(), catalog)
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
