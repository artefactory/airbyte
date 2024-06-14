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
from integration.test_table import _SCHEMA, _TABLE, a_snowflake_response, _config, _given_get_timezone, _given_read_schema, _given_table_catalog, \
    _given_table_with_primary_keys, table_request, _REQUESTID, snowflake_response, _HANDLE, _source
from airbyte_cdk.test.mock_http import HttpMocker
from airbyte_cdk.test.mock_http.response_builder import FieldPath

from integration.response_builder import JsonPath
from test.state_builder import StateBuilder
from freezegun import freeze_time




class FullRefreshCDC(TestCase):

    def setUp(self):
        # new file because of the presence of not supported geography type in cdc
        self.file_name = "response_get_table_cdc_full_refresh"

        self._a_record = lambda: a_snowflake_response(self.file_name)

        config_builder = _config()
        self.config_builder = config_builder.with_cdc()

        sync_mode = SyncMode.full_refresh
        self.catalog = CatalogBuilder().with_stream(f"{_SCHEMA}.{_TABLE}", sync_mode).build()
        self.cdc_meta_data_columns = ('METADATA$ACTION', 'METADATA$ISUPDATE', 'METADATA$ROW_ID', 'updated_at')

    @freeze_time("2024-06-14T07:01:38.626628")
    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_given_one_page_when_read_then_return_records(self, uuid_mock, mock_auth, http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_read_schema(http_mocker, file_name=self.file_name)
        _given_table_catalog(http_mocker)
        _given_table_with_primary_keys(http_mocker)

        http_mocker.post(
            table_request()
            .with_table(_TABLE)
            .with_requestID(_REQUESTID)
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
            snowflake_response(self.file_name)
            .with_record(a_snowflake_response(self.file_name, JsonPath("$.'data'.[*]")))
            .with_record(a_snowflake_response(self.file_name, JsonPath("$.'data'.[*]")))
            .with_record(a_snowflake_response(self.file_name, JsonPath("$.'data'.[*]")))
            .build()
        )

        output = self._read(self.config_builder, self.catalog)
        assert len(output.records) == 3
        for airbyte_message in output.records:
            assert all([cdc_meta_data_column in airbyte_message.record.data for cdc_meta_data_column in self.cdc_meta_data_columns])
            assert airbyte_message.record.data['updated_at'] == '2024-06-14T00:01:38.626628-07:00'

    @freeze_time("2024-06-14T07:01:38.626628")
    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_given_three_pages_read_then_return_records(self, uuid_mock, mock_auth, http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_read_schema(http_mocker, file_name=self.file_name)
        _given_table_catalog(http_mocker)
        _given_table_with_primary_keys(http_mocker)

        http_mocker.post(
            table_request()
            .with_table(_TABLE)
            .with_requestID(_REQUESTID)
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
            snowflake_response(self.file_name)
            .with_record(a_snowflake_response(self.file_name, JsonPath("$.'data'.[*]")))
            .with_record(a_snowflake_response(self.file_name, JsonPath("$.'data'.[*]")))
            .with_record(a_snowflake_response(self.file_name, JsonPath("$.'data'.[*]")))
            .with_pagination()
            .build()
        )

        http_mocker.get(
            table_request()
            .with_handle(_HANDLE)
            .with_partition(1)
            .build(is_get=True),
            snowflake_response(self.file_name)
            .with_record(a_snowflake_response(self.file_name, JsonPath("$.'data'.[*]")))
            .with_record(a_snowflake_response(self.file_name, JsonPath("$.'data'.[*]")))
            .with_pagination()
            .build()
        )

        http_mocker.get(
            table_request()
            .with_handle(_HANDLE)
            .with_partition(2)
            .build(is_get=True),
            snowflake_response(self.file_name)
            .with_record(a_snowflake_response(self.file_name, JsonPath("$.'data'.[*]")))
            .with_pagination()
            .build()
        )

        output = self._read(self.config_builder, self.catalog)
        assert len(output.records) == 6
        for airbyte_message in output.records:
            assert all([cdc_meta_data_column in airbyte_message.record.data for cdc_meta_data_column in self.cdc_meta_data_columns])
            assert airbyte_message.record.data['updated_at'] == '2024-06-14T00:01:38.626628-07:00'

    @classmethod
    def _read(cls,
              config_builder: ConfigBuilder,
              catalog: ConfiguredAirbyteCatalog,
              state: Optional[List[AirbyteStateMessage]] = None,
              expecting_exception: bool = False
              ) -> EntrypointOutput:
        config = config_builder.build()
        return read(_source(catalog, config, state), config, catalog, state, expecting_exception)


class IncrementalCDC(TestCase):

    def setUp(self):
        # new file because of the presence of not supported geography type in cdc
        self.file_name = "response_get_table_cdc_full_refresh"

        self._a_record = lambda: a_snowflake_response(self.file_name)

        config_builder = _config()
        self.config_builder = config_builder.with_cdc()

        sync_mode = SyncMode.incremental
        self.catalog = CatalogBuilder().with_stream(f"{_SCHEMA}.{_TABLE}", sync_mode).build()
        self.cdc_meta_data_columns = ('METADATA$ACTION', 'METADATA$ISUPDATE', 'METADATA$ROW_ID', 'updated_at')

    @freeze_time("2024-06-14T07:01:38.626628")
    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_given_one_page_when_read_then_return_records(self, uuid_mock, mock_auth, http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_read_schema(http_mocker, file_name=self.file_name)
        _given_table_catalog(http_mocker)
        _given_table_with_primary_keys(http_mocker)

        http_mocker.post(
            table_request()
            .with_table(_TABLE)
            .with_requestID(_REQUESTID)
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
            snowflake_response(self.file_name)
            .with_record(a_snowflake_response(self.file_name, JsonPath("$.'data'.[*]")))
            .with_record(a_snowflake_response(self.file_name, JsonPath("$.'data'.[*]")))
            .with_record(a_snowflake_response(self.file_name, JsonPath("$.'data'.[*]")))
            .build()
        )

        output = self._read(self.config_builder, self.catalog)
        assert len(output.records) == 3
        assert output.state_messages[0].state.stream.stream_state.last_update_date == '2024-06-14T00:01:38.626628-07:00'
        for airbyte_message in output.records:
            assert all([cdc_meta_data_column in airbyte_message.record.data for cdc_meta_data_column in self.cdc_meta_data_columns])
            assert airbyte_message.record.data['updated_at'] == '2024-06-14T00:01:38.626628-07:00'

    @classmethod
    def _read(cls,
              config_builder: ConfigBuilder,
              catalog: ConfiguredAirbyteCatalog,
              state: Optional[List[AirbyteStateMessage]] = None,
              expecting_exception: bool = False
              ) -> EntrypointOutput:
        config = config_builder.build()
        return read(_source(catalog, config, state), config, catalog, state, expecting_exception)
