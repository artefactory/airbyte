from typing import Any, Dict, List, Optional
from unittest import TestCase, expectedFailure, mock
from airbyte_cdk import ConfiguredAirbyteCatalog, SyncMode
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, discover
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.mock_http import HttpMocker
from airbyte_protocol.models import (
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    FailureType,
    StreamDescriptor,
    SyncMode,
)

from integration.config import ConfigBuilder
from integration.request_builder import SnowflakeRequestBuilder
from integration.response_builder import JsonPath
from integration.test_table import a_snowflake_response, snowflake_response
from source_snowflake.source import SourceSnowflake



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


def _catalog(sync_mode: SyncMode) -> ConfiguredAirbyteCatalog:
    return CatalogBuilder().build()


def _source(catalog: ConfiguredAirbyteCatalog, config: Dict[str, Any], state: Optional[List[AirbyteStateMessage]]) -> SourceSnowflake:
    return SourceSnowflake()


def _given_table_catalog(http_mocker: HttpMocker) -> None:
    http_mocker.post(
        table_request().with_show_catalog().with_requestID(_REQUESTID).build(),
        snowflake_response("check_connection").with_record(a_snowflake_response("check_connection")).build()
    )


def _given_table_with_primary_keys(http_mocker: HttpMocker, table) -> None:
    http_mocker.post(
        table_request().with_table(table).with_show_primary_keys().with_requestID(_REQUESTID).build(),
        snowflake_response("primary_keys").with_record(a_snowflake_response("primary_keys")).build()
    )

def _given_read_schema(http_mocker: HttpMocker, table) -> None:
    http_mocker.post(
        table_request().with_table(table).with_get_schema().with_requestID(_REQUESTID).build(),
        snowflake_response("response_get_table", JsonPath("$")).with_record(
            a_snowflake_response("response_get_table", JsonPath("$"))).build()
    )

def _given_get_timezone(http_mocker: HttpMocker) -> None:
    http_mocker.post(
        table_request().with_requestID(_REQUESTID).with_timezone().build(),
        snowflake_response("timezone", JsonPath("$.'data'")).build())


def _discover(
        config_builder: ConfigBuilder,
        sync_mode: SyncMode,
        state: Optional[List[AirbyteStateMessage]] = None,
        expecting_exception: bool = False
) -> EntrypointOutput:
        catalog = _catalog(sync_mode)
        config = config_builder.build()
        return discover(_source(catalog, config, state), config, expecting_exception)


class DiscoverTest(TestCase):

    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_discover_no_pagination(self,  uuid_mock, mock_auth, http_mocker: HttpMocker)->None:
        def a_table_record():
            return a_snowflake_response("check_connection", JsonPath("$.'data'.[*]"), id_path=JsonPath("$.[1]"))
        
        _given_get_timezone(http_mocker)
       
        for i in ["TEST_TABLE_2","TEST_TABLE_1"]:
            _given_table_with_primary_keys(http_mocker, i)
            _given_read_schema(http_mocker,i )
       

        http_mocker.post(
        table_request().with_show_catalog().with_requestID(_REQUESTID).build(),
        snowflake_response("check_connection", JsonPath("$.'data'"))
        .with_record(
            a_table_record().with_id("TEST_TABLE_1")
        )
        .with_record(
            a_table_record().with_id("TEST_TABLE_2"))
        .build()
        )
        
        
        output= _discover(_config(), sync_mode=SyncMode.full_refresh)
        assert len(output.catalog.catalog.streams)
    
    
    @expectedFailure
    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_discover_no_pagination(self,  uuid_mock, mock_auth, http_mocker: HttpMocker)->None:
        def a_table_record():
            return a_snowflake_response("check_connection", JsonPath("$.'data'.[*]"), id_path=JsonPath("$.[1]"))
        
        _given_get_timezone(http_mocker)
       
        for i in ["TEST_TABLE_1","TEST_TABLE_2"]:
            _given_table_with_primary_keys(http_mocker, i)
            _given_read_schema(http_mocker,i )
       

        http_mocker.post(
        table_request().with_show_catalog().with_requestID(_REQUESTID).build(),
        snowflake_response("check_connection", JsonPath("$.'data'"))
        .with_record(
            a_table_record().with_id("TEST_TABLE_1")
        )
        .with_record(
            a_table_record().with_id("TEST_TABLE_2"))
        .with_pagination()
        .with_handle(_HANDLE)
        .build()
        )

        http_mocker.get(
            table_request().with_partition(1).with_handle(_HANDLE).build(is_get=True),
            snowflake_response("check_connection", JsonPath("$.'data'"))
            .with_record(
                a_table_record().with_id("TEST_TABLE_3")
            )
            .with_pagination()
            .with_handle(_HANDLE)
            .build()
        )
        output= _discover(_config(), sync_mode=SyncMode.full_refresh)
        assert len(output.catalog.catalog.streams)

