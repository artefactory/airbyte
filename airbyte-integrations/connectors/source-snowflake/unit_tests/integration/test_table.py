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
from tomlkit import table, value




from integration.pagination import SnowflakePaginationStrategy
from integration.request_builder import SnowflakeRequestBuilder
from integration.config import ConfigBuilder
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

def table_request()->SnowflakeRequestBuilder :
    return SnowflakeRequestBuilder.statement_endpoint(host=_HOST, schema=_SCHEMA, database=_DATABASE, role=_ROLE, warehouse=_WAREHOUSE)

def _catalog(sync_mode: SyncMode) -> ConfiguredAirbyteCatalog:
    return CatalogBuilder().with_stream(f"{_SCHEMA}.TEST_TABLE", sync_mode).build()


def _source(catalog: ConfiguredAirbyteCatalog, config: Dict[str, Any], state: Optional[List[AirbyteStateMessage]]) -> SourceSnowflake:
    return SourceSnowflake()


def snowflake_response(file_name,datafield:Path=FieldPath("data")) -> SnowflakeResponseBuilder:
    return create_response_builder(
        find_template(file_name, __file__),
        datafield,
        pagination_strategy=SnowflakePaginationStrategy()
    )
def a_snowflake_response(file_name, datafield:Path=FieldPath("data"), id_path = None, cursor_path = None) -> RecordBuilder:
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
        snowflake_response("primary_keys" ).with_record(a_snowflake_response("primary_keys")).build()
    )


def _given_read_schema(http_mocker:HttpMocker)-> None:
    http_mocker.post(
        table_request().with_table(_TABLE).with_get_schema().with_requestID(_REQUESTID).build(),
        snowflake_response("reponse_get_table",JsonPath("$")).with_record(a_snowflake_response("reponse_get_table",JsonPath("$"))).build()

    )

def _given_get_timezone(http_mocker:HttpMocker)-> None:
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
    def test_given_one_page_when_read_then_return_records(self,uuid_mock, mock_auth,http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_read_schema(http_mocker)
        _given_table_catalog(http_mocker)
        _given_table_with_primary_keys(http_mocker)
        http_mocker.post(
            table_request().with_table(_TABLE).with_requestID(_REQUESTID).with_async().build(),
            snowflake_response("async_response",FieldPath("statementStatusUrl")).with_handle(_HANDLE).build()
        )
        http_mocker.get(
            table_request().with_handle(_HANDLE).build(is_get=True),
            snowflake_response("reponse_get_table",JsonPath("$.'data'")).with_record(a_snowflake_response("reponse_get_table",JsonPath("$.'data'.[*]")).build()
        ))
        

        output = _read(_config(), sync_mode=SyncMode.full_refresh)
        print(output.records)
    
    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_given_three_pages_read_then_return_records(self,uuid_mock, mock_auth,http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_read_schema(http_mocker)
        _given_table_catalog(http_mocker)
        _given_table_with_primary_keys(http_mocker)
        http_mocker.post(
            table_request().with_table(_TABLE).with_requestID(_REQUESTID).with_async().build(),
            snowflake_response("async_response",FieldPath("statementStatusUrl")).with_handle(_HANDLE).build()
        )
        http_mocker.get(
            table_request().with_handle(_HANDLE).build(is_get=True),
            snowflake_response("reponse_get_table",JsonPath("$.'data'")).with_record(a_snowflake_response("reponse_get_table",JsonPath("$.'data'.[*]"))).with_pagination().build()
        )
        http_mocker.get(
            table_request().with_handle(_HANDLE).with_partition(1).build(is_get=True),
            snowflake_response("reponse_get_table",JsonPath("$.'data'")).with_record(a_snowflake_response("reponse_get_table",JsonPath("$.'data'.[*]"))).with_pagination().build()
        )
        http_mocker.get(
            table_request().with_handle(_HANDLE).with_partition(2).build(is_get=True),
            snowflake_response("reponse_get_table",JsonPath("$.'data'")).with_record(a_snowflake_response("reponse_get_table",JsonPath("$.'data'.[*]"))).with_pagination().build()
        )
        output = _read(_config(), sync_mode=SyncMode.full_refresh)
        assert len(output.records) == 3




        
        
