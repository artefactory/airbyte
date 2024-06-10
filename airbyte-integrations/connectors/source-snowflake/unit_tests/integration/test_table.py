import re
from typing import Any, Dict, List, Optional
from unittest import TestCase, mock

import freezegun
from airbyte_cdk.sources.source import TState
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, read
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_cdk.test.mock_http.response_builder import (
    FieldPath,
    HttpResponseBuilder,
    NestedPath,
    RecordBuilder,
    create_record_builder,
    create_response_builder,
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
import pytest
from tomlkit import table, value




from integration.pagination import SnowflakePaginationStrategy
from integration.request_builder import SnowflakeRequestBuilder
from integration.config import ConfigBuilder
from source_snowflake import SourceSnowflake

_WAREHOUSE = "WAREHOUSE"
_DATABASE = "DATABASE"
_SCHEMA = "SCHEMA"
_TABLE = "TABLE"
_ROLE = "ROLE"
_HOST = "host.com"



def _config() -> ConfigBuilder:
    return ConfigBuilder(jwt_token="123", host=_HOST, schema=_SCHEMA, database=_DATABASE, role=_ROLE, warehouse=_WAREHOUSE)

def table_request()->SnowflakeRequestBuilder :
    return SnowflakeRequestBuilder(resource="test",jwt_token="123",host=_HOST, schema=_SCHEMA, database=_DATABASE, role=_ROLE, warehouse=_WAREHOUSE)

def _catalog(sync_mode: SyncMode) -> ConfiguredAirbyteCatalog:
    return CatalogBuilder().with_stream("test.test", sync_mode).build()


def _source(catalog: ConfiguredAirbyteCatalog, config: Dict[str, Any], state: Optional[List[AirbyteStateMessage]]) -> SourceSnowflake:
    return SourceSnowflake()


def snowflake_response() -> HttpResponseBuilder:
    return create_response_builder(
        find_template("table_stream", __file__),
        FieldPath("data"),
        pagination_strategy=SnowflakePaginationStrategy()
    )
def a_snowflake_response() -> RecordBuilder:
    return create_record_builder(
        find_template("table_stream", __file__),
        FieldPath("data"),
    )

def _given_show_table(http_mocker: HttpMocker) -> None:
    http_mocker.get(
        table_request().with_show_catalog().build(),
        snowflake_response().with_record(a_snowflake_response()).build()
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
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_given_one_page_when_read_then_return_records(self,mock_auth,http_mocker: HttpMocker) -> None:
        _given_show_table(http_mocker)
        http_mocker.get(
            table_request().with_table(_TABLE).build(),
            snowflake_response().with_record(a_snowflake_response()).build()
        )
        

        output = _read(_config(), sync_mode=SyncMode.full_refresh)
        assert len(output.records)>0