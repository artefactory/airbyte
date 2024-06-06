from typing import Any, Dict, List, Optional
from unittest import TestCase
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




from integration.pagination import SnowflakePaginationStrategy
from integration.request_builder import SnowflakeRequestBuilder
from integration.config import ConfigBuilder
from source_snowflake import SourceSnowflake


def _config() -> ConfigBuilder:
    return ConfigBuilder().with_account_identifier("123").with_jwt_token("123")

def table_request()->SnowflakeRequestBuilder :
    return SnowflakeRequestBuilder.statement_endpoint(jwt_token="123", account_identifier="123")

def _catalog(sync_mode: SyncMode) -> ConfiguredAirbyteCatalog:
    return CatalogBuilder().with_stream("table_stream", sync_mode).build()


def _source(catalog: ConfiguredAirbyteCatalog, config: Dict[str, Any], state: Optional[List[AirbyteStateMessage]]) -> SourceSnowflake:
    return SourceSnowflake(catalog, config, state)


def snowflake_response() -> HttpResponseBuilder:
    return create_response_builder(
        find_template("table_stream", __file__),
        FieldPath("data"),
        pagination_strategy=SnowflakePaginationStrategy()
    )


def _read(
    config_builder: ConfigBuilder,
    sync_mode: SyncMode,
    state: Optional[List[AirbyteStateMessage]] = None,
    expecting_exception: bool = False
) -> EntrypointOutput:
    catalog = _catalog(sync_mode)
    config = config_builder().build()
    return read(_source(catalog, config, state), config, catalog, state, expecting_exception)


class FullRefreshTest(TestCase):
    @HttpMocker()
    def test_given_one_page_when_read_then_return_records(self, http_mocker: HttpMocker) -> None:
        http_mocker.get(
            table_request().with_table("test","test","test").build(),
            snowflake_response().build()
        )

        output = _read(_config().build())
