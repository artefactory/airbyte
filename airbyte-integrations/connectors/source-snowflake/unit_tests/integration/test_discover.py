from typing import Any, Dict, List, Optional
from unittest import TestCase, expectedFailure, mock
from airbyte_cdk import ConfiguredAirbyteCatalog, SyncMode
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, discover
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from integration.mocker import CustomHttpMocker, HttpMocker
from integration.pagination import SnowflakePaginationStrategy
from integration.response_builder import JsonPath, SnowflakeResponseBuilder, create_response_builder
from airbyte_protocol.models import (
    AirbyteStateMessage,
    ConfiguredAirbyteCatalog,
    SyncMode,
)
from airbyte_cdk.test.mock_http.response_builder import (
    FieldPath,
    Path,
    RecordBuilder,
    create_record_builder,
    find_template,
)

from integration.config import ConfigBuilder
from integration.request_builder import SnowflakeRequestBuilder
from integration.response_builder import JsonPath
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


def snowflake_response(file_name, datafield: Path = FieldPath("data")) -> SnowflakeResponseBuilder:
    return create_response_builder(
        find_template(file_name, __file__),
        datafield,
        pagination_strategy=SnowflakePaginationStrategy()
    )


def a_snowflake_record(file_name, datafield: Path = FieldPath("data"), id_path=None, cursor_path=None) -> RecordBuilder:
    return create_record_builder(
        find_template(file_name, __file__),
        datafield,
        record_id_path=id_path,
        record_cursor_path=cursor_path
    )


def _given_table_catalog(http_mocker: HttpMocker) -> None:
    http_mocker.post(
        table_request().with_show_catalog().with_requestID(_REQUESTID).build(),
        snowflake_response("check_connection").with_record(a_snowflake_record("check_connection")).build()
    )


def _given_table_with_primary_keys(http_mocker: HttpMocker, table) -> None:
    http_mocker.post(
        table_request().with_table(table).with_show_primary_keys().with_requestID(_REQUESTID).build(),
        snowflake_response("primary_keys").with_record(a_snowflake_record("primary_keys")).build()
    )

def _given_read_schema(http_mocker: HttpMocker, table) -> None:
    http_mocker.post(
        table_request().with_table(table).with_get_schema().with_requestID(_REQUESTID).build(),
        snowflake_response("response_get_table", JsonPath("$")).with_record(
            a_snowflake_record("response_get_table", JsonPath("$"))).build()
    )

def _given_get_timezone(http_mocker: HttpMocker) -> None:
    http_mocker.post(
        table_request().with_requestID(_REQUESTID).with_timezone().build(),
        snowflake_response("timezone", JsonPath("$.'data'")).build())


def _discover(
        config_builder: ConfigBuilder,
        sync_mode: SyncMode,
        expecting_exception: bool = False
) -> EntrypointOutput:
        catalog = _catalog(sync_mode)
        config = config_builder.build()
        return discover(_source(catalog, config, None), config, expecting_exception)

@mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
@mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
class DiscoverTest(TestCase):

    def setUp(self) -> None:
        self._http_mocker = CustomHttpMocker()
        self._http_mocker.__enter__()
        _given_get_timezone(self._http_mocker)
    
    def tearDown(self) -> None:
        self._http_mocker.__exit__(None,None,None)

    @classmethod
    def _a_table_record(cls):
        return a_snowflake_record("check_connection", JsonPath("$.'data'.[*]"), id_path=JsonPath("$.[1]"))
    
    @classmethod
    def _a_primary_key_record(cls):
            return a_snowflake_record("primary_keys",JsonPath("$.data.[*]"), id_path=JsonPath("$.[4]"))

    def test_discover_no_pagination(self,  uuid_mock, mock_auth)->None:
            
        #_given_get_timezone(self._http_mocker)
        for i in ["TEST_TABLE_2","TEST_TABLE_1"]:
            _given_table_with_primary_keys(self._http_mocker, i)
            _given_read_schema(self._http_mocker,i)
       

        self._http_mocker.post(
        table_request().with_show_catalog().with_requestID(_REQUESTID).build(),
        snowflake_response("check_connection", JsonPath("$.'data'"))
        .with_record(
            self._a_table_record().with_id("TEST_TABLE_1")
        )
        .with_record(
            self._a_table_record().with_id("TEST_TABLE_2"))
        .build()
        )
        
        
        output= _discover(_config(), sync_mode=SyncMode.full_refresh)
        assert len(output.catalog.catalog.streams)==2
    
    # expected failure doesn't work as the matchers are evaluated in the tear down , uncomment to have the full test
    @expectedFailure # Not Implemented in the source
    def test_discover_pagination(self,  uuid_mock, mock_auth)->None:

        for i in ["TEST_TABLE_1","TEST_TABLE_2"]:#,"TEST_TABLE_3"]:
            _given_table_with_primary_keys(self._http_mocker, i)
            _given_read_schema(self._http_mocker,i )
       

        self._http_mocker.post(
        table_request().with_show_catalog().with_requestID(_REQUESTID).build(),
        snowflake_response("check_connection", JsonPath("$.'data'"))
        .with_record(
            self._a_table_record().with_id("TEST_TABLE_1")
        )
        .with_record(
            self._a_table_record().with_id("TEST_TABLE_2"))
        .with_pagination()
        .with_handle(_HANDLE)
        .build()
        )

        # self._http_mocker.get(
        #     table_request().with_partition(1).with_handle(_HANDLE).build(is_get=True),
        #     snowflake_response("check_connection", JsonPath("$.'data'"))
        #     .with_record(
        #         self._a_table_record().with_id("TEST_TABLE_3")
        #     )
        #     .with_pagination()
        #     .with_handle(_HANDLE)
        #     .build()
        # )


        output= _discover(_config(), sync_mode=SyncMode.full_refresh)
        assert len(output.catalog.catalog.streams)==3

    
    def test_discover_primary_keys(self,  uuid_mock, mock_auth):
        
        _given_read_schema(self._http_mocker,"TEST_TABLE")
        _given_table_catalog(self._http_mocker)

        self._http_mocker.post(
            table_request().with_table(_TABLE).with_show_primary_keys().with_requestID(_REQUESTID).build(),
            snowflake_response("primary_keys",JsonPath("$.data")).with_record(self._a_primary_key_record().with_id("TEST_COLUMN")).build()
        )

        output= _discover(_config(), sync_mode=SyncMode.full_refresh)
        assert output.catalog.catalog.streams[0].source_defined_primary_key[0] == ["TEST_COLUMN"]

    
    @expectedFailure # Not Implemented in the source
    def test_discover_composite_primary_key(self,  uuid_mock, mock_auth):
        
        _given_read_schema(self._http_mocker,"TEST_TABLE")
        _given_table_catalog(self._http_mocker)

        self._http_mocker.post(
            table_request().with_table(_TABLE).with_show_primary_keys().with_requestID(_REQUESTID).build(),
            snowflake_response("primary_keys",JsonPath("$.data")).with_record(self._a_primary_key_record().with_id("TEST_COLUMN")).with_record(self._a_primary_key_record().with_id("TEST_COLUMN_2")).build()
        )
        output= _discover(_config(), sync_mode=SyncMode.full_refresh)
        print(output.catalog.catalog.streams[0].source_defined_primary_key[0])
        assert output.catalog.catalog.streams[0].source_defined_primary_key[0] == ["TEST_COLUMN","TEST_COLUMN2"]
    




