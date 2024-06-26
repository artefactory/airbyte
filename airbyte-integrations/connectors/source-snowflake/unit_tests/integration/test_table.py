
from ast import Raise
import random
from typing import Any, Dict, List, Optional

from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, read
from integration.mocker import CustomHttpMocker, HttpMocker
from datetime import datetime
from unittest import TestCase, mock
from parameterized import parameterized


from integration.response_builder import JsonPath, SnowflakeResponseBuilder, create_response_builder
from airbyte_cdk.test.mock_http import HttpMocker
from airbyte_cdk.test.mock_http.response_builder import (
    FieldPath,
    Path,
    RecordBuilder,
    create_record_builder,
    find_template,
)
from airbyte_cdk.test.state_builder import StateBuilder


from airbyte_protocol.models import (
    AirbyteStateBlob,
    AirbyteStateMessage,
    ConfiguredAirbyteCatalog,
    FailureType,
    StreamDescriptor,
    SyncMode,
)


from integration.pagination import SnowflakePaginationStrategy
from integration.request_builder import SnowflakeRequestBuilder
from integration.config import ConfigBuilder
from integration.snowflake_stream_builder import SnowflakeStreamBuilder
from source_snowflake import SourceSnowflake
from integration.configured_snowflake_streamBuilder import SnowflakeStreamBuilder

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


def _given_read_schema(http_mocker: HttpMocker, file_name="response_get_table") -> None:
    http_mocker.post(
        table_request().with_table(_TABLE).with_get_schema().with_requestID(_REQUESTID).build(),
        snowflake_response(file_name, JsonPath("$")).with_record(
            a_snowflake_response(file_name, JsonPath("$"))).build()

    )


def _given_get_timezone(http_mocker: HttpMocker) -> None:
    http_mocker.post(
        table_request().with_requestID(_REQUESTID).with_timezone().build(),
        snowflake_response("timezone", JsonPath("$")).build()
    )


def _read(
    config_builder: ConfigBuilder,
    sync_mode: SyncMode,
    state: Optional[List[AirbyteStateMessage]] = None,
    expecting_exception: bool = False,
    cursor_field: Optional[List[str]] = None
) -> EntrypointOutput:
    catalog = _catalog(sync_mode,cursor_field)
    config = config_builder.build()
    return read(_source(catalog, config, state), config, catalog, state, expecting_exception)

@mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
@mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
class FullRefreshTest(TestCase):

    def setUp(self) -> None:
        self._http_mocker = CustomHttpMocker()
        self._http_mocker.__enter__()
        _given_get_timezone(self._http_mocker)
        _given_read_schema(self._http_mocker)
        _given_table_catalog(self._http_mocker)
        _given_table_with_primary_keys(self._http_mocker)
    
    def tearDown(self) -> None:
        self._http_mocker.__exit__(None,None,None)
        
        
    def test_given_one_page_when_read_then_return_records(self, uuid_mock, mock_auth) -> None:
        self._http_mocker.post(
            table_request().with_table(_TABLE).with_requestID(_REQUESTID).with_async().build(),
            snowflake_response("async_response", FieldPath("statementStatusUrl")).with_handle(_HANDLE).build()
        )
        
        self._http_mocker.get(
            table_request().with_handle(_HANDLE).build(is_get=True),
            snowflake_response("response_get_table", JsonPath("$.'data'"))
            .with_record(a_snowflake_response("response_get_table", JsonPath("$.'data'.[*]")))
            .build())
        
        output = _read(_config(), sync_mode=SyncMode.full_refresh)
        assert len(output.records)==1

    def test_given_three_pages_read_then_return_records(self, uuid_mock, mock_auth) -> None:
        self._http_mocker.post(
            table_request().with_table(_TABLE).with_requestID(_REQUESTID).with_async().build(),
            snowflake_response("async_response", FieldPath("statementStatusUrl")).with_handle(_HANDLE).build()
        )

        self._http_mocker.get(
            table_request().with_handle(_HANDLE).build(is_get=True),
            snowflake_response("response_get_table", JsonPath("$.'data'")).with_record(
                a_snowflake_response("response_get_table", JsonPath("$.'data'.[*]"))).with_pagination().build()
        )
        self._http_mocker.get(
            table_request().with_handle(_HANDLE).with_partition(1).build(is_get=True),
            snowflake_response("response_get_table", JsonPath("$.'data'")).with_record(
                a_snowflake_response("response_get_table", JsonPath("$.'data'.[*]"))).with_pagination().build()
        )
        self._http_mocker.get(
            table_request().with_handle(_HANDLE).with_partition(2).build(is_get=True),
            snowflake_response("response_get_table", JsonPath("$.'data'")).with_record(
                a_snowflake_response("response_get_table", JsonPath("$.'data'.[*]"))).with_pagination().build()
        )
        output = _read(_config(), sync_mode=SyncMode.full_refresh)
        assert len(output.records) == 3

    

    @parameterized.expand(
            [
                (["TEST_COLUMN_20"],12,0,3,"1970-01-04"),
                (["TEST_COLUMN_21"],13,30000, 300001, "1970-01-04T11:20:01.000000"),
                (["TEST_COLUMN_2"],11,3,4,4),
                (["TEST_COLUMN_14"],5,"aa","ab","ab")
            ],
    )
    def test_given_cursor_set_state(
        self,
        mock_auth,
        uuid_mock,
        cursor_field,
        cursor_index,
        value_first_record,
        value_second_record,
        expected_state_cursor_value,
        
        ) -> None:

        def a_record():
            return a_snowflake_response("response_get_table",JsonPath("$.'data'[*]"), cursor_path=JsonPath(f"$.[{cursor_index}]"))

        self._http_mocker.post(
            table_request().with_table(_TABLE).with_requestID(_REQUESTID).with_async().with_order_by(cursor_field).build(),
            snowflake_response("async_response",FieldPath("statementStatusUrl")).with_handle(_HANDLE).build()
        )
        self._http_mocker.get(
            table_request().with_handle(_HANDLE).build(is_get=True),
            snowflake_response("response_get_table",JsonPath("$.'data'"))
             .with_record(
                 a_record()
                 .with_cursor(value_first_record))
             .with_record(
                 a_record()
                 .with_cursor(value_second_record))
             .build()
            )

        output = _read(_config(), sync_mode=SyncMode.full_refresh, cursor_field=cursor_field, state={})
        #assert output.most_recent_state.stream_state == AirbyteStateBlob(**{f"{cursor_field[0]}":expected_state_cursor_value})

    @parameterized.expand(
            [
                (429),
                (random.randrange(500, 600)),
                (random.randrange(500, 600)),
            ],
    )
    @mock.patch("source_snowflake.streams.snowflake_parent_stream.SnowflakeStream.retry_factor", return_value=0, new_callable=mock.PropertyMock)
    def test_retry_on_post_error(self,error_code,property_mock, mock_auth, uuid_mock ) -> None:
        self._http_mocker.post(
            table_request().with_table(_TABLE).with_requestID(_REQUESTID).with_async().build(),
            [
            snowflake_response("async_response",FieldPath("statementStatusUrl"))
            .with_handle(_HANDLE)
            .with_status_code(error_code)
            .build(),
            snowflake_response("async_response",FieldPath("statementStatusUrl"))
            .with_handle(_HANDLE)
            .build()
            ]
        )
        self._http_mocker.get(
            table_request().with_handle(_HANDLE).build(is_get=True),
            snowflake_response("response_get_table",JsonPath("$.'data'")).with_record(a_snowflake_response("response_get_table",JsonPath("$.'data'[*]"))).build()
        )
        output = _read(_config(), sync_mode=SyncMode.full_refresh)
        assert len(output.records) == 1

    

    @parameterized.expand(
            [
                (429),
                (202),
                (random.randrange(500, 600)),
                (random.randrange(500, 600)),
            ],
    )
    @mock.patch("source_snowflake.streams.snowflake_parent_stream.SnowflakeStream.retry_factor", return_value=0, new_callable=mock.PropertyMock)
    def test_retry_on_get_error(
        self,error_code,property_mock, mock_auth, uuid_mock ) -> None:
        self._http_mocker.post(
            table_request()
            .with_table(_TABLE)
            .with_requestID(_REQUESTID)
            .with_async()
            .build(),
            snowflake_response("async_response",FieldPath("statementStatusUrl"))
            .with_handle(_HANDLE)
            .build()
        )
        self._http_mocker.get(
            table_request()
            .with_handle(_HANDLE)
            .build(is_get=True),
            [
            snowflake_response("response_get_table",JsonPath("$.'data'"))
            .with_record(a_snowflake_response("response_get_table",JsonPath("$.'data'[*]")))
            .with_status_code(error_code).build(),
            snowflake_response("response_get_table",JsonPath("$.'data'"))
            .with_record(a_snowflake_response("response_get_table",JsonPath("$.'data'[*]"))).build(),
            ]
        )
        output = _read(_config(), sync_mode=SyncMode.full_refresh)
        assert len(output.records) == 1
    
    @parameterized.expand(
            [
                (400),
                (429),
                (random.randrange(400, 600)),
                (random.randrange(400, 600)),
                (random.randrange(400, 600)),
            ],
    )
    @mock.patch("source_snowflake.streams.snowflake_parent_stream.SnowflakeStream.retry_factor", return_value=0, new_callable=mock.PropertyMock)
    @mock.patch("source_snowflake.streams.snowflake_parent_stream.SnowflakeStream.max_retries", return_value=0, new_callable=mock.PropertyMock)
    def test_fail_on_post_error_and_max_retry(
        self,
        error_code, 
        mock_retries, 
        mock_time, 
        uuid_mock, 
        mock_auth) -> None:

        self._http_mocker.post(
            table_request().with_table(_TABLE).with_requestID(_REQUESTID).with_async().build(),
            snowflake_response("async_response",FieldPath("statementStatusUrl")).with_handle(_HANDLE).with_status_code(error_code).build()
        )
        output = _read(_config(), sync_mode=SyncMode.full_refresh, expecting_exception=True)
        assert output.errors[-1].trace.error.failure_type == FailureType.config_error
    

class IncrementalTest(TestCase):
    """
    See the documentation about the values of record in the function source_snowflake.schema_builder.format_field
    """

    def setUp(self):
        self.sync_mode = SyncMode.incremental
        self.stream_name = f"{_SCHEMA}.{_TABLE}"
        self._a_record = lambda cursor_path=None: a_snowflake_response(file_name="response_get_table",
                                                                       datafield=JsonPath("$.'data'[*]"),
                                                                       cursor_path=cursor_path)

    @parameterized.expand([
        ("ID", 0, 3, ("1", "2", "3"), 'ORDER BY ID ASC'),
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
            .with_statement(statement)
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

        stream_builder = (SnowflakeStreamBuilder().with_name(self.stream_name)
                          .with_cursor_field([cursor_field])
                          .with_sync_mode(self.sync_mode))
        catalog = CatalogBuilder().with_stream(stream_builder).build()

        output = self._read(_config(), catalog)
        most_recent_state = output.most_recent_state

        assert len(output.records) == 3
        assert most_recent_state.stream_descriptor == StreamDescriptor(name=self.stream_name)
        assert most_recent_state.stream_state == AirbyteStateBlob(**{f"{cursor_field}": expected_cursor_value})

    @parameterized.expand([
        ("ID", 0, 3, ("1", "2", "3"), 2, "ID>=2 ORDER BY ID ASC"),
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
        print('-'*30)
        print('initial_state', initial_state)
        print('-'*30)
        cursor_path = JsonPath(f"$.[{cursor_index}]")
        cursor_value_1, cursor_value_2, cursor_value_3 = cursor_values

        http_mocker.post(
            table_request()
            .with_table(_TABLE)
            .with_requestID(_REQUESTID)
            .with_statement(statement)
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

        stream_builder = (SnowflakeStreamBuilder().with_name(self.stream_name)
                          .with_cursor_field([cursor_field])
                          .with_sync_mode(self.sync_mode))
        catalog = CatalogBuilder().with_stream(stream_builder).build()
        state = StateBuilder().with_stream_state(self.stream_name, initial_state).build()

        output = self._read(_config(), catalog, state)
        most_recent_state = output.most_recent_state

        assert len(output.records) == 3
        assert most_recent_state.stream_descriptor == StreamDescriptor(name=self.stream_name)
        assert most_recent_state.stream_state == AirbyteStateBlob(**{f"{cursor_field}": expected_cursor_value})

    @parameterized.expand([
        ("ID", 0, 4, ("1", "2", "3"), 4, "ID>=4 ORDER BY ID ASC"),
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
    def test_incremental_with_initial_higher_than_highest_record_state(self,
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
            .with_statement(statement)
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

        stream_builder = (SnowflakeStreamBuilder().with_name(self.stream_name)
                          .with_cursor_field([cursor_field])
                          .with_sync_mode(self.sync_mode))
        catalog = CatalogBuilder().with_stream(stream_builder).build()
        state = StateBuilder().with_stream_state(self.stream_name, initial_state).build()

        output = self._read(_config(), catalog, state)
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


class UnrecognizedTypeTest(TestCase):
    @mock.patch("source_snowflake.streams.snowflake_parent_stream.uuid.uuid4", return_value=_REQUESTID)
    @mock.patch("source_snowflake.source.SnowflakeJwtAuthenticator")
    @HttpMocker()
    def test_unrecognized_type(self, uuid_mock, mock_auth, http_mocker: HttpMocker) -> None:
        _given_get_timezone(http_mocker)
        _given_table_catalog(http_mocker)

        http_mocker.post(
            table_request()
            .with_table(_TABLE)
            .with_get_schema()
            .with_requestID(_REQUESTID)
            .build(),
            snowflake_response("response_get_table_with_unknown_type", JsonPath("$"))
            .build()
        )
        output = _read(_config(), sync_mode=SyncMode.full_refresh)
        expected_error = ('The type UNKNOWN TYPE is not recognized. '
                          'Please, contact Airbyte support to update the connector to handle this new type')
        assert output.errors[0].trace.error.internal_message == expected_error
        assert "SnowflakeTypeNotRecognizedError" in output.errors[0].trace.error.stack_trace
