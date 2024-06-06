import threading
import uuid
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union

import requests
from airbyte_protocol.models import SyncMode

from source_snowflake.schema_builder import date_and_time_snowflake_type_airbyte_type, string_snowflake_type_airbyte_type, \
    mapping_snowflake_type_airbyte_type, get_generic_type_from_schema_type, convert_time_zone_time_stamp_suffix_to_offset_hours, \
    convert_utc_to_time_zone, convert_utc_to_time_zone_date
from source_snowflake.snowflake_exceptions import CursorFieldNotPresentInSchemaError, emit_airbyte_error_message, \
    SnowflakeTypeNotRecognizedError, StartHistoryTimeNotSetError

from source_snowflake.streams.snowflake_parent_stream import SnowflakeStream


class TableCatalogStream(SnowflakeStream):
    DATABASE_NAME_COLUMN = "name"
    SCHEMA_NAME_COLUMN = "schema_name"
    RETENTION_TIME_COLUMN = "retention_time"
    CREATED_ON_COLUMN = "created_on"
    CHANGE_TRACKING_COLUMN = "change_tracking"

    def __init__(self, url_base, config, authenticator):
        super().__init__(authenticator=authenticator)
        self._url_base = url_base
        self._config = config

    @property
    def statement(self):
        database = self.config["database"]
        schema = self.config.get('schema', '')
        if not schema:
            return f"SHOW TABLES IN DATABASE {database}"

        return f"SHOW TABLES IN SCHEMA {database}.{schema}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        column_names_to_be_extracted_from_records = [self.DATABASE_NAME_COLUMN, self.SCHEMA_NAME_COLUMN,
                                                     self.RETENTION_TIME_COLUMN, self.CREATED_ON_COLUMN,
                                                     self.CHANGE_TRACKING_COLUMN]
        index_of_columns_from_names = self.get_index_of_columns_from_names(response_json, column_names_to_be_extracted_from_records)

        database_name_index = index_of_columns_from_names[self.DATABASE_NAME_COLUMN]
        schema_name_index = index_of_columns_from_names[self.SCHEMA_NAME_COLUMN]
        retention_time_index = index_of_columns_from_names[self.RETENTION_TIME_COLUMN]
        created_on_index = index_of_columns_from_names[self.CREATED_ON_COLUMN]
        change_tracking_index = index_of_columns_from_names[self.CHANGE_TRACKING_COLUMN]

        for record in response_json.get("data", []):
            try:
                yield {'schema': record[schema_name_index],
                       'table': record[database_name_index],
                       'retention_time': record[retention_time_index],
                       'created_on': record[created_on_index],
                       'change_tracking': record[change_tracking_index],
                       }
            except Exception:
                error_message = 'Unexpected error while reading record'
                emit_airbyte_error_message(error_message)


class TableSchemaStream(SnowflakeStream):
    def __init__(self, url_base, config, table_object, authenticator):
        super().__init__(authenticator=authenticator)
        self._url_base = url_base
        self._config = config
        self._table_object = table_object

    @property
    def statement(self):
        database = self.config["database"]
        schema = self.table_object["schema"]
        table = self.table_object["table"]

        return f'SELECT TOP 1 * FROM "{database}"."{schema}"."{table}"'

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        # checks in the response nested fields response -> resultSetMetaData -> rowType
        for row_type in response_json.get('resultSetMetaData', {'rowType': []}).get('rowType', []):
            try:
                yield {'column_name': row_type['name'],
                       'type': row_type['type'],
                       'extTypeName': row_type.get('extTypeName', None), }  # Special attribute in metadata to flag geography data
            except Exception:
                error_message = 'Unexpected error while reading record'
                emit_airbyte_error_message(error_message)

    def __str__(self):
        return f"Current stream has this table object as constructor {self.table_object}"


class PrimaryKeyStream(SnowflakeStream):
    PRIMARY_COLUMN_NAME = 'column_name'

    def __init__(self, url_base, config, table_object, authenticator):
        super().__init__(authenticator=authenticator)
        self._url_base = url_base
        self._config = config
        self._table_object = table_object

    @property
    def statement(self):
        database = self.config["database"]
        schema = self.table_object["schema"]
        table = self.table_object["table"]

        return f'SHOW PRIMARY KEYS IN "{database}"."{schema}"."{table}"'

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        column_names_to_be_extracted_from_records = [self.PRIMARY_COLUMN_NAME]
        index_of_columns_from_names = self.get_index_of_columns_from_names(response_json, column_names_to_be_extracted_from_records)

        primary_column_name_index = index_of_columns_from_names[self.PRIMARY_COLUMN_NAME]
        for record in response_json.get("data", []):
            try:
                yield {'primary_key': record[primary_column_name_index]}
            except Exception:
                error_message = 'Unexpected error while reading record'
                emit_airbyte_error_message(error_message)

    def __str__(self):
        return f"Current stream has this table object as constructor {self.table_object}"


class CurrentTimeZoneStream(SnowflakeStream):
    CURRENT_TIME_COLUMN_NAME = 'CURRENT_TIME'
    _is_set = False
    offset = None
    _lock = threading.Lock()

    def __init__(self, url_base, config, authenticator):
        super().__init__(authenticator=authenticator)
        self._url_base = url_base
        self._config = config


    @property
    def statement(self):
        return f'SELECT TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()) as {self.CURRENT_TIME_COLUMN_NAME}'

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        column_names_to_be_extracted_from_records = [self.CURRENT_TIME_COLUMN_NAME]
        index_of_columns_from_names = self.get_index_of_columns_from_names(response_json, column_names_to_be_extracted_from_records)

        current_time_column_name_index = index_of_columns_from_names[self.CURRENT_TIME_COLUMN_NAME]
        for record in response_json.get("data", []):
            try:
                yield {'current_time': record[current_time_column_name_index]}
            except Exception:
                error_message = 'Unexpected error while reading record'
                emit_airbyte_error_message(error_message)


    @classmethod
    def set_off_set(cls, url_base, config, authenticator):
        with cls._lock:
            if not cls._is_set:
                current_time_zone_stream = cls(url_base, config, authenticator)
                current_time_record = next(current_time_zone_stream.read_records(SyncMode.full_refresh))
                current_time_with_time_zone = current_time_record['current_time']
                current_time_time_zone_suffix = current_time_with_time_zone.split(' ')[1]
                cls.offset = convert_time_zone_time_stamp_suffix_to_offset_hours(current_time_time_zone_suffix)
                cls._is_set = True

    @classmethod
    def get_current_time_snowflake_time_zone(cls, url_base, config, authenticator):
        cls.set_off_set(url_base, config, authenticator)
        current_time_date_utc = datetime.now(timezone.utc)
        return convert_utc_to_time_zone_date(current_time_date_utc, cls.offset)


class StreamLauncher(SnowflakeStream):

    def __init__(self, url_base, config, table_object, current_state, cursor_field, authenticator, where_clause=None,
                 start_history_timestamp=None):
        super().__init__(authenticator=authenticator)
        self._json_schema_set = False
        self._url_base = url_base
        self._config = config
        self._table_object = table_object
        self._json_schema = None
        self.table_schema_stream = TableSchemaStream(url_base=url_base,
                                                     config=config,
                                                     table_object=table_object,
                                                     authenticator=authenticator)
        self.current_state = current_state
        self._cursor_field = cursor_field
        self.where_clause = where_clause

        self.start_history_timestamp = start_history_timestamp


    @property
    def cursor_field(self):
        return self._cursor_field

    @cursor_field.setter
    def cursor_field(self, new_cursor_field):
        self._cursor_field = new_cursor_field

    @property
    def name(self):
        return f"stream_launcher_{self.table_object['schema']}.{self.table_object['table']}"

    @property
    def statement(self):
        database = self.config["database"]
        schema = self.table_object["schema"]
        table = self.table_object["table"]
        statement = f'SELECT * FROM "{database}"."{schema}"."{table}"'

        if self.where_clause:
            statement = f'{statement} WHERE {self.where_clause}'

        return statement

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        params = {
            "requestId": str(uuid.uuid4()),
            "async": "true"
        }
        return params

    def get_updated_statement(self):
        """
        Can be used consistently only in request_body_json
        otherwise we are not sure stream slice is the next slice and _cursor_value is updated with the correct data
        """

        updated_statement = self.statement
        current_state_value = None

        if self.current_state:
            current_state_value = self.current_state.get(self.cursor_field, None)

        if current_state_value:
            state_sql_condition = self._get_state_sql_condition(current_state_value)
            key_word_where = " where "  # spaces in case there is a where in a table name
            if key_word_where in self.statement.lower():
                updated_statement = f"{self.statement} AND {state_sql_condition}"
            else:
                updated_statement = f"{self.statement} WHERE {state_sql_condition}"

        if self.cursor_field:
            updated_statement = f"{updated_statement} ORDER BY {self.cursor_field} ASC"

        return updated_statement

    def _get_state_sql_condition(self, current_state_value):
        """
        The schema must have been generated before
        """

        state_sql_condition = None

        if not self._json_schema_set:
            self.get_json_schema()

        if self.cursor_field.upper() not in self._json_schema['properties']:
            error_message = f'this field {self.cursor_field} should be present in schema. Make sure the column is present in your stream'
            emit_airbyte_error_message(error_message)
            raise CursorFieldNotPresentInSchemaError(error_message)

        schema_type = self._json_schema['properties'][self.cursor_field.upper()]

        generic_type = get_generic_type_from_schema_type(schema_type)

        if generic_type == "timestamp_with_timezone":
            state_sql_condition = f"TO_TIMESTAMP_TZ({self.cursor_field})>=TO_TIMESTAMP_TZ('{current_state_value}')"

        if generic_type == "date":
            state_sql_condition = f"TO_TIMESTAMP({self.cursor_field})>=TO_TIMESTAMP('{current_state_value}')"

        elif generic_type == "number":
            state_sql_condition = f"{self.cursor_field}>={current_state_value}"

        elif generic_type == "string":
            state_sql_condition = f"{self.cursor_field}>='{current_state_value}'"

        return state_sql_condition

    def request_body_json(
            self,
            stream_state: Optional[Mapping[str, Any]],
            stream_slice: Optional[Mapping[str, Any]] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:

        current_statement = self.get_updated_statement()
        json_payload = {
            "statement": current_statement,
            "role": self.config['role'],
            "warehouse": self.config['warehouse'],
            "database": self.config['database'],
            "timeout": self.TIME_OUT_IN_SECONDS,
        }

        schema = self.table_object.get('schema', '')
        if schema:
            json_payload['schema'] = schema

        return json_payload

    def get_json_schema(self) -> Mapping[str, Any]:

        if self._json_schema_set:
            return self._json_schema

        properties = {}
        json_schema = {
            "$schema": "https://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": True,
            "properties": properties,
        }

        for column_object in self.table_schema_stream.read_records(sync_mode=SyncMode.full_refresh):
            column_name = column_object['column_name']
            snowflake_column_type = column_object['type'].upper()
            if snowflake_column_type not in mapping_snowflake_type_airbyte_type:
                error_message = (f"The type {snowflake_column_type} is not recognized. "
                                 f"Please, contact Airbyte support to update the connector to handle this new type")
                emit_airbyte_error_message(error_message)
                raise SnowflakeTypeNotRecognizedError(error_message)

            airbyte_column_type_object = mapping_snowflake_type_airbyte_type[snowflake_column_type]
            properties[column_name] = airbyte_column_type_object

        self._json_schema = json_schema
        self._json_schema_set = True

        return self._json_schema

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        yield [response_json]


class StreamLauncherChangeDataCapture(StreamLauncher):
    mapping_cdc_metadata_columns_to_types = {
        "METADATA$ACTION": "text",
        "METADATA$ISUPDATE": "boolean",
        "METADATA$ROW_ID": "text",
    }
    def __init__(self, url_base, config, table_object, current_state, cursor_field, authenticator, where_clause=None,
                 start_history_timestamp=None, is_full_refresh=False):
        super().__init__(url_base, config, table_object, current_state, cursor_field, authenticator, where_clause,
                         start_history_timestamp)
        self.start_history_timestamp = start_history_timestamp
        self.is_full_refresh = is_full_refresh

    @property
    def statement(self):
        database = self.config["database"]
        schema = self.table_object["schema"]
        table = self.table_object["table"]

        if self.start_history_timestamp is None and not self.is_full_refresh:
            error_message = f'{self.start_history_timestamp} should be set to read history'
            emit_airbyte_error_message(error_message)
            raise StartHistoryTimeNotSetError(error_message)

        if not self.is_full_refresh:
            statement = (f'SELECT * FROM "{database}"."{schema}"."{table}" '
                         f'CHANGES(INFORMATION => DEFAULT) AT(TIMESTAMP => TO_TIMESTAMP_LTZ(\'{self.start_history_timestamp}\'))')
        else:
            statement = f'SELECT * FROM "{database}"."{schema}"."{table}"'

        if self.where_clause:
            statement = f'{statement} WHERE {self.where_clause}'

        return statement

    def get_json_schema(self) -> Mapping[str, Any]:
        properties = {}
        json_schema = {
            "$schema": "https://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": True,
            "properties": properties,
        }

        for column_object in self.table_schema_stream.read_records(sync_mode=SyncMode.full_refresh):
            column_name = column_object['column_name']
            snowflake_column_type = column_object['type'].upper()

            if snowflake_column_type not in mapping_snowflake_type_airbyte_type:
                error_message = (f"The type {snowflake_column_type} is not recognized. "
                                 f"Please, contact Airbyte support to update the connector to handle this new type")
                emit_airbyte_error_message(error_message)
                raise SnowflakeTypeNotRecognizedError(error_message)

            airbyte_column_type_object = mapping_snowflake_type_airbyte_type[snowflake_column_type]
            properties[column_name] = airbyte_column_type_object

        if not self.is_full_refresh:
            for column_name, column_type in self.mapping_cdc_metadata_columns_to_types.items():
                properties[column_name] = mapping_snowflake_type_airbyte_type[column_type.upper()]

        return json_schema

    def request_body_json(
            self,
            stream_state: Optional[Mapping[str, Any]],
            stream_slice: Optional[Mapping[str, Any]] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:

        json_payload = {
            "statement": self.statement,  # The statement is provided directly because state should not have any effect on the behavior
            "role": self.config['role'],
            "warehouse": self.config['warehouse'],
            "database": self.config['database'],
            "timeout": self.TIME_OUT_IN_SECONDS,
        }

        schema = self.table_object.get('schema', '')
        if schema:
            json_payload['schema'] = schema

        return json_payload
      
