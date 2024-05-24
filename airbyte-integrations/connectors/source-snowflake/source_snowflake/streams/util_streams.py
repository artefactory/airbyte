import uuid
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union

import requests
from airbyte_protocol.models import SyncMode

from source_snowflake.schema_builder import date_and_time_snowflake_type_airbyte_type, string_snowflake_type_airbyte_type, \
    mapping_snowflake_type_airbyte_type
from source_snowflake.streams.snowflake_parent_stream import SnowflakeStream


class TableCatalogStream(SnowflakeStream):
    DATABASE_NAME_COLUMN = "name"
    SCHEMA_NAME_COLUMN = "schema_name"
    RETENTION_TIME_COLUMN = "retention_time"

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
        column_names_to_be_extracted_from_records = [self.DATABASE_NAME_COLUMN, self.SCHEMA_NAME_COLUMN, self.RETENTION_TIME_COLUMN]
        index_of_columns_from_names = self.get_index_of_columns_from_names(response_json, column_names_to_be_extracted_from_records)

        database_name_index = index_of_columns_from_names[self.DATABASE_NAME_COLUMN]
        schema_name_index = index_of_columns_from_names[self.SCHEMA_NAME_COLUMN]
        retention_time_index = index_of_columns_from_names[self.RETENTION_TIME_COLUMN]

        for record in response_json.get("data", []):
            yield {'schema': record[schema_name_index],
                   'table': record[database_name_index],
                   'retention_time': record[retention_time_index],
                   }


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
            yield {'column_name': row_type['name'],
                   'type': row_type['type']}

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
            yield {'primary_key': record[primary_column_name_index]}

    def __str__(self):
        return f"Current stream has this table object as constructor {self.table_object}"


class StreamLauncher(SnowflakeStream):

    def __init__(self, url_base, config, table_object, current_state, cursor_field, authenticator, where_clause=None):
        super().__init__(authenticator=authenticator)
        self._url_base = url_base
        self._config = config
        self._table_object = table_object
        self._json_schema_properties = None
        self.table_schema_stream = TableSchemaStream(url_base=url_base,
                                                     config=config,
                                                     table_object=table_object,
                                                     authenticator=authenticator)
        self.current_state = current_state
        self._cursor_field = cursor_field
        self.where_clause = where_clause


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
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
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
            # TODO MAKE SURE THE CURSOR IS SINGLE VALUE AND NOT A STARTING AND ENDING VALUE (ex: window)
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
        state_sql_condition = f"{self.cursor_field}>={current_state_value}"
        if self.cursor_field.upper() not in self._json_schema_properties:
            raise ValueError(f'this field {self.cursor_field} should be present in schema. Make sure the column is present in your stream')

        if self._json_schema_properties[self.cursor_field.upper()]["type"].upper() in date_and_time_snowflake_type_airbyte_type:
            state_sql_condition = f"TO_TIMESTAMP({self.cursor_field})>=TO_TIMESTAMP({current_state_value})"

        if self._json_schema_properties[self.cursor_field.upper()]["type"].upper() in string_snowflake_type_airbyte_type:
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
                raise ValueError(f"The type {snowflake_column_type} is not recognized. "
                                 f"Please, contact Airbyte support to update the connector to handle this new type")
            airbyte_column_type_object = mapping_snowflake_type_airbyte_type[snowflake_column_type]
            properties[column_name] = airbyte_column_type_object

        self._json_schema_properties = properties
        return json_schema

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        yield [response_json]


class StreamLauncherChangeDataCapture(StreamLauncher):
    RETENTION_DAYS = 30

    @property
    def statement(self):
        database = self.config["database"]
        schema = self.table_object["schema"]
        table = self.table_object["table"]

        history_date = datetime.now() - timedelta(days=self.RETENTION_DAYS)
        history_timestamp = history_date.strftime("%Y-%m-%d %H:%M:%S")

        statement = (f'SELECT * FROM "{database}"."{schema}"."{table}" '
                        f'CHANGES(INFORMATION => DEFAULT) AT(TIMESTAMP => TO_TIMESTAMP(\'{history_timestamp}\'))')

        if self.where_clause:
            statement = f'{statement} WHERE {self.where_clause}'
        print('statement', statement)
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
                raise ValueError(f"The type {snowflake_column_type} is not recognized. "
                                 f"Please, contact Airbyte support to update the connector to handle this new type")
            airbyte_column_type_object = mapping_snowflake_type_airbyte_type[snowflake_column_type]
            properties[column_name] = airbyte_column_type_object

        mapping_cdc_metadata_columns_to_types = {
            "METADATA$ACTION": "text",
            "METADATA$ISUPDATE": "boolean",
            "METADATA$ROW_ID": "text",
        }
        for column_name, column_type in mapping_cdc_metadata_columns_to_types.items():
            properties[column_name] = mapping_snowflake_type_airbyte_type[column_type.upper()]

        return json_schema


