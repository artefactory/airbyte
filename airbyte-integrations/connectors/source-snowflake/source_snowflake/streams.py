#
import uuid
from abc import ABC
from collections import OrderedDict
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_protocol.models import SyncMode
from .schema_builder import mapping_snowflake_type_airbyte_type, format_field

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


# Basic full refresh stream
class SnowflakeStream(HttpStream, ABC):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class SnowflakeStream(HttpStream, ABC)` which is the current class

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalSnowflakeStream((SnowflakeStream), ABC)`

    See the reference docs for the full list of configurable options.
    """
    url_suffix = "api/v2/statements"
    url_base = ""

    @property
    def statement(self):
        return None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        params = {
            "requestId": str(uuid.uuid4()),
            "async": "false"
        }
        return params

    @property
    def http_method(self) -> str:
        return "POST"

    def request_headers(
            self,
            stream_state: Optional[Mapping[str, Any]],
            stream_slice: Optional[Mapping[str, Any]] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        headers = {
            'User-Agent': 'myApplication/1.0',
            'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT',  # to be changed when authentication method is set
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }

        return headers

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        yield from response_json

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        """
        :return: string if single primary key, list of strings if composite primary key, list of list of strings if composite primary key consisting of nested fields.
          If the stream has no primary keys, return None.
        """
        return None


class CheckConnectionStream(SnowflakeStream):

    def __init__(self, url_base, config, **kwargs):
        super().__init__(**kwargs)
        self._url_base = url_base
        self.config = config

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
            path of request
        """

        return f"{self.url_base}/{self.url_suffix}"

    @property
    def url_base(self):
        return self._url_base

    @property
    def statement(self):

        """
        Assumptions:
            if we can see the table when showing schema, we assume we have access to the table (stream)
            We don't need to request the table in order to make sure it is working properly
            SHOW TABLES IN DATABASE statement does not include "system tables" in the dataset0
            if schema is provided by the use we replace the search of tables (streams) in database by search in shema

        TODO: Validate that the streams in the pushdown filter configuration are available
        """
        database = self.config["database"]
        schema = self.config.get('schema', "")
        if not schema:
            return f"SHOW TABLES IN DATABASE {database}"

        return f"SHOW TABLES IN SCHEMA {database}.{schema}"

    def request_body_json(
            self,
            stream_state: Optional[Mapping[str, Any]],
            stream_slice: Optional[Mapping[str, Any]] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        json_payload = {
            "statement": self.statement,
            "role": self.config['role'],
            "warehouse": self.config['warehouse'],
            "database": self.config['database'],
            "timeout": "1000",
        }
        schema = self.config.get('schema', '')
        if schema:
            json_payload['schema'] = schema
        return json_payload

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        records = response_json.get("data", [])
        yield from records


class TableCatalogStream(SnowflakeStream):
    DATABASE_NAME_COLUMN = "name"
    SCHEMA_NAME_COLUMN = "schema_name"

    def __init__(self, url_base, config, **kwargs):
        super().__init__(**kwargs)
        self._url_base = url_base
        self.config = config

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
            path of request
        """

        return f"{self.url_base}/{self.url_suffix}"

    @property
    def url_base(self):
        return self._url_base

    @property
    def statement(self):
        database = self.config["database"]
        schema = self.config.get('schema', '')
        if not schema:
            return f"SHOW TABLES IN DATABASE {database}"

        return f"SHOW TABLES IN SCHEMA {database}.{schema}"

    def request_body_json(
            self,
            stream_state: Optional[Mapping[str, Any]],
            stream_slice: Optional[Mapping[str, Any]] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        json_payload = {
            "statement": self.statement,
            "role": self.config['role'],
            "warehouse": self.config['warehouse'],
            "database": self.config['database'],
            "timeout": "1000",
        }
        schema = self.config.get('schema', '')
        if schema:
            json_payload['schema'] = schema
        print('json payload', json_payload, flush=True)
        return json_payload

    @classmethod
    def get_index_of_columns_from_names(cls, metadata_object: Mapping[Any, any], column_names: Iterable[str]) -> Mapping[str, Any]:
        mapping_column_name_to_index = {column_name: -1 for column_name in column_names}
        for current_index, column_object in enumerate(metadata_object["resultSetMetaData"]["rowType"]):
            for column_name in mapping_column_name_to_index:
                if column_object['name'] == column_name:
                    mapping_column_name_to_index[column_name] = current_index

        column_name_index_updated_filter = [0 if key_word_index == -1 else 1 for key_word_index in mapping_column_name_to_index.values()]

        if not all(column_name_index_updated_filter):
            raise ValueError('At least one index of column names is not updated. The error might a wrong key word '
                             'or a change in the naming of keys in resultSetMetaData of Snowflake API')

        return mapping_column_name_to_index

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        column_names_to_be_extracted_from_records = [self.DATABASE_NAME_COLUMN, self.SCHEMA_NAME_COLUMN]
        index_of_columns_from_names = self.get_index_of_columns_from_names(response_json, column_names_to_be_extracted_from_records)

        database_name_index = index_of_columns_from_names[self.DATABASE_NAME_COLUMN]
        schema_name_index = index_of_columns_from_names[self.SCHEMA_NAME_COLUMN]

        for record in response_json.get("data", []):
            yield {'schema': record[schema_name_index],
                   'table': record[database_name_index]}


class TableSchemaStream(SnowflakeStream):
    def __init__(self, url_base, config, table_object, **kwargs):
        super().__init__(**kwargs)
        self._url_base = url_base
        self.config = config
        self.table_object = table_object

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
            path of request
        """

        return f"{self.url_base}/{self.url_suffix}"

    @property
    def url_base(self):
        return self._url_base

    @property
    def statement(self):
        database = self.config["database"]
        schema = self.table_object["schema"]
        table = self.table_object["table"]

        return f"SELECT TOP 1 * FROM {database}.{schema}.{table}"

    def request_body_json(
            self,
            stream_state: Optional[Mapping[str, Any]],
            stream_slice: Optional[Mapping[str, Any]] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        json_payload = {
            "statement": self.statement,
            "role": self.config['role'],
            "warehouse": self.config['warehouse'],
            "database": self.config['database'],
            "timeout": "1000",
        }

        schema = self.table_object.get('schema', '')
        if schema:
            json_payload['schema'] = schema
        return json_payload

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        # checks in the response nested fields response -> resultSetMetaData -> rowType
        for row_type in response_json.get('resultSetMetaData', {'rowType': []}).get('rowType', []):
            yield {'column_name': row_type['name'],
                   'type': row_type['type'],
                   }

    def __str__(self):
        return f"Current stream has this table object as constructor {self.table_object}"


class TableStream(SnowflakeStream, IncrementalMixin):
    primary_key = None
    state_checkpoint_interval = None

    def __init__(self, url_base, config, table_object, **kwargs):
        stream_filtered_kwargs = {k: v for k, v in kwargs.items() if k in SnowflakeStream.__init__.__annotations__}
        super().__init__(**stream_filtered_kwargs)
        self._url_base = url_base
        self.config = config
        self.table_object = table_object
        self.table_schema_stream = TableSchemaStream(url_base=url_base, config=config, table_object=table_object,
                                                     **stream_filtered_kwargs)
        self._namespace = None
        self._cursor = None
        schema_generator = self.table_schema_stream.read_records(sync_mode=SyncMode.full_refresh)
        first_column = next(schema_generator)["column_name"]
        self._cursor_field = first_column

    @property
    def cursor_field(self):
        return self._cursor_field

    @cursor_field.setter
    def cursor_field(self, new_cursor_field):
        self._cursor_field = new_cursor_field

    @property
    def state(self):
        if not self.cursor_field:
            return {}
        return {self.cursor_field: self._cursor}

    @state.setter
    def state(self, new_state):
        if not (new_state is None or not new_state):
            self.cursor_field = list(new_state.keys())[0]
            self._cursor = new_state[self.cursor_field]

    @property
    def source_defined_cursor(self) -> bool:
        return False

    @property
    def supports_incremental(self) -> bool:
        return True

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        latest_record_state = latest_record[self.cursor_field]
        stream_state = current_stream_state.get(self.cursor_field)
        if stream_state:
            self._cursor = max(latest_record_state, stream_state)
            self.state = {self.cursor_field: self._cursor}
            return {self.cursor_field: self._cursor}
        self._cursor = latest_record_state
        self.state = {self.cursor_field: self._cursor}
        return {self.cursor_field: self._cursor}

    @property
    def namespace(self):
        return self._namespace

    @namespace.setter
    def namespace(self, namespace):
        self._namespace = namespace

    @property
    def name(self):
        return f"{self.table_object['schema']}.{self.table_object['table']}"

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
            path of request
        """

        return f"{self.url_base}/{self.url_suffix}"

    @property
    def url_base(self):
        return self._url_base

    @property
    def statement(self):
        database = self.config["database"]
        schema = self.table_object["schema"]
        table = self.table_object["table"]

        return f"SELECT * FROM {database}.{schema}.{table}"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, cursor_field=None, sync_mode=None, **kwargs) -> Iterable[
        Optional[Mapping[str, any]]]:
        if sync_mode == SyncMode.incremental:
            if isinstance(cursor_field, list) and cursor_field:
                self.cursor_field = cursor_field[0]
            elif cursor_field:
                self.cursor_field = cursor_field

            if stream_state:
                self._cursor = stream_state.get(self.cursor_field)

            yield {self.cursor_field: self._cursor}
        else:
            yield None

    def get_updated_statement(self, stream_slice):
        """
        Can be used consistently only in request_body_json
        otherwise we are not sure stream slice is the next slice and _cursor is updated with the correct data
        """

        updated_statement = self.statement

        if stream_slice:
            # TODO MAKE SURE THE CURSOR IS SINGLE VALUE AND NOT A STARTING AND ENDING VALUE (ex: window)
            self._cursor = stream_slice.get(self.cursor_field, None)

        if self._cursor:
            condition_of_state = f"{self.cursor_field}>={self._cursor}"
            key_word_where = " where "  # spaces in case there is a where in a table name
            if key_word_where in self.statement.lower():
                updated_statement = f"{self.statement} AND {condition_of_state}"
            else:
                updated_statement = f"{self.statement} WHERE {condition_of_state}"

            updated_statement = f"{updated_statement} ORDER BY {self.cursor_field} ASC"

        return updated_statement

    def request_body_json(
            self,
            stream_state: Optional[Mapping[str, Any]],
            stream_slice: Optional[Mapping[str, Any]] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:

        current_statement = self.get_updated_statement(stream_slice)
        json_payload = {
            "statement": current_statement,
            "role": self.config['role'],
            "warehouse": self.config['warehouse'],
            "database": self.config['database'],
            "timeout": "1000",
        }

        schema = self.table_object.get('schema', '')
        if schema:
            json_payload['schema'] = schema

        return json_payload

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        ordered_mapping_names_types = OrderedDict(
            [(row_type['name'], row_type['type'])
             for row_type in response_json.get('resultSetMetaData', {'rowType': []}).get('rowType', [])]
        )
        print("-"*30)
        print(response_json.get("data", []))
        print("-"*30)
        for record in response_json.get("data", []):
            yield {column_name: format_field(column_value, ordered_mapping_names_types[column_name])
                   for column_name, column_value in zip(ordered_mapping_names_types.keys(), record)}

    def __str__(self):
        return f"Current stream has this table object as constructor {self.table_object}"

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
        return json_schema


class PushDownFilterStream(TableStream):
    # Remark maybe change the class name

    def __init__(self, name, url_base, config, where_clause, parent_stream, namespace=None, **kwargs):
        kwargs['url_base'] = url_base
        kwargs['config'] = config
        kwargs['table_object'] = parent_stream.table_object
        kwargs['table_schema_stream'] = parent_stream.table_schema_stream
        TableStream.__init__(self, **kwargs)
        self._name = name
        self._namespace = namespace
        self._url_base = url_base
        self.config = config
        self._table_object = parent_stream.table_object
        self.where_clause = where_clause
        self.table_schema_stream = parent_stream.table_schema_stream

    @property
    def name(self):
        return f"{self._name}"

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
            path of request
        """

        return f"{self.url_base}/{self.url_suffix}"

    @property
    def url_base(self):
        return self._url_base

    @property
    def statement(self):
        database = self.config["database"]
        schema = self.table_object["schema"]
        table = self.table_object["table"]
        print(self.name)
        return f"SELECT * FROM {database}.{schema}.{table} WHERE {self.where_clause}"

    def __str__(self):
        return f"Current stream has this table object as constructor: {self.table_object} and as where clause: {self.where_clause}"
