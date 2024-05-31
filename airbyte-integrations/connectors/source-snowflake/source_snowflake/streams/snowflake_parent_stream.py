import logging
import uuid
from abc import ABC
from datetime import datetime, timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union

import requests
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_protocol.models import SyncMode

from source_snowflake.snowflake_exceptions import emit_airbyte_error_message
from source_snowflake.utils import handle_no_permissions_error


class SnowflakeStream(HttpStream, ABC):


    url_suffix = "api/v2/statements"
    TIME_OUT_IN_SECONDS = "1000"
    _delta_time_between_snowflake_and_airbyte_server = None

    def __init__(self, authenticator=None):
        super().__init__(authenticator=authenticator)
        self._authenticator = authenticator
        self._url_base = None
        self._config = None
        self._table_object = {}
        self._delta_time_between_snowflake_and_airbyte_server = None

    @property
    def url_base(self):
        return self._url_base

    @property
    def config(self):
        return self._config

    @property
    def statement(self):
        return None

    @property
    def table_object(self):
        return self._table_object

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
            'User-Agent': 'Airbyte',
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

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
            path of request
        """

        return f"{self.url_base}/{self.url_suffix}"

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
            "timeout": self.TIME_OUT_IN_SECONDS,
        }

        schema = self.table_object.get('schema', '')
        if schema:
            json_payload['schema'] = schema
        return json_payload

    @classmethod
    def get_index_of_columns_from_names(cls, metadata_object: Mapping[Any, any], column_names: Iterable[str]) -> Mapping[str, Any]:
        mapping_column_name_to_index = {column_name: -1 for column_name in column_names}
        for current_index, column_object in enumerate(metadata_object["resultSetMetaData"]["rowType"]):
            for column_name in mapping_column_name_to_index:
                if column_object['name'].lower() == column_name.lower():
                    mapping_column_name_to_index[column_name] = current_index

        column_name_index_updated_filter = [0 if key_word_index == -1 else 1 for key_word_index in mapping_column_name_to_index.values()]

        if not all(column_name_index_updated_filter):
            error_message = ('At least one index of column names is not updated. The error might be a wrong key word '
                             'or a change in the naming of keys in resultSetMetaData of Snowflake API.\n'
                             'To resolve this issue, compare the column name provided with keys of resultSetMetaData of Snowflake API '
                             'and update your column names.\n'
                             'For example, for class TableCatalogStream, compare TableCatalogStream.DATABASE_NAME_COLUMN '
                             'and TableCatalogStream.SCHEMA_NAME_COLUMN with the keys representing this variables in resultSetMetaData '
                             'present in the Snowflake API response')
            emit_airbyte_error_message(error_message)
            raise ValueError(error_message)

        return mapping_column_name_to_index

    @handle_no_permissions_error
    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:

        return super().read_records(sync_mode, cursor_field, stream_slice, stream_state)
