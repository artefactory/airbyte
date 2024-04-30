#
import uuid
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources.streams.http import HttpStream

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
            "requestId": uuid.uuid4(),
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
        schema = self.config["schema"]
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
        if self.config['schema']:
            json_payload['schema'] = self.config['schema']
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
        schema = self.config["schema"]
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
        if self.config['schema']:
            json_payload['schema'] = self.config['schema']
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
