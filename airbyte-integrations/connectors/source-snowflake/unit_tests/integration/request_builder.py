from ctypes import sizeof
from datetime import datetime
import json
from sys import getsizeof
from typing import Any, List, Optional, Mapping

from airbyte_cdk.test.mock_http import HttpRequest
from airbyte_cdk.test.mock_http.request import ANY_QUERY_PARAMS
import requests


class SnowflakeRequestBuilder:

    @classmethod
    def statement_endpoint(cls, host: str, schema: str, database: str, role: str, warehouse: str) -> "SnowflakeRequestBuilder":
        return cls(host, schema, database, role, warehouse)

    def __init__(self, host: str, schema: str, database: str, role: str, warehouse: str) -> None:
        self._host = host
        self._table = None
        self._schema = schema
        self._database = database
        self._warehouse = warehouse
        self._role = role
        self._show_catalog = False
        self._async = "false"
        self._requestID = None
        self._show_primary_keys = False
        self._handle = None
        self._get_schema = False
        self._timezone = False
        self._partition = None
        self._where_clause = None
        self._condition_value = None
        self._column_name = None
        self._cursor_field = None
        self._column_generic_type = None

    def with_table(self, table: str):
        self._table = table
        return self

    def with_show_catalog(self):
        self._show_catalog = True
        return self

    def with_show_primary_keys(self):
        self._show_primary_keys = True
        return self

    def with_requestID(self, requestID: str):
        self._requestID = requestID
        return self

    def with_async(self):
        self._async = "true"
        return self

    def with_partition(self, partition: int):
        self._partition = partition
        return self

    def with_handle(self, handle: str):
        self._handle = handle
        return self

    def with_get_schema(self):
        self._get_schema = True
        return self

    def with_timezone(self):
        self._timezone = True
        return self

    def with_where_clause(self, where_statement: str):
        self._where_clause = where_statement
        return self

    def with_statement(self, column_name, condition_value):
        self._column_name = column_name
        self._condition_value = condition_value
        return self

    def with_column_generic_type(self, cursor_generic_type):
        self._column_generic_type = cursor_generic_type
        return self

    def _build_query_params(self, is_get: bool = False):
        query_params = {"requestId": self._requestID, "async": self._async}
        if is_get:
            if self._partition:
                query_params = {"partition": self._partition}
            else:
                query_params = {}
        return query_params

    def build(self, is_get: bool = False) -> HttpRequest:

        query_params = self._build_query_params(is_get)
        statement = None
        if self._table:
            prefixed_table = f'"{self._database}"."{self._schema}"."{self._table}"'
            statement_builder = (SnowflakeStatementBuilder(prefixed_table)
                                 .with_where_clause(self._where_clause)
                                 .with_column_name(self._column_name)
                                 .with_condition_value(self._condition_value))

            if self._column_generic_type == "timestamp_with_timezone":
                statement_builder = statement_builder.with_column_type_timestamp_timezone()

            if self._column_generic_type == "date":
                statement_builder = statement_builder.with_column_type_date()

            if self._column_generic_type == "number":
                statement_builder = statement_builder.with_column_type_number()

            if self._column_generic_type == "string":
                statement_builder = statement_builder.with_column_type_string()

            statement = statement_builder.build()

        if self._show_catalog:
            statement = f"SHOW TABLES IN SCHEMA {self._database}.{self._schema}"
        if self._show_primary_keys and self._table:
            statement = f'SHOW PRIMARY KEYS IN "{self._database}"."{self._schema}"."{self._table}"'
        if self._get_schema:
            statement = f'SELECT TOP 1 * FROM "{self._database}"."{self._schema}"."{self._table}"'
        if self._timezone:
            statement = "SELECT TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()) as CURRENT_TIME"

        body = {
            'statement': statement,
            'role': self._role,
            'warehouse': self._warehouse,
            'database': self._database,
            'timeout': "1000"
        }

        return HttpRequest(
            url=f"https://{self._host}/api/v2/statements{'/' + self._handle if self._handle else ''}",
            headers={'User-Agent': 'Airbyte', 'Accept-Encoding': 'gzip, deflate', 'Accept': 'application/json', 'Connection': 'keep-alive',
                     'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT', 'Content-Type': 'application/json',
                     'Content-Length': self.get_content_length(body)},
            body=json.dumps(body),
            query_params=query_params
        )

    def get_content_length(self, body):
        req = requests.Request('POST', 'https://dummy.com', json=body)
        prepared = req.prepare()
        content_length = prepared.headers.get('Content-Length', 0)
        return content_length


class SnowflakeStatementBuilder:

    def __init__(self, prefixed_table):
        self._prefixed_table = prefixed_table
        self._where_clause = None
        self._condition_value = None
        self._column_name = None
        self._column_type_date = False
        self._column_type_string = False
        self._column_type_timestamp_timezone = False
        self._column_type_number = False

    def with_where_clause(self, where_clause):
        self._where_clause = where_clause
        return self

    def with_condition_value(self, condition_value):
        self._condition_value = condition_value
        return self

    def with_column_name(self, column_name):
        self._column_name = column_name
        return self

    def with_column_type_date(self):
        self._column_type_date = True
        return self

    def with_column_type_string(self):
        self._column_type_string = True
        return self

    def with_column_type_timestamp_timezone(self):
        self._column_type_timestamp_timezone = True
        return self

    def with_column_type_number(self):
        self._column_type_number = True
        return self

    def build(self):
        statement = f"SELECT * FROM {self._prefixed_table}"
        condition = None
        if self._condition_value:
            if self._column_type_date:
                condition = f"TO_TIMESTAMP({self._column_name})>=TO_TIMESTAMP('{self._condition_value}')"

            if self._column_type_timestamp_timezone:
                condition = f"TO_TIMESTAMP_TZ({self._column_name})>=TO_TIMESTAMP_TZ('{self._condition_value}')"

            if self._column_type_number:
                condition = f"{self._column_name}>={self._condition_value}"

            if self._column_type_string:
                condition = f"{self._column_name}>='{self._condition_value}'"

        if self._where_clause:
            if condition:
                statement = f"{statement} WHERE {self._where_clause} AND {condition}"
            else:
                statement = f"{statement} WHERE {self._where_clause}"
        else:
            if condition:
                statement = f"{statement} WHERE {condition}"

        if self._column_name:
            statement = f"{statement} ORDER BY {self._column_name} ASC"

        return statement

