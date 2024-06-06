from datetime import datetime
from typing import Any, List, Optional, Mapping

from airbyte_cdk.test.mock_http import HttpRequest
from airbyte_cdk.test.mock_http.request import ANY_QUERY_PARAMS


class SnowflakeRequestBuilder:

    @classmethod
    def statement_endpoint(cls, jwt_token: str, account_identifier:str) -> "SnowflakeRequestBuilder":
        return cls("statement",jwt_token, account_identifier)

    def __init__(self, resource: str, jwt_token: str, account_identifier: str) -> None:
        self._resource = resource
        self._account_identifier = account_identifier
        self._jwt_token = jwt_token


    def with_table(self , database:str, schema:str, table:str):
        self._database=database
        self._schema=schema
        self._table=table
        return self
        

    def build(self) -> HttpRequest:
        body = {}
        statement = ""
        if self._table:
            statement  = f'SELECT * FROM "{self._database}"."{self._schema}"."{self._table}"'
        
        body["statement"]=statement

        return HttpRequest(
            url=f"https://{self._account_identifier}.snowflakecomputing.com/api/v2/{self._resource}",
            headers={ "Authorization": f"Bearer {self._jwt_token}"},
            body = body
        )