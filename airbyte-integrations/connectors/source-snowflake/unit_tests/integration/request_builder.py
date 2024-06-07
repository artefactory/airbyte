from datetime import datetime
from typing import Any, List, Optional, Mapping

from airbyte_cdk.test.mock_http import HttpRequest
from airbyte_cdk.test.mock_http.request import ANY_QUERY_PARAMS


class SnowflakeRequestBuilder:

    @classmethod
    def statement_endpoint(cls, jwt_token: str, host:str, schema:str, database:str, role:str, warehouse:str) -> "SnowflakeRequestBuilder":
        return cls("statement",jwt_token, host, schema, database, role)

    def __init__(self, resource: str, jwt_token: str, host: str, schema:str, database:str, role:str, warehouse:str) -> None:
        self._resource = resource
        self._host = host
        self._jwt_token = jwt_token
        self._table = None
        self._schema = schema
        self._database = database
        self._warehouse = warehouse
        self._role = role
        self._show_catalog = False


    def with_table(self ,table:str):
        self._table=table
        return self
    
    def with_show_catalog(self):
        self._show_catalog=True
        return self
        

    def build(self) -> HttpRequest:
        body = {}
        statement = ""
        if self._table:
            statement  = f'SELECT * FROM "{self._database}"."{self._schema}"."{self._table}"'
        if self._show_catalog:
            statement = f"SHOW TABLES IN SCHEMA {self._database}.{self._schema}"
        
        body ={
            "statement":statement,
            "role":self._role,
            "database":self._database,
            "warehouse":self._warehouse,
            "schema":self._schema
        } 

        return HttpRequest(
            url=f"https://{self._host}/api/v2/{self._resource}",
            headers={'User-Agent': 'Airbyte', 'Accept-Encoding': 'gzip, deflate', 'Accept': 'application/json', 'Connection': 'keep-alive', 'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT', 'Content-Type': 'application/json', 'Content-Length': '132'},
            body = body
        )