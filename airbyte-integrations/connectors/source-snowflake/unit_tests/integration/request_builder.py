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
    def statement_endpoint(cls, jwt_token: str, host:str, schema:str, database:str, role:str, warehouse:str) -> "SnowflakeRequestBuilder":
        return cls("statements",jwt_token, host, schema, database, role, warehouse)

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
        self._async = "false"
        self._requestID = None


    def with_table(self ,table:str):
        self._table=table
        return self
    
    def with_show_catalog(self):
        self._show_catalog=True
        return self

    def with_requestID(self, requestID:str):
        self._requestID = requestID
        return self
    
    def with_async(self):
        self._async = "true"
        return self
        

    def build(self) -> HttpRequest:
        body = {}
        query_params = {"requestId":None,"async":self._async}
        statement = ""
        if self._table:
            statement  = f'SELECT * FROM {self._database}.{self._schema}.{self._table}'
        if self._show_catalog:
            statement = f"SHOW TABLES IN SCHEMA {self._database}.{self._schema}"
        if self._requestID:
            query_params["requestId"] = self._requestID

        
        body ={
            'statement':statement,
            'role':self._role,
            'warehouse':self._warehouse,
            'database':self._database,
            'timeout': "1000"
        } 

        

        return HttpRequest(
            url=f"https://{self._host}/api/v2/{self._resource}",
            headers={'User-Agent': 'Airbyte', 'Accept-Encoding': 'gzip, deflate', 'Accept': 'application/json', 'Connection': 'keep-alive', 'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT', 'Content-Type': 'application/json', 'Content-Length': self.get_content_length(body)},
            body = body,
            query_params=query_params
        )

    def get_content_length(self, body):
        req = requests.Request('POST','https://dummy.com',json=body)
        prepared = req.prepare()
        content_length = prepared.headers.get('Content-Length',0)
        return content_length