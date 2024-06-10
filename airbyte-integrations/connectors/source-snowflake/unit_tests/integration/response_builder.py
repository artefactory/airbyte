import functools
import json
from abc import ABC, abstractmethod
from pathlib import Path as FilePath
from typing import Any, Dict, List, Optional, Union
from airbyte_cdk.test.mock_http.response_builder import HttpResponseBuilder, HttpResponse, Path, FieldPath, NestedPath, PaginationStrategy
from jsonpath_ng import parse




class JsonPath(Path):
    def __init__(self, path: str):
        self._path = parse(path)

    def write(self, template: Dict[str, Any], value: Any) -> None:
        self._path.update_or_create(template, value)
    
    def update(self, template: Dict[str, Any], value: Any) -> None:
        self._path.update(template, value)
    
    def extract(self, template: Dict[str, Any]) -> Any:

        a = [match.value for match in self._path.find(template)]
        return a

class SnowflakeResponseBuilder(HttpResponseBuilder):

    def with_handle(self,handle:str):
        self._response["statementHandle"] = handle
        return self



def create_response_builder(
    response_template: Dict[str, Any], records_path: Union[FieldPath, NestedPath], pagination_strategy: Optional[PaginationStrategy] = None
) -> SnowflakeResponseBuilder:
    return SnowflakeResponseBuilder(response_template, records_path, pagination_strategy)



        
    
