import functools
import json
from abc import ABC, abstractmethod
from pathlib import Path as FilePath
from typing import Any, Dict, List, Optional, Union
from airbyte_cdk.test.mock_http.response_builder import HttpResponseBuilder, HttpResponse, FieldPath, NestedPath, PaginationStrategy

class SnowflakeResponseBuilder(HttpResponseBuilder):

    def with_handle(self,handle:str):
        self._response["statementHandle"] = handle
        return self



def create_response_builder(
    response_template: Dict[str, Any], records_path: Union[FieldPath, NestedPath], pagination_strategy: Optional[PaginationStrategy] = None
) -> SnowflakeResponseBuilder:
    return SnowflakeResponseBuilder(response_template, records_path, pagination_strategy)