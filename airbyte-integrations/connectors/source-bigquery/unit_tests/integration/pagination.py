# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from typing import Any, Dict

from airbyte_cdk.test.mock_http.response_builder import PaginationStrategy


class BigqueryPaginationStrategy(PaginationStrategy):
    @staticmethod
    def update(response: Dict[str, Any]) -> None:
        response["nextPageToken"] = "XXXXXX"
