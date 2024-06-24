# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from typing import Any, Dict

from airbyte_cdk.test.mock_http.response_builder import PaginationStrategy


class BigqueryPaginationStrategy:
    class NextPageToken(PaginationStrategy):
        key = "nextPageToken"
        @staticmethod
        def update(response: Dict[str, Any]) -> None:
            response["NextPageToken".key] = "ABCDEF123456"

    class PageToken(PaginationStrategy):
        key = "pageToken"
        @staticmethod
        def update(response: Dict[str, Any]) -> None:
            response["PageToken".key] = "ABCDEF123456"
