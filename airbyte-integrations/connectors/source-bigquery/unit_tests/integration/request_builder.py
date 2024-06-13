# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from datetime import datetime
from typing import List, Optional

from airbyte_cdk.test.mock_http import HttpRequest
from airbyte_cdk.test.mock_http.request import ANY_QUERY_PARAMS


class Paths:
    DATASETS = "bigquery/v2/projects/{project_id}/datasets"
    TABLES = "bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables"
    TABLE = "bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
    TABLE_DATA = "bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}/data"
    QUERIES = "bigquery/v2/projects/{project_id}/queries"
    QUERY = "bigquery/v2/projects/{project_id}/queries/{job_id}"


class BigqueryRequestBuilder:

    @classmethod
    def datasets_endpoint(cls, project_id: str) -> "BigqueryRequestBuilder":
        return cls(Paths.DATASETS.format(project_id=project_id))

    @classmethod
    def tables_endpoint(cls, project_id: str, dataset_id: str) -> "BigqueryRequestBuilder":
        return cls(Paths.TABLES.format(project_id=project_id, dataset_id=dataset_id))

    @classmethod
    def table_endpoint(cls, project_id: str, dataset_id: str, table_id: str) -> "BigqueryRequestBuilder":
        return cls(Paths.TABLE.format(project_id=project_id, dataset_id=dataset_id, table_id=table_id))

    @classmethod
    def table_data_endpoint(cls, project_id: str, dataset_id: str, table_id: str) -> "BigqueryRequestBuilder":
        return cls(Paths.TABLE_DATA.format(project_id=project_id, dataset_id=dataset_id, table_id=table_id))

    @classmethod
    def queries_endpoint(cls, project_id: str) -> "BigqueryRequestBuilder":
        return cls(Paths.QUERIES.format(project_id=project_id))

    @classmethod
    def query_endpoint(cls, project_id: str, job_id: str) -> "BigqueryRequestBuilder":
        return cls(Paths.QUERY.format(project_id=project_id, job_id=job_id))

    def __init__(self, resource: str) -> None:
        self._resource = resource
        self._body: Optional[dict] = None
        self._max_results: Optional[int] = None
        self._location: Optional[str] = None
        self._page_token: Optional[str] = None
        self._next_page_token: Optional[str] = None

    def with_body(self, body: dict) -> "BigqueryRequestBuilder":
        self._body = body
        return self
    
    def with_max_results(self, max_results: int) -> "BigqueryRequestBuilder":
        self._max_results = max_results
        return self
    
    def with_location(self, location: str) -> "BigqueryRequestBuilder":
        self._location = location
        return self
    
    def with_page_token(self, page_token: str) -> "BigqueryRequestBuilder":
        self._page_token = page_token
        return self
    
    def with_next_page_token(self, next_page_token: str) -> "BigqueryRequestBuilder":
        self._next_page_token = next_page_token
        return self
    
    def build(self) -> HttpRequest:
        return HttpRequest(
            url=f"https://bigquery.googleapis.com/{self._resource}",
            # headers={"Authorization": f"Bearer toto"},
            body=self._body,
            query_params=(
                {} | (
                    {"maxResults": self._max_results} if self._max_results else {}
                ) | (
                    {"location": self._location} if self._location else {}
                ) | (
                    {"pageToken": self._page_token} if self._page_token else {}
                ) | (
                    {"nextPageToken": self._next_page_token} if self._next_page_token else {}
                )
            ) or ANY_QUERY_PARAMS
        )
