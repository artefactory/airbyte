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
    def query_endpoint(cls, project_id: str, job_id: str = None) -> "BigqueryRequestBuilder":
        return cls(Paths.QUERY.format(project_id=project_id))

    def __init__(self, resource: str) -> None:
        self._resource = resource
        self._any_query_params = False
        self._created_gte: Optional[datetime] = None
        self._created_lte: Optional[datetime] = None
        self._limit: Optional[int] = None
        self._object: Optional[str] = None
        self._starting_after_id: Optional[str] = None
        self._types: List[str] = []
        self._expands: List[str] = []

    def with_created_gte(self, created_gte: datetime) -> "BigqueryRequestBuilder":
        self._created_gte = created_gte
        return self

    def with_created_lte(self, created_lte: datetime) -> "BigqueryRequestBuilder":
        self._created_lte = created_lte
        return self

    def with_limit(self, limit: int) -> "BigqueryRequestBuilder":
        self._limit = limit
        return self

    def with_object(self, object_name: str) -> "BigqueryRequestBuilder":
        self._object = object_name
        return self

    def with_starting_after(self, starting_after_id: str) -> "BigqueryRequestBuilder":
        self._starting_after_id = starting_after_id
        return self

    def with_any_query_params(self) -> "BigqueryRequestBuilder":
        self._any_query_params = True
        return self

    def with_types(self, types: List[str]) -> "BigqueryRequestBuilder":
        self._types = types
        return self

    def with_expands(self, expands: List[str]) -> "BigqueryRequestBuilder":
        self._expands = expands
        return self

    def build(self) -> HttpRequest:
        query_params = {}
        if self._created_gte:
            query_params["created[gte]"] = str(int(self._created_gte.timestamp()))
        if self._created_lte:
            query_params["created[lte]"] = str(int(self._created_lte.timestamp()))
        if self._limit:
            query_params["limit"] = str(self._limit)
        if self._starting_after_id:
            query_params["starting_after"] = self._starting_after_id
        if self._types:
            query_params["types[]"] = self._types
        if self._object:
            query_params["object"] = self._object
        if self._expands:
            query_params["expand[]"] = self._expands

        if self._any_query_params:
            if query_params:
                raise ValueError(f"Both `any_query_params` and {list(query_params.keys())} were configured. Provide only one of none but not both.")
            query_params = ANY_QUERY_PARAMS

        return HttpRequest(
            url=f"https://bigquery.googleapis.com/{self._resource}",
            query_params=query_params,
            # headers={"Authorization": f"Bearer toto"},
        )
