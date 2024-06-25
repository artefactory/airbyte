# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from typing import Mapping, Set

from integration.bq_query_builder import build_query
from integration.request_builder import BigqueryRequestBuilder
from integration.response_builder import BigqueryResponseBuilder


def mock_discover_calls(http_mocker, tables: Mapping[str, Mapping[str, Set[str]]]) -> None:
    """
    Mocks the calls to the BigQuery API to discover the tables in the project.

    :param http_mocker: The HttpMocker instance to use for mocking the calls.
    :param tables: A mapping from project_id to a mapping from dataset_id to a set of table_ids.

    Example:
    {
        "project_id_1": {
            "dataset_id_1": {
                "table_id_1",
                "table_id_2",
            },
            "dataset_id_2": {
                "table_id_3",
            },
        },
        "project_id_2": {
            "dataset_id_3": {
                "table_id_4",
            },
        },
    }
    """
    for project_id, datasets in tables.items():
        http_mocker.get(
            BigqueryRequestBuilder.datasets_endpoint(project_id=project_id).with_max_results(10000).build(),
            BigqueryResponseBuilder.datasets(dataset_ids=list(datasets)).build()
        )
        for dataset_id, tables in datasets.items():
            http_mocker.get(
                BigqueryRequestBuilder.tables_endpoint(project_id=project_id, dataset_id=dataset_id).build(),
                BigqueryResponseBuilder.tables(table_ids=list(tables)).build()
            )
            for table_id in tables:
                http_mocker.post(
                    BigqueryRequestBuilder.queries_endpoint(project_id=project_id).with_body(
                        build_query(
                            dataset_id=dataset_id,
                            table_id=table_id,
                            dry_run=True,
                        )
                    ).build(),
                    BigqueryResponseBuilder.queries().build()
                )
                http_mocker.post(
                    BigqueryRequestBuilder.queries_endpoint(project_id=project_id).with_body(
                        build_query(
                            project_id=project_id,
                            dataset_id=dataset_id,
                            table_id="INFORMATION_SCHEMA.KEY_COLUMN_USAGE",
                            where=f"table_name='{table_id}'",
                            query_end_char=";",
                            timeout_ms=30000,
                        )
                    ).build(),
                    BigqueryResponseBuilder.query_information_schema().build()
                )