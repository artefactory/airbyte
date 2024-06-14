# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from unittest import TestCase

import freezegun
from airbyte_cdk.sources.source import TState
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, discover
from airbyte_cdk.test.mock_http import HttpMocker, HttpResponse
from airbyte_cdk.test.mock_http.response_builder import (
    FieldPath,
    HttpResponseBuilder,
    RecordBuilder,
    create_response_builder,
    create_record_builder,
    find_template,
)
from airbyte_cdk.test.state_builder import StateBuilder
from airbyte_protocol.models import AirbyteStateBlob, AirbyteStreamState, ConfiguredAirbyteCatalog, FailureType, StreamDescriptor, SyncMode
# from integration.utils import discover
from integration.config import ConfigBuilder
from integration.request_builder import BigqueryRequestBuilder
from integration.response_builder import BigqueryResponseBuilder
from source_bigquery import SourceBigquery


_NOW = datetime.now(timezone.utc)
_NO_STATE = StateBuilder().build()
_NO_CATALOG = CatalogBuilder().build()


@freezegun.freeze_time(_NOW.isoformat())
class DiscoverTest(TestCase):

    @HttpMocker()
    def test_simple_discovery(self, http_mocker: HttpMocker) -> None:
        config = ConfigBuilder().default().build()

        dataset_ids = ["dataset_id_1", "dataset_id_2"]
        tables_ids = ["table_id_1", "table_id_2"]

        http_mocker.get(
            BigqueryRequestBuilder.datasets_endpoint(project_id=config["project_id"]).with_max_results(10000).build(),
            BigqueryResponseBuilder.datasets(dataset_ids=dataset_ids).build()
        )

        for dataset_id in dataset_ids:
            http_mocker.get(
                BigqueryRequestBuilder.tables_endpoint(project_id=config["project_id"], dataset_id=dataset_id).build(),
                BigqueryResponseBuilder.tables(table_ids=tables_ids).build()
            )

            for table_id in tables_ids:
                http_mocker.post(
                    BigqueryRequestBuilder.queries_endpoint(project_id=config["project_id"]).with_body({
                        "kind": "bigquery#queryRequest",
                        "query": f"SELECT * FROM `{dataset_id}.{table_id}`",
                        "useLegacySql": False,
                        "dryRun": True,
                    }).build(),
                    BigqueryResponseBuilder.queries().build()
                )
                http_mocker.post(
                    BigqueryRequestBuilder.queries_endpoint(project_id=config["project_id"]).with_body({
                        "kind": "bigquery#queryRequest",
                        "query": f"SELECT * FROM `{config['project_id']}.{dataset_id}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` WHERE table_name='{table_id}';",
                        "useLegacySql": False,
                        "timeoutMs": 30000,
                    }).build(),
                    BigqueryResponseBuilder.query_information_schema().build()
                )

        source = SourceBigquery(_NO_CATALOG, config, _NO_STATE)
        
        output = discover(source, config, expecting_exception=False)

        assert [
            stream.name for stream in output._messages[0].catalog.streams
        ] == [
            f"{dataset_id}.{table_id}" for dataset_id in dataset_ids for table_id in tables_ids
        ]
