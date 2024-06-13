# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from unittest import TestCase

import freezegun
from airbyte_cdk.sources.source import TState
from airbyte_cdk.test.catalog_builder import CatalogBuilder, ConfiguredAirbyteStreamBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, read
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
from integration.config import ConfigBuilder
from integration.bq_query_builder import build_query
from integration.request_builder import BigqueryRequestBuilder
from integration.response_builder import BigqueryResponseBuilder
from source_bigquery import SourceBigquery

# _STREAM_NAME = "events"
_NOW = datetime.now(timezone.utc)
# _A_START_DATE = _NOW - timedelta(days=60)
# _ACCOUNT_ID = "account_id"
# _CLIENT_SECRET = "client_secret"
_NO_STATE = StateBuilder().build()
_NO_CATALOG = CatalogBuilder().build()
# _AVOIDING_INCLUSIVE_BOUNDARIES = timedelta(seconds=1)
# _SECOND_REQUEST = timedelta(seconds=1)
# _THIRD_REQUEST = timedelta(seconds=2)




@freezegun.freeze_time(_NOW.isoformat())
class ReadFullRefreshTest(TestCase):

    @HttpMocker()
    def test_basic(self, http_mocker: HttpMocker) -> None:
        config = ConfigBuilder().default().build()

        dataset_id = "dataset_id_1"
        table_id = "table_id_1"

        http_mocker.get(
            BigqueryRequestBuilder.datasets_endpoint(project_id=config["project_id"]).build(),
            BigqueryResponseBuilder.datasets(dataset_ids=[dataset_id]).build()
        )

        http_mocker.get(
            BigqueryRequestBuilder.tables_endpoint(project_id=config["project_id"], dataset_id=dataset_id).build(),
            BigqueryResponseBuilder.tables(table_ids=[table_id]).build()
        )

        http_mocker.post(
            BigqueryRequestBuilder.queries_endpoint(project_id=config["project_id"]).with_body(
                build_query(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    timeout_ms=30000,
                    max_results=10000,
                )
            ).build(),
            BigqueryResponseBuilder.queries().build()
        )

        http_mocker.post(
            BigqueryRequestBuilder.queries_endpoint(project_id=config["project_id"]).with_body(
                build_query(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    dry_run=True,
                )
            ).build(),
            BigqueryResponseBuilder.queries().build()
        )

        http_mocker.post(
            BigqueryRequestBuilder.queries_endpoint(project_id=config["project_id"]).with_body(
                build_query(
                    project_id=config["project_id"],
                    dataset_id=dataset_id,
                    table_id="INFORMATION_SCHEMA.KEY_COLUMN_USAGE",
                    where=f"table_name='{table_id}'",
                    query_end_char=";",
                    timeout_ms=30000,
                )
            ).build(),
            BigqueryResponseBuilder.query_information_schema().build()
        )

        catalog = CatalogBuilder().with_stream(
            ConfiguredAirbyteStreamBuilder().with_name(
                f"{dataset_id}.{table_id}"
            ).with_primary_key(
                None
            ).with_sync_mode(
                SyncMode.full_refresh
            )
        ).build()

        source = SourceBigquery(catalog, config, _NO_STATE)
        
        output = read(source, config, catalog, expecting_exception=False)

        assert len(output.records) == 1

        assert not output.errors

        assert len(output.state_messages) == 1