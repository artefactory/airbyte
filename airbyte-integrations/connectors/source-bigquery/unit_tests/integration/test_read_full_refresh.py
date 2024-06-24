# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from unittest import TestCase, mock

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
from integration.mocked import mock_discover_calls
from integration.config import ConfigBuilder
from integration.bq_query_builder import build_query
from integration.request_builder import BigqueryRequestBuilder
from integration.response_builder import BigqueryResponseBuilder
from source_bigquery import SourceBigquery

_NOW = datetime.now(timezone.utc)
_NO_STATE = StateBuilder().build()
_NO_CATALOG = CatalogBuilder().build()
_PUSHDOWN_FILTER_CONDITION = "TRUE"



@freezegun.freeze_time(_NOW.isoformat())
class ReadFullRefreshTest(TestCase):

    @HttpMocker()
    def test_basic(self, http_mocker: HttpMocker) -> None:
        config = ConfigBuilder().default().build()

        dataset_id = "dataset_id_1"
        table_id = "table_id_1"

        http_mocker.get(
            BigqueryRequestBuilder.table_endpoint(project_id=config["project_id"], dataset_id=dataset_id, table_id=table_id).with_max_results(10000).build(),
            BigqueryResponseBuilder.table(dataset_id, table_id).build()
        )

        http_mocker.post(
            BigqueryRequestBuilder.queries_endpoint(project_id=config["project_id"]).with_body(
                build_query(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    dry_run=True,
                    use_legacy_sql=False
                )
            ).build(),
            BigqueryResponseBuilder.queries().build()
        )
        
        http_mocker.post(
            BigqueryRequestBuilder.queries_endpoint(project_id=config["project_id"]).with_body(
                build_query(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    timeout_ms=30000,
                    max_results=10000,
                    use_legacy_sql=False
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

    @HttpMocker()
    def test_pushdown_filter(self, http_mocker: HttpMocker) -> None:
        stream_name = "filtered_stream"
        dataset_id = "dataset_id_1"
        table_id = "table_id_1"

        config = ConfigBuilder().default().with_filtered_stream(
            stream_name=stream_name,
            parent_stream_name=f"{dataset_id}.{table_id}",
            where_clause=_PUSHDOWN_FILTER_CONDITION
        ).build()

        http_mocker.get(
            BigqueryRequestBuilder.table_endpoint(project_id=config["project_id"], dataset_id=dataset_id, table_id=table_id).with_max_results(10000).build(),
            BigqueryResponseBuilder.table(dataset_id, table_id).build()
        )
        
        http_mocker.post(
            BigqueryRequestBuilder.queries_endpoint(project_id=config["project_id"]).with_body(
                build_query(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    timeout_ms=30000,
                    max_results=10000,
                    where=_PUSHDOWN_FILTER_CONDITION,
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
                    where=_PUSHDOWN_FILTER_CONDITION,
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
                stream_name
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