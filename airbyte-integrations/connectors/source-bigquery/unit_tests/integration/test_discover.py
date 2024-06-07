# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from unittest import TestCase

import freezegun
from airbyte_cdk.sources.source import TState
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
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
from integration.utils import discover
from integration.config import ConfigBuilder
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
class DiscoverTest(TestCase):

    @HttpMocker()
    def test_given_one_page_when_read_then_return_records(self, http_mocker: HttpMocker) -> None:
        config = ConfigBuilder().default().build()

        dataset_ids = ["dataset_id_1", "dataset_id_2"]
        tables_ids = ["table_id_1", "table_id_2"]

        datasets_req = BigqueryRequestBuilder.datasets_endpoint(project_id=config["project_id"]).build()
        datasets_resp = BigqueryResponseBuilder.datasets(dataset_ids=dataset_ids).build()
        http_mocker.get(datasets_req, datasets_resp)

        for dataset_id in dataset_ids:
            tables_req = BigqueryRequestBuilder.tables_endpoint(project_id=config["project_id"], dataset_id=dataset_id).build()
            tables_resp = BigqueryResponseBuilder.tables(table_ids=tables_ids).build()
            http_mocker.get(tables_req, tables_resp)

        queries_req = BigqueryRequestBuilder.queries_endpoint(project_id=config["project_id"]).build()
        queries_resp = BigqueryResponseBuilder.queries().build()
        http_mocker.post(queries_req, queries_resp)

        source = SourceBigquery(_NO_CATALOG, config, _NO_STATE)
        
        output = discover(source, config, expecting_exception=False)
        errors = output.errors
        breakpoint()
        assert len(output.records) == 2
