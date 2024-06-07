# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

import json

from typing import List, Optional
from airbyte_cdk.test.mock_http import HttpResponse
from airbyte_cdk.test.mock_http.response_builder import (
    FieldPath,
    NestedPath,
    HttpResponseBuilder,
    RecordBuilder,
    create_response_builder,
    create_record_builder,
    find_template,
)

from integration.pagination import BigqueryPaginationStrategy


def _a_record(template, field_path, id_path, cursor_path) -> RecordBuilder:
    return create_record_builder(
        find_template(template, __file__),
        field_path,
        record_id_path=id_path,
        record_cursor_path=cursor_path,
    )


def _a_response(template, field_path) -> HttpResponseBuilder:
    return create_response_builder(find_template(template, __file__), field_path, pagination_strategy=BigqueryPaginationStrategy())


class BigqueryResponseBuilder:
    _http_response = None


    @classmethod
    def datasets(cls, dataset_ids: List[str]) -> HttpResponseBuilder:
        http_response_builder = _a_response("datasets", FieldPath("datasets"))

        for dataset_id in dataset_ids:
            http_response_builder = http_response_builder.with_record(_a_record("datasets", FieldPath("datasets"), NestedPath(["datasetReference", "datasetId"]), None).with_field(NestedPath(["datasetReference", "datasetId"]), dataset_id))
        return http_response_builder

    @classmethod
    def tables(cls, table_ids: List[str]) -> HttpResponseBuilder:
        http_response_builder = _a_response("tables", FieldPath("tables"))

        for table_id in table_ids:
            http_response_builder = http_response_builder.with_record(_a_record("tables", FieldPath("tables"), NestedPath(["tableReference", "tableId"]), None).with_field(NestedPath(["tableReference", "tableId"]), table_id))
        return http_response_builder

    @classmethod
    def table(cls) -> HttpResponseBuilder:
        pass

    @classmethod
    def table_data(cls) -> HttpResponseBuilder:
        pass

    @classmethod
    def queries(cls) -> HttpResponseBuilder:
        http_response_builder = _a_response("queries", FieldPath("queries"))

        return http_response_builder

    @classmethod
    def query(cls) -> HttpResponseBuilder:
        pass
