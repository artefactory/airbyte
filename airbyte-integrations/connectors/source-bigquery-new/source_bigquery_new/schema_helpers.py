#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from copy import deepcopy
from typing import Any, Dict

from airbyte_cdk.models import AirbyteStream
from airbyte_cdk.models.airbyte_protocol import DestinationSyncMode, SyncMode

logger: logging.Logger = logging.getLogger("airbyte")


class SchemaTypes:

    string: Dict = {"type": ["null", "string"]}

    number: Dict = {"type": ["null", "number"]}

    boolean: Dict = {"type": ["null", "boolean"]}

    array: Dict = {"type": ["null", "array"], "items": {}}

    object: Dict = {"type": ["null", "object"]}


# More info about internal Airtable Data Types
# https://airtable.com/developers/web/api/field-model
SIMPLE_BIGQUERY_TYPES: Dict = {
    "BOOL": SchemaTypes.boolean,
    "INT64": SchemaTypes.number,
    "FLOAT64": SchemaTypes.number,
    "NUMERIC": SchemaTypes.number,
    "BIGNUMERIC": SchemaTypes.number,
    "STRING": SchemaTypes.string,
    "BYTES": SchemaTypes.string,
    "DATE": SchemaTypes.string,
    "DATETIME": SchemaTypes.string,
    "TIMESTAMP": SchemaTypes.string,
    "TIME": SchemaTypes.string,
    "ARRAY": SchemaTypes.array,
    "STRUCT": SchemaTypes.object,
    "GEOGRAPHY": SchemaTypes.string
}

# returns the `array of Any` where Any is based on Simple Types.
# the final array is fulled with some simple type.
# COMPLEX_AIRTABLE_TYPES: Dict = {
#     "formula": SchemaTypes.array_with_any,
#     "lookup": SchemaTypes.array_with_any,
#     "multipleLookupValues": SchemaTypes.array_with_any,
#     "rollup": SchemaTypes.array_with_any,
# }

ARRAY_FORMULAS = ("ARRAYCOMPACT", "ARRAYFLATTEN", "ARRAYUNIQUE", "ARRAYSLICE")


class SchemaHelpers:
    @staticmethod
    def clean_name(name_str: str) -> str:
        return name_str.replace(" ", "_").lower().strip()

    @staticmethod
    def get_json_schema(table: Dict[str, Any]) -> Dict[str, str]:
        properties: Dict = {
            "_airtable_id": SchemaTypes.string,
            "_airtable_created_time": SchemaTypes.string,
            "_airtable_table_name": SchemaTypes.string,
        }
        # import ipdb
        # ipdb.set_trace()

        # fields: Dict = table.get("fields", {})

        json_schema: Dict = {
            "$schema": "https://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": True,
            "properties": properties,
        }

        return json_schema

    @staticmethod
    def get_airbyte_stream(stream_name: str, json_schema: Dict[str, Any]) -> AirbyteStream:
        return AirbyteStream(
            name=stream_name,
            json_schema=json_schema,
            supported_sync_modes=[SyncMode.full_refresh],
            supported_destination_sync_modes=[DestinationSyncMode.overwrite, DestinationSyncMode.append_dedup],
        )
