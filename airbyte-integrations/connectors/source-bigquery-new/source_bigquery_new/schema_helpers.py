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
    "INTEGER": SchemaTypes.number,
    "STRING": SchemaTypes.string,
    "BYTES": SchemaTypes.string,
    "DATE": SchemaTypes.string,
    "DATETIME": SchemaTypes.string,
    "TIMESTAMP": SchemaTypes.string,
    "TIME": SchemaTypes.string,
    "STRUCT": SchemaTypes.object,
    "GEOGRAPHY": SchemaTypes.string
}

# returns the `array of Any` where Any is based on Simple Types.
# the final array is fulled with some simple type.
COMPLEX_BIGQUERY_TYPES: Dict = {
    "ARRAY": SchemaTypes.array,
    "RECORD": SchemaTypes.array
}


class SchemaHelpers:
    @staticmethod
    def clean_name(name_str: str) -> str:
        return name_str.replace(" ", "_").lower().strip()

    @staticmethod
    def get_json_schema(table: Dict[str, Any]) -> Dict[str, str]:
        properties: Dict = {
            "_bigquery_table_id": SchemaTypes.string,
            "_bigquery_created_time": SchemaTypes.string,
            "_airbyte_raw_id": SchemaTypes.string,
            "_airbyte_extracted_at": SchemaTypes.string,
            "_airbyte_meta": SchemaTypes.object
        }
        # import ipdb
        # ipdb.set_trace()

        fields: Dict = table.get("schema", {})["fields"]
        print(fields)
        # import ipdb
        # ipdb.set_trace()

        for field in fields:
            name: str = field.get("name")
            original_type: str = field.get("type")
            mode: str = field.get("mode")
            # field_type: str = field.get("type")
            if original_type in SIMPLE_BIGQUERY_TYPES.keys():
                properties.update(**{name: deepcopy(SIMPLE_BIGQUERY_TYPES.get(original_type))})
            elif original_type in COMPLEX_BIGQUERY_TYPES.keys():
                if original_type == "ARRAY" or original_type == "RECORD":
                    sub_fields: Dict = table.get("fields")
                    complex_type = deepcopy(COMPLEX_BIGQUERY_TYPES.get(original_type))
                    complex_type["items"] = []

                    for sfield in sub_fields:
                        sub_name: str = sfield.get("name")
                        original_sub_type: str = sfield.get("type")
                        complex_type["items"].append(deepcopy(SIMPLE_BIGQUERY_TYPES.get(original_sub_type)))

                    properties.update(**{name: complex_type})
            else:
                properties.update(**{name: SchemaTypes.string})

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
