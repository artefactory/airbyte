#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict

import pendulum
import pytz
from airbyte_cdk.models import AirbyteStream
from airbyte_cdk.models.airbyte_protocol import DestinationSyncMode, SyncMode

logger: logging.Logger = logging.getLogger("airbyte")
CHANGE_FIELDS = {"_CHANGE_TIMESTAMP": "change_timestamp", "_CHANGE_TYPE": "change_type"}


class SchemaTypes:
    string: Dict = {"type": ["null", "string"]}
    number: Dict = {"type": ["null", "number"]}
    integer: Dict = {"type": ["null", "integer"]}
    boolean: Dict = {"type": ["null", "boolean"]}
    array: Dict = {"type": ["null", "array"], "items": {}}
    object: Dict = {"type": ["null", "object"]}
    timestamp_with_timezone: Dict = {"type": ["null", "string"], "airbyte_type": "timestamp_with_timezone"}
    timestamp_without_timezone: Dict = {"type": ["null", "string"], "airbyte_type": "timestamp_without_timezone"}
    time_without_timezone: Dict = {"type": ["null", "string"], "airbyte_type": "time_without_timezone"}
    date: Dict = {"type": ["null", "date"]}


# https://docs.airbyte.com/integrations/sources/bigquery
SIMPLE_BIGQUERY_TYPES: Dict = {
    "BOOL": SchemaTypes.boolean,
    "INT64": SchemaTypes.integer,
    "FLOAT64": SchemaTypes.number,
    "NUMERIC": SchemaTypes.number,
    "BIGNUMERIC": SchemaTypes.number,
    "INTEGER": SchemaTypes.number,
    "STRING": SchemaTypes.string,
    "BYTES": SchemaTypes.string,
    "DATE": SchemaTypes.string,  # TODO: update to new date type
    "DATETIME": SchemaTypes.timestamp_without_timezone,
    "TIMESTAMP": SchemaTypes.timestamp_with_timezone,
    "TIME": SchemaTypes.time_without_timezone,
    "GEOGRAPHY": SchemaTypes.string,
}

COMPLEX_BIGQUERY_TYPES: Dict = {
    "ARRAY": SchemaTypes.array,
    "RECORD": SchemaTypes.object,
    "STRUCT": SchemaTypes.object,
    "JSON": SchemaTypes.object,
}

TIME_TYPES = [SchemaTypes.time_without_timezone, SchemaTypes.timestamp_with_timezone, SchemaTypes.timestamp_without_timezone]


class SchemaHelpers:
    @staticmethod
    def clean_name(name_str: str) -> str:
        return name_str.replace(" ", "_").lower().strip()

    @staticmethod
    def get_json_schema(table: Dict[str, Any]) -> Dict[str, str]:
        properties: Dict = {"_bigquery_table_id": SchemaTypes.string, "_bigquery_created_time": SchemaTypes.string}

        fields: Dict = table.get("schema")["fields"]
        for field in fields:
            name: str = CHANGE_FIELDS.get(field.get("name"), field.get("name"))
            original_type: str = field.get("type")
            # mode: str = field.get("mode")
            # field_type: str = field.get("type")
            if original_type in SIMPLE_BIGQUERY_TYPES.keys():
                properties.update(**{name: deepcopy(SIMPLE_BIGQUERY_TYPES.get(original_type))})
            elif original_type in COMPLEX_BIGQUERY_TYPES.keys():
                complex_type = deepcopy(COMPLEX_BIGQUERY_TYPES.get(original_type))
                if original_type == "ARRAY":
                    sub_fields: Dict = field.get("fields")
                    # add the type of each sub column
                    for sfield in sub_fields:
                        # sub_name: str = sfield.get("name")
                        original_sub_type: str = sfield.get("type")
                        complex_type["items"] = deepcopy(SIMPLE_BIGQUERY_TYPES.get(original_sub_type))
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
            supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
            supported_destination_sync_modes=[DestinationSyncMode.overwrite, DestinationSyncMode.append, DestinationSyncMode.append_dedup],
        )

    @staticmethod
    def format_field(field, field_type):
        if field_type == "TIMESTAMP" and isinstance(field, str):
            ts = float(field)
            dt = pendulum.from_timestamp(ts)
            return dt.isoformat(timespec="microseconds")
        if SIMPLE_BIGQUERY_TYPES.get(field_type) == SchemaTypes.integer and field:
            return int(field)
        elif SIMPLE_BIGQUERY_TYPES.get(field_type) == SchemaTypes.number and field:
            return float(field)
        return field
