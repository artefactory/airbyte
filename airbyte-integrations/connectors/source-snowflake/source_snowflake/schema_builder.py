import json
from datetime import datetime
from typing import Dict

import pytz


class SchemaTypes:
    string: Dict = {"type": ["null", "string"]}
    number: Dict = {"type": ["null", "number"]}
    integer: Dict = {"type": ["null", "integer"]}
    boolean: Dict = {"type": ["null", "boolean"]}
    date: Dict = {"type": ["null", "string"], "format": "date"}
    timestamp_with_timezone: Dict = {"type": ["null", "string"], "format": "date-time", "airbyte_type": "timestamp_with_timezone"}
    timestamp_without_timezone: Dict = {"type": ["null", "string"], "format": "date-time", "airbyte_type": "timestamp_without_timezone"}
    time_without_timezone: Dict = {"type": ["null", "string"], "format": "time", "airbyte_type": "time_without_timezone"}
    array_with_any: Dict = {"type": ["null", "array"], "items": {}}
    object: Dict = {"type": ["null", "object"]}


numeric_snowflake_type_airbyte_type = {
    # integers
    "INT": SchemaTypes.integer,
    "INTEGER": SchemaTypes.integer,
    "BIGINT": SchemaTypes.integer,
    "SMALLINT": SchemaTypes.integer,
    "TINYINT": SchemaTypes.integer,
    "BYTEINT": SchemaTypes.integer,
    # numbers
    "NUMBER": SchemaTypes.number,
    "DECIMAL": SchemaTypes.number,
    "NUMERIC": SchemaTypes.number,
    "FLOAT": SchemaTypes.number,
    "FLOAT4": SchemaTypes.number,
    "FLOAT8": SchemaTypes.number,
    "DOUBLE": SchemaTypes.number,
    "DOUBLE PRECISION": SchemaTypes.number,
    "REAL": SchemaTypes.number,

    # Not present in documentation
    'FIXED': SchemaTypes.number,
}

string_snowflake_type_airbyte_type = {
    "VARCHAR": SchemaTypes.string,
    "CHAR": SchemaTypes.string,
    "CHARACTER": SchemaTypes.string,
    "STRING": SchemaTypes.string,
    "TEXT": SchemaTypes.string,
    "BINARY": SchemaTypes.string,
    "VARBINARY": SchemaTypes.string
}

logical_snowflake_type_airbyte_type = {
    "BOOLEAN": SchemaTypes.boolean
}

date_and_time_snowflake_type_airbyte_type = {
    "DATE": SchemaTypes.date,

    "DATETIME": SchemaTypes.timestamp_with_timezone,
    "TIMESTAMP_LTZ": SchemaTypes.timestamp_with_timezone,
    "TIMESTAMP_TZ": SchemaTypes.timestamp_with_timezone,

    "TIMESTAMP_NTZ": SchemaTypes.timestamp_without_timezone,
    "TIMESTAMP": SchemaTypes.timestamp_without_timezone,

    "TIME": SchemaTypes.time_without_timezone,
}

semi_structured_snowflake_type_airbyte_type = {
    'VARIANT': SchemaTypes.string,
    'ARRAY': SchemaTypes.array_with_any,
    'OBJECT': SchemaTypes.object,
}

geospatial_snowflake_type_airbyte_type = {
    'GEOGRAPHY': SchemaTypes.string,
    'GEOMETRY': SchemaTypes.string,
}

vector_snowflake_type_airbyte_type = {
    'VECTOR': SchemaTypes.array_with_any,
}

mapping_snowflake_type_airbyte_type = {
    **numeric_snowflake_type_airbyte_type,
    **string_snowflake_type_airbyte_type,
    **logical_snowflake_type_airbyte_type,
    **date_and_time_snowflake_type_airbyte_type,
    **semi_structured_snowflake_type_airbyte_type,
    **geospatial_snowflake_type_airbyte_type,
    **vector_snowflake_type_airbyte_type,
}


def format_field(field_value, field_type):

    if field_type.upper() in ('OBJECT', 'ARRAY'):
        return json.loads(field_value)

    if field_type.upper() in date_and_time_snowflake_type_airbyte_type.keys() and field_value:
        try:
            if isinstance(field_value, str):
                ts = float(field_value.split(' ')[0])
            else:
                ts = float(field_value)
            dt = datetime.fromtimestamp(ts, pytz.timezone("UTC"))
            return dt.isoformat(timespec='microseconds')
        except ValueError:
            # maybe add warning
            return field_value

    if field_type.upper() in ('INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'BYTEINT') and field_value:
        return int(field_value)

    if (field_type.upper() in ('NUMBER', 'DECIMAL', 'NUMERIC', 'FLOAT', 'FLOAT4', 'FLOAT8', 'DOUBLE', 'DOUBLE PRECISION', 'REAL', 'FIXED')
            and field_value):
        return float(field_value)

    return field_value