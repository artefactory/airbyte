import json
from datetime import datetime, timedelta
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


def get_generic_type_from_schema_type(schema_type):

    if schema_type == SchemaTypes.timestamp_with_timezone:
        return "timestamp_with_timezone"

    if schema_type in (SchemaTypes.timestamp_without_timezone, SchemaTypes.time_without_timezone):
        return "date"

    if schema_type in (SchemaTypes.number, SchemaTypes.integer, SchemaTypes.boolean):
        return 'number'

    # Default consider it a string
    return "string"


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


def convert_time_zone_time_stamp_suffix_to_offset_hours(tz_time_stamp_suffix):
    raw_offset_minutes = int(tz_time_stamp_suffix)
    delta = raw_offset_minutes / 60
    offset_hours = delta - 24
    return offset_hours


def convert_utc_to_time_zone(utc_date, offset_hours):
    offset = timedelta(hours=offset_hours)
    local_date = utc_date + offset
    sign = '+' if offset_hours >= 0 else '-'
    abs_offset_hours = int(abs(offset_hours))
    abs_offset_minutes = int((abs(offset_hours) * 60) % 60)

    offset_str = f"{sign}{abs_offset_hours:02}:{abs_offset_minutes:02}"

    return local_date.strftime(f'%Y-%m-%dT%H:%M:%S.%f{offset_str}')

def convert_utc_to_time_zone_date(utc_date, offset_hours):
    offset = timedelta(hours=offset_hours)
    local_date = utc_date + offset
    sign = '+' if offset_hours >= 0 else '-'
    abs_offset_hours = int(abs(offset_hours))
    abs_offset_minutes = int((abs(offset_hours) * 60) % 60)

    offset_str = f"{sign}{abs_offset_hours:02}:{abs_offset_minutes:02}"

    return datetime.strptime(local_date.strftime(f'%Y-%m-%dT%H:%M:%S.%f{offset_str}'), '%Y-%m-%dT%H:%M:%S.%f%z')


def format_field(field_value, field_type):
    if field_type is None or field_value is None:
        # maybe add warning
        return field_value

    if isinstance(field_value, datetime):
        return field_value

    if field_type.upper() in ('OBJECT', 'ARRAY'):
        return json.loads(field_value)

    if field_type.upper() in date_and_time_snowflake_type_airbyte_type.keys() and field_value:
        try:
            if isinstance(field_value, str) and ' ' in field_value:  # time_stamp with timezone
                unix_time_stamp = float(field_value.split(' ')[0])
                time_zone_time_stamp_suffix = field_value.split(' ')[1]
                offset_hours = convert_time_zone_time_stamp_suffix_to_offset_hours(time_zone_time_stamp_suffix)
                utc_date = datetime.fromtimestamp(unix_time_stamp, pytz.timezone("UTC"))
                return convert_utc_to_time_zone(utc_date, offset_hours)
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
