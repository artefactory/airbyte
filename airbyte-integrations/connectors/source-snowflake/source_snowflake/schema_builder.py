from typing import Dict


class SchemaTypes:
    string: Dict = {"type": ["null", "string"]}
    number: Dict = {"type": ["null", "number"]}
    integer: Dict = {"type": ["null", "number"]}
    boolean: Dict = {"type": ["null", "boolean"]}
    date: Dict = {"type": ["null", "string"], "format": "date"}
    array_with_any: Dict = {"type": ["null", "array"], "items": {}}
    object: Dict = {"type": ["null", "object"]}


mapping_snowflake_type_airbyte_type = {
    # Numeric data types
    'float': SchemaTypes.number,
    'number': SchemaTypes.integer,
    'boolean': SchemaTypes.boolean,

    # string data
    'varchar': SchemaTypes.string,
    'char': SchemaTypes.string,
    'character': SchemaTypes.string,
    'string': SchemaTypes.string,
    'text': SchemaTypes.string,
    'binary': SchemaTypes.string,
    'varbinary': SchemaTypes.string,

    # Date and time
    'timestamp_tz': SchemaTypes.string,
    'timestamp_ltz': SchemaTypes.string,
    'timestamp_ntz': SchemaTypes.string,
    'time': SchemaTypes.string,
    'date': SchemaTypes.date,

    # semi structured objects
    'array': SchemaTypes.array_with_any,
    'object': SchemaTypes.object,

    # Not present in documentation
    'fixed': SchemaTypes.integer,
}


