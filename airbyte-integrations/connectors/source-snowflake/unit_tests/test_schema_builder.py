import json
import unittest
from datetime import datetime
from parameterized import parameterized

from source_snowflake.schema_builder import format_field, get_generic_type_from_schema_type, SchemaTypes, \
    convert_time_zone_time_stamp_suffix_to_offset_hours, convert_utc_to_time_zone, convert_utc_to_time_zone_date


class TestSchemaBuilderFunctions(unittest.TestCase):
    @parameterized.expand([
        (SchemaTypes.timestamp_with_timezone, "timestamp_with_timezone"),
        (SchemaTypes.timestamp_without_timezone, "date"),
        (SchemaTypes.date, "date"),
        (SchemaTypes.time_without_timezone, "date"),
        (SchemaTypes.number, "number"),
        (SchemaTypes.integer, "number"),
        (SchemaTypes.boolean, "number"),
        (SchemaTypes.string, "string"),
        (SchemaTypes.array_with_any, "string"),
        (SchemaTypes.object, "string")
    ])
    def test_get_generic_type_from_schema_type(self, schema_type, expected):
        assert get_generic_type_from_schema_type(schema_type) == expected


    @parameterized.expand([
        ("720", -12),
        ("-120", -26),
        ("1560", 2),
        ("1440", 0),
        ("1080", -6)
    ])
    def test_convert_time_zone_time_stamp_suffix_to_offset_hours(self, time_stamp_suffix, expected):
        assert convert_time_zone_time_stamp_suffix_to_offset_hours(time_stamp_suffix) == expected

    def test_convert_time_zone_time_stamp_suffix_to_offset_hours_invalid_input(self):
        # Test case for invalid input (non-numeric)
        with self.assertRaises(ValueError):
            convert_time_zone_time_stamp_suffix_to_offset_hours("abc")


    @parameterized.expand([
        (datetime(2023, 1, 1, 12, 0, 0), 3, '2023-01-01T15:00:00.000000+03:00'),
        (datetime(2023, 1, 1, 12, 0, 0), -5, '2023-01-01T07:00:00.000000-05:00'),
        (datetime(2023, 1, 1, 12, 0, 0), 0, '2023-01-01T12:00:00.000000+00:00'),
        (datetime(2023, 1, 1, 12, 0, 0), 5.5, '2023-01-01T17:30:00.000000+05:30'),
        (datetime(2023, 1, 1, 12, 0, 0), -3.75, '2023-01-01T08:15:00.000000-03:45'),
        (datetime(2023, 1, 1, 12, 0, 0), 14, '2023-01-02T02:00:00.000000+14:00'),
        (datetime(2023, 1, 1, 12, 0, 0), -12, '2023-01-01T00:00:00.000000-12:00')
    ])
    def test_convert_utc_to_time_zone(self, utc_date, offset_hours, expected):
        assert convert_utc_to_time_zone(utc_date, offset_hours) == expected


    @parameterized.expand([
        (datetime(2023, 1, 1, 12, 0, 0), 3, '2023-01-01T15:00:00.000000+03:00'),
        (datetime(2023, 1, 1, 12, 0, 0), -5, '2023-01-01T07:00:00.000000-05:00'),
        (datetime(2023, 1, 1, 12, 0, 0), 0, '2023-01-01T12:00:00.000000+00:00'),
        (datetime(2023, 1, 1, 12, 0, 0), 5.5, '2023-01-01T17:30:00.000000+05:30'),
        (datetime(2023, 1, 1, 12, 0, 0), -3.75, '2023-01-01T08:15:00.000000-03:45'),
        (datetime(2023, 1, 1, 12, 0, 0), 14, '2023-01-02T02:00:00.000000+14:00'),
        (datetime(2023, 1, 1, 12, 0, 0), -12, '2023-01-01T00:00:00.000000-12:00')
    ])
    def test_convert_utc_to_time_zone_date(self, utc_date, offset_hours, expected_str):
        expected = datetime.strptime(expected_str, '%Y-%m-%dT%H:%M:%S.%f%z')
        assert convert_utc_to_time_zone_date(utc_date, offset_hours) == expected


class TestFormatField(unittest.TestCase):

    @parameterized.expand(
        [
            ("value", None, "value"),
            (None, "INT", None)
        ],
    )
    def test_none_input(self, input_value, field_type, expected):
        assert format_field(input_value, field_type) == expected

    @parameterized.expand([
        (json.dumps({"key": "value"}), "OBJECT", {"key": "value"}),
        (json.dumps([1, 2, 3]), "ARRAY", [1, 2, 3])
    ])
    def test_complex_types(self, input_value, field_type, expected):
        assert format_field(input_value, field_type) == expected

    @parameterized.expand([
        (datetime(2020, 1, 1), "DATE", datetime(2020, 1, 1)),
        ("", "DATE", None)
    ])
    def test_date_object_input(self, input_value, field_type, expected):
        assert format_field(input_value, field_type) == expected

    def test_raises_value_error_when_having_int_input(self):
        with self.assertRaises(ValueError):
            format_field(32323, "DATE")

    @parameterized.expand(
        [
            ("1577836800 30", "TIMESTAMP_TZ", "2019-12-31T00:30:00.000000-23:30"),
            ("-1577836800 1560", "TIMESTAMP_TZ", "1920-01-02T02:00:00.000000+02:00"),
            ("1577836800 1560", "TIMESTAMP_TZ", "2020-01-01T02:00:00.000000+02:00"),
            ("1577836800 1080", "TIMESTAMP_TZ", "2019-12-31T18:00:00.000000-06:00")
        ]
    )
    def test_timestamp_with_timezone_with_offset_in_field_value_field_type(self, input_value, field_type, expected):
        """
        The format time of type TIMESTAMP_TZ in snowflake api response is:
        "1577836800 1560"
        where:
         1577836800 represents the number of seconds from the date 1970-01-01 either positive or negative number of seconds
         1560 represents an amount of minutes that is deduced from 24 to get the offset of time zone
              precise computation: (1560/60) - 24 - = 26 - 24 = +2 which will be the offset of time zone
        """
        assert expected == format_field(input_value, field_type)

    @parameterized.expand(
        [
            ("1577836800", "TIMESTAMP_LTZ", 5.5, "2020-01-01T05:30:00.000000+05:30"),
            ("1577836800", "TIMESTAMP_LTZ", 6, "2020-01-01T06:00:00.000000+06:00"),
            ("1577836800", "TIMESTAMP_LTZ", -6, "2019-12-31T18:00:00.000000-06:00"),
            ("1577836800", "TIMESTAMP_LTZ", 0, "2020-01-01T00:00:00.000000+00:00")
        ]
    )
    def test_timestamp_with_timezone_without_offset_in_field_value_field_type(self, input_value, field_type, offset, expected):
        """
        The format time of type TIMESTAMP_LTZ in snowflake api response is:
        "1577836800"
        where:
         1577836800 represents the number of seconds from the date 1970-01-01 either positive or negative number of seconds
        The offset is given as an input.
        The offset is fetched by requesting the current time in snowflake and computing offset independently.
        if no offset is provided, the default value of 0 is applied.
        """
        assert expected == format_field(input_value, field_type, local_time_zone_offset_hours=offset)

    @parameterized.expand(
        [
            ("825", "DATE", "1972-04-05"),
            ("-365", "DATE", "1969-01-01")
        ]
    )
    def test_date_field_type(self, input_value, field_type, expected):
        """
        The format time of type DATE in snowflake api response is:
        "825"
        where:
         825 represents the number of days from the date 1970-01-01 either positive or negative number of days
         1970-01-01 + 825 days = 1972-04-05
        """
        assert expected == format_field(input_value, field_type)


    @parameterized.expand(
        [
            ("1577836800.1", "DATETIME", "2020-01-01T00:00:00.100000"),
            ("-1577836800", "DATETIME", "1920-01-02T00:00:00.000000"),
            ("0", "DATETIME", "1970-01-01T00:00:00.000000"),
            ("1577836800", "TIMESTAMP_NTZ", "2020-01-01T00:00:00.000000"),
            ("-1577836800.000003", "TIMESTAMP_NTZ", "1920-01-01T23:59:59.999997"),
            ("0", "TIMESTAMP_NTZ", "1970-01-01T00:00:00.000000"),
            ("1577836800", "TIMESTAMP", "2020-01-01T00:00:00.000000"),
            ("-1577836800.5", "TIMESTAMP", "1920-01-01T23:59:59.500000"),
            ("0", "TIMESTAMP", "1970-01-01T00:00:00.000000")
        ]
    )
    def test_timestamp_without_timezone_field_type(self, input_value, field_type, expected):
        """
        The format time of type DATETIME, TIMESTAMP_NTZ, TIMESTAMP in snowflake api response is:
        "1577836800"
        where:
         1577836800 represents the number of seconds from the date 1970-01-01 either positive or negative number of seconds
        """
        assert expected == format_field(input_value, field_type)


    @parameterized.expand(
        [
            ("65000", "TIME", "18:03:20"),
            ("1", "TIME", "00:00:01"),
            ("-65000", "TIME", "05:56:40"),
            ("21400", "TIME", "05:56:40"),
            ("86402", "TIME", "00:00:02"),
            ("2", "TIME", "00:00:02")
        ]
    )
    def test_time_field_type(self, input_value, field_type, expected):
        """
        The format time of type TIME in snowflake api response is:
        "65000"
        where:
         65000 represents the number of seconds from the time 00-00-00 either positive  number of seconds only
        """
        assert expected == format_field(input_value, field_type)

    @parameterized.expand(
        [
            ("123", "INT", 123),
            ("514", "INTEGER", 514),
            ("5415", "BIGINT", 5415),
            ("87", "SMALLINT", 87),
            ("7", "TINYINT", 7),
            ("6", "BYTEINT", 6)
        ]
    )
    def test_integer_field_type(self, input_value, field_type, expected):
        assert expected == format_field(input_value, field_type)

    @parameterized.expand(
        [
            ("123.45", "NUMBER", 123.45),
            ("123.45", "DECIMAL", 123.45),
            ("123.45", "NUMERIC", 123.45),
            ("123.45", "FLOAT", 123.45),
            ("123.45", "FLOAT4", 123.45),
            ("123.45", "FLOAT8", 123.45),
            ("123.45", "DOUBLE", 123.45),
            ("123.45", "DOUBLE PRECISION", 123.45),
            ("123.45", "REAL", 123.45),
            ("123.45", "FIXED", 123.45),
            ("123", "NUMBER", 123.0),
            ("123", "DECIMAL", 123.0),
            ("123", "NUMERIC", 123.0),
            ("123", "FLOAT", 123.0),
            ("123", "FLOAT4", 123.0),
            ("123", "FLOAT8", 123.0),
            ("123", "DOUBLE", 123.0),
            ("123", "DOUBLE PRECISION", 123.0),
            ("123", "REAL", 123.0),
            ("123", "FIXED", 123.0)
        ]
    )
    def test_number_field_type(self, input_value, field_type, expected):
        assert expected == format_field(input_value, field_type)

    @parameterized.expand(
        [
            ("true", "BOOLEAN", True),
            ("false", "BOOLEAN", False)
        ]
    )
    def test_boolean_field_type(self, input_value, field_type, expected):
        assert expected == format_field(input_value, field_type)

    @parameterized.expand(
        [
            (json.dumps({"type": "Point", "coordinates": [125.6, 10.1]}), "GEOGRAPHY", str({"type": "Point", "coordinates": [125.6, 10.1]})),
            (json.dumps([1, 2, 3]), "VECTOR", str([1, 2, 3]))
        ]
    )
    def test_geospatial_vector_field_type(self, input_value, field_type, expected):
        assert expected == format_field(input_value, field_type)

    @parameterized.expand(
        [
            ("some string", "VARCHAR", "some string"),
            ("some unknown type", "UNKNOWN", "some unknown type")
        ]
    )
    def test_fallback(self, input_value, field_type, expected):
        assert expected == format_field(input_value, field_type)
