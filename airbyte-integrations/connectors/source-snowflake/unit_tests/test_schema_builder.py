import json
import unittest
from datetime import datetime

from source_snowflake.schema_builder import format_field, get_generic_type_from_schema_type, SchemaTypes, \
    convert_time_zone_time_stamp_suffix_to_offset_hours, convert_utc_to_time_zone, convert_utc_to_time_zone_date
import pytest


def test_get_generic_type_from_schema_type():
    assert get_generic_type_from_schema_type(SchemaTypes.timestamp_with_timezone) == "timestamp_with_timezone"
    assert get_generic_type_from_schema_type(SchemaTypes.timestamp_without_timezone) == "date"
    assert get_generic_type_from_schema_type(SchemaTypes.date) == "date"
    assert get_generic_type_from_schema_type(SchemaTypes.time_without_timezone) == "date"
    assert get_generic_type_from_schema_type(SchemaTypes.number) == "number"
    assert get_generic_type_from_schema_type(SchemaTypes.integer) == "number"
    assert get_generic_type_from_schema_type(SchemaTypes.boolean) == "number"
    assert get_generic_type_from_schema_type(SchemaTypes.string) == "string"
    assert get_generic_type_from_schema_type(SchemaTypes.array_with_any) == "string"
    assert get_generic_type_from_schema_type(SchemaTypes.object) == "string"


def test_convert_time_zone_time_stamp_suffix_to_offset_hours():
    assert convert_time_zone_time_stamp_suffix_to_offset_hours("720") == -12
    assert convert_time_zone_time_stamp_suffix_to_offset_hours("-120") == -26
    assert convert_time_zone_time_stamp_suffix_to_offset_hours("1560") == 2
    assert convert_time_zone_time_stamp_suffix_to_offset_hours("1440") == 0
    assert convert_time_zone_time_stamp_suffix_to_offset_hours("1080") == -6

    # Test case for invalid input (non-numeric)
    with pytest.raises(ValueError):
        convert_time_zone_time_stamp_suffix_to_offset_hours("abc")


def test_convert_utc_to_time_zone():
    # Test case: positive offset
    utc_date = datetime(2023, 1, 1, 12, 0, 0)
    offset_hours = 3
    expected = '2023-01-01T15:00:00.000000+03:00'
    assert convert_utc_to_time_zone(utc_date, offset_hours) == expected

    # Test case: negative offset
    utc_date = datetime(2023, 1, 1, 12, 0, 0)
    offset_hours = -5
    expected = '2023-01-01T07:00:00.000000-05:00'
    assert convert_utc_to_time_zone(utc_date, offset_hours) == expected

    # Test case: zero offset
    utc_date = datetime(2023, 1, 1, 12, 0, 0)
    offset_hours = 0
    expected = '2023-01-01T12:00:00.000000+00:00'
    assert convert_utc_to_time_zone(utc_date, offset_hours) == expected

    # Test case: fractional positive offset
    utc_date = datetime(2023, 1, 1, 12, 0, 0)
    offset_hours = 5.5
    expected = '2023-01-01T17:30:00.000000+05:30'
    assert convert_utc_to_time_zone(utc_date, offset_hours) == expected

    # Test case: fractional negative offset
    utc_date = datetime(2023, 1, 1, 12, 0, 0)
    offset_hours = -3.75
    expected = '2023-01-01T08:15:00.000000-03:45'
    assert convert_utc_to_time_zone(utc_date, offset_hours) == expected

    # Test case: large positive offset
    utc_date = datetime(2023, 1, 1, 12, 0, 0)
    offset_hours = 14
    expected = '2023-01-02T02:00:00.000000+14:00'
    assert convert_utc_to_time_zone(utc_date, offset_hours) == expected

    # Test case: large negative offset
    utc_date = datetime(2023, 1, 1, 12, 0, 0)
    offset_hours = -12
    expected = '2023-01-01T00:00:00.000000-12:00'
    assert convert_utc_to_time_zone(utc_date, offset_hours) == expected


def test_convert_utc_to_time_zone_date():
    # Test case: positive offset
    utc_date = datetime(2023, 1, 1, 12, 0, 0)
    offset_hours = 3
    expected = datetime.strptime('2023-01-01T15:00:00.000000+03:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    assert convert_utc_to_time_zone_date(utc_date, offset_hours) == expected

    # Test case: negative offset
    utc_date = datetime(2023, 1, 1, 12, 0, 0)
    offset_hours = -5
    expected = datetime.strptime('2023-01-01T07:00:00.000000-05:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    assert convert_utc_to_time_zone_date(utc_date, offset_hours) == expected

    # Test case: zero offset
    utc_date = datetime(2023, 1, 1, 12, 0, 0)
    offset_hours = 0
    expected = datetime.strptime('2023-01-01T12:00:00.000000+00:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    assert convert_utc_to_time_zone_date(utc_date, offset_hours) == expected

    # Test case: fractional positive offset
    utc_date = datetime(2023, 1, 1, 12, 0, 0)
    offset_hours = 5.5
    expected = datetime.strptime('2023-01-01T17:30:00.000000+05:30', '%Y-%m-%dT%H:%M:%S.%f%z')
    assert convert_utc_to_time_zone_date(utc_date, offset_hours) == expected

    # Test case: fractional negative offset
    utc_date = datetime(2023, 1, 1, 12, 0, 0)
    offset_hours = -3.75
    expected = datetime.strptime('2023-01-01T08:15:00.000000-03:45', '%Y-%m-%dT%H:%M:%S.%f%z')
    assert convert_utc_to_time_zone_date(utc_date, offset_hours) == expected

    # Test case: large positive offset
    utc_date = datetime(2023, 1, 1, 12, 0, 0)
    offset_hours = 14
    expected = datetime.strptime('2023-01-02T02:00:00.000000+14:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    assert convert_utc_to_time_zone_date(utc_date, offset_hours) == expected

    # Test case: large negative offset
    utc_date = datetime(2023, 1, 1, 12, 0, 0)
    offset_hours = -12
    expected = datetime.strptime('2023-01-01T00:00:00.000000-12:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    assert convert_utc_to_time_zone_date(utc_date, offset_hours) == expected


class TestFormatField(unittest.TestCase):

    def test_none_field_type(self):
        self.assertEqual("value", format_field("value", None))
        self.assertIsNone(format_field(None, "INT"))

    def test_object_array_field_type(self):
        self.assertEqual({"key": "value"}, format_field(json.dumps({"key": "value"}), "OBJECT"))
        self.assertEqual([1, 2, 3], (format_field(json.dumps([1, 2, 3]), "ARRAY")))

    def test_edge_case_date_field_type(self):
        # already formatted date
        self.assertEqual(datetime(2020, 1, 1), format_field(datetime(2020, 1, 1), "DATE"))

        # not a string or empty string
        with pytest.raises(ValueError):
            self.assertEqual(32323, format_field(32323, "DATE"))

        self.assertIsNone(format_field("", "DATE"))

    def test_timestamp_with_timezone_with_offset_in_field_value_field_type(self):
        """
        The format time of type TIMESTAMP_TZ in snowflake api response is:
        "1577836800 1560"
        where:
         1577836800 represents the number of seconds from the date 1970-01-01 either positive or negative number of seconds
         1560 represents an amount of minutes that is deduced from 24 to get the offset of time zone
              precise computation: (1560/60) - 24 - = 26 - 24 = +2 which will be the offset of time zone
        """
        self.assertEqual("2019-12-31T00:30:00.000000-23:30", format_field("1577836800 30", "TIMESTAMP_TZ"))
        self.assertEqual("1920-01-02T02:00:00.000000+02:00", format_field("-1577836800 1560", "TIMESTAMP_TZ"))
        self.assertEqual("2020-01-01T02:00:00.000000+02:00", format_field("1577836800 1560", "TIMESTAMP_TZ"))
        self.assertEqual("2019-12-31T18:00:00.000000-06:00", format_field("1577836800 1080", "TIMESTAMP_TZ"))

    def test_timestamp_with_timezone_without_offset_in_field_value_field_type(self):
        """
        The format time of type TIMESTAMP_LTZ in snowflake api response is:
        "1577836800"
        where:
         1577836800 represents the number of seconds from the date 1970-01-01 either positive or negative number of seconds
        The offset is given as an input.
        The offset is fetched by requesting the current time in snowflake and computing offset independently.
        if no offset is provided, the default value of 0 is applied.
        """
        self.assertEqual("2020-01-01T05:30:00.000000+05:30",
                         format_field("1577836800", "TIMESTAMP_LTZ", local_time_zone_offset_hours=5.5))
        self.assertEqual("2020-01-01T06:00:00.000000+06:00",
                         format_field("1577836800", "TIMESTAMP_LTZ", local_time_zone_offset_hours=6))
        self.assertEqual("2019-12-31T18:00:00.000000-06:00",
                         format_field("1577836800", "TIMESTAMP_LTZ", local_time_zone_offset_hours=-6))
        self.assertEqual("2020-01-01T00:00:00.000000+00:00",
                         format_field("1577836800", "TIMESTAMP_LTZ"))

    def test_date_field_type(self):
        """
        The format time of type DATE in snowflake api response is:
        "825"
        where:
         825 represents the number of days from the date 1970-01-01 either positive or negative number of days
         1970-01-01 + 825 days = 1972-04-05
        """
        self.assertEqual("1972-04-05", format_field("825", "DATE"))
        self.assertEqual("1969-01-01", format_field("-365", "DATE"))

    def test_timestamp_without_timezone_field_type(self):
        """
        The format time of type DATETIME, TIMESTAMP_NTZ, TIMESTAMP in snowflake api response is:
        "1577836800"
        where:
         1577836800 represents the number of seconds from the date 1970-01-01 either positive or negative number of seconds
        """
        self.assertEqual("2020-01-01T00:00:00.100000", format_field("1577836800.1", "DATETIME"))
        self.assertEqual("1920-01-02T00:00:00.000000", format_field("-1577836800", "DATETIME"))
        self.assertEqual("1970-01-01T00:00:00.000000", format_field("0", "DATETIME"))

        self.assertEqual("2020-01-01T00:00:00.000000", format_field("1577836800", "TIMESTAMP_NTZ"))
        self.assertEqual("1920-01-01T23:59:59.999997", format_field("-1577836800.000003", "TIMESTAMP_NTZ"))
        self.assertEqual("1970-01-01T00:00:00.000000", format_field("0", "TIMESTAMP_NTZ"))

        self.assertEqual("2020-01-01T00:00:00.000000", format_field("1577836800", "TIMESTAMP"))
        self.assertEqual("1920-01-01T23:59:59.500000", format_field("-1577836800.5", "TIMESTAMP"))
        self.assertEqual("1970-01-01T00:00:00.000000", format_field("0", "TIMESTAMP"))

    def test_time_field_type(self):
        """
        The format time of type TIME in snowflake api response is:
        "65000"
        where:
         65000 represents the number of seconds from the time 00-00-00 either positive  number of seconds only
        """
        self.assertEqual("18:03:20", format_field("65000", "TIME"))
        self.assertEqual("00:00:01", format_field("1", "TIME"))

        # edge case that should never happen but it tests the expected result in this case

        # In case of negative value with this time we just go back with that number of seconds
        self.assertEqual("05:56:40", format_field("-65000", "TIME"))
        self.assertEqual("05:56:40", format_field("21400", "TIME"))

        # In case of exceeding the number of seconds per days, we just take the rest of division by 86400
        self.assertEqual("00:00:02", format_field("86402", "TIME"))
        self.assertEqual("00:00:02", format_field("2", "TIME"))

    def test_integer_field_type(self):
        self.assertEqual(format_field("123", "INT"), 123)
        self.assertEqual(format_field("514", "INTEGER"), 514)
        self.assertEqual(format_field("5415", "BIGINT"), 5415)
        self.assertEqual(format_field("87", "SMALLINT"), 87)
        self.assertEqual(format_field("7", "TINYINT"), 7)
        self.assertEqual(format_field("6", "BYTEINT"), 6)

    def test_number_field_type(self):
        self.assertEqual(format_field("123.45", "NUMBER"), 123.45)
        self.assertEqual(format_field("123.45", "DECIMAL"), 123.45)
        self.assertEqual(format_field("123.45", "NUMERIC"), 123.45)
        self.assertEqual(format_field("123.45", "FLOAT"), 123.45)
        self.assertEqual(format_field("123.45", "FLOAT4"), 123.45)
        self.assertEqual(format_field("123.45", "FLOAT8"), 123.45)
        self.assertEqual(format_field("123.45", "DOUBLE"), 123.45)
        self.assertEqual(format_field("123.45", "DOUBLE PRECISION"), 123.45)
        self.assertEqual(format_field("123.45", "REAL"), 123.45)
        self.assertEqual(format_field("123.45", "FIXED"), 123.45)

        # Valid integer conversions
        self.assertEqual(format_field("123", "NUMBER"), 123.0)
        self.assertEqual(format_field("123", "DECIMAL"), 123.0)
        self.assertEqual(format_field("123", "NUMERIC"), 123.0)
        self.assertEqual(format_field("123", "FLOAT"), 123.0)
        self.assertEqual(format_field("123", "FLOAT4"), 123.0)
        self.assertEqual(format_field("123", "FLOAT8"), 123.0)
        self.assertEqual(format_field("123", "DOUBLE"), 123.0)
        self.assertEqual(format_field("123", "DOUBLE PRECISION"), 123.0)
        self.assertEqual(format_field("123", "REAL"), 123.0)
        self.assertEqual(format_field("123", "FIXED"), 123.0)

    def test_boolean_field_type(self):
        self.assertTrue(format_field("true", "BOOLEAN"))
        self.assertFalse(format_field("false", "BOOLEAN"))

    def test_geospatial_vector_field_type(self):
        self.assertEqual(format_field(json.dumps({"type": "Point", "coordinates": [125.6, 10.1]}), "GEOGRAPHY"),
                         str({"type": "Point", "coordinates": [125.6, 10.1]}))
        self.assertEqual(format_field(json.dumps([1, 2, 3]), "VECTOR"), str([1, 2, 3]))

    def test_fallback(self):
        self.assertEqual(format_field("some string", "VARCHAR"), "some string")
        self.assertEqual(format_field("some unknown type", "UNKNOWN"), "some unknown type")
