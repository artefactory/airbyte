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
