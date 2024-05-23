#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

from source_snowflake.source import SourceSnowflake

#TODO all tests
def test_check_connection(mocker):
    source = SourceSnowflake()
    logger_mock, config_mock = MagicMock(), MagicMock()
    assert source.check_connection(logger_mock, config_mock) == (True, None)


def test_streams(mocker):
    source = SourceSnowflake()
    config_mock = MagicMock()
    streams = source.streams(config_mock)
    # TODO: replace this with your streams number
    expected_streams_number = 2
    assert len(streams) == expected_streams_number