#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from http import HTTPStatus
from unittest.mock import MagicMock

import pytest
from source_airbyte_api.source import AirbyteApiStream, AirbyteApiSubStream


@pytest.fixture
def patch_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(AirbyteApiStream, "path", "v0/example_endpoint")
    mocker.patch.object(AirbyteApiStream, "primary_key", "test_primary_key")
    mocker.patch.object(AirbyteApiStream, "__abstractmethods__", set())
    mocker.patch.object(AirbyteApiSubStream, "__abstractmethods__", set())
    mocker.patch.object(AirbyteApiSubStream, "primary_key", "test_primary_key_sub")

@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        ({"stream_slice": None, "stream_state": None, "next_page_token": {"next":"http://dummy_url.com/dummy_path?limit=1&offset=1"}}, {"offset": "1", "limit": "1"}),
        ({"stream_slice": None, "stream_state": None, "next_page_token": {}}, {}),
        ({"stream_slice": None, "stream_state": None, "next_page_token": None}, {})
    ],
)
def test_request_params_Stream(patch_base_class,inputs, expected):
    stream = AirbyteApiStream()
    inputs = inputs
    expected_params = expected
    assert stream.request_params(**inputs) == expected_params

@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        ({"stream_slice": {"parent":{"test_primary_key":"1234"}}, "stream_state": None, "next_page_token": {"next":"http://dummy_url.com/dummy_path?limit=1&offset=1"}}, {"offset": "1", "limit": "1","test_primary_key":"1234"}),
        ({"stream_slice": {"parent":{"test_primary_key":"1234"}}, "stream_state": None, "next_page_token": {}}, {"test_primary_key":"1234"}),
    ],
)
def test_request_params_SubStream(patch_base_class,inputs, expected):
    substream = AirbyteApiSubStream(parent=AirbyteApiStream())
    input = inputs
    expected_params = expected
    assert substream.request_params(**input) == expected_params


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        ({"next":"dummy_next","data":[{"key":"value"}]}, {"next":"dummy_next"}),
        ({"previous":"dummy_previous"}, None),
        ({"data":[{"key":"value"}]}, None)
    ],
)
def test_next_page_token(patch_base_class,inputs,expected):
    stream = AirbyteApiStream()
    response = MagicMock()
    response.json.return_value=inputs
    inputs = {"response": response}
    expected_token = expected
    assert stream.next_page_token(**inputs) == expected_token

@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        ({"next":"dummy_next","data":[{"key":"value"}]}, {"key":"value"}),
        ({"previous":"dummy_previous"}, "finished"),
        ({"data":[{"key":"value"}]}, {"key":"value"})
    ],
)
def test_parse_response(patch_base_class,inputs,expected):
    stream = AirbyteApiStream()
    # TODO: replace this with your input parameters
    response = MagicMock()
    response.json.return_value=inputs
    inputs = {"response": response}
    # TODO: replace this with your expected parced object
    expected_parsed_object = expected
    assert next(stream.parse_response(**inputs),"finished") == expected_parsed_object


def test_request_headers(patch_base_class):
    stream = AirbyteApiStream()
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_headers = {"user-agent": "source-airbyte-api"}
    assert stream.request_headers(**inputs) == expected_headers


def test_http_method(patch_base_class):
    stream = AirbyteApiStream()
    expected_method = "GET"
    assert stream.http_method == expected_method


@pytest.mark.parametrize(
    ("http_status", "should_retry"),
    [
        (HTTPStatus.OK, False),
        (HTTPStatus.BAD_REQUEST, False),
        (HTTPStatus.TOO_MANY_REQUESTS, True),
        (HTTPStatus.INTERNAL_SERVER_ERROR, True),
    ],
)
def test_should_retry(patch_base_class, http_status, should_retry):
    response_mock = MagicMock()
    response_mock.status_code = http_status
    stream = AirbyteApiStream()
    assert stream.should_retry(response_mock) == should_retry


def test_backoff_time(patch_base_class):
    response_mock = MagicMock()
    stream = AirbyteApiStream()
    expected_backoff_time = None
    assert stream.backoff_time(response_mock) == expected_backoff_time
