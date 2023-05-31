# #
# # Copyright (c) 2023 Airbyte, Inc., all rights reserved.
# #


from source_airbyte_api.source import AirbyteApiStream
from airbyte_cdk.models import SyncMode
from pytest import fixture
from unittest import mock
import pytest
from source_airbyte_api.source import Jobs


@fixture
def patch_incremental_base_class(mocker):
    mocker.patch.object(AirbyteApiStream, "path", "v0/example_endpoint")
    mocker.patch.object(AirbyteApiStream, "primary_key", "test_parent_primary_key")
    mocker.patch.object(AirbyteApiStream, "__abstractmethods__", set())
    mocker.patch.object(Jobs, "path", "v0/example_endpoint")
    mocker.patch.object(Jobs, "primary_key", "test_primary_key")
    mocker.patch.object(Jobs, "__abstractmethods__", set())


def test_cursor_field(patch_incremental_base_class):
    stream = Jobs(parent=AirbyteApiStream())
    # TODO: replace this with your expected cursor field
    expected_cursor_field = "lastUpdatedAt"
    assert stream.cursor_field == expected_cursor_field


@pytest.mark.parametrize(
    ("inputs", "records", "expected"),
    [
        (
            {
                "sync_mode": SyncMode.incremental,
                "cursor_field": "lastUpdatedAt",
                "stream_slice": {"parent": {"test_parent_primary_key": "d760f9b9-aada-4966-ba8c-804688b5066a"}},
                "stream_state": {"d760f9b9-aada-4966-ba8c-804688b5066a": "2023-05-30T14:41:36Z"},
            },
            [
                {
                    "jobId": 2386992,
                    "status": "failed",
                    "jobType": "sync",
                    "startTime": "2023-05-30T14:23:35Z",
                    "lastUpdatedAt": "2023-05-30T14:41:36Z",
                    "duration": "PT18M1S",
                    "connectionId": "d760f9b9-aada-4966-ba8c-804688b5066a",
                }
            ],
            {"d760f9b9-aada-4966-ba8c-804688b5066a": "2023-05-30T14:41:36Z"},
        ),
        (
            {
                "sync_mode": SyncMode.incremental,
                "cursor_field": "lastUpdatedAt",
                "stream_slice": {"parent": {"test_parent_primary_key": "d760f9b9-aada-4966-ba8c-804688b5066a"}},
                "stream_state": {"d760f9b9-aada-4966-ba8c-804688b5066a": "2023-05-30T14:41:36Z"},
            },
            [
                {
                    "jobId": 2386992,
                    "status": "failed",
                    "jobType": "sync",
                    "startTime": "2023-05-30T14:23:35Z",
                    "lastUpdatedAt": "2023-05-29T14:41:36Z",
                    "duration": "PT18M1S",
                    "connectionId": "d760f9b9-aada-4966-ba8c-804688b5066a",
                }
            ],
            {"d760f9b9-aada-4966-ba8c-804688b5066a": "2023-05-30T14:41:36Z"},
        ),
        (
            {
                "sync_mode": SyncMode.incremental,
                "cursor_field": "lastUpdatedAt",
                "stream_slice": {"parent": {"test_parent_primary_key": "d760f9b9-aada-4966-ba8c-804688b5066a"}},
                "stream_state": None,
            },
            [
                {
                    "jobId": 2386992,
                    "status": "failed",
                    "jobType": "sync",
                    "startTime": "2023-05-30T14:23:35Z",
                    "lastUpdatedAt": "2023-05-30T14:41:36Z",
                    "duration": "PT18M1S",
                    "connectionId": "d760f9b9-aada-4966-ba8c-804688b5066a",
                }
            ],
            {"d760f9b9-aada-4966-ba8c-804688b5066a": "2023-05-30T14:41:36Z"},
        ),
    ],
)
def test_read_records(patch_incremental_base_class, mocker, inputs, records, expected):
    stream = Jobs(AirbyteApiStream())
    mocker.patch.object(stream, "_read_pages", return_value=iter(records))
    for a in stream.read_records(**inputs):
        a
    assert stream.state == expected


# def test_stream_slices(patch_incremental_base_class):
#     stream = IncrementalAirbyteApiStream()
#     # TODO: replace this with your input parameters
#     inputs = {"sync_mode": SyncMode.incremental, "cursor_field": [], "stream_state": {}}
#     # TODO: replace this with your expected stream slices list
#     expected_stream_slice = [None]
#     assert stream.stream_slices(**inputs) == expected_stream_slice


# def test_supports_incremental(patch_incremental_base_class, mocker):
#     mocker.patch.object(IncrementalAirbyteApiStream, "cursor_field", "dummy_field")
#     stream = IncrementalAirbyteApiStream()
#     assert stream.supports_incremental


# def test_stream_checkpoint_interval(patch_incremental_base_class):
#     stream = IncrementalAirbyteApiStream()
#     # TODO: replace this with your expected checkpoint interval
#     expected_checkpoint_interval = None
#     assert stream.state_checkpoint_interval == expected_checkpoint_interval
