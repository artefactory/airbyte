#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Callable, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.streams.http.http import HttpStream

import requests
from urllib.parse import urlparse, parse_qsl

from urllib.parse import urljoin
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from datetime import datetime


BASE_URL = "https://api.airbyte.com/v1/"
DEFAULT_HEADER = {}


# Basic full refresh stream
class AirbyteApiStream(HttpStream, ABC):
    url_base = BASE_URL

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if next := response.json().get("next"):
            return {"next": next}
        return None

    @property
    def use_cache(self):
        return True

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        if next_page_token is not None and (next := next_page_token.get("next", [])):
            parsed_url = urlparse(next)
            captured_params = dict(parse_qsl(parsed_url.query))
            return captured_params

        return DEFAULT_HEADER

    def transform(
        self, record: Mapping[str, Any] = None, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return record

    def parse_response(
        self,
        response: requests.Response,
        *,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        for entry in response.json().get("data", []):
            yield self.transform(entry, stream_state, stream_slice)

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {"user-agent": "source-airbyte-api"}


class AirbyteApiSubStream(AirbyteApiStream, HttpSubStream):
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        base_dict = super().request_params(stream_state, stream_slice, next_page_token)

        base_dict[self.parent.primary_key] = stream_slice["parent"][self.parent.primary_key]
        return base_dict

    def transform(
        self, record: Mapping[str, Any] = None, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        record[self.parent.primary_key] = stream_slice["parent"][self.parent.primary_key]
        return record


class Workspaces(AirbyteApiStream):
    primary_key = "workspaceId"

    def path(self, **kwargs) -> str:
        return "workspaces"


class Sources(AirbyteApiSubStream):
    primary_key = "sourceId"

    def path(self, **kwargs) -> str:
        return "sources"


class Connections(AirbyteApiSubStream):
    primary_key = "connectionId"

    def path(self, **kwargs) -> str:
        return "connections"


class Jobs(AirbyteApiSubStream, IncrementalMixin):
    primary_key = "jobId"
    cursor_field = "lastUpdatedAt"
    # state_checkpoint_interval = 1
    _state = {}
    _most_recent_dates_runs = None

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._state = value

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[StreamData]:
        connection_id = stream_slice.get("parent").get(self.parent.primary_key)
        for record in self._get_first_record_date_and_continue(
            lambda req, res, state, _slice: self.parse_response(res, stream_slice=_slice, stream_state=state), stream_slice, stream_state
        ):
            if (
                (date_state := self._state.get(connection_id))
                and (date_state >= record.get(self.cursor_field))
                and sync_mode == SyncMode.incremental
            ):
                break
            else:
                yield record

        if sync_mode == SyncMode.incremental:
            self._state[connection_id] = max(self._most_recent_dates_runs, self._state.get(connection_id, self._most_recent_dates_runs))

    def _get_first_record_date_and_continue(
        self,
        records_generator_fn: Callable[
            [requests.PreparedRequest, requests.Response, Mapping[str, Any], Mapping[str, Any]], Iterable[StreamData]
        ],
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[StreamData]:
        records = self._read_pages(records_generator_fn, stream_slice, stream_state)
        first_record = next(records)
        self._most_recent_dates_runs = first_record[self.cursor_field]
        yield first_record
        yield from records

    def path(self, **kwargs) -> str:
        return "jobs"


class SourceAirbyteApi(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        headers = {"authorization": f"Bearer {config.get('apiKey')}", "accept": "application/json", "user-agent": "source-airbyte-api"}

        response = requests.get(f"{BASE_URL}/workspaces", headers=headers)
        if response.status_code != 200:
            return False, response.text
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config.get("apiKey"))
        workspaces = Workspaces(authenticator=auth)
        sources = Sources(parent=workspaces, authenticator=auth)
        connections = Connections(parent=workspaces, authenticator=auth)
        jobs = Jobs(parent=connections, authenticator=auth)

        return [workspaces, sources, connections, jobs]
