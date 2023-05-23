#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urlparse, parse_qsl

from urllib.parse import urljoin
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator


BASE_URL = "https://api.airbyte.com/v1/"

# Basic full refresh stream
class AirbyteApiStream(HttpStream, ABC):
   
    url_base = BASE_URL


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if next  := response.json().get("next"):
            return {"next": next}
        return {}
        

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        if next_page_token is not None and (next := next_page_token.get("next",[])):
            parsed_url = urlparse(next)
            captured_params = dict(parse_qsl(parsed_url.query))
            return captured_params

        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for entry in response.json().get("data",[]):
            yield entry
    
    def request_headers(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Mapping[str, Any]:
        return {"user-agent": "source-airbyte-api"}


# Basic incremental stream
class IncrementalAirbyteApiStream(AirbyteApiStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


class Employees(IncrementalAirbyteApiStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the cursor_field. Required.
    cursor_field = "start_date"

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "employee_id"

    def path(self, **kwargs) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
        return "single". Required.
        """
        return "employees"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """
        TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

        Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
        This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
        section of the docs for more information.

        The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
        necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
        This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

        An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
        craft that specific request.

        For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
        this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
        till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
        the date query param.
        """
        raise NotImplementedError("Implement stream slices or delete this method!")

class workspaces(AirbyteApiStream):
    primary_key = "workspaceId"

    def path(self, **kwargs) -> str:
        return "workspaces"

# Source
class SourceAirbyteApi(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        headers = {
            "authorization": f"Bearer {config.get('apiKey')}",
            "accept": "application/json",
            "user-agent": "source-airbyte-api"
            }

        response = requests.get(f"{BASE_URL}/workspaces",headers=headers)
        if response.status_code != 200:
            return False, response.text
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config.get("apiKey"))  # Oauth2Authenticator is also available if you need oauth support
        return [workspaces(authenticator=auth)]