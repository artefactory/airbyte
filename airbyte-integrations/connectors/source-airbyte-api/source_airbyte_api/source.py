#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources.streams.http.http import HttpStream

import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urlparse, parse_qsl

from urllib.parse import urljoin
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream , HttpSubStream
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

class AirbyteApiSubStream(AirbyteApiStream,HttpSubStream):
    
    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        base_dict =  super().request_params(stream_state, stream_slice, next_page_token)
        
        base_dict[self.parent.primary_key]=stream_slice["parent"][self.parent.primary_key]
        return base_dict

class Workspaces(AirbyteApiStream):
    primary_key = "workspaceId"

    def path(self, **kwargs) -> str:
        return "workspaces"

class Sources(AirbyteApiSubStream):
    primary_key = "sourceId"

    def path(self, **kwargs)-> str:
        return "sources"

        
class Connections(AirbyteApiSubStream):
    primary_key = "connectionId"

    def path(self, **kwargs)-> str:
        return "connections"


class Jobs(AirbyteApiSubStream):
    primary_key = "jobId"

    def path(self, **kwargs)->str:
        return "jobs"
    
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
        auth = TokenAuthenticator(token=config.get("apiKey")) 
        workspaces = Workspaces(authenticator=auth)
        sources = Sources(parent = workspaces, authenticator=auth)
        connections = Connections(parent = workspaces, authenticator=auth)
        jobs = Jobs(parent=connections, authenticator=auth)

        return [workspaces, sources,connections,jobs]