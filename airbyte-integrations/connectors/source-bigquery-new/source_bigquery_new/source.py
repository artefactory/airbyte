#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from datetime import datetime
from time import gmtime, strftime
from oauth2client.service_account import ServiceAccountCredentials
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_protocol.models import SyncMode
from airbyte_cdk.models import AirbyteCatalog, AirbyteMessage, AirbyteStateMessage, ConfiguredAirbyteCatalog
from airbyte_cdk.logger import AirbyteLogger

from .schema_helpers import SchemaHelpers, SchemaTypes
from .auth import BigqueryAuth
"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""
URL_BASE: str = "https://bigquery.googleapis.com"


# Basic full refresh stream
class BigqueryDatasets(HttpStream, ABC):
    """
    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class BigqueryDatasets(HttpStream, ABC)` which is the current class
    `class Customers(BigqueryDatasets)` contains behavior to pull data for customers using v1/customers
    `class Employees(BigqueryDatasets)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalBigqueryDatasets((BigqueryDatasets), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """ 
    # sourceOperations = new BigQuerySourceOperations()

    url_base = URL_BASE
    name = "datasets"
    primary_key = "id"
    raise_on_http_errors = True

    def __init__(self, project_id: list, **kwargs):
        super().__init__(**kwargs)
        self.project_id = project_id

    def path(self, **kwargs) -> str:
        """
        Documentation: https://cloud.google.com/bigquery/docs/reference/rest#rest-resource:-v2.datasets
        """
        # print(kwargs)
        return f"/bigquery/v2/projects/{self.project_id}/datasets"
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        # next_page = response.json().get("offset")
        # if next_page:
        #     return next_page
        return None

    # def request_params(
    #     self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    # ) -> MutableMapping[str, Any]:
    #     """
    #     TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
    #     Usually contains common params e.g. pagination size etc.
    #     """
    #     return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        records = response.json().get(self.name)
        for dataset in records:
            yield dataset


class BigqueryTables(BigqueryDatasets):
    name = "tables"

    def __init__(self, dataset_id: list, project_id: list, **kwargs):
        super().__init__(project_id=project_id, **kwargs)
        self.dataset_id = dataset_id
        self.project_id = project_id

    def path(self, **kwargs) -> str:
        """
        Documentation: https://cloud.google.com/bigquery/docs/reference/rest#rest-resource:-v2.tables
        """
        return f"{super().path()}/{self.dataset_id}/tables"


class BigqueryTable(BigqueryTables):
    name = "table"

    def __init__(self, dataset_id: list, project_id: list, table_id: list, **kwargs):
        super().__init__(dataset_id=dataset_id, project_id=project_id, **kwargs)
        self.table_id = table_id

    def path(self, **kwargs) -> str:
        """
        Documentation: https://cloud.google.com/bigquery/docs/reference/rest#rest-resource:-v2.tables
                       https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/get
        """
        return f"{super().path()}/{self.table_id}"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        record = response.json()
        yield record


class BigqueryStream(HttpStream, ABC):
    """
    """ 
    url_base = URL_BASE
    primary_key = "id"
    raise_on_http_errors = True

    def __init__(self, stream_path: str, stream_name: str, stream_schema, table_name: str, **kwargs):
        super().__init__(**kwargs)
        self.stream_path = stream_path
        self.stream_name = stream_name
        self.stream_schema = stream_schema
        self.table_name = table_name

    @property
    def name(self):
        return self.stream_name

    # def should_retry(self, response: requests.Response) -> bool:
    #     if response.status_code == 403 or response.status_code == 422:
    #         self.logger.error(f"Stream {self.name}: permission denied or entity is unprocessable. Skipping.")
    #         setattr(self, "raise_on_http_errors", False)
    #         return False
    #     return super().should_retry(response)

    # def backoff_time(self, response: requests.Response) -> Optional[float]:
    #     """
    #     Based on official docs: https://airtable.com/developers/web/api/rate-limits
    #     when 429 is received, we should wait at least 30 sec.
    #     """
    #     if response.status_code == 429:
    #         self.logger.error(f"Stream {self.name}: rate limit exceeded")
    #         return 30.0
    #     return None

    def get_json_schema(self) -> Mapping[str, Any]:
        return self.stream_schema

    def next_page_token(self, response: requests.Response, **kwargs) -> Optional[Mapping[str, Any]]:
        # import ipdb
        # ipdb.set_trace()
        next_page = response.json().get("offset")
        if next_page:
            return next_page
        return None

    # def request_params(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
    #     """
    #     All available params: https://airtable.com/developers/web/api/list-records#query
    #     """
    #     params = {}
    #     if next_page_token:
    #         params["offset"] = next_page_token
    #     return params

    def process_records(self, records) -> Iterable[Mapping[str, Any]]:
        # import ipdb
        # ipdb.set_trace()
        for record in records:
            data = record.get("schema", {})["fields"]
            if len(data) > 0:
                yield {
                    "_bigquery_table_id": record.get("tableReference")["tableId"],
                    "_bigquery_created_time": record.get("creationTime"),
                    "_airbyte_raw_id": record.get("id"),
                    "_airbyte_extracted_at": strftime("%Y-%m-%d %H:%M:%S", datetime.now()),
                    **{k: v for k, v in data.items()},
                }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        # records = response.json().get("records", [])
        # for dataset in records:
        #     yield dataset
        records = response.json().get("records", [])
        yield from self.process_records(records)
        # yield from self.process_records(records)

    def path(self, **kwargs) -> str:
        return self.stream_path


# Basic incremental stream
class IncrementalBigqueryDatasets(BigqueryDatasets, ABC):
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


# Source
class SourceBigqueryNew(AbstractSource):
    LOGGER = logging.getLogger("airbyte")
    QUOTE = "`"
    CONFIG_DATASET_ID = "dataset_id"
    CONFIG_PROJECT_ID = "project_id"
    CONFIG_CREDS = "credentials_json"
    _dbConfig = {}
    _auth: BigqueryAuth = None
    streams_catalog: Iterable[Mapping[str, Any]] = []

    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        """
        Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        auth = BigqueryAuth(config)

        try:
            # try reading first table from each base, to check the connectivity,
            for dataset in BigqueryDatasets(project_id=config["project_id"], authenticator=auth).read_records(sync_mode=SyncMode.full_refresh):
                print(dataset)
                dataset_id = dataset.get("datasetReference")["datasetId"]
                dataset_name = dataset.get("name")
                # print(dataset_id)
                # print(dataset_name)
                # self.logger.info(f"Reading first table info for base: {dataset_name}")
                next(BigqueryTables(dataset_id=dataset_id, project_id=config["project_id"], authenticator=auth).read_records(sync_mode=SyncMode.full_refresh))
            return True, None
        except Exception as e:
            return False, str(e)

    def discover(self, logger: AirbyteLogger, config) -> AirbyteCatalog:
        """
        Override to provide the dynamic schema generation capabilities,
        using resource available for authenticated user.

        Retrieve: Bases, Tables from each Base, generate JSON Schema for each table.
        """
        auth = self._auth or BigqueryAuth(config)
        # list all bases available for authenticated account
        for dataset in BigqueryDatasets(project_id=config["project_id"], authenticator=auth).read_records(sync_mode=SyncMode.full_refresh):
            dataset_id = dataset.get("datasetReference")["datasetId"]
            # dataset_name = SchemaHelpers.clean_name(dataset.get("name"))
            # list and process each table under each base to generate the JSON Schema
            # print(BigqueryTables(dataset_id=dataset_id, project_id=config["project_id"], authenticator=auth).read_records(sync_mode=SyncMode.full_refresh))
            for table_info in BigqueryTables(dataset_id=dataset_id, project_id=config["project_id"], authenticator=auth).read_records(sync_mode=SyncMode.full_refresh):
                table_id = table_info.get("tableReference")["tableId"]
                # table = BigqueryTable(dataset_id=dataset_id, project_id=config["project_id"], table_id=table_id, authenticator=auth).read_records(sync_mode=SyncMode.full_refresh)
                # print(BigqueryTable(dataset_id=dataset_id, project_id=config["project_id"], table_id=table_id, authenticator=auth).read_records(sync_mode=SyncMode.full_refresh))
                table_obj = BigqueryTable(dataset_id=dataset_id, project_id=config["project_id"], table_id=table_id, authenticator=auth)
                for table in table_obj.read_records(sync_mode=SyncMode.full_refresh):
                    self.streams_catalog.append(
                        {
                            "stream_path": f"{table_obj.path()}",
                            "stream": SchemaHelpers.get_airbyte_stream(
                                f"{dataset_id}/{table_id}",
                                SchemaHelpers.get_json_schema(table),
                            ),
                            "table_name": table_id,
                        }
                    )
        return AirbyteCatalog(streams=[stream["stream"] for stream in self.streams_catalog])
    
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # auth = TokenAuthenticator(token="api_key")  # Oauth2Authenticator is also available if you need oauth support
        auth = BigqueryAuth(config)
        # self._auth = AirtableAuth(config)
        if not self.streams_catalog:
            self.discover(None, config)
        for stream in self.streams_catalog:
            yield BigqueryStream(
                stream_path=stream["stream_path"],
                stream_name=stream["stream"].name,
                stream_schema=stream["stream"].json_schema,
                table_name=stream["table_name"],
                authenticator=auth,
            )