#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import logging
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from datetime import datetime
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_protocol.models import SyncMode, Type
from airbyte_cdk.models import AirbyteCatalog, AirbyteMessage, AirbyteStateMessage, ConfiguredAirbyteCatalog
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.utils.traced_exception import AirbyteTracedException, FailureType
from .schema_helpers import SchemaHelpers

"""
This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""
URL_BASE: str = "https://bigquery.googleapis.com"

CHANGE_FIELDS = {"_CHANGE_TIMESTAMP": "change_timestamp", "_CHANGE_TYPE": "change_type"}

class BigqueryStream(HttpStream, ABC):
    """
    """ 
    url_base = URL_BASE
    primary_key = None
    raise_on_http_errors = True

    def __init__(self, stream_path: str, stream_name: str, stream_schema, stream_data=None, **kwargs):
        super().__init__(**kwargs)
        self.stream_path = stream_path
        self.stream_name = stream_name
        self.stream_schema = stream_schema
        self.stream_data = stream_data

    
    @property
    def name(self):
        return self.stream_name

    def get_json_schema(self) -> Mapping[str, Any]:
        return self.stream_schema

    def next_page_token(self, response: requests.Response, **kwargs) -> Optional[Mapping[str, Any]]:
        # TODO: check if correct
        next_page = response.json().get("offset")
        if next_page:
            return next_page
        return None

    def process_records(self, record) -> Iterable[Mapping[str, Any]]:
        fields = record.get("schema", {})["fields"]
        stream_data = self.stream_data.read_records(sync_mode=SyncMode.full_refresh)

        for data in stream_data:
            rows = data.get("f")
            yield {
                "_bigquery_table_id": record.get("tableReference")["tableId"],
                "_bigquery_created_time": record.get("creationTime"),
                **{element["name"]: SchemaHelpers.format_field(rows[fields.index(element)]["v"], element["type"]) for element in fields},
            }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        records = response.json()
        yield from self.process_records(records)

    def path(self, **kwargs) -> str:
        return self.stream_path


class BigqueryDatasets(BigqueryStream):
    """
    """
    name = "datasets"

    def __init__(self, project_id: list, **kwargs):
        self.project_id = project_id
        super().__init__(self.path(), self.name, self.get_json_schema(), **kwargs)

    def path(self, **kwargs) -> str:
        """
        Documentation: https://cloud.google.com/bigquery/docs/reference/rest#rest-resource:-v2.datasets
        """
        return f"/bigquery/v2/projects/{self.project_id}/datasets"
    
    def get_json_schema(self) -> Mapping[str, Any]:
        return {}
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        try:
            datasets = response.json().get(self.name)
            for dataset in datasets:
                yield dataset
        except TypeError as e:
            raise AirbyteTracedException(
                            internal_message=str(e),
                            failure_type=FailureType.config_error,
                            message="Provided crendentials do not give access to any datasets or project has no datasets",
                        )


class BigqueryTables(BigqueryDatasets):
    name = "tables"

    def __init__(self, dataset_id: list, project_id: list, **kwargs):
        self.dataset_id = dataset_id
        self.project_id = project_id
        super().__init__(project_id=project_id, **kwargs)

    def path(self, **kwargs) -> str:
        """
        Documentation: https://cloud.google.com/bigquery/docs/reference/rest#rest-resource:-v2.tables
        """
        return f"{super().path()}/{self.dataset_id}/tables"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        try:
            tables = response.json().get(self.name)
            for table in tables:
                yield table
        except TypeError as e:
            self.logger.warning(f"Dataset has no tables causing the error {str(e)}")


class BigqueryTable(BigqueryTables):
    name = "table"

    def __init__(self, dataset_id: list, project_id: list, table_id: list, **kwargs):
        self.table_id = table_id
        super().__init__(dataset_id=dataset_id, project_id=project_id, **kwargs)

    def path(self, **kwargs) -> str:
        """
        Documentation: https://cloud.google.com/bigquery/docs/reference/rest#rest-resource:-v2.tables
                       https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/get
        """
        return f"{super().path()}/{self.table_id}"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """

        :return an iterable containing each record in the response
        """
        record = response.json()
        yield record


class BigqueryTableData(BigqueryTable):
    name = "table_data"

    def __init__(self, dataset_id: list, project_id: list, table_id: list, **kwargs):
        super().__init__(dataset_id=dataset_id, project_id=project_id, table_id=table_id, **kwargs)

    def path(self, **kwargs) -> str:
        """
        Documentation: https://cloud.google.com/bigquery/docs/reference/rest#rest-resource:-v2.tabledata
        """
        return f"{super().path()}/data"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        records = response.json().get("rows", [])
        for record in records:
            yield record

class BigqueryResultStream(BigqueryStream):
    """
    """ 
    def __init__(self, stream_path: str, stream_name: str, stream_schema, stream_request=None, stream_data=None, **kwargs):
        self.request_body = stream_request
        super().__init__(stream_path, stream_name, stream_schema, stream_data, **kwargs)

    @property
    def http_method(self) -> str:
        return "POST"

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        return self.request_body
    
    def process_records(self, record) -> Iterable[Mapping[str, Any]]:
        fields = record.get("schema")["fields"]
        stream_data = record.get("rows", [])

        for data in stream_data:
            rows = data.get("f")
            yield {
                "_bigquery_table_id": record.get("jobReference")["jobId"],
                "_bigquery_created_time": None, #TODO: Update this to row insertion time
                **{element["name"]: SchemaHelpers.format_field(rows[fields.index(element)]["v"], element["type"]) for element in fields},
            }


class TableQueryResult(BigqueryResultStream):
    """  
    """ 
    name = "query_results"

    def __init__(self, project_id: list, parent_stream: str, where_clause: str, **kwargs):
        self.project_id = project_id
        self.parent_stream = parent_stream
        self.where_clause = where_clause
        super().__init__(self.path(), self.name, self.get_json_schema(), **kwargs)
    
    def path(self, **kwargs) -> str:
        """
        Documentation: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
        """
        return f"/bigquery/v2/projects/{self.project_id}/queries"

    def get_json_schema(self) -> Mapping[str, Any]:
        return {}
    
    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        query_string = f"select * from {self.parent_stream} where {self.where_clause}"
        request_body = {
            "kind": "bigquery#queryRequest",
            "query": query_string,
            "useLegacySql": False
            }
        
        return request_body

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        record = response.json()
        yield record
    

class BigqueryIncrementalStream(BigqueryResultStream, IncrementalMixin):
    """
    """ 
    # _state = {}
    cursor_field = "_bigquery_created_time"
    primary_key = None
    state_checkpoint_interval = None
    
    def __init__(self, stream_path, stream_name, stream_schema, stream_request=None, **kwargs):
        super().__init__(stream_path, stream_name, stream_schema, stream_request, **kwargs)
        # self.config = config
        self._cursor = None
        # self._state = {self.cursor_field: "2024-04-26T10:51:26.154000"}
        
    @property
    def name(self):
        return self.stream_name
    
    
    @property
    def state(self):
        return {
            self.cursor_field: self._cursor,
        }
    # @property
    # def state(self):
    #     return self._state
    
    @state.setter
    def state(self, value):
        self.cursor_field = list(value.keys())[0]
        self._cursor = value[self.cursor_field]
        # self._state[self.cursor_field] = value[self.cursor_field]
    
    @property
    def source_defined_cursor(self) -> bool:
        return False

    @property
    def supports_incremental(self) -> bool:
        return True
    
    # @property
    # def sync_mode(self):
    #     return SyncMode.incremental

    # @property
    # def supported_sync_modes(self):
    #     return [SyncMode.incremental]
    
    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        latest_record_state = latest_record[self.cursor_field]
        stream_state = current_stream_state.get(self.cursor_field)
        if stream_state:
            self._cursor = max(latest_record_state, stream_state)
            self.state = {self.cursor_field: self._cursor} 
            return {self.cursor_field: self._cursor}
        self._cursor = latest_record_state
        self.state = {self.cursor_field: self._cursor} 
        return {self.cursor_field: self._cursor}

    def stream_slices(self, stream_state: Mapping[str, Any] = None, cursor_field=None, sync_mode=None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        if sync_mode != SyncMode.incremental:#
            #super().stream_slices(sync_mode=sync_mode, cursor_field=cursor_field, stream_state=stream_state)
            yield {
                self.cursor_field : None
            } 
        
        if isinstance(cursor_field,list) and cursor_field:
            self.cursor_field = cursor_field[0]
        elif cursor_field:
            self.cursor_field = cursor_field

        cursor_value = None
        if stream_state:
            cursor_value = stream_state.get(self.cursor_field) #or self._state.get(self.cursor_field)

        yield {
                self.cursor_field : cursor_value
            }

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        query_string = f"select * from {self.stream_name}"
        if stream_slice:
            cursor_value = stream_slice.get(self.cursor_field, None)
            if cursor_value:
                query_string = f"select * from {self.stream_name} where {self.cursor_field}>={cursor_value}"
        # try:
        #     cursor_value = stream_state.get(self.cursor_field, None)
        #     if cursor_value:
        #         # query_string = f"select * from APPENDS(TABLE {self.stream_name},'{cursor_value}',NULL)"
        #         query_string = f"select * from {self.stream_name} where {self.cursor_field}>={cursor_value}"
        # except Exception as e:
        #     print(str(e))
    
        request_body = {
            "kind": "bigquery#queryRequest",
            "query": query_string,
            "useLegacySql": False
            }
        return request_body
    
    def process_records(self, record) -> Iterable[Mapping[str, Any]]:
        fields = record.get("schema")["fields"]
        stream_data = record.get("rows", [])
        for data in stream_data:
            rows = data.get("f")
            yield {
                "_bigquery_table_id": record.get("jobReference")["jobId"],
                "_bigquery_created_time": None, #TODO: Update this to row insertion time
                **{CHANGE_FIELDS.get(element["name"], element["name"]): SchemaHelpers.format_field(rows[fields.index(element)]["v"], element["type"]) for element in fields},
            }


class TableAppendsResult(BigqueryIncrementalStream):
    """  
    """ 
    # name = "appends_results"
    # cursor_field = "_CHANGE_TIMESTAMP"
    primary_key = None
    state_checkpoint_interval = None
    
    def __init__(self, project_id: list, dataset_id: str, table_id: str, **kwargs):
        self.project_id = project_id
        self.parent_stream = dataset_id + "." + table_id
        # self.start_date = start_date
        super().__init__(self.path(), self.parent_stream, self.get_json_schema(), **kwargs)
    
    @property
    def name(self):
        return self.parent_stream
    
    def path(self, **kwargs) -> str:
        """
        Documentation: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
        """
        return f"/bigquery/v2/projects/{self.project_id}/queries"

    def get_json_schema(self) -> Mapping[str, Any]:
        return {}
    
    # def request_body_json(
    #     self,
    #     stream_state: Optional[Mapping[str, Any]],
    #     stream_slice: Optional[Mapping[str, Any]] = None,
    #     next_page_token: Optional[Mapping[str, Any]] = None,
    # ) -> Optional[Mapping[str, Any]]:
    #     query_string = f"select * from APPENDS(TABLE {self.parent_stream},NULL,NULL)"
    #     request_body = {
    #         "kind": "bigquery#queryRequest",
    #         "query": query_string,
    #         "useLegacySql": False
    #         }
        
    #     return request_body
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        record = response.json()
        yield record

    # def process_records(self, record) -> Iterable[Mapping[str, Any]]:
    #     fields = record.get("schema")["fields"]
    #     stream_data = record.get("rows", [])
    #     for data in stream_data:
    #         rows = data.get("f")
    #         yield {
    #             "_bigquery_table_id": record.get("jobReference")["jobId"],
    #             "_bigquery_created_time": None, #TODO: Update this to row insertion time
    #             **{CHANGE_FIELDS.get(element["name"], element["name"]): SchemaHelpers.format_field(rows[fields.index(element)]["v"], element["type"]) for element in fields},
    #         }

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
