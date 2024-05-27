#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import logging
import sys
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
import pytz
from datetime import datetime, timedelta
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.core import Stream, StreamData
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_protocol.models import SyncMode, Type
from airbyte_cdk.models import AirbyteCatalog, AirbyteMessage, AirbyteStateMessage, ConfiguredAirbyteCatalog, AirbyteStateType, AirbyteStreamState, StreamDescriptor, AirbyteStateBlob
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.utils.traced_exception import AirbyteTracedException, FailureType
from airbyte_cdk.sources.streams.concurrent.cursor import Cursor
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
        next_page = response.json().get("pageToken")
        if next_page:
            return next_page
        return None

    def process_records(self, record) -> Iterable[Mapping[str, Any]]:
        fields = record.get("schema")["fields"]
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
                **{CHANGE_FIELDS.get(element["name"], element["name"]): SchemaHelpers.format_field(rows[fields.index(element)]["v"], element["type"]) for element in fields},
            }

    def checkpoint(self, stream_name, stream_state, stream_namespace):
        """
        Checkpoint state.
        """
        state = AirbyteMessage(
            type=Type.STATE,
            state=AirbyteStateMessage(
                type= AirbyteStateType.STREAM,
                stream=AirbyteStreamState(
                    stream_descriptor=StreamDescriptor(name=stream_name, namespace=stream_namespace),
                    stream_state=AirbyteStateBlob.parse_obj(stream_state),
                    )
            ),
        )
        self.logger.info(f"Checkpoint state of {self.name} is {stream_state}")
        print(state.json(exclude_unset=True))  # Emit state


class TableQueryRecord(BigqueryResultStream):
    """  
    """ 
    name = "query_record"

    def __init__(self, project_id: list, parent_stream: str, order: str, column: str, **kwargs):
        self.project_id = project_id
        self.parent_stream = parent_stream
        self.order = order
        self.column = column
        self.data = None
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
        query_string = f"select * from APPENDS(TABLE `{self.parent_stream}`,NULL,NULL) ORDER BY _CHANGE_TIMESTAMP {self.order} LIMIT 1"
        # query_string = f"select * from `{self.parent_stream}` ORDER BY {self.column} {self.order} LIMIT 1"
        request_body = {
            "kind": "bigquery#queryRequest",
            "query": query_string,
            "useLegacySql": False
            }
        
        return request_body

    def process_records(self, record) -> Iterable[Mapping[str, Any]]:
        fields = record.get("schema")["fields"]
        try:
            row = record["rows"][0]
            data = row.get("f")
        except KeyError:
            yield {}
        formated_data =  {
                "_bigquery_table_id": record.get("jobReference")["jobId"],
                "_bigquery_created_time": None, #TODO: Update this to row insertion time
                **{CHANGE_FIELDS.get(element["name"], element["name"]): SchemaHelpers.format_field(data[fields.index(element)]["v"], element["type"]) for element in fields},
            }
        self.data = formated_data
        yield formated_data


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
        where_clause = self.where_clause.replace("\"", "'")
        query_string = f"select * from `{self.parent_stream}` where {where_clause}"
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
        self.request_body = stream_request
        self._cursor = None
        self._checkpoint_time = datetime.now()
        
    @property
    def name(self):
        return self.stream_name
    
    @property
    def state(self):
        if not self._cursor:
            return {}
        return {
            self.cursor_field: self._cursor,
        }
    
    @state.setter
    def state(self, value):
        self.cursor_field = list(value.keys())[0]
        self._cursor = value[self.cursor_field]
    
    @property
    def source_defined_cursor(self) -> bool:
        return False

    @property
    def supports_incremental(self) -> bool:
        return True
    
    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        latest_record_state = latest_record[self.cursor_field]
        stream_state = current_stream_state.get(self.cursor_field)

        if stream_state:
            self._cursor = max(latest_record_state, stream_state)
        else:
            self._cursor = latest_record_state
        self.state = {self.cursor_field: self._cursor} 
        # self.checkpoint(self.name, self.state, self.namespace)
        if datetime.now() >= self._checkpoint_time + timedelta(minutes=15):
            self.checkpoint(self.name, self.state, self.namespace)
            self._checkpoint_time = datetime.now()

        return self.state

    def stream_slices(self, stream_state: Mapping[str, Any] = None, cursor_field=None, sync_mode=None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        if sync_mode == SyncMode.incremental:
            if isinstance(cursor_field,list) and cursor_field:
                self.cursor_field = cursor_field[0]
            elif cursor_field:
                self.cursor_field = cursor_field

            if stream_state:
                self._cursor = stream_state.get(self.cursor_field) #or self._state.get(self.cursor_field)

            yield {
                    self.cursor_field : self._cursor
                } 
        else:
            yield
    
    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        query_string = f"select * from `{self.name}`"
        if stream_slice:
            self._cursor = stream_slice.get(self.cursor_field, None)
            if self._cursor:
                if isinstance(self._cursor, str):
                    self._cursor = f"'{self._cursor}'"
                query_string = f"select * from `{self.name}` where {self.cursor_field}>={self._cursor}" #TODO: maybe add order by cursor_field
    
        request_body = {
            "kind": "bigquery#queryRequest",
            "query": query_string,
            "useLegacySql": False,
            "timeoutMs": 30000,
            "maxResults": 100000
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


class IncrementalQueryResult(BigqueryIncrementalStream):
    """  
    """ 
    primary_key = None
    
    def __init__(self, project_id: list, dataset_id: str, table_id: str, **kwargs):
        self.project_id = project_id
        self.parent_stream = dataset_id + "." + table_id
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
    
    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        query_string = f"select * from `{self.stream_name}`"
        if stream_slice:
            self._cursor = stream_slice.get(self.cursor_field, None)
            if self._cursor:
                if isinstance(self._cursor, str):
                    self._cursor = f"'{self._cursor}'"
                query_string = f"select * from `{self.stream_name}` where {self.cursor_field}>={self._cursor}" #TODO: add order by cursor_field
    
        request_body = {
            "kind": "bigquery#queryRequest",
            "query": query_string,
            "useLegacySql": False,
            "dryRun": True
            }
        return request_body
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        record = response.json()
        yield record


class BigqueryCDCStream(BigqueryResultStream, IncrementalMixin):
    """  
    """ 
    primary_key = None
    state_checkpoint_interval = None

    def __init__(self, stream_path, stream_name, stream_schema, stream_request=None, fallback_start=None,**kwargs):
        super().__init__(stream_path, stream_name, stream_schema, stream_request, **kwargs)
        self.request_body = stream_request
        self._cursor = None
        self._checkpoint_time = datetime.now()
        self.fallback_start = fallback_start
        self.end_date = None
        self.first_record = TableQueryRecord("sb-airbyte-connector-1ee6", stream_name, "ASC", self.cursor_field, **kwargs)
        self.last_record = TableQueryRecord("sb-airbyte-connector-1ee6", stream_name, "DESC", self.cursor_field, **kwargs)
        # self._stream_slicer_cursor = None
    
    @property
    def name(self):
        return self.stream_name
    
    @property
    def cursor_field(self) -> str:
        """
        Name of the field in the API response body used as cursor.
        """
        return "change_timestamp"
    
    @property
    def state(self):
        return {
            self.cursor_field: self._cursor,
        }
    
    @state.setter
    def state(self, value):
        self._cursor = value[self.cursor_field]
    
    @property
    def source_defined_cursor(self) -> bool:
        return True

    @property
    def supports_incremental(self) -> bool:
        return True

    def _updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        latest_record_state = latest_record[self.cursor_field]
        stream_state = current_stream_state.get(self.cursor_field)
        if stream_state:
            max_date = max(datetime.strptime(latest_record_state, '%Y-%m-%dT%H:%M:%S.%f%z'), datetime.strptime(stream_state, '%Y-%m-%dT%H:%M:%S.%f%z'))
            self._cursor = max_date.isoformat(timespec='microseconds')
        else:
            self._cursor = latest_record_state
        self.state = {self.cursor_field: self._cursor} 
        self.logger.info(f"current state of {self.name} is {self.state}")
        if datetime.now() >= self._checkpoint_time + timedelta(minutes=15):
            self.checkpoint(self.name, self.state, self.namespace)
            self._checkpoint_time = datetime.now()
        return self.state

    def _chunk_dates(self, start_date, end_date, table_start) -> Iterable[Tuple[datetime, datetime]]:
        slice_range = 1
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S.%f%z')
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S.%f%z')
        if isinstance(table_start, str):
            table_start = datetime.strptime(table_start, '%Y-%m-%dT%H:%M:%S.%f%z')
        if start_date > end_date:
            #TODO: maybe raise exception
            return
        step = timedelta(minutes=slice_range)
        new_start_date = start_date
        if start_date == end_date:
            yield start_date, start_date + step
            return
        while new_start_date < end_date+step:
            before_date = min(end_date+step, new_start_date + step)
            if before_date < table_start:
                before_date = table_start
            yield new_start_date, before_date
            new_start_date = before_date
            
    def stream_slices(self, stream_state: Mapping[str, Any] = None, cursor_field=None, sync_mode=None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        start_time = next(self.first_record.read_records(sync_mode=SyncMode.full_refresh)).get(self.cursor_field, None)
        end_time = next(self.last_record.read_records(sync_mode=SyncMode.full_refresh)).get(self.cursor_field, None)
        default_start = self.fallback_start
        if stream_state:
            self._cursor = stream_state.get(self.cursor_field) #or self._state.get(self.cursor_field)
        if self._cursor:
            default_start = self._cursor
        if start_time and end_time:
            for start, end in self._chunk_dates(default_start, end_time, start_time):
                yield {
                    "start" : start.isoformat(timespec='microseconds'), "end": end.isoformat(timespec='microseconds')
                } 
        else:
            yield {
                    "start" : None, "end": None
                } 

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        query_string = f"select * from APPENDS(TABLE `{self.stream_name}`,NULL,NULL)"
        if stream_slice:
            start = stream_slice.get("start", None)
            end = stream_slice.get("end", None)
            if start and end:
                query_string = f"select * from APPENDS(TABLE `{self.stream_name}`,'{start}','{end}')"
        request_body = {
            "kind": "bigquery#queryRequest",
            "query": query_string,
            "useLegacySql": False,
            "timeoutMs": 30000,
            "maxResults": 100000
            }
        return request_body
    
    def process_records(self, record) -> Iterable[Mapping[str, Any]]:
        fields = record.get("schema")["fields"]
        stream_data = record.get("rows", [])
        formated_rows = []
        for data in stream_data:
            rows = data.get("f")
            row = {
                "_bigquery_table_id": record.get("jobReference")["jobId"],
                "_bigquery_created_time": None, #TODO: Update this to row insertion time
                **{CHANGE_FIELDS.get(element["name"], element["name"]): SchemaHelpers.format_field(rows[fields.index(element)]["v"], element["type"]) for element in fields},
            }
            formated_rows.append(row)
            self._updated_state(self.state, row)
            yield row


class TableChangeHistory(BigqueryCDCStream):
    """  
    """ 
    primary_key = None
    
    def __init__(self, project_id: list, dataset_id: str, table_id: str, **kwargs):
        self.project_id = project_id
        self.parent_stream = dataset_id + "." + table_id
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

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        query_string = f"select * from APPENDS(TABLE `{self.name}`,NULL,NULL)"
        if stream_slice:
            self._cursor = stream_slice.get(self.cursor_field, None)
            if self._cursor:
                query_string = f"select * from APPENDS(TABLE `{self.name}`,'{self._cursor}',NULL)"

        request_body = {
            "kind": "bigquery#queryRequest",
            "query": query_string,
            "useLegacySql": False,
            "dryRun": True
            }
        return request_body

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        record = response.json()
        yield record
    