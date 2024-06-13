#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import logging
import sys
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
import pytz
from dateutil import parser
from datetime import datetime, timedelta
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.core import Stream, StreamData
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_protocol.models import SyncMode, Type
from airbyte_cdk.models import AirbyteCatalog, AirbyteMessage, AirbyteStateMessage, ConfiguredAirbyteCatalog, AirbyteStateType, AirbyteStreamState, StreamDescriptor, AirbyteStateBlob
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
PAGE_SIZE = 10000
TIMEOUT = 30000 # 30 seconds
SLICE_RANGE = 525600 # 1 year in minutes

class BigqueryStream(HttpStream, ABC):
    """
    """
    url_base = URL_BASE
    primary_key = None
    raise_on_http_errors = True
    page_size = PAGE_SIZE

    def __init__(self, stream_path: str, stream_name: str, stream_schema, stream_data=None, **kwargs):
        super().__init__(**kwargs)
        self._auth = kwargs["authenticator"]
        self.stream_path = stream_path
        self.stream_name = stream_name
        self.stream_schema = stream_schema
        self.stream_data = stream_data

    @property
    def name(self):
        return self.stream_name

    def get_json_schema(self) -> Mapping[str, Any]:
        return self.stream_schema()

    def next_page_token(self, response: requests.Response, **kwargs) -> Optional[Mapping[str, Any]]:
        return None

    def process_records(self, record) -> Iterable[Mapping[str, Any]]:
        fields = record.get("schema")["fields"]
        rows = record.get("rows", [])
        for row in rows:
            data = row.get("f")
            yield {
                "_bigquery_table_id": record.get("tableReference")["tableId"],
                "_bigquery_created_time": record.get("creationTime"),
                **{CHANGE_FIELDS.get(element["name"], element["name"]): SchemaHelpers.format_field(data[fields.index(element)]["v"], element["type"]) for element in fields},
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

    def next_page_token(self, response: requests.Response, **kwargs) -> Optional[Mapping[str, Any]]:
        record = response.json()
        next_page = record.get("nextPageToken", None)
        return next_page
    
    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        """
        Override this method to define the query parameters that should be set on an outgoing HTTP request given the inputs.
        """
        params = {
            "maxResults": self.page_size,
        }
        if next_page_token:
            params["pageToken"] = next_page_token
        return params
    
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
                            message="Provided credentials do not give access to any datasets or project has no datasets",
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

    def next_page_token(self, response: requests.Response, **kwargs) -> Optional[Mapping[str, Any]]:
        record = response.json()
        next_page = record.get("nextPageToken", None)
        return next_page
    
    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        """
        Override this method to define the query parameters that should be set on an outgoing HTTP request given the inputs.
        """
        params = {
            "maxResults": self.page_size,
        }
        if next_page_token:
            params["pageToken"] = next_page_token
        return params
    
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
            self.logger.warning(f"Dataset named {self.dataset_id} has no tables")


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

    def next_page_token(self, response: requests.Response, **kwargs) -> Optional[Mapping[str, Any]]:
        record = response.json()
        next_page = record.get("nextPageToken", None)
        return next_page
    
    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        """
        Override this method to define the query parameters that should be set on an outgoing HTTP request given the inputs.
        """
        params = {
            "maxResults": self.page_size,
        }
        if next_page_token:
            params["pageToken"] = next_page_token
        return params
    
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

    def next_page_token(self, response: requests.Response, **kwargs) -> Optional[Mapping[str, Any]]:
        record = response.json()
        next_page = record.get("nextPageToken", None)
        return next_page
    
    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        """
        Override this method to define the query parameters that should be set on an outgoing HTTP request given the inputs.
        """
        params = {
            "maxResults": self.page_size,
        }
        if next_page_token:
            params["pageToken"] = next_page_token
        return params
    
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
    def __init__(self, project_id, stream_path: str, stream_name: str, stream_schema, stream_request=None, stream_data=None,**kwargs):
        self.request_body = stream_request
        self.project_id = project_id
        self.retry_policy = kwargs.pop("retry_policy", None)
        super().__init__(stream_path, stream_name, stream_schema, stream_data, **kwargs)

    def path(self, **kwargs) -> str:
        """
        Documentation: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
        """
        return f"/bigquery/v2/projects/{self.project_id}/queries"
    
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

    def should_retry(self, response: requests.Response) -> bool:
        if self.retry_policy:
            return self.retry_policy(response)
        return False

    def process_records(self, record) -> Iterable[Mapping[str, Any]]:
        fields = record.get("schema")["fields"]
        rows = record.get("rows", [])
        for row in rows:
            data = row.get("f")
            yield {
                "_bigquery_table_id": record.get("jobReference")["jobId"],
                "_bigquery_created_time": None, #TODO: Update this to row insertion time
                **{CHANGE_FIELDS.get(element["name"], element["name"]): SchemaHelpers.format_field(data[fields.index(element)]["v"], element["type"]) for element in fields},
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


class CHTableQueryRecord(BigqueryResultStream):
    """
    """
    name = "ch_query_record"

    def __init__(self, project_id: list, parent_stream: str, order: str, column: str, where_clause, **kwargs):
        self.project_id = project_id
        self.parent_stream = parent_stream
        self.order = order
        self.column = column
        self.where_clause = where_clause.replace("\"", "'")
        self.data = None
        super().__init__(project_id, self.path(), self.name, self.get_json_schema(), **kwargs)

    def get_json_schema(self) -> Mapping[str, Any]:
        return {}

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        query_string = f"SELECT * FROM APPENDS(TABLE `{self.parent_stream}`,NULL,NULL) ORDER BY {self.column} {self.order} LIMIT 1"
        if self.where_clause:
            index = query_string.find("ORDER BY")
            query_string = query_string[:index] + f" WHERE {self.where_clause} " + query_string[index:]
        request_body = {
            "kind": "bigquery#queryRequest",
            "query": query_string,
            "useLegacySql": False
            }
        return request_body

    def process_records(self, record) -> Iterable[Mapping[str, Any]]:
        try:
            fields = record.get("schema")["fields"]
            row = record["rows"][0]
            data = row.get("f")
        except (KeyError, TypeError) as e:
            self.logger.warning(f"Table has no rows")
            yield {}
        else:
            self.data =  {
                "_bigquery_table_id": record.get("jobReference")["jobId"],
                "_bigquery_created_time": None, #TODO: Update this to row insertion time
                **{CHANGE_FIELDS.get(element["name"], element["name"]): SchemaHelpers.format_field(data[fields.index(element)]["v"], element["type"]) for element in fields},
            }
            yield self.data


class TableQueryRecord(CHTableQueryRecord):
    """
    """
    name = "query_record"

    def __init__(self, project_id: list, parent_stream: str, order: str, column: str, where_clause, **kwargs):
        self.project_id = project_id
        self.parent_stream = parent_stream
        self.order = order
        self.column = column
        self.where_clause = where_clause.replace("\"", "'")
        super().__init__(project_id, parent_stream, order, column, where_clause, **kwargs)

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        query_string = f"SELECT * FROM `{self.parent_stream}` ORDER BY {self.column} {self.order} LIMIT 1"
        if self.where_clause:
            index = query_string.find("ORDER BY")
            query_string = query_string[:index] + f" WHERE {self.where_clause} " + query_string[index:]
        request_body = {
            "kind": "bigquery#queryRequest",
            "query": query_string,
            "useLegacySql": False
            }
        return request_body
    

class TableQueryResult(BigqueryResultStream):
    """
    """
    name = "query_results"

    def __init__(self, project_id: list, parent_stream: str, where_clause: str, **kwargs):
        self.project_id = project_id
        self.parent_stream = parent_stream
        self.where_clause = where_clause.replace("\"", "'")
        super().__init__(project_id, self.path(), self.name, self.get_json_schema(), **kwargs)

    def get_json_schema(self) -> Mapping[str, Any]:
        return {}

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        where_clause = self.where_clause.replace("\"", "'")
        query_string = f"SELECT * FROM `{self.parent_stream}` WHERE {where_clause}"
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
    cursor_field = []
    primary_key = None
    state_checkpoint_interval = None

    def __init__(self, project_id, dataset_id, table_id, stream_path, stream_schema, fallback_start:str, \
                 given_name=None, where_clause=None, stream_request=None, slice_range=SLICE_RANGE, **kwargs):
        self.stream_name = dataset_id + "." + table_id
        super().__init__(project_id, stream_path, self.stream_name, stream_schema, stream_request, **kwargs)
        self.request_body = stream_request
        self._cursor = None
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.given_name = given_name
        self.where_clause = where_clause.replace("\"", "'")
        self.slice_range = slice_range
        self._checkpoint_time = datetime.now()
        self.fallback_start = fallback_start
        self.start_time = None
        self.end_time = None
        self._is_primary_key_set = False
        self.configured_sync_mode = None

    @property
    def namespace(self):
        return None # TODO: update to self.dataset_id
    
    @property
    def name(self):
        return self.given_name or self.stream_name

    @property
    def state(self):
        if not self.cursor_field:
            return {}
        return {
            self.cursor_field: self._cursor,
        }

    @state.setter
    def state(self, value):
        if value:
            self.cursor_field = list(value.keys())[0]
            self._cursor = value[self.cursor_field]
    
    @property
    def supported_sync_modes(self):
        return [SyncMode.incremental, SyncMode.full_refresh]
    
    @property
    def source_defined_cursor(self) -> bool:
        return False

    @property
    def supports_incremental(self) -> bool:
        return True

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        """
        :return: string if single primary key, list of strings if composite primary key,
        list of list of strings if composite primary key consisting of nested fields.
          If the stream has no primary keys, return None.
        """
        if not self._is_primary_key_set:
            self.set_primary_key()
        return self._primary_key
    
    def set_primary_key(self):
        self._primary_key = self._get_primary_key()
        self._is_primary_key_set = True

    def _get_primary_key(self):
        primary_key_stream = InformationSchemaStream(self.project_id, self.dataset_id, self.table_id, authenticator=self._auth)
        primary_key_result = []
        for record in primary_key_stream.read_records(sync_mode=SyncMode.full_refresh):
            if record["constraint_name"] == f"{self.table_id}.pk$":
                primary_key_result.append(record['column_name'])
        if not len(primary_key_result):
            return None
        elif len(primary_key_result) == 1:
            return primary_key_result[0]
        else:
            return primary_key_result
    
    def next_page_token(self, response: requests.Response, **kwargs) -> Optional[Mapping[str, Any]]:
        # Always return None as this endpoint always returns the first page
        return None
    
    def _updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        if not self.cursor_field:
            return self.state
        latest_record_state = latest_record[self.cursor_field]
        stream_state = current_stream_state.get(self.cursor_field, None)
        if stream_state:
            self._cursor = max(latest_record_state, stream_state)
        else:
            self._cursor = latest_record_state
        self.state = {self.cursor_field: self._cursor}
        if datetime.now() >= self._checkpoint_time + timedelta(minutes=15):
            self.checkpoint(self.name, self.state, self.namespace)
            self._checkpoint_time = datetime.now()
        return self.state

    def _extract_borders(self):
        if not self.start_time and not self.end_time:
            first_record = TableQueryRecord(self.project_id, self.stream_name, "ASC", self.cursor_field, self.where_clause, authenticator=self._auth)
            last_record = TableQueryRecord(self.project_id, self.stream_name, "DESC", self.cursor_field, self.where_clause, authenticator=self._auth)
            self.start_time = next(first_record.read_records(sync_mode=SyncMode.full_refresh)).get(self.cursor_field, None)
            self.end_time = next(last_record.read_records(sync_mode=SyncMode.full_refresh)).get(self.cursor_field, None)
            if self.start_time and isinstance(self.start_time, str):
                self.start_time = parser.parse(self.start_time)
            if self.end_time and isinstance(self.end_time, str):
                self.end_time = parser.parse(self.end_time)
        return self.start_time, self.end_time

    def _chunk_dates(self, start_date: datetime, end_date: datetime, table_start: datetime) -> Iterable[Tuple[datetime, datetime]]:
        step = timedelta(minutes=self.slice_range)
        increment = timedelta(seconds=1)
        new_start_date = start_date
        if start_date == end_date:
            yield start_date, start_date + step
            return
        while new_start_date < end_date+increment:
            before_date = end_date + increment
            try:
                if new_start_date + step < end_date + increment:
                    before_date = new_start_date + step                    
            except OverflowError as e:
                pass
            if table_start and before_date < table_start:
                before_date = table_start + step
            yield new_start_date, before_date
            new_start_date = before_date

    def stream_slices(self, stream_state: Mapping[str, Any] = None, cursor_field=None, sync_mode=None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        if isinstance(cursor_field,list) and cursor_field:
            self.cursor_field = cursor_field[0]
        elif cursor_field:
            self.cursor_field = cursor_field
        default_start = self.fallback_start
        slice = {}
        if stream_state:
            self._cursor = list(stream_state.values())[0]
            if self._cursor:
                default_start = parser.parse(self._cursor) - timedelta(seconds=30) # loopback window
        if self.cursor_field and sync_mode == SyncMode.incremental:
            start_time, end_time = self._extract_borders()
            if start_time and end_time:
                if not isinstance(start_time, datetime):
                    raise AirbyteTracedException(
                                internal_message=f"Cursor field should be a timestamp type for stream {self.dataset_id}.{self.table_id}",
                                failure_type=FailureType.config_error,
                                message=f"Cursor field should be a timestamp type for stream {self.dataset_id}.{self.table_id}",
                            )
                if not end_time.tzinfo and default_start.tzinfo:
                    # to avoid TypeError when comparing timezone aware and unaware datetimes
                    default_start = default_start.replace(tzinfo=None)
                elif end_time.tzinfo and not default_start.tzinfo:
                    default_start = default_start.replace(tzinfo=pytz.utc)
                if default_start <= end_time:
                        for start, end in self._chunk_dates(default_start, end_time, start_time):
                            slice["start"] = start.isoformat(timespec='microseconds')
                            slice["end"] = end.isoformat(timespec='microseconds')
                            yield slice
        else:
            yield

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        query_string = f"SELECT * FROM `{self.stream_name}`"
        if stream_slice:
            start = stream_slice.get("start", None)
            end = stream_slice.get("end", None)
            if start and end:
                query_string = f"SELECT * FROM `{self.stream_name}` WHERE {self.cursor_field}>='{start}' AND {self.cursor_field}<'{end}' ORDER BY {self.cursor_field}"
        elif stream_state:
            self._cursor = list(stream_state.values())[0]
            state_field = list(stream_state.keys())[0]
            if self._cursor:
                query_string = f"SELECT * FROM `{self.stream_name}` WHERE {state_field}>='{self._cursor}' ORDER BY {state_field}"
        index = query_string.find("ORDER BY")
        if self.where_clause and index==-1:
            query_string = query_string + f" WHERE {self.where_clause}"
        elif self.where_clause:
            query_string = query_string[:index] + f" AND {self.where_clause} " + query_string[index:]
        request_body = {
            "kind": "bigquery#queryRequest",
            "query": query_string,
            "useLegacySql": False,
            "timeoutMs": TIMEOUT,
            "maxResults": self.page_size
            }
        return request_body

    def process_records(self, record) -> Iterable[Mapping[str, Any]]:
        fields = record.get("schema")["fields"]
        rows = record.get("rows", [])
        job_id = record.get("jobReference")["jobId"]
        next_page = record.get("pageToken", None)
        if next_page:
            query_job = GetQueryResults(self.project_id, self.dataset_id, self.table_id, \
                                        job_id, record["jobReference"]["location"], \
                                        next_page, authenticator=self._auth)
            records = query_job.read_records(sync_mode=SyncMode.full_refresh)
            for record in records:
                yield record
                self._updated_state(self.state, record)
        else:
            for row in rows:
                data = row.get("f")
                formated_data = {
                    "_bigquery_table_id": record.get("jobReference")["jobId"],
                    "_bigquery_created_time": datetime.now(tz=pytz.timezone("UTC")).isoformat(timespec='microseconds'), #TODO: Update this to row insertion time
                    **{CHANGE_FIELDS.get(element["name"], element["name"]): SchemaHelpers.format_field(data[fields.index(element)]["v"], element["type"]) for element in fields},
                }
                self._updated_state(self.state, formated_data)
                yield formated_data
        if not self.state:
            self.state = {"__ab_full_refresh_sync_complete": True}

    
    def should_retry(self, response: requests.Response) -> bool:
        """
        Override to set different conditions for backoff based on the response from the server.

        By default, back off on the following HTTP response statuses:
         - jobComplete false indicates query job not completed

        Unexpected but transient exceptions (connection timeout, DNS resolution failed, etc..) are retried by default.
        """
        retry = False
        records = response.json()
        if records.get("jobComplete", None):
            retry = (records["jobComplete"] == False)
        return retry


class IncrementalQueryResult(BigqueryResultStream):
    """
    """
    primary_key = None
    cursor_field = []

    def __init__(self, project_id: list, dataset_id: str, table_id: str, given_name=None, where_clause: str="", fallback_start=None, slice_range=SLICE_RANGE, **kwargs):
        self.project_id = project_id
        self.parent_stream = dataset_id + "." + table_id
        self.given_name = given_name
        self.where_clause = where_clause.replace("\"", "'")
        self._json_schema = {}
        super().__init__(project_id, self.path(), self.parent_stream, self.get_json_schema, **kwargs)
        self.stream_obj = BigqueryIncrementalStream(project_id, dataset_id, table_id, self.path(), self.get_json_schema, \
                                                    fallback_start=fallback_start, given_name=given_name, where_clause=where_clause,\
                                                    slice_range=slice_range, **kwargs)

    @property
    def namespace(self):
        return None # TODO: update to self.dataset_id
    
    @property
    def name(self):
        return self.given_name or self.parent_stream

    @property
    def stream(self):
        return self.stream_obj
    
    @property
    def supported_sync_modes(self):
        return [SyncMode.incremental, SyncMode.full_refresh]
    
    @property
    def source_defined_cursor(self) -> bool:
        return False

    @property
    def supports_incremental(self) -> bool:
        return True

    def get_json_schema(self) -> Mapping[str, Any]:
        if not self._json_schema:
            for table in self.read_records(sync_mode=SyncMode.full_refresh):
                self._json_schema = SchemaHelpers.get_json_schema(table)
        return self._json_schema

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        query_string = f"SELECT * FROM `{self.parent_stream}`"
        if self.where_clause:
            query_string = query_string + f" WHERE {self.where_clause}"
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
    
    def should_retry(self, response: requests.Response) -> bool:
        """
        Override to set different conditions for backoff based on the response from the server.

        By default, back off on the following HTTP response statuses:
         - jobComplete false indicates query job not completed

        Unexpected but transient exceptions (connection timeout, DNS resolution failed, etc..) are retried by default.
        """
        retry = False
        records = response.json()
        if records.get("jobComplete", None):
            retry = (records["jobComplete"] == False)
        return retry


class BigqueryCDCStream(BigqueryResultStream, IncrementalMixin):
    """
    """
    state_checkpoint_interval = None

    def __init__(self, project_id, dataset_id, table_id, stream_path, stream_schema, given_name=None, \
                 where_clause: str="", fallback_start=None, slice_range=SLICE_RANGE, **kwargs):
        self.stream_name = dataset_id + "." + table_id
        super().__init__(project_id, stream_path, self.stream_name, stream_schema, **kwargs)
        self._cursor = None
        self._checkpoint_time = datetime.now()
        self.fallback_start = fallback_start
        self.project_id = project_id
        self.query_job = None
        self._is_primary_key_set = False
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.given_name = given_name
        self.where_clause = where_clause.replace("\"", "'")
        self.slice_range = slice_range
        self.start_time = None
        self.end_time = None
        self.configured_sync_mode = None
        # self._stream_slicer_cursor = None

    @property
    def namespace(self):
        return None # TODO: update to self.dataset_id
    
    @property
    def name(self):
        return self.given_name or self.stream_name

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
    
    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        """
        :return: string if single primary key, list of strings if composite primary key,
        list of list of strings if composite primary key consisting of nested fields.
          If the stream has no primary keys, return None.
        """
        if not self._is_primary_key_set:
            self.set_primary_key()
        return self._primary_key
    
    def set_primary_key(self):
        self._primary_key = self._get_primary_key()
        self._is_primary_key_set = True

    def _get_primary_key(self):
        primary_key_stream = InformationSchemaStream(self.project_id, self.dataset_id, self.table_id, authenticator=self._auth)
        primary_key_result = []
        for record in primary_key_stream.read_records(sync_mode=SyncMode.full_refresh):
            if record["constraint_name"] == f"{self.table_id}.pk$":
                primary_key_result.append(record['column_name'])
        if not len(primary_key_result):
            return None
        elif len(primary_key_result) == 1:
            return primary_key_result[0]
        else:
            return primary_key_result

    def next_page_token(self, response: requests.Response, **kwargs) -> Optional[Mapping[str, Any]]:
        # Always return None as this endpoint always returns the first page
        return None

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
        if datetime.now() >= self._checkpoint_time + timedelta(minutes=15):
            self.checkpoint(self.name, self.state, self.namespace)
            self._checkpoint_time = datetime.now()
        return self.state

    def _extract_borders(self):
        if not self.start_time and not self.end_time:
            cursor_column = next(key for key, value in CHANGE_FIELDS.items() if value == self.cursor_field)
            first_record = CHTableQueryRecord(self.project_id, self.stream_name, "ASC", cursor_column, self.where_clause, authenticator=self._auth)
            last_record = CHTableQueryRecord(self.project_id, self.stream_name, "DESC", cursor_column, self.where_clause, authenticator=self._auth)
            self.start_time = next(first_record.read_records(sync_mode=SyncMode.full_refresh)).get(self.cursor_field, None)
            self.end_time = next(last_record.read_records(sync_mode=SyncMode.full_refresh)).get(self.cursor_field, None)
            if self.start_time and isinstance(self.start_time, str):
                self.start_time = datetime.strptime(self.start_time, '%Y-%m-%dT%H:%M:%S.%f%z')
            if self.end_time and isinstance(self.end_time, str):
                self.end_time = datetime.strptime(self.end_time, '%Y-%m-%dT%H:%M:%S.%f%z')
        return self.start_time, self.end_time
    
    def _chunk_dates(self, start_date: datetime, end_date: datetime, table_start: datetime) -> Iterable[Tuple[datetime, datetime]]:
        step = timedelta(minutes=self.slice_range)
        new_start_date = start_date
        if start_date == end_date:
            yield start_date, start_date + step
            return
        while new_start_date < end_date+timedelta(seconds=1):
            before_date = min(end_date+timedelta(seconds=1), new_start_date + step)
            if table_start and before_date < table_start:
                before_date = table_start + step
            yield new_start_date, before_date
            new_start_date = before_date

    def stream_slices(self, stream_state: Mapping[str, Any] = None, cursor_field=None, sync_mode=None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        start_time, end_time = self._extract_borders()
        default_start = self.fallback_start
        if stream_state:
            self._cursor = stream_state.get(self.cursor_field)
            default_start =  datetime.strptime(self._cursor, '%Y-%m-%dT%H:%M:%S.%f%z') - timedelta(seconds=30)
        if end_time and default_start <= end_time:
            for start, end in self._chunk_dates(default_start, end_time, start_time):
                yield {
                    "start" : start.isoformat(timespec='microseconds'), "end": end.isoformat(timespec='microseconds')
                }

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        query_string = f"SELECT * FROM APPENDS(TABLE `{self.stream_name}`,NULL,NULL)"
        if stream_slice:
            start = stream_slice.get("start", None)
            end = stream_slice.get("end", None)
            if start and end:
                query_string = f"SELECT * FROM APPENDS(TABLE `{self.stream_name}`,'{start}','{end}')"
        if self.where_clause:
            query_string = query_string + f" WHERE {self.where_clause}"
        request_body = {
            "kind": "bigquery#queryRequest",
            "query": query_string,
            "useLegacySql": False,
            "timeoutMs": TIMEOUT,
            "maxResults": self.page_size
            }
        return request_body

    def process_records(self, record) -> Iterable[Mapping[str, Any]]:
        fields = record.get("schema")["fields"]
        rows = record.get("rows", [])
        job_id = record.get("jobReference")["jobId"]
        next_page = record.get("pageToken", None)
        if next_page:
            query_job = GetQueryResults(self.project_id, self.dataset_id, self.table_id, \
                                        job_id, record["jobReference"]["location"], \
                                        next_page, authenticator=self._auth)
            records = query_job.read_records(sync_mode=SyncMode.full_refresh)
            for record in records:
                yield record
                self._updated_state(self.state, record)
        else:
            for row in rows:
                data = row.get("f")
                formated_data = {
                    "_bigquery_table_id": job_id,
                    "_bigquery_created_time": datetime.now(tz=pytz.timezone("UTC")).isoformat(timespec='microseconds'), #TODO: Update this to row insertion time
                    **{CHANGE_FIELDS.get(element["name"], element["name"]): SchemaHelpers.format_field(data[fields.index(element)]["v"], element["type"]) for element in fields},
                }
                yield formated_data
                self._updated_state(self.state, formated_data)

    def should_retry(self, response: requests.Response) -> bool:
        """
        Override to set different conditions for backoff based on the response from the server.

        By default, back off on the following HTTP response statuses:
         - jobComplete false indicates query job not completed

        Unexpected but transient exceptions (connection timeout, DNS resolution failed, etc..) are retried by default.
        """
        retry = False
        records = response.json()
        if records.get("jobComplete", None):
            retry = (records["jobComplete"] == False)
        return retry


class TableChangeHistory(BigqueryResultStream):
    """
    """
    primary_key = None

    def __init__(self, project_id: list, dataset_id: str, table_id: str, given_name=None, \
                 where_clause: str="", fallback_start: datetime=None, slice_range=SLICE_RANGE, **kwargs):
        self.project_id = project_id
        self.given_name = given_name
        self.table_qualifier = dataset_id + "." + table_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.where_clause = where_clause.replace("\"", "'")
        self._json_schema = {}
        super().__init__(project_id, self.path(), self.table_qualifier, self.project_id, self.get_json_schema, retry_policy=self.should_retry, **kwargs)
        self.stream_obj = BigqueryCDCStream(project_id, dataset_id, table_id, self.path(), self.get_json_schema, \
                                            given_name, where_clause, fallback_start=fallback_start, slice_range=slice_range, **kwargs)

    @property
    def namespace(self):
        return None # TODO: update to self.dataset_id
    
    @property
    def name(self):
        return self.given_name or self.table_qualifier

    @property
    def stream(self):
        return self.stream_obj

    def get_json_schema(self) -> Mapping[str, Any]:
        if not self._json_schema:
            for table in self.read_records(sync_mode=SyncMode.full_refresh):
                self._json_schema = SchemaHelpers.get_json_schema(table)
        return self._json_schema

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        query_string = f"SELECT * FROM APPENDS(TABLE `{self.table_qualifier}`,NULL,NULL)"
        if self.where_clause:
            query_string = f"SELECT * FROM APPENDS(TABLE `{self.table_qualifier}`,NULL,NULL) WHERE {self.where_clause}"
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

    def should_retry(self, response: requests.Response) -> bool:
        """
        Override to set different conditions for backoff based on the response from the server.

        By default, back off on the following HTTP response statuses:
         - jobComplete false indicates query job not completed

        Unexpected but transient exceptions (connection timeout, DNS resolution failed, etc..) are retried by default.
        """
        retry = False
        records = response.json()
        if records.get("jobComplete", None):
            retry = (records["jobComplete"] == False)
        return retry
    

class GetQueryResults(BigqueryStream):
    """
    """
    primary_key = None

    def __init__(self, project_id: list, dataset_id: str, table_id: str, job_id: str, job_location: str, page_token: str, **kwargs):
        self.project_id = project_id
        self.parent_stream = dataset_id + "." + table_id
        self.job_id = job_id
        self.job_location = job_location
        self.page_token = page_token
        super().__init__(self.path(), self.parent_stream, self.project_id, self.get_json_schema, **kwargs)

    @property
    def name(self):
        return self.parent_stream

    @property
    def http_method(self) -> str:
        return "GET"

    def path(self, **kwargs) -> str:
        """
        Documentation: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
        """
        return f"/bigquery/v2/projects/{self.project_id}/queries/{self.job_id}"

    def get_json_schema(self) -> Mapping[str, Any]:
        for table in self.read_records(sync_mode=SyncMode.full_refresh):
            return SchemaHelpers.get_json_schema(table)
        return {}

    def next_page_token(self, response: requests.Response, **kwargs) -> Optional[Mapping[str, Any]]:
        record = response.json()
        next_page = record.get("pageToken", None)
        return next_page

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        """
        Override this method to define the query parameters that should be set on an outgoing HTTP request given the inputs.
        """
        params = {
            "maxResults": self.page_size,
            "location": self.job_location
        }
        if next_page_token:
            params["pageToken"] = next_page_token
        return params

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        return None
    
    def process_records(self, record) -> Iterable[Mapping[str, Any]]:
        fields = record.get("schema")["fields"]
        rows = record.get("rows", [])
        for row in rows:
            data = row.get("f")
            yield {
                "_bigquery_table_id": record.get("jobReference")["jobId"],
                "_bigquery_created_time": None, #TODO: Update this to row insertion time
                **{CHANGE_FIELDS.get(element["name"], element["name"]): SchemaHelpers.format_field(data[fields.index(element)]["v"], element["type"]) for element in fields},
            }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        records = response.json()
        yield from self.process_records(records)


class JobsQueryStream(BigqueryStream):
    """
    """
    def __init__(self, project_id: str, dataset_id: str, table_id, **kwargs):
        stream_name = dataset_id + "." + table_id
        self.project_id = project_id
        super().__init__(self.path(), stream_name, self.get_json_schema, **kwargs)

    @property
    def http_method(self) -> str:
        return "POST"
    
    def path(self, **kwargs) -> str:
        """
        Documentation: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/
        """
        return f"/bigquery/v2/projects/{self.project_id}/queries"
    
    def get_json_schema(self) -> Mapping[str, Any]:
        return {}
    

class InformationSchemaStream(JobsQueryStream):
    """
    """
    primary_key = None

    def __init__(self, project_id: list, dataset_id: str, table_id: str, **kwargs):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.stream_name = project_id + "." + dataset_id + ".INFORMATION_SCHEMA.KEY_COLUMN_USAGE." + table_id
        super().__init__(project_id, dataset_id, table_id, **kwargs)

    @property
    def name(self):
        return self.stream_name

    def get_json_schema(self) -> Mapping[str, Any]:
        for table in self.read_records(sync_mode=SyncMode.full_refresh):
            return SchemaHelpers.get_json_schema(table)
        return {}
    
    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        query_string = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` WHERE table_name='{self.table_id}';"
        request_body = {
            "kind": "bigquery#queryRequest",
            "query": query_string,
            "useLegacySql": False,
            "timeoutMs": TIMEOUT
            }
        return request_body
    
    def process_records(self, record) -> Iterable[Mapping[str, Any]]:
        try:
            fields = record.get("schema")["fields"]
            rows = record.get("rows", [])
            for row in rows:
                data = row.get("f")
                yield {
                    **{CHANGE_FIELDS.get(element["name"], element["name"]): SchemaHelpers.format_field(data[fields.index(element)]["v"], element["type"]) for element in fields},
                }            
        except TypeError as e:
            self.logger.error(f"Incorrect response {record} to Table {self.dataset_id}.{self.table_id} request to get primary keys gives {str(e)}")

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        records = response.json()
        yield from self.process_records(records)

    def should_retry(self, response: requests.Response) -> bool:
        """
        Override to set different conditions for backoff based on the response from the server.

        By default, back off on the following HTTP response statuses:
         - jobComplete false indicates query job not completed

        Unexpected but transient exceptions (connection timeout, DNS resolution failed, etc..) are retried by default.
        """
        retry = False
        records = response.json()
        if records.get("jobComplete", None):
            retry = (records["jobComplete"] == False)
        return retry
