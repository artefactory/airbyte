import logging
import uuid
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.models import (AirbyteMessage, AirbyteStateMessage, AirbyteStateType,
                                AirbyteStreamState, StreamDescriptor, AirbyteStateBlob)
from airbyte_cdk.sources.utils.schema_helpers import InternalConfig
from airbyte_cdk.sources.utils.slice_logger import SliceLogger
from airbyte_protocol.models import SyncMode, Type, ConfiguredAirbyteStream

from source_snowflake.schema_builder import mapping_snowflake_type_airbyte_type, format_field, date_and_time_snowflake_type_airbyte_type, \
    string_snowflake_type_airbyte_type
from .snowflake_parent_stream import SnowflakeStream
from .util_streams import TableSchemaStream, StreamLauncher


class TableStream(SnowflakeStream, IncrementalMixin):
    primary_key = None
    state_checkpoint_interval = None

    def __init__(self, url_base, config, table_object, **kwargs):
        stream_filtered_kwargs = {k: v for k, v in kwargs.items() if k in SnowflakeStream.__init__.__annotations__}
        super().__init__(**stream_filtered_kwargs)
        self._kwargs = kwargs
        self._url_base = url_base
        self.config = config
        self.table_object = table_object
        self.table_schema_stream = TableSchemaStream(url_base=url_base, config=config, table_object=table_object,
                                                     **stream_filtered_kwargs)
        self._namespace = None
        self._state_value = None
        self._cursor_field = []
        self._json_schema_properties = None
        self.checkpoint_time = datetime.now()
        self.ordered_mapping_names_types = None

        self.number_of_partitions = None
        self.number_of_read_partitions = 0

        # Specific attribute of post response that configures the get
        self.statement_handle = None
        self.statement_status_url = None  # unused for the moment but useful to fetch the status

    ######################################
    ###### Properties
    ######################################

    @property
    def cursor_field(self):
        return self._cursor_field

    @cursor_field.setter
    def cursor_field(self, new_cursor_field):
        self._cursor_field = new_cursor_field

    @property
    def state(self):
        if not self.cursor_field or isinstance(self.cursor_field, list):
            return {}
        return {self.cursor_field: self._state_value}

    @state.setter
    def state(self, new_state):
        if not (new_state is None or not new_state):
            self.cursor_field = list(new_state.keys())[0]
            self._state_value = new_state[self.cursor_field]

    @property
    def source_defined_cursor(self) -> bool:
        return False

    @property
    def supports_incremental(self) -> bool:
        return True

    @property
    def namespace(self):
        return self._namespace

    @namespace.setter
    def namespace(self, namespace):
        self._namespace = namespace

    @property
    def name(self):
        return f"{self.table_object['schema']}.{self.table_object['table']}"

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
            path of request
        """
        if not self.statement_handle:
            raise ValueError('self.statement_handle should be set to fetch the results. '
                             'To solve this issue, you must ensure that the method set_statement_handle is called before the get request')
        return f"{self.url_base}/{self.url_suffix}/{self.statement_handle}"

    @property
    def url_base(self):
        return self._url_base

    ######################################
    ###### HTTP configuration
    ######################################

    @property
    def http_method(self) -> str:
        return "GET"

    def request_headers(
            self,
            stream_state: Optional[Mapping[str, Any]],
            stream_slice: Optional[Mapping[str, Any]] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        """
        We set the statement handle here because this is the first method called
        before launching any request
        This is appropriate as we have to launch the POST request before the GET
        """
        self.set_statement_handle()
        request_headers = dict(super().request_headers(stream_state, stream_slice, next_page_token))
        if next_page_token and "partition" in next_page_token:
            request_headers["partition"] = next_page_token["partition"]
        return request_headers

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        params = {
            "async": "true",
        }
        return params

    ######################################
    ###### Pagination
    ######################################
    def set_statement_handle(self):
        if self.statement_handle:
            return

        stream_launcher = StreamLauncher(url_base=self.url_base,
                                         config=self.config,
                                         table_object=self.table_object,
                                         current_state=self.state,
                                         cursor_field=self.cursor_field,
                                         **self._kwargs)
        post_response_iterable = stream_launcher.read_records(sync_mode=SyncMode.full_refresh)
        for post_response in post_response_iterable:
            if post_response:
                self.statement_handle = post_response['statementHandle']

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if self.number_of_partitions is None:
            response_json = response.json()
            partition_info_value = response_json.get('resultSetMetaData', {'partitionInfo': []}).get('partitionInfo', [])
            self.number_of_partitions = len(partition_info_value)

        next_partition_index = self.number_of_read_partitions + 1

        if next_partition_index >= self.number_of_partitions:
            return None

        self.number_of_read_partitions = next_partition_index

        return {"partition": next_partition_index}


    ######################################
    ###### Response processing
    ######################################

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        if not self.ordered_mapping_names_types:
            self.ordered_mapping_names_types = OrderedDict([(row_type['name'], row_type['type'])
                                                            for row_type in
                                                            response_json.get('resultSetMetaData', {'rowType': []}).get('rowType', [])])

        for record in response_json.get("data", []):
            yield {column_name: format_field(column_value, self.ordered_mapping_names_types[column_name])
                   for column_name, column_value in zip(self.ordered_mapping_names_types.keys(), record)}

    def read_records(
            self,
            sync_mode: SyncMode,
            cursor_field: Optional[List[str]] = None,
            stream_slice: Optional[Mapping[str, Any]] = None,
            stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        self.cursor_field = self._process_cursor_field(cursor_field)
        for record in super().read_records(sync_mode, cursor_field, stream_slice, stream_state):
            if isinstance(self.cursor_field, str):
                self.state = self._get_updated_state(record)
            yield record

    def get_json_schema(self) -> Mapping[str, Any]:
        properties = {}
        json_schema = {
            "$schema": "https://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": True,
            "properties": properties,
        }

        for column_object in self.table_schema_stream.read_records(sync_mode=SyncMode.full_refresh):
            column_name = column_object['column_name']
            snowflake_column_type = column_object['type'].upper()
            if snowflake_column_type not in mapping_snowflake_type_airbyte_type:
                raise ValueError(f"The type {snowflake_column_type} is not recognized. "
                                 f"Please, contact Airbyte support to update the connector to handle this new type")
            airbyte_column_type_object = mapping_snowflake_type_airbyte_type[snowflake_column_type]
            properties[column_name] = airbyte_column_type_object

        self._json_schema_properties = properties
        return json_schema

    ######################################
    ###### State, cursor management and checkpointing
    ######################################

    def read(  # type: ignore  # ignoring typing for ConnectorStateManager because of circular dependencies
            self,
            configured_stream: ConfiguredAirbyteStream,
            logger: logging.Logger,
            slice_logger: SliceLogger,
            stream_state: MutableMapping[str, Any],
            state_manager,
            internal_config: InternalConfig,
    ) -> Iterable[StreamData]:
        self.cursor_field = self._process_cursor_field(configured_stream.cursor_field)
        return super().read(configured_stream,
                            logger,
                            slice_logger,
                            stream_state,
                            state_manager,
                            internal_config)

    def _get_updated_state(self, latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        latest_record_state = latest_record[self.cursor_field]
        if self.state is not None and len(self.state) > 0:
            current_state_value = self.state[self.cursor_field]
            self._state_value = max(latest_record_state,
                                    current_state_value) if current_state_value is not None else latest_record_state
            self.state = {self.cursor_field: self._state_value}
        else:
            self._state_value = latest_record_state
            self.state = {self.cursor_field: self._state_value}

        if datetime.now() >= self.checkpoint_time + timedelta(minutes=15):
            self.checkpoint(self.name, self.state, self.namespace)
            self.checkpoint_time = datetime.now()

        return self.state

    @classmethod
    def _process_cursor_field(cls, cursor_field):
        processed_cursor_field = cursor_field
        if isinstance(cursor_field, list) and cursor_field:
            if len(cursor_field) == 1:
                processed_cursor_field = cursor_field[0]
            else:
                raise ValueError('When cursor_field is a list, its size must be 1')
        return processed_cursor_field

    def stream_slices(self, stream_state: Mapping[str, Any] = None, cursor_field=None, sync_mode=None, **kwargs) -> Iterable[
        Optional[Mapping[str, any]]]:

        if sync_mode == SyncMode.incremental:
            self.cursor_field = self._process_cursor_field(cursor_field)

            if stream_state:
                self._state_value = stream_state.get(self.cursor_field)

            yield {self.cursor_field: self._state_value}
        else:
            yield {}

    def checkpoint(self, stream_name, stream_state, stream_namespace):
        """
        Checkpoint state.
        """
        state = AirbyteMessage(
            type=Type.STATE,
            state=AirbyteStateMessage(
                type=AirbyteStateType.STREAM,
                stream=AirbyteStreamState(
                    stream_descriptor=StreamDescriptor(name=stream_name, namespace=stream_namespace),
                    stream_state=AirbyteStateBlob.parse_obj(stream_state),
                )
            ),
        )
        self.logger.info(f"Checkpoint state of {self.name} is {stream_state}")
        print(state.json(exclude_unset=True))  # Emit state

    ######################################
    ###### Dunder methods
    ######################################

    def __str__(self):
        return f"Current stream has this table object as constructor {self.table_object}"