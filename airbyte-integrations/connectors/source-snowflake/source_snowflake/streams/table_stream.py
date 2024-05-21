import logging
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
from .util_streams import TableSchemaStream


class TableStream(SnowflakeStream, IncrementalMixin):
    primary_key = None
    state_checkpoint_interval = None

    def __init__(self, url_base, config, table_object, **kwargs):
        stream_filtered_kwargs = {k: v for k, v in kwargs.items() if k in SnowflakeStream.__init__.__annotations__}
        super().__init__(**stream_filtered_kwargs)
        self._url_base = url_base
        self.config = config
        self.table_object = table_object
        self.table_schema_stream = TableSchemaStream(url_base=url_base, config=config, table_object=table_object,
                                                     **stream_filtered_kwargs)
        self._namespace = None
        self._cursor_value = None
        self._cursor_field = []
        self._json_schema_properties = None
        self.checkpoint_time = datetime.now()


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
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, new_state):
        if not (new_state is None or not new_state):
            self.cursor_field = list(new_state.keys())[0]
            self._cursor_value = new_state[self.cursor_field]

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

        return f"{self.url_base}/{self.url_suffix}"

    @property
    def url_base(self):
        return self._url_base

    @property
    def statement(self):
        database = self.config["database"]
        schema = self.table_object["schema"]
        table = self.table_object["table"]

        return f'SELECT * FROM "{database}"."{schema}"."{table}"'

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
            self._cursor_value = max(latest_record_state, current_state_value) if current_state_value is not None else latest_record_state
            self.state = {self.cursor_field: self._cursor_value}
        else:
            self._cursor_value = latest_record_state
            self.state = {self.cursor_field: self._cursor_value}

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
                self._cursor_value = stream_state.get(self.cursor_field)

            yield {self.cursor_field: self._cursor_value}
        else:
            yield {}

    def get_updated_statement(self, stream_slice):
        """
        Can be used consistently only in request_body_json
        otherwise we are not sure stream slice is the next slice and _cursor_value is updated with the correct data
        """

        updated_statement = self.statement

        if stream_slice:
            # TODO MAKE SURE THE CURSOR IS SINGLE VALUE AND NOT A STARTING AND ENDING VALUE (ex: window)
            self._cursor_value = stream_slice.get(self.cursor_field, None)

        if self._cursor_value:
            state_sql_condition = self._get_state_sql_condition()
            key_word_where = " where "  # spaces in case there is a where in a table name
            if key_word_where in self.statement.lower():
                updated_statement = f"{self.statement} AND {state_sql_condition}"
            else:
                updated_statement = f"{self.statement} WHERE {state_sql_condition}"

        if self.cursor_field:
            updated_statement = f"{updated_statement} ORDER BY {self.cursor_field} ASC"

        return updated_statement

    def _get_state_sql_condition(self):
        """
        The schema must have been generated before
        """
        state_sql_condition = f"{self.cursor_field}>={self._cursor_value}"
        if self.cursor_field.upper() not in self._json_schema_properties:
            raise ValueError(f'this field {self.cursor_field} should be present in schema. Make sure the column is present in your stream')

        if self._json_schema_properties[self.cursor_field.upper()]["type"].upper() in date_and_time_snowflake_type_airbyte_type:
            state_sql_condition = f"TO_TIMESTAMP({self.cursor_field})>=TO_TIMESTAMP({self._cursor_value})"

        if self._json_schema_properties[self.cursor_field.upper()]["type"].upper() in string_snowflake_type_airbyte_type:
            state_sql_condition = f"{self.cursor_field}>='{self._cursor_value}'"

        return state_sql_condition

    def request_body_json(
            self,
            stream_state: Optional[Mapping[str, Any]],
            stream_slice: Optional[Mapping[str, Any]] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:

        current_statement = self.get_updated_statement(stream_slice)
        json_payload = {
            "statement": current_statement,
            "role": self.config['role'],
            "warehouse": self.config['warehouse'],
            "database": self.config['database'],
            "timeout": "1000",
        }

        schema = self.table_object.get('schema', '')
        if schema:
            json_payload['schema'] = schema

        return json_payload

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
    ###### Response processing
    ######################################

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        ordered_mapping_names_types = OrderedDict(
            [(row_type['name'], row_type['type'])
             for row_type in response_json.get('resultSetMetaData', {'rowType': []}).get('rowType', [])]
        )

        for record in response_json.get("data", []):
            yield {column_name: format_field(column_value, ordered_mapping_names_types[column_name])
                   for column_name, column_value in zip(ordered_mapping_names_types.keys(), record)}

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

    def __str__(self):
        return f"Current stream has this table object as constructor {self.table_object}"


