#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import logging
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import pendulum
import pendulum.parsing
import pendulum.parsing.exceptions
import pytz
import requests
from dateutil import parser
from requests import codes, exceptions  # type: ignore[import]
from datetime import datetime, timedelta
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_protocol.models import SyncMode, Type
from airbyte_cdk.models import AirbyteCatalog, AirbyteMessage, AirbyteStateMessage, ConfiguredAirbyteCatalog, Level, ConfiguredAirbyteStream
from airbyte_cdk.logger import AirbyteLogFormatter
from airbyte_cdk.utils.traced_exception import AirbyteTracedException, FailureType
from airbyte_cdk.sources.concurrent_source.concurrent_source_adapter import ConcurrentSourceAdapter
from airbyte_cdk.sources.concurrent_source.concurrent_source import ConcurrentSource
from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.message import InMemoryMessageRepository
from airbyte_cdk.sources.source import TState
from airbyte_cdk.sources.streams.concurrent.adapters import StreamFacade
from airbyte_cdk.sources.streams.concurrent.cursor import ConcurrentCursor, CursorField, FinalStateCursor
from airbyte_cdk.sources.streams.concurrent.state_converters.datetime_stream_state_converter import EpochValueConcurrentStreamStateConverter, IsoMillisConcurrentStreamStateConverter

from .auth import BigqueryAuth
from .streams import BigqueryDatasets, BigqueryTables, BigqueryDataset, BigqueryTable, CDCFirstSyncStream, BigqueryIncrementalStream, IncrementalQueryResult, TableChangeHistory, BigqueryCDCStream
from .schema_helpers import TIME_TYPES

"""
This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""
_DEFAULT_CONCURRENCY = 10
_MAX_CONCURRENCY = 10
_DEFAULT_SLICE_RANGE = 525600 # 1 year by default
FALLBACK_START =  datetime.strptime("0001-01-01T00:00:00.000Z", '%Y-%m-%dT%H:%M:%S.%f%z')
CHANGE_HISTORY_START = datetime.now(tz=pytz.timezone("UTC")) - timedelta(days=7) + timedelta(seconds=30)

# Source
class SourceBigquery(ConcurrentSourceAdapter):
    logger = logging.getLogger("airbyte")
    streams_catalog: Iterable[Mapping[str, Any]] = []
    _auth: BigqueryAuth = None
    _SLICE_BOUNDARY_FIELDS_BY_IMPLEMENTATION = {
        BigqueryIncrementalStream: ("start", "end"),
        BigqueryCDCStream: ("start", "end"),
    }

    message_repository = InMemoryMessageRepository(Level(AirbyteLogFormatter.level_mapping[logger.level]))

    def __init__(self, catalog: Optional[ConfiguredAirbyteCatalog], config: Optional[Mapping[str, Any]], state: Optional[TState], **kwargs):
        if config:
            concurrency_level = min(config.get("num_workers", _DEFAULT_CONCURRENCY), _MAX_CONCURRENCY)
        else:
            concurrency_level = _DEFAULT_CONCURRENCY
        self.logger.info(f"Using concurrent cdk with concurrency level {concurrency_level}")
        concurrent_source = ConcurrentSource.create(
            concurrency_level, concurrency_level // 2, self.logger, self._slice_logger, self.message_repository
        )
        super().__init__(concurrent_source)
        self.catalog = catalog
        self.state = state
        self._normal_streams = []
        self._concurrent_streams = []

    @staticmethod
    def validate_config(config: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
        slice_range = float(config.get("slice_range", _DEFAULT_SLICE_RANGE))
        if slice_range and slice_range <= 0:
            message = f"Invalid slice range {slice_range}. Please use only positive integer values."
            raise AirbyteTracedException(
                message=message,
                internal_message=message,
                failure_type=FailureType.config_error,
            )
        return config
    
    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        """
        Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        
        try:
            self.validate_config(config)
            self._auth = BigqueryAuth(config)
            # try reading first table from each dataset, to check the connectivity,
            for dataset in BigqueryDatasets(project_id=config["project_id"], authenticator=self._auth).read_records(sync_mode=SyncMode.full_refresh):
                dataset_id = dataset.get("datasetReference")["datasetId"]
                for table_info in BigqueryTables(dataset_id=dataset_id, project_id=config["project_id"], authenticator=self._auth).read_records(sync_mode=SyncMode.full_refresh):
                    table_id = table_info.get("tableReference")["tableId"]
                    table_info = BigqueryTable(dataset_id=dataset_id, project_id=config["project_id"], table_id=table_id, authenticator=self._auth)
                    next(table_info.read_records(sync_mode=SyncMode.full_refresh))
        except exceptions.HTTPError as error:
            error_msg = f"An error occurred: {error.response.text}"
            try:
                error_data = error.response.json()[0]
            except (KeyError, requests.exceptions.JSONDecodeError) as e:
                raise AirbyteTracedException(
                    internal_message=str(e),
                    failure_type=FailureType.system_error,
                    message=error_msg
                )
            else:
                error_code = error_data.get("errorCode")
                if error.response.status_code == codes.FORBIDDEN and error_code == "REQUEST_LIMIT_EXCEEDED":
                    self.logger.warning(f"API Call limit is exceeded. Error message: '{error_data.get('message')}'")
                    error_msg = "API Call limit is exceeded. Make sure that you have enough API allocation for your organization needs or retry later. For more information, see https://cloud.google.com/bigquery/quotas"
                    raise AirbyteTracedException(
                        internal_message=error_msg,
                        failure_type=FailureType.transient_error,
                        message=error_msg,
                    )
            return False, error_msg
        return True, None
    
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        self.validate_config(config)
        project_id = config["project_id"]
        dataset_id = config.get("dataset_id", None)
        self._auth = BigqueryAuth(config)
        streams = config.get("streams", [])
        sync_method = config["replication_method"]["method"]
        slice_range = float(config.get("slice_range", _DEFAULT_SLICE_RANGE))

        self._add_filtered_streams(streams, project_id, sync_method, slice_range)
        if self.catalog and self.catalog.streams:
            self._use_catalog_streams(project_id, sync_method, slice_range)
        else:
            if dataset_id:
                for table_info in BigqueryTables(dataset_id=dataset_id, project_id=project_id, authenticator=self._auth).read_records(sync_mode=SyncMode.full_refresh):
                    table_id = table_info.get("tableReference")["tableId"]
                    self._add_tables(project_id, dataset_id, table_id, sync_method, slice_range, table_info)
            else:
                for dataset in BigqueryDatasets(project_id=project_id, authenticator=self._auth).read_records(sync_mode=SyncMode.full_refresh):
                    dataset_id = dataset.get("datasetReference")["datasetId"]
                    for table_info in BigqueryTables(dataset_id=dataset_id, project_id=project_id, authenticator=self._auth).read_records(sync_mode=SyncMode.full_refresh):
                        table_id = table_info.get("tableReference")["tableId"]
                        self._add_tables(project_id, dataset_id, table_id, sync_method, slice_range, table_info)

        self._set_cursor_field()
        self._set_sync_mode()
        state_manager = ConnectorStateManager(stream_instance_map={stream.name: stream for stream in self._concurrent_streams}, state=self.state)
        return [
            self._to_concurrent(
                stream,
                stream.fallback_start,
                slice_range,
                state_manager  
            )
            for stream in self._concurrent_streams
        ]
    
    def _add_filtered_streams(self, streams, project_id, sync_method, slice_range):
        for stream in streams:
            filter_dataset_id, table_id = stream['parent_stream'].split(".")
            where_clause = stream["where_clause"]
            stream_name = stream["name"]
            table_info = next(BigqueryTable(dataset_id=filter_dataset_id, project_id=project_id, table_id=table_id, authenticator=self._auth).read_records(sync_mode=SyncMode.full_refresh))
            self._add_tables(project_id, filter_dataset_id, table_id, sync_method, slice_range, table_info, stream_name, where_clause)

    def _use_catalog_streams(self, project_id, sync_method, slice_range):
        for configured_stream in self.catalog.streams:
            try:
                dataset_id, table_id = configured_stream.stream.name.split(".")
                table_info = next(BigqueryTable(dataset_id=dataset_id, project_id=project_id, table_id=table_id, authenticator=self._auth).read_records(sync_mode=SyncMode.full_refresh))
                self._add_tables(project_id, dataset_id, table_id, sync_method, slice_range, table_info)
            except ValueError:
                # This could happen for pushdown filter streams
                pass
            except exceptions.HTTPError as error:
                if error.response.status_code == 404:
                    # This could happen for pushdown filter streams
                    pass
                else:
                    raise error 

    def _set_cursor_field(self):
        for stream in self._concurrent_streams:
            if not stream.cursor_field and self.catalog:
                for configured_stream in self.catalog.streams:
                    if configured_stream.stream.name == stream.name and configured_stream.cursor_field:
                        stream.cursor_field = configured_stream.cursor_field[0]

    def _set_sync_mode(self):
        for stream in self._concurrent_streams:
            if not stream.configured_sync_mode and self.catalog:
                for configured_stream in self.catalog.streams:
                    if configured_stream.stream.name == stream.name and configured_stream.sync_mode:
                        stream.configured_sync_mode = configured_stream.sync_mode

    def _add_tables(self, project_id, dataset_id, table_id, sync_method, slice_range, table_info, stream_name=None, where_clause=""):
        # list and process each table under each base to generate the JSON Schema
        if sync_method == "Standard":
            table_obj = IncrementalQueryResult(project_id, dataset_id, table_id, given_name=stream_name, where_clause=where_clause, fallback_start=FALLBACK_START, slice_range=slice_range, authenticator=self._auth)
        else:
            table_creation_datetime = pendulum.from_timestamp(float(table_info["creationTime"])/1000.0) # timestamps returned are in milliseconds hence the /1000
            table_obj = TableChangeHistory(project_id, dataset_id, table_id, given_name=stream_name, where_clause=where_clause, \
                                           fallback_start=table_creation_datetime, slice_range=slice_range, authenticator=self._auth)
            try:
                records = next(table_obj.read_records(sync_mode=SyncMode.full_refresh))
            except exceptions.HTTPError as error:
                if error.response.status_code == 400:
                    table_obj = None
                    #TODO: add link to documentation
                else:
                    raise error 
            else:
                stream_state = self._get_stream_state(table_obj)
                #If first read then use normal stream to get records before max time travel window
                #records["totalBytesProcessed"] is 0 when time travel window is bigger than what's allowed
                if float(records["totalBytesProcessed"]) == 0 and not stream_state:
                    table_obj = CDCFirstSyncStream(project_id, dataset_id, table_id, table_obj.path(), table_obj.get_json_schema, \
                                                    given_name=stream_name, where_clause=where_clause, \
                                                    fallback_start=table_creation_datetime, slice_range=slice_range, authenticator=self._auth)
        if table_obj:
            self._concurrent_streams.append(table_obj.stream)

    def _get_stream_state(self, table_obj):
        state_manager = ConnectorStateManager(stream_instance_map={table_obj.name: table_obj}, state=self.state)
        return state_manager.get_stream_state(table_obj.name, table_obj.namespace)

    def _to_concurrent(
        self, stream: Stream, fallback_start: datetime, slice_range: timedelta, state_manager: ConnectorStateManager
    ) -> Stream:
        #if no catalog given then we are not in read so no need for concurrency
        #and if cursor field is not of type datetime concurrency will not work
        if not (self.catalog and self.catalog.streams) or (stream.cursor_field and not stream.get_json_schema()["properties"][stream.cursor_field] in TIME_TYPES):
            return stream
        state = state_manager.get_stream_state(stream.name, stream.namespace)
        state = self._format_state(state, stream)
        if stream.configured_sync_mode==SyncMode.full_refresh or not stream.cursor_field:
            return StreamFacade.create_from_stream(
                stream,
                self,
                self.logger,
                state,
                FinalStateCursor(stream_name=stream.name, stream_namespace=stream.namespace, message_repository=self.message_repository),
            )

        slice_boundary_fields = self._SLICE_BOUNDARY_FIELDS_BY_IMPLEMENTATION.get(type(stream))
        if slice_boundary_fields:
            cursor_field = CursorField(stream.cursor_field) if isinstance(stream.cursor_field, str) else CursorField(stream.cursor_field[0])
            converter = IsoMillisConcurrentStreamStateConverter()
            cursor = ConcurrentCursor(
                stream.name,
                stream.namespace,
                state,
                self.message_repository,
                state_manager,
                converter,
                cursor_field,
                slice_boundary_fields,
                fallback_start,
                converter.get_end_provider(),
                slice_range,
                slice_range,
            )
            return StreamFacade.create_from_stream(stream, self, self.logger, state, cursor)

        return stream

    def _format_state(self, state, stream):
        """
        This is to prevent parse_timestamp in IsoMillisConcurrentStreamStateConverter from crashing because of unexpected timestamp format
        """
        if state and stream.get_json_schema()["properties"][list(state.keys())[0]] in TIME_TYPES:
            try:
                pendulum.parse(list(state.values())[0])
            except pendulum.parsing.exceptions.ParserError as e:
                state[list(state.keys())[0]] = self._format_timestamp(list(state.values())[0])
        return state

    def _format_timestamp(self, timestamp: str) -> str:
        ts = parser.parse(timestamp)
        return ts.isoformat(timespec='microseconds')

    def _create_empty_state(self) -> MutableMapping[str, Any]:
        return {}
