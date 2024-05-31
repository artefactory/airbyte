#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import logging
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import pytz
import requests
from requests import codes, exceptions  # type: ignore[import]
from datetime import datetime, timedelta
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_protocol.models import SyncMode, Type
from airbyte_cdk.models import AirbyteCatalog, AirbyteMessage, AirbyteStateMessage, ConfiguredAirbyteCatalog, Level, ConfiguredAirbyteStream
from airbyte_cdk.logger import AirbyteLogger, AirbyteLogFormatter
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
from .streams import BigqueryDatasets, BigqueryTables, BigqueryStream, BigqueryTable, BigqueryTableData, BigqueryIncrementalStream, IncrementalQueryResult, TableChangeHistory, BigqueryCDCStream
from .schema_helpers import SchemaHelpers

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
            self._auth = BigqueryAuth(config)
            # try reading first table from each dataset, to check the connectivity,
            for dataset in BigqueryDatasets(project_id=config["project_id"], authenticator=self._auth).read_records(sync_mode=SyncMode.full_refresh):
                dataset_id = dataset.get("datasetReference")["datasetId"]
                for table_info in BigqueryTables(dataset_id=dataset_id, project_id=config["project_id"], authenticator=self._auth).read_records(sync_mode=SyncMode.full_refresh):
                    table_id = table_info.get("tableReference")["tableId"]
                    BigqueryTable(dataset_id=dataset_id, project_id=config["project_id"], table_id=table_id, authenticator=self._auth)
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
                    logger.warn(f"API Call limit is exceeded. Error message: '{error_data.get('message')}'")
                    error_msg = "API Call limit is exceeded. Make sure that you have enough API allocation for your organization needs or retry later. For more information, see https://cloud.google.com/bigquery/quotas"
                    raise AirbyteTracedException(
                        internal_message=error_msg,
                        failure_type=FailureType.transient_error,
                        message=error_msg,
                    )
            return True, error_msg
        return True, None
    
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        project_id = config["project_id"]
        self._auth = BigqueryAuth(config)
        streams = config.get("streams", [])
        sync_method = config["replication_method"]["method"]
        fallback_start =  datetime.strptime("0001-01-01T00:00:00.000Z", '%Y-%m-%dT%H:%M:%S.%f%z')
        change_history_start = datetime.now(tz=pytz.timezone("UTC")) - timedelta(days=7) + timedelta(seconds=30)
        streams_catalog = []

        for stream in streams:
                dataset_id, table_id = stream['parent_stream'].split(".")
                where_clause = stream["where_clause"]
                stream_name = stream["name"]
                if sync_method == "Standard":
                    table_obj = IncrementalQueryResult(project_id, dataset_id, table_id, stream_name, where_clause, fallback_start=fallback_start,authenticator=self._auth)
                else:
                    try:
                        table_obj = TableChangeHistory(project_id, dataset_id, table_id, stream_name, where_clause=where_clause, fallback_start=change_history_start, authenticator=self._auth)
                        next(table_obj.read_records(sync_mode=SyncMode.full_refresh))
                    except Exception as e:
                        self.logger.warn(str(e))
                        table_obj = IncrementalQueryResult(project_id, dataset_id, table_id, stream_name, where_clause, fallback_start=fallback_start, authenticator=self._auth)
                streams_catalog.append(table_obj.stream)
        for dataset in BigqueryDatasets(project_id=project_id, authenticator=self._auth).read_records(sync_mode=SyncMode.full_refresh):
            dataset_id = dataset.get("datasetReference")["datasetId"]
            # list and process each table under each base to generate the JSON Schema
            for table_info in BigqueryTables(dataset_id=dataset_id, project_id=project_id, authenticator=self._auth).read_records(sync_mode=SyncMode.full_refresh):
                table_id = table_info.get("tableReference")["tableId"]
                if sync_method == "Standard":
                    table_obj = IncrementalQueryResult(project_id, dataset_id, table_id, fallback_start=fallback_start,authenticator=self._auth)
                else:
                    try:
                        table_obj = TableChangeHistory(project_id, dataset_id, table_id, fallback_start=change_history_start, authenticator=self._auth)
                        next(table_obj.read_records(sync_mode=SyncMode.full_refresh))
                    except Exception as e:
                        self.logger.warn(str(e))
                        table_obj = IncrementalQueryResult(project_id, dataset_id, table_id, fallback_start=fallback_start, authenticator=self._auth)
                streams_catalog.append(table_obj.stream)
        state_manager = ConnectorStateManager(stream_instance_map={stream.name: stream for stream in streams_catalog}, state=self.state)
        return [
            self._to_concurrent(
                stream,
                fallback_start,
                timedelta(minutes=1),
                state_manager  
            )
            for stream in streams_catalog
        ]

    def _to_concurrent(
        self, stream: Stream, fallback_start: datetime, slice_range: timedelta, state_manager: ConnectorStateManager
    ) -> Stream:
        if not stream.cursor_field:
            for configured_stream in self.catalog.streams:
                if configured_stream.stream.name == stream.name and configured_stream.cursor_field:
                    stream.cursor_field = configured_stream.cursor_field[0]

        if self._stream_state_is_full_refresh(stream.state) or not stream.cursor_field:
            return StreamFacade.create_from_stream(
                stream,
                self,
                self.logger,
                self._create_empty_state(),
                FinalStateCursor(stream_name=stream.name, stream_namespace=stream.namespace, message_repository=self.message_repository),
            )

        state = state_manager.get_stream_state(stream.name, stream.namespace)
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
                timedelta(minutes=1),
                slice_range,
            )
            return StreamFacade.create_from_stream(stream, self, self.logger, state, cursor)

        return stream

    def _create_empty_state(self) -> MutableMapping[str, Any]:
        return {}
