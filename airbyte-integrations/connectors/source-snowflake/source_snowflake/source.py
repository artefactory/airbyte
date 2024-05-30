#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
import logging
import traceback
from datetime import datetime, timedelta
from typing import Any, List, Mapping, Tuple, Iterable, Optional, MutableMapping

import pytz
from airbyte_cdk.logger import AirbyteLogFormatter
from airbyte_cdk.sources.concurrent_source.concurrent_source import ConcurrentSource
from airbyte_cdk.sources.concurrent_source.concurrent_source_adapter import ConcurrentSourceAdapter
from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.message import InMemoryMessageRepository
from airbyte_cdk.sources.source import TState
from airbyte_cdk.sources.streams.concurrent.adapters import StreamFacade
from airbyte_cdk.sources.streams.concurrent.cursor import FinalStateCursor, CursorField, ConcurrentCursor
from airbyte_cdk.sources.streams.concurrent.state_converters.datetime_stream_state_converter import IsoMillisConcurrentStreamStateConverter

from source_snowflake.streams.util_streams import TableCatalogStream
from source_snowflake.streams.table_stream import TableStream, TableChangeDataCaptureStream
from source_snowflake.streams.check_connection import CheckConnectionStream
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_protocol.models import SyncMode, AirbyteMessage, AirbyteTraceMessage, TraceType, AirbyteErrorTraceMessage, FailureType, Level, \
    ConfiguredAirbyteCatalog
from airbyte_cdk.models import Type as AirbyteType
from .authenticator import SnowflakeJwtAuthenticator
from .snowflake_exceptions import InconsistentPushDownFilterParentStreamNameError, NotEnabledChangeTrackingOptionError, \
    DuplicatedPushDownFilterStreamNameError, IncorrectHostFormat, emit_airbyte_error_message, UnknownUpdateMethodError
from .streams.push_down_filter_stream import PushDownFilterStream, PushDownFilterChangeDataCaptureStream

# Concurrency parameters
_DEFAULT_CONCURRENCY = 10
_MAX_CONCURRENCY = 10


class SourceSnowflake(ConcurrentSourceAdapter):
    SNOWFLAKE_URL_SUFFIX = ".snowflakecomputing.com"
    HTTP_PREFIX = "https://"
    update_methods = ('standard', 'history')
    logger = logging.getLogger("airbyte")
    streams_catalog: Iterable[Mapping[str, Any]] = []
    _auth = None
    _SLICE_BOUNDARY_FIELDS = ("start", "end")

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

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement connection using oAyth2.0
        Done: Connection use JWT token

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """

        try:
            host = config['host']
            self.check_host_format(host)
            self.check_update_method(config=config)
            self.check_push_down_filter_name_unicity(config=config)
            url_base = self.format_url_base(host)
            authenticator = SnowflakeJwtAuthenticator.from_config(config)
            self._auth = authenticator

            self.check_existence_of_at_least_one_stream(url_base=url_base, config=config, authenticator=authenticator)
            self.check_push_down_filters_parent_stream_consistency(url_base=url_base, config=config, authenticator=authenticator)

        except IncorrectHostFormat:
            error_message = f"your host is not ending with {self.SNOWFLAKE_URL_SUFFIX}"
            emit_airbyte_error_message(error_message=error_message,
                                       failure_type=FailureType.config_error)
            raise ValueError(error_message)

        except UnknownUpdateMethodError as e:
            error_message = f'Update method {e.unknown_update_method} not recognized'
            emit_airbyte_error_message(error_message=error_message,
                                       failure_type=FailureType.config_error)
            raise ValueError(error_message)

        except DuplicatedPushDownFilterStreamNameError:
            error_message = 'There are pushdown filters with same name which is not possible'
            emit_airbyte_error_message(error_message=error_message,
                                       failure_type=FailureType.config_error)
            raise ValueError(error_message)

        except InconsistentPushDownFilterParentStreamNameError as e:
            error_message = (f'You have provided inconsistent pushdown filters configuration. Parent stream not found or mistake in  '
                             f'spelling parent stream name. Correct spelling must be your_schema_name.your_table_name and make sure '
                             f'you have the rights to read the table.\n. Here is the '
                             f'list of the streams affected by this error: {e.push_down_filters_without_consistent_parent}')
            emit_airbyte_error_message(error_message=error_message,
                                       failure_type=FailureType.config_error)
            raise ValueError(error_message)

        except StopIteration:
            error_message = "There is no stream available for the connection specification provided"
            emit_airbyte_error_message(error_message=error_message,
                                       failure_type=FailureType.config_error)
            raise StopIteration(error_message)

        except requests.exceptions.HTTPError as error:
            error_message = error.__str__()
            error_code = error.args[0]
            if error_code == 412:
                error_message = ("SQL execution error for check.\n"
                                 "The origin of the error is very likely to be:\n"
                                 "- The configuration provided does not have enough permissions to access the requested database/schema.\n"
                                 "- The configuration is not consistent (example: schema not present is database.")
            emit_airbyte_error_message(error_message=error_message,
                                       failure_type=FailureType.config_error)
            raise requests.exceptions.HTTPError(error_message)

        return True, None

    @classmethod
    def check_host_format(cls, host):
        if not host.endswith(cls.SNOWFLAKE_URL_SUFFIX):
            raise IncorrectHostFormat()

    @classmethod
    def check_update_method(cls, config):
        update_method = config.get('replication_method', {'method': 'standard'}).get('method', 'standard')
        if update_method not in cls.update_methods:
            raise UnknownUpdateMethodError(unknown_update_method=update_method)

    @classmethod
    def check_push_down_filter_name_unicity(cls, config):
        if 'streams' in config and config['streams']:
            push_down_filters_names = [stream['name'] for stream in config['streams']]
            if len(push_down_filters_names) != len(set(push_down_filters_names)):
                raise DuplicatedPushDownFilterStreamNameError()


    @classmethod
    def check_existence_of_at_least_one_stream(cls, url_base, config, authenticator):
        check_connection_stream = CheckConnectionStream(url_base=url_base, config=config, authenticator=authenticator)
        records = check_connection_stream.read_records(sync_mode=SyncMode.full_refresh)
        next(records)

    @classmethod
    def check_push_down_filters_parent_stream_consistency(cls, url_base, config, authenticator):
        table_catalog_stream = TableCatalogStream(url_base=url_base,
                                                  config=config,
                                                  authenticator=authenticator)
        standard_stream_names = []
        for table_object in table_catalog_stream.read_records(sync_mode=SyncMode.full_refresh):
            standard_stream = TableStream(url_base=url_base,
                                          config=config,
                                          authenticator=authenticator,
                                          table_object=table_object)
            standard_stream_names.append(standard_stream.name)

        push_down_filters_streams_without_consistent_parent = []
        if 'streams' in config and config['streams']:
            for push_down_filter_stream_config in config['streams']:
                parent_stream_name = push_down_filter_stream_config['parent_stream']
                push_down_filter_stream_name = push_down_filter_stream_config['name']
                if parent_stream_name not in standard_stream_names:
                    push_down_filters_streams_without_consistent_parent.append(push_down_filter_stream_name)

        if push_down_filters_streams_without_consistent_parent:
            raise InconsistentPushDownFilterParentStreamNameError(push_down_filters_streams_without_consistent_parent)
    @classmethod
    def format_url_base(cls, host: str) -> str:
        url_base = host
        if not host.startswith(cls.HTTP_PREFIX):
            url_base = f"{cls.HTTP_PREFIX}{host}"
        return url_base

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        host = config['host']
        url_base = self.format_url_base(host)
        authenticator = SnowflakeJwtAuthenticator.from_config(config)
        self._auth = authenticator
        table_catalog_stream = TableCatalogStream(url_base=url_base,
                                                  config=config,
                                                  authenticator=authenticator)
        update_method = config.get('replication_method', {'method': 'standard'}).get('method', 'standard')
        stream_class = None
        push_down_filters_class = None

        if update_method.lower() == 'standard':
            stream_class = TableStream
            push_down_filters_class = PushDownFilterStream

        elif update_method.lower() == 'history':
            stream_class = TableChangeDataCaptureStream
            push_down_filters_class = PushDownFilterChangeDataCaptureStream

        standard_streams = self._get_standard_streams(stream_class, config, url_base, table_catalog_stream, authenticator)
        push_down_filters_streams = self._get_push_down_filters_streams(push_down_filters_class,
                                                                        config, url_base, authenticator, standard_streams)
        streams = list(standard_streams.values()) + push_down_filters_streams

        state_manager = ConnectorStateManager(stream_instance_map={stream.name: stream for stream in streams}, state=self.state)

        fallback_start = datetime.now(tz=pytz.timezone("UTC")) - timedelta(days=7)
        partitioner = config.get("partitioner", None)

        return [
            self._to_concurrent(
                stream,
                fallback_start,
                timedelta(minutes=1),
                state_manager,
                partitioner
            )
            for stream in streams
        ]

    def _to_concurrent(
        self, stream: Stream, fallback_start: datetime, slice_range: timedelta, state_manager: ConnectorStateManager, partitioner: str
    ) -> Stream:
        if self._stream_state_is_full_refresh(stream.state):
            return StreamFacade.create_from_stream(
                stream,
                self,
                self.logger,
                self._create_empty_state(),
                FinalStateCursor(stream_name=stream.name, stream_namespace=stream.namespace, message_repository=self.message_repository),
            )
        state = state_manager.get_stream_state(stream.name, stream.namespace)

        if stream.cursor_field:
            cursor_field = CursorField(stream.cursor_field) if isinstance(stream.cursor_field, str) else CursorField(stream.cursor_field[0])
        else:
            #TODO: check partitioner exists in schema
            cursor_field = CursorField(partitioner)
        converter = IsoMillisConcurrentStreamStateConverter()
        cursor = ConcurrentCursor(
            stream.name,
            stream.namespace,
            state,
            self.message_repository,
            state_manager,
            converter,
            cursor_field,
            self._SLICE_BOUNDARY_FIELDS,
            fallback_start,
            converter.get_end_provider(),
            timedelta(minutes=1),
            slice_range,
        )
        return StreamFacade.create_from_stream(stream, self, self.logger, state, cursor)

    def _create_empty_state(self) -> MutableMapping[str, Any]:
        return {}

    @classmethod
    def _get_standard_streams(cls, stream_class, config, url_base, table_catalog_stream, authenticator):
        standard_streams = {}
        for table_object in table_catalog_stream.read_records(sync_mode=SyncMode.full_refresh):
            standard_stream = stream_class(url_base=url_base,
                                           config=config,
                                           authenticator=authenticator,
                                           table_object=table_object)
            standard_streams[standard_stream.name] = standard_stream

        return standard_streams

    @classmethod
    def _get_push_down_filters_streams(cls, push_down_filter_stream_class, config, url_base, authenticator, standard_streams):
        push_down_filters_streams = []
        if 'streams' in config and config['streams']:
            for push_down_filter_stream_config in config['streams']:
                push_down_filter_namespace = push_down_filter_stream_config.get('namespace', None)

                parent_stream_name = push_down_filter_stream_config['parent_stream']
                if parent_stream_name not in standard_streams:
                    continue

                parent_stream = standard_streams[parent_stream_name]
                parent_namespace = push_down_filter_stream_config.get('parent_namespace', False)
                if parent_stream and parent_namespace:
                    parent_stream.namespace = parent_namespace
                push_down_filter_stream = push_down_filter_stream_class(name=push_down_filter_stream_config['name'],
                                                                        url_base=url_base,
                                                                        config=config,
                                                                        where_clause=push_down_filter_stream_config['where_clause'],
                                                                        parent_stream=parent_stream,
                                                                        namespace=push_down_filter_namespace,
                                                                        authenticator=authenticator, )
                push_down_filters_streams.append(push_down_filter_stream)

        return push_down_filters_streams
