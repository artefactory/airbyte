#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
from typing import Any, List, Mapping, Tuple

from source_snowflake.streams.util_streams import TableCatalogStream
from source_snowflake.streams.table_stream import TableStream
from source_snowflake.streams.check_connection import CheckConnectionStream
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_protocol.models import SyncMode

from .authenticator import SnowflakeJwtAuthenticator
from .snowflake_exceptions import InconsistentPushDownFilterParentStreamName
from .streams.push_down_filter_stream import PushDownFilterStream


# Source
class SourceSnowflake(AbstractSource):
    SNOWFLAKE_URL_SUFFIX = ".snowflakecomputing.com"
    HTTP_PREFIX = "https://"

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement connection using oAyth2.0
        Done: Connection use JWT token

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """

        host = config['host']

        if not host.endswith(self.SNOWFLAKE_URL_SUFFIX):
            error_message = f"your host is not ending with {self.SNOWFLAKE_URL_SUFFIX}"
            return False, error_message

        # TODO: ask of unicity of name in pushdown filters otherwise change the way we set the name
        if 'streams' in config and config['streams']:
            push_down_filters_names = [stream['name'] for stream in config['streams']]
            if len(push_down_filters_names) != len(set(push_down_filters_names)):
                raise ValueError('There are pushdown filters with same name which is not possible')

        url_base = self.format_url_base(host)

        authenticator = SnowflakeJwtAuthenticator.from_config(config)

        try:
            self.check_existence_of_at_least_one_stream(url_base=url_base, config=config, authenticator=authenticator)
            self.check_push_down_filters_parent_stream_consistency(url_base=url_base, config=config, authenticator=authenticator)
        except InconsistentPushDownFilterParentStreamName as e:
            error_message = (f'You have provided inconsistent pushdown filters configuration. Parent stream not found or mistake in  '
                             f'spelling parent stream name. Correct spelling must be your_schema_name.your_table_name.\n. Here is the '
                             f'list of the streams affected by this error: {e.push_down_filters_without_consistent_parent}')
            raise ValueError(error_message)
        except StopIteration:
            error_message = "There is no stream available for the connection specification provided"
            raise StopIteration(error_message)
        except requests.exceptions.HTTPError as error:
            error_message = error.__str__()
            error_code = error.args[0]
            if error_code == 412:
                error_message = ("SQL execution error for check.\n"
                                 "The origin of the error is very likely to be:\n"
                                 "- The configuration provided does not have enough permissions to access the requested database/schema.\n"
                                 "- The configuration is not consistent (example: schema not present is database.")
            raise requests.exceptions.HTTPError(error_message)

        return True, None

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

        push_down_filters_without_consistent_parent = []
        if 'streams' in config and config['streams']:
            for push_down_filter_stream_config in config['streams']:
                parent_stream_name = push_down_filter_stream_config['parent_stream']
                push_down_filter_stream_name = push_down_filter_stream_config['name']
                if parent_stream_name not in standard_stream_names:
                    push_down_filters_without_consistent_parent.append(push_down_filter_stream_name)

        if push_down_filters_without_consistent_parent:
            raise InconsistentPushDownFilterParentStreamName(push_down_filters_without_consistent_parent)
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

        table_catalog_stream = TableCatalogStream(url_base=url_base,
                                                  config=config,
                                                  authenticator=authenticator)
        standard_streams = {}
        for table_object in table_catalog_stream.read_records(sync_mode=SyncMode.full_refresh):
            standard_stream = TableStream(url_base=url_base,
                                          config=config,
                                          authenticator=authenticator,
                                          table_object=table_object)
            standard_streams[standard_stream.name] = standard_stream

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
                    # should we set parent namespace ?
                    parent_stream.namespace = parent_namespace
                push_down_filter_stream = PushDownFilterStream(name=push_down_filter_stream_config['name'],
                                                               url_base=url_base,
                                                               config=config,
                                                               where_clause=push_down_filter_stream_config['where_clause'],
                                                               parent_stream=parent_stream,
                                                               namespace=push_down_filter_namespace,
                                                               authenticator=authenticator,)
                push_down_filters_streams.append(push_down_filter_stream)

        streams = [stream for stream in standard_streams.values()] + push_down_filters_streams

        return streams
