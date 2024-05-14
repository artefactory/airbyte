#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
import uuid
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from .streams import SnowflakeStream, CheckConnectionStream, TableCatalogStream, TableStream, PushDownFilterStream
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_protocol.models import SyncMode

from .authenticator import SnowflakeJwtAuthenticator


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

        check_connection_stream = CheckConnectionStream(url_base=url_base, config=config, authenticator=authenticator)
        try:
            records = check_connection_stream.read_records(sync_mode=SyncMode.full_refresh)
            next(records)
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
            for stream in config['streams']:
                parent_stream_name = stream['parent_stream']
                if parent_stream_name not in standard_streams:
                    continue
                parent_stream = standard_streams[parent_stream_name]
                parent_namespace = stream.get('parent_namespace', False)
                if parent_stream and parent_namespace:
                    # should we set prent namespace ?
                    parent_stream.namespace = parent_namespace
                stream_name_space = stream.get('namespace', None)
                push_down_filter_stream = PushDownFilterStream(name=stream['name'],
                                                               url_base=url_base,
                                                               config=config,
                                                               where_clause=stream['where_clause'],
                                                               parent_stream=parent_stream,
                                                               namespace=stream_name_space,
                                                               authenticator=authenticator,)
                push_down_filters_streams.append(push_down_filter_stream)

        streams = [stream for stream in standard_streams.values()] + push_down_filters_streams

        return streams
