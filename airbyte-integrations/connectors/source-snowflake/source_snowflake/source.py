#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
import urllib
import uuid
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from .streams import SnowflakeStream, CheckConnectionStream, TableCatalogStream, TableStream
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

        url_base = self.format_url_base(host)

        authenticator = SnowflakeJwtAuthenticator.from_config(config)

        check_connection_stream = CheckConnectionStream(url_base=url_base, config=config, authenticator=authenticator)
        try:
            check_connection_stream.read_records(sync_mode=SyncMode.full_refresh)
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 400:
                error_response = error.response.json()
                error_message = error_response.get('message', '').lower()
                if error_message.startswith("statement") and error_message.endswith("not found"):
                    return True, None

            if error.response.status_code == 401:
                return False, "You have an issue in your configuration or you do not have access to the requested resource"
            raise error
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

        yield from [TableStream(url_base=url_base, config=config, authenticator=authenticator, table_object=table_object)
                for table_object in table_catalog_stream.read_records(sync_mode=SyncMode.full_refresh)]
