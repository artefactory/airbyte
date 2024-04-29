#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
import uuid
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from .streams import SnowflakeStream, CheckConnectionStream
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

        url_base = host

        if not url_base.startswith(self.HTTP_PREFIX):
            url_base = f"{self.HTTP_PREFIX}{url_base}"

        authenticator = SnowflakeJwtAuthenticator.from_config(config)

        stream = CheckConnectionStream(url_base=url_base, config=config, authenticator=authenticator)
        try:
            records = stream.read_records(sync_mode=SyncMode.full_refresh)
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

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        return []



