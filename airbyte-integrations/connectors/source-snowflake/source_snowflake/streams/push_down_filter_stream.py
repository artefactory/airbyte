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
from .table_stream import TableStream


class PushDownFilterStream(TableStream):

    def __init__(self, name, url_base, config, where_clause, parent_stream, namespace=None, **kwargs):
        kwargs['url_base'] = url_base
        kwargs['config'] = config
        kwargs['table_object'] = parent_stream.table_object
        kwargs['table_schema_stream'] = parent_stream.table_schema_stream
        TableStream.__init__(self, **kwargs)
        self._name = name
        self._namespace = namespace
        self._url_base = url_base
        self.config = config
        self._table_object = parent_stream.table_object
        self.where_clause = where_clause
        self.table_schema_stream = parent_stream.table_schema_stream

    @property
    def name(self):
        return f"{self._name}"

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
        return f'SELECT * FROM "{database}"."{schema}"."{table}" WHERE {self.where_clause}'

    def __str__(self):
        return f"Current stream has this table object as constructor: {self.table_object} and as where clause: {self.where_clause}"
